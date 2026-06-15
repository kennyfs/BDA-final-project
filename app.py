"""
SafeRoute Taiwan — Flask REST API
==================================
Serves traffic accident risk scores and analytics via a REST API.
Reads pre-computed data from the ETL pipeline output.

Usage:
    python app.py
    # API will be available at http://localhost:5000
"""

import os
import math
import json
from flask import Flask, request, jsonify

import pandas as pd

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
GRID_RESOLUTION = 0.01

# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------


def find_csv_in_dir(dir_path):
    """Find the actual CSV file inside a Spark-output directory."""
    if not os.path.isdir(dir_path):
        return None
    for f in os.listdir(dir_path):
        if f.endswith(".csv") and not f.startswith("_") and not f.startswith("."):
            return os.path.join(dir_path, f)
    return None


def load_data():
    """Load pre-computed data from ETL output."""
    data = {}

    # Risk grid
    csv_path = find_csv_in_dir(os.path.join(OUTPUT_DIR, "risk_grid_csv"))
    if csv_path:
        data["risk_grid"] = pd.read_csv(csv_path)
        print(f"Loaded risk grid: {len(data['risk_grid'])} cells")
    else:
        print("WARNING: risk_grid_csv not found. Run etl_pipeline.py first.")
        data["risk_grid"] = pd.DataFrame()

    # City stats
    csv_path = find_csv_in_dir(os.path.join(OUTPUT_DIR, "city_stats_csv"))
    if csv_path:
        data["city_stats"] = pd.read_csv(csv_path)
        print(f"Loaded city stats: {len(data['city_stats'])} cities")
    else:
        data["city_stats"] = pd.DataFrame()

    # Hourly stats
    csv_path = find_csv_in_dir(os.path.join(OUTPUT_DIR, "hourly_stats_csv"))
    if csv_path:
        data["hourly_stats"] = pd.read_csv(csv_path)
        print(f"Loaded hourly stats: {len(data['hourly_stats'])} hours")
    else:
        data["hourly_stats"] = pd.DataFrame()

    # Cause stats
    csv_path = find_csv_in_dir(os.path.join(OUTPUT_DIR, "cause_stats_csv"))
    if csv_path:
        data["cause_stats"] = pd.read_csv(csv_path)
        print(f"Loaded cause stats: {len(data['cause_stats'])} causes")
    else:
        data["cause_stats"] = pd.DataFrame()

    return data


DATA = {}


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def haversine(lat1, lng1, lat2, lng2):
    """Calculate distance in km between two coordinates."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlng / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def snap_to_grid(val):
    """Snap a coordinate to the nearest grid cell."""
    return round(round(val / GRID_RESOLUTION) * GRID_RESOLUTION, 6)


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------


@app.route("/")
def index():
    """API documentation landing page."""
    return jsonify(
        {
            "service": "SafeRoute Taiwan — Traffic Accident Risk API",
            "version": "1.0.0",
            "endpoints": {
                "GET /api/risk?lat=<lat>&lng=<lng>": "Get risk score for a location",
                "GET /api/risk?lat=<lat>&lng=<lng>&radius=<km>": "Get risk in radius",
                "GET /api/hotspots?top=<n>": "Get top accident hotspots",
                "GET /api/stats": "Get overall dataset statistics",
                "GET /api/stats/hourly": "Get hourly accident distribution",
                "GET /api/stats/causes": "Get accident cause breakdown",
                "GET /api/stats/cities": "Get city-level statistics",
            },
            "data_source": "Taiwan Ministry of Transportation — 114年 (2025) Traffic Accident Open Data",
            "data_url": "https://data.gov.tw/dataset/177136",
        }
    )


@app.route("/api/risk")
def get_risk():
    """Get risk score for a specific location."""
    lat = request.args.get("lat", type=float)
    lng = request.args.get("lng", type=float)
    radius = request.args.get("radius", default=1.0, type=float)

    if lat is None or lng is None:
        return jsonify({"error": "lat and lng parameters are required"}), 400

    grid = DATA.get("risk_grid")
    if grid is None or grid.empty:
        return jsonify({"error": "Data not loaded. Run ETL pipeline first."}), 503

    # Find nearest grid cell
    grid_lat = snap_to_grid(lat)
    grid_lng = snap_to_grid(lng)

    # Search in radius
    nearby = grid.copy()
    nearby["distance_km"] = nearby.apply(
        lambda r: haversine(lat, lng, r["grid_lat"], r["grid_lng"]), axis=1
    )
    nearby = nearby[nearby["distance_km"] <= radius].sort_values("distance_km")

    if nearby.empty:
        return jsonify(
            {
                "query": {"lat": lat, "lng": lng, "radius_km": radius},
                "result": {
                    "risk_score": 0,
                    "risk_level": "NO_DATA",
                    "message": "No accident data found within the specified radius",
                    "nearby_cells": 0,
                },
            }
        )

    # Aggregate nearby cells
    total_accidents = int(nearby["total_accidents"].sum())
    total_deaths = int(nearby["total_deaths"].sum())
    total_injuries = int(nearby["total_injuries"].sum())
    avg_risk = round(float(nearby["risk_score"].mean()), 1)
    max_risk = round(float(nearby["risk_score"].max()), 1)

    # Nearest cell details
    nearest = nearby.iloc[0]

    return jsonify(
        {
            "query": {"lat": lat, "lng": lng, "radius_km": radius},
            "result": {
                "risk_score": avg_risk,
                "max_risk_score": max_risk,
                "risk_level": (
                    "HIGH" if avg_risk >= 70 else "MEDIUM" if avg_risk >= 40 else "LOW"
                ),
                "total_accidents": total_accidents,
                "total_deaths": total_deaths,
                "total_injuries": total_injuries,
                "cells_in_radius": len(nearby),
                "nearest_cell": {
                    "grid_lat": round(float(nearest["grid_lat"]), 4),
                    "grid_lng": round(float(nearest["grid_lng"]), 4),
                    "distance_km": round(float(nearest["distance_km"]), 3),
                    "accidents": int(nearest["total_accidents"]),
                    "risk_score": round(float(nearest["risk_score"]), 1),
                },
            },
        }
    )


@app.route("/api/hotspots")
def get_hotspots():
    """Get the top accident hotspots."""
    top = request.args.get("top", default=20, type=int)
    top = min(top, 100)

    grid = DATA.get("risk_grid")
    if grid is None or grid.empty:
        return jsonify({"error": "Data not loaded"}), 503

    hotspots = grid.nlargest(top, "risk_score")

    results = []
    for _, row in hotspots.iterrows():
        results.append(
            {
                "grid_lat": round(float(row["grid_lat"]), 4),
                "grid_lng": round(float(row["grid_lng"]), 4),
                "risk_score": round(float(row["risk_score"]), 1),
                "risk_level": str(row.get("risk_level", "N/A")),
                "total_accidents": int(row["total_accidents"]),
                "total_deaths": int(row["total_deaths"]),
                "total_injuries": int(row["total_injuries"]),
                "a1_fatal_count": int(row.get("a1_fatal_count", 0)),
                "a2_injury_count": int(row.get("a2_injury_count", 0)),
            }
        )

    return jsonify(
        {
            "top": top,
            "hotspots": results,
        }
    )


@app.route("/api/stats")
def get_stats():
    """Get overall dataset statistics."""
    grid = DATA.get("risk_grid")
    city = DATA.get("city_stats")

    if grid is None or grid.empty:
        return jsonify({"error": "Data not loaded"}), 503

    stats = {
        "dataset": {
            "source": "Taiwan MOTC Open Data — 114年 (2025)",
            "url": "https://data.gov.tw/dataset/177136",
        },
        "summary": {
            "total_grid_cells": len(grid),
            "total_accidents": int(grid["total_accidents"].sum()),
            "total_deaths": int(grid["total_deaths"].sum()),
            "total_injuries": int(grid["total_injuries"].sum()),
            "a1_fatal_accidents": int(grid["a1_fatal_count"].sum()),
            "a2_injury_accidents": int(grid["a2_injury_count"].sum()),
        },
        "risk_distribution": {
            "high_risk_cells": int((grid["risk_score"] >= 70).sum()),
            "medium_risk_cells": int(
                ((grid["risk_score"] >= 40) & (grid["risk_score"] < 70)).sum()
            ),
            "low_risk_cells": int((grid["risk_score"] < 40).sum()),
            "avg_risk_score": round(float(grid["risk_score"].mean()), 2),
            "max_risk_score": round(float(grid["risk_score"].max()), 1),
        },
    }

    if city is not None and not city.empty:
        stats["top_cities"] = city.head(10).to_dict(orient="records")

    return jsonify(stats)


@app.route("/api/stats/hourly")
def get_hourly_stats():
    """Get hourly accident distribution."""
    hourly = DATA.get("hourly_stats")
    if hourly is None or hourly.empty:
        return jsonify({"error": "Data not loaded"}), 503

    return jsonify({"hourly_distribution": hourly.to_dict(orient="records")})


@app.route("/api/stats/causes")
def get_cause_stats():
    """Get accident cause breakdown."""
    causes = DATA.get("cause_stats")
    if causes is None or causes.empty:
        return jsonify({"error": "Data not loaded"}), 503

    return jsonify({"top_causes": causes.to_dict(orient="records")})


@app.route("/api/stats/cities")
def get_city_stats():
    """Get city-level statistics."""
    city = DATA.get("city_stats")
    if city is None or city.empty:
        return jsonify({"error": "Data not loaded"}), 503

    return jsonify({"cities": city.to_dict(orient="records")})


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Loading pre-computed data...")
    DATA = load_data()
    print(f"\nStarting SafeRoute Taiwan API on http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)
