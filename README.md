# SafeRoute Taiwan — Traffic Accident Risk API

A data monetization system that transforms Taiwan's open traffic accident data into a location-based risk scoring API, targeting insurance companies and fleet management operators.

**Student ID**: b12902023  
**Course**: Big Data Systems, Spring 2026, National Taiwan University  
**Data Source**: [Taiwan MOTC — 114年 Traffic Accident Open Data](https://data.gov.tw/dataset/177136)

---

## System Overview

SafeRoute Taiwan ingests ~910,000 traffic accident records from Taiwan's 2025 open data, processes them through a PySpark ETL pipeline, and serves risk scores via a Flask REST API.

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  data.gov.tw │     │   PySpark    │     │   Parquet/   │     │  Flask REST  │
│   CSV Files  │───▶│     ETL      │───▶│     CSV      │───▶│     API      │
│  (~910K rows)│     │  (batch)     │     │  (storage)   │     │  (delivery)  │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
```

## Quick Start

### Prerequisites

- Python 3.9+
- Java 17 (specifically required to avoid PySpark compatibility issues with Java 24+)

### Installation

```bash
pip install -r requirements.txt
```

### Step 1: Run the ETL Pipeline

```bash
# Process all accident data and generate risk grid
python etl_pipeline.py
# or
spark-submit etl_pipeline.py
```

This reads CSV files from `data/114年傷亡道路交通事故資料/`, cleans and aggregates the data into ~1 km² grid cells, and outputs results to `output/`.

### Step 2: Start the API Server

```bash
python app.py
```

The API will be available at `http://localhost:5000`.

### Step 3: Query the API

```bash
# Get risk score for a location (Taipei Main Station)
curl "http://localhost:5000/api/risk?lat=25.0478&lng=121.5170"

# Get top 10 accident hotspots
curl "http://localhost:5000/api/hotspots?top=10"

# Get overall statistics
curl "http://localhost:5000/api/stats"

# Get hourly distribution
curl "http://localhost:5000/api/stats/hourly"

# Get accident causes
curl "http://localhost:5000/api/stats/causes"

# Get city-level stats
curl "http://localhost:5000/api/stats/cities"
```

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | API documentation |
| `GET /api/risk?lat=...&lng=...&radius=...` | Risk score for a location (radius in km, default 1.0) |
| `GET /api/hotspots?top=N` | Top N accident hotspots |
| `GET /api/stats` | Overall dataset statistics |
| `GET /api/stats/hourly` | Hourly accident distribution |
| `GET /api/stats/causes` | Top accident causes |
| `GET /api/stats/cities` | City-level statistics |

## Data

The data is sourced from the Taiwan Ministry of Transportation and Communications (MOTC) open data portal. It includes:

- **A1 accidents**: Fatal accidents (death within 24 hours) — ~4,137 records
- **A2 accidents**: Injury accidents — ~906,000 records

Each record contains 52 fields including location (lat/lng), time, weather, road type, vehicle type, driver demographics, and cause analysis.

To obtain the data:
1. Visit https://data.gov.tw/dataset/177136
2. Download the 114年 (2025) CSV files
3. Place them in `data/114年傷亡道路交通事故資料/`

## Project Structure

```
├── README.md                 # This file
├── app.py                    # Flask REST API
├── etl_pipeline.py           # PySpark ETL pipeline
├── requirements.txt          # Python dependencies
├── data/                     # Raw data (not in git)
│   └── 114年傷亡道路交通事故資料/
│       ├── 114年度A1交通事故資料.csv
│       └── 114年度A2交通事故資料_*.csv
└── output/                   # ETL output (not in git)
    ├── risk_grid.parquet/
    ├── risk_grid_csv/
    ├── city_stats_csv/
    ├── hourly_stats_csv/
    └── cause_stats_csv/
```
