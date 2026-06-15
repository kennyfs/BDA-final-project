"""
SafeRoute Taiwan — PySpark ETL Pipeline
========================================
Reads Taiwan 2025 (114年) traffic accident data from CSV files,
cleans and transforms the data, aggregates into geographic grid cells,
and outputs risk scores as Parquet and CSV.

Usage:
    spark-submit etl_pipeline.py
    # or
    python etl_pipeline.py
"""

import os
import sys

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "114年傷亡道路交通事故資料"
)
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")

# Grid resolution: 0.01 degree ≈ 1.1 km at Taiwan's latitude
GRID_RESOLUTION = 0.01

# Taiwan bounding box for filtering invalid coordinates
TW_LAT_MIN, TW_LAT_MAX = 21.5, 26.5
TW_LNG_MIN, TW_LNG_MAX = 119.0, 122.5

# ===========================================================================
# PySpark ETL Implementation
# ===========================================================================


def run_pyspark_etl():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, IntegerType

    print("\nInitializing PySpark...")
    # Force use of Java 17 to avoid Java 25 incompatibility (Subject.getSubject removed)
    java17_path = "/usr/lib/jvm/java-17-openjdk-amd64"
    if os.path.exists(java17_path):
        os.environ["JAVA_HOME"] = java17_path
    java_opens = (
        "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.ssl=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
    )
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f'--driver-java-options="{java_opens}" pyspark-shell'
    )

    spark = (
        SparkSession.builder.appName("SafeRoute-Taiwan-ETL")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.extraJavaOptions", java_opens)
        .config("spark.executor.extraJavaOptions", java_opens)
        .getOrCreate()
    )

    print("\n[1/4] Loading raw data...")
    csv_files = sorted(
        [
            os.path.join(DATA_DIR, f)
            for f in os.listdir(DATA_DIR)
            if f.endswith(".csv") and f.startswith("114年度")
        ]
    )

    if not csv_files:
        print(f"ERROR: No CSV files found in {DATA_DIR}")
        sys.exit(1)

    print(f"Found {len(csv_files)} CSV files to process")
    for f in csv_files:
        print(f"  - {os.path.basename(f)}")

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .option("multiLine", "false")
        .csv(csv_files)
    )
    print(f"Total raw records: {df.count():,}")

    print("\n[2/4] Cleaning data...")
    df = (
        df.withColumn("經度", F.col("經度").cast(DoubleType()))
        .withColumn("緯度", F.col("緯度").cast(DoubleType()))
        .withColumn("速限_int", F.col("速限-第1當事者").cast(IntegerType()))
        .withColumn("年齡_int", F.col("當事者事故發生時年齡").cast(IntegerType()))
        .withColumn("發生月份_int", F.col("發生月份").cast(IntegerType()))
    )

    # Filter to valid Taiwan coordinates
    df = df.filter(
        (F.col("經度") >= TW_LNG_MIN)
        & (F.col("經度") <= TW_LNG_MAX)
        & (F.col("緯度") >= TW_LAT_MIN)
        & (F.col("緯度") <= TW_LAT_MAX)
    )

    # Extract hour from time string (HHMMSS)
    df = df.withColumn("hour", F.substring(F.col("發生時間"), 1, 2).cast(IntegerType()))

    # Parse casualty counts from "死亡X;受傷Y"
    df = df.withColumn(
        "deaths",
        F.regexp_extract(F.col("死亡受傷人數"), r"死亡(\d+)", 1).cast(IntegerType()),
    ).withColumn(
        "injuries",
        F.regexp_extract(F.col("死亡受傷人數"), r"受傷(\d+)", 1).cast(IntegerType()),
    )

    df = df.fillna({"deaths": 0, "injuries": 0})

    # Assign grid cell
    df = df.withColumn(
        "grid_lat", F.round(F.col("緯度") / GRID_RESOLUTION) * GRID_RESOLUTION
    ).withColumn("grid_lng", F.round(F.col("經度") / GRID_RESOLUTION) * GRID_RESOLUTION)

    # Deduplicate: keep one row per party per accident
    df_accidents = df.filter(F.col("當事者順位") == "1")

    # Cache df_accidents for performance
    df_accidents.cache()
    print(f"Cleaned accident-level records: {df_accidents.count():,}")

    print("\n[3/4] Aggregating risk grid...")
    grid = df_accidents.groupBy("grid_lat", "grid_lng").agg(
        F.count("*").alias("total_accidents"),
        F.sum("deaths").alias("total_deaths"),
        F.sum("injuries").alias("total_injuries"),
        F.sum(F.when(F.col("事故類別名稱") == "A1", 1).otherwise(0)).alias(
            "a1_fatal_count"
        ),
        F.sum(F.when(F.col("事故類別名稱") == "A2", 1).otherwise(0)).alias(
            "a2_injury_count"
        ),
        F.sum(
            F.when((F.col("hour") >= 6) & (F.col("hour") < 12), 1).otherwise(0)
        ).alias("morning_count"),
        F.sum(
            F.when((F.col("hour") >= 12) & (F.col("hour") < 18), 1).otherwise(0)
        ).alias("afternoon_count"),
        F.sum(
            F.when((F.col("hour") >= 18) & (F.col("hour") < 24), 1).otherwise(0)
        ).alias("evening_count"),
        F.sum(F.when((F.col("hour") >= 0) & (F.col("hour") < 6), 1).otherwise(0)).alias(
            "night_count"
        ),
        F.sum(F.when(F.col("天候名稱") == "雨", 1).otherwise(0)).alias("rainy_count"),
        F.sum(F.when(F.col("道路型態大類別名稱") == "交岔路", 1).otherwise(0)).alias(
            "intersection_count"
        ),
        F.first("肇因研判子類別名稱-主要").alias("sample_cause"),
        F.avg("速限_int").alias("avg_speed_limit"),
        F.collect_set("發生月份_int").alias("active_months"),
    )

    grid = grid.withColumn(
        "risk_score_raw",
        F.col("total_deaths") * 10
        + F.col("total_injuries") * 3
        + F.col("total_accidents"),
    )

    max_raw_row = grid.agg(F.max("risk_score_raw")).collect()
    max_raw = max_raw_row[0][0] if max_raw_row else 0

    if max_raw and max_raw > 0:
        grid = grid.withColumn(
            "risk_score",
            F.least(F.lit(100.0), F.round(F.col("risk_score_raw") / max_raw * 100, 1)),
        )
    else:
        grid = grid.withColumn("risk_score", F.lit(0.0))

    grid = grid.withColumn(
        "risk_level",
        F.when(F.col("risk_score") >= 70, "HIGH")
        .when(F.col("risk_score") >= 40, "MEDIUM")
        .otherwise("LOW"),
    )

    grid = grid.withColumn(
        "active_months_str", F.array_join(F.sort_array("active_months"), ",")
    ).drop("active_months")

    grid.cache()
    print(f"Total grid cells: {grid.count():,}")

    # City stats
    df_city = df_accidents.withColumn(
        "city", F.regexp_extract(F.col("處理單位名稱警局層"), r"^(.{2,3}(?:縣|市))", 1)
    ).filter(F.col("city") != "")

    city_stats = (
        df_city.groupBy("city")
        .agg(
            F.count("*").alias("total_accidents"),
            F.sum("deaths").alias("total_deaths"),
            F.sum("injuries").alias("total_injuries"),
            F.avg("速限_int").alias("avg_speed_limit"),
        )
        .orderBy(F.col("total_accidents").desc())
    )

    # Hourly stats
    hourly_stats = (
        df_accidents.groupBy("hour")
        .agg(
            F.count("*").alias("accident_count"),
            F.sum("deaths").alias("deaths"),
            F.sum("injuries").alias("injuries"),
        )
        .orderBy("hour")
    )

    # Cause stats
    cause_stats = (
        df_accidents.groupBy("肇因研判子類別名稱-主要")
        .agg(
            F.count("*").alias("count"),
            F.sum("deaths").alias("deaths"),
        )
        .orderBy(F.col("count").desc())
        .limit(30)
    )

    print("\n[4/4] Writing output...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    grid.coalesce(1).write.mode("overwrite").parquet(
        os.path.join(OUTPUT_DIR, "risk_grid.parquet")
    )
    grid.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        os.path.join(OUTPUT_DIR, "risk_grid_csv")
    )
    city_stats.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        os.path.join(OUTPUT_DIR, "city_stats_csv")
    )
    hourly_stats.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        os.path.join(OUTPUT_DIR, "hourly_stats_csv")
    )
    cause_stats.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        os.path.join(OUTPUT_DIR, "cause_stats_csv")
    )

    # Summary
    print("\n" + "=" * 60)
    print("ETL Pipeline Complete!")
    print("=" * 60)
    total_accidents = df_accidents.count()
    total_deaths = df_accidents.agg(F.sum("deaths")).collect()[0][0]
    total_injuries = df_accidents.agg(F.sum("injuries")).collect()[0][0]

    print(f"  Accidents processed: {total_accidents:,}")
    print(f"  Total deaths: {total_deaths:,}")
    print(f"  Total injuries: {total_injuries:,}")
    print(f"  Grid cells generated: {grid.count():,}")
    print(f"  Output directory: {OUTPUT_DIR}")

    print("\nTop 10 Riskiest Grid Cells:")
    grid.orderBy(F.col("risk_score").desc()).select(
        "grid_lat",
        "grid_lng",
        "total_accidents",
        "total_deaths",
        "total_injuries",
        "risk_score",
        "risk_level",
    ).show(10, truncate=False)

    print("\nAccidents by City:")
    city_stats.show(25, truncate=False)

    spark.stop()


# ===========================================================================
# Main
# ===========================================================================


def main():
    print("=" * 60)
    print("SafeRoute Taiwan — ETL Pipeline")
    print("=" * 60)

    print("Running PySpark ETL pipeline...")
    try:
        run_pyspark_etl()
    except Exception as e:
        print(f"\n[ERROR] PySpark pipeline failed:")
        print(e)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
