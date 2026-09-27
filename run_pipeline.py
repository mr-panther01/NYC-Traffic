"""Run the NYC TLC monthly operational metrics pipeline."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb
import pandas as pd
import requests


TRIP_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{period}.parquet"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def configure_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("nyc_tlc_pipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    for handler in (logging.FileHandler(log_path, encoding="utf-8"), logging.StreamHandler()):
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def download_once(url: str, destination: Path, logger: logging.Logger) -> None:
    """Download to a temporary file, then atomically preserve the raw input."""
    if destination.exists() and destination.stat().st_size > 0:
        logger.info("Using existing raw input: %s", destination)
        return
    temporary = destination.with_suffix(destination.suffix + ".part")
    logger.info("Downloading %s", url)
    with requests.get(url, stream=True, timeout=(30, 300)) as response:
        response.raise_for_status()
        expected_length = response.headers.get("Content-Length")
        bytes_written = 0
        with temporary.open("wb") as output:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    output.write(chunk)
                    bytes_written += len(chunk)
    if bytes_written == 0 or (expected_length and bytes_written != int(expected_length)):
        temporary.unlink(missing_ok=True)
        raise IOError(f"Incomplete download for {url}: received {bytes_written} bytes")
    temporary.replace(destination)
    logger.info("Preserved raw input: %s (%s bytes)", destination, bytes_written)


def retrieve_sources(period: str, raw_dir: Path, logger: logging.Logger) -> tuple[Path, Path]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    trip_path = raw_dir / f"yellow_tripdata_{period}.parquet"
    zone_path = raw_dir / "taxi_zone_lookup.csv"
    download_once(TRIP_URL.format(period=period), trip_path, logger)
    download_once(ZONE_URL, zone_path, logger)
    return trip_path, zone_path


def validate_and_model(con: duckdb.DuckDBPyConnection, period: str, quarantine_path: Path) -> tuple[int, int]:
    duration = "epoch(tpep_dropoff_datetime - tpep_pickup_datetime) / 60.0"
    valid_rule = f"""
        tpep_dropoff_datetime > tpep_pickup_datetime
        AND {duration} BETWEEN 1 AND 360
        AND trip_distance > 0 AND trip_distance < 200
        AND fare_amount >= 0 AND total_amount >= 0
        AND PULocationID NOT IN (264, 265) AND DOLocationID NOT IN (264, 265)
        AND trip_distance / NULLIF({duration} / 60.0, 0) <= 80
    """
    con.execute(f"""
        CREATE OR REPLACE TABLE valid_trips AS
        SELECT VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
               PULocationID, DOLocationID, passenger_count, trip_distance,
               fare_amount, total_amount, {duration} AS trip_duration_minutes,
               trip_distance / NULLIF({duration} / 60.0, 0) AS average_speed_mph,
               '{period}' AS period
        FROM raw_trips WHERE {valid_rule}
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE quarantined_trips AS
        SELECT *, CASE
            WHEN tpep_dropoff_datetime <= tpep_pickup_datetime THEN 'Timestamp mismatch'
            WHEN {duration} NOT BETWEEN 1 AND 360 THEN 'Duration outside 1-360 minutes'
            WHEN trip_distance <= 0 THEN 'Zero/negative distance'
            WHEN trip_distance >= 200 THEN 'Distance outside 0-200 miles'
            WHEN fare_amount < 0 OR total_amount < 0 THEN 'Negative fare/total'
            WHEN PULocationID IN (264, 265) OR DOLocationID IN (264, 265) THEN 'Unknown zone'
            ELSE 'Speed anomaly' END AS quarantine_reason
        FROM raw_trips WHERE NOT ({valid_rule})
    """)
    quarantine_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY quarantined_trips TO '{quarantine_path}' (FORMAT PARQUET)")
    return (
        con.execute("SELECT COUNT(*) FROM valid_trips").fetchone()[0],
        con.execute("SELECT COUNT(*) FROM quarantined_trips").fetchone()[0],
    )


def calculate_metrics(con: duckdb.DuckDBPyConnection, period: str, total_count: int, valid_count: int) -> pd.DataFrame:
    return con.execute(f"""
        WITH zone_trips AS (
            SELECT v.*, pz.Borough AS pickup_borough, dz.Borough AS dropoff_borough
            FROM valid_trips v
            LEFT JOIN taxi_zones pz ON v.PULocationID = pz.LocationID
            LEFT JOIN taxi_zones dz ON v.DOLocationID = dz.LocationID
        ), corridor AS (
            SELECT quantile_cont(trip_duration_minutes, 0.95) AS p95_duration
            FROM zone_trips WHERE pickup_borough = 'Manhattan' AND dropoff_borough = 'Queens'
        ), congestion AS (
            SELECT AVG(CASE WHEN hour(tpep_pickup_datetime) BETWEEN 8 AND 10 THEN trip_duration_minutes END)
                / NULLIF(AVG(CASE WHEN hour(tpep_pickup_datetime) BETWEEN 1 AND 3 THEN trip_duration_minutes END), 0) AS penalty_ratio
            FROM zone_trips
        )
        SELECT '{period}' AS reporting_period,
               ROUND({valid_count} * 100.0 / NULLIF({total_count}, 0), 2) AS data_validity_pct,
               ROUND(c.p95_duration, 2) AS p95_airport_corridor_duration_mins,
               ROUND(SUM(z.fare_amount) / NULLIF(SUM(z.trip_duration_minutes), 0), 3) AS revenue_yield_per_min,
               ROUND(g.penalty_ratio, 2) AS peak_offpeak_duration_ratio
        FROM zone_trips z CROSS JOIN corridor c CROSS JOIN congestion g
        GROUP BY c.p95_duration, g.penalty_ratio
    """).df()


def persist_metrics(con: duckdb.DuckDBPyConnection, metrics: pd.DataFrame, period: str, processed_dir: Path) -> None:
    processed_dir.mkdir(parents=True, exist_ok=True)
    con.execute("CREATE TABLE IF NOT EXISTS monthly_operational_metrics AS SELECT * FROM metrics LIMIT 0")
    con.execute("DELETE FROM monthly_operational_metrics WHERE reporting_period = ?", [period])
    con.register("metrics_df", metrics)
    con.execute("INSERT INTO monthly_operational_metrics SELECT * FROM metrics_df")
    metrics.to_csv(processed_dir / f"metrics_summary_{period}.csv", index=False)


def run(period: str, base_dir: Path = Path("."), retrieve: bool = True) -> pd.DataFrame:
    year, month = period.split("-")
    if len(year) != 4 or not 1 <= int(month) <= 12:
        raise ValueError("period must be YYYY-MM")
    raw_dir = base_dir / "data" / "raw"
    processed_dir = base_dir / "data" / "processed"
    logger = configure_logging(base_dir / "pipeline.log")
    if retrieve:
        trip_path, zone_path = retrieve_sources(period, raw_dir, logger)
    else:
        trip_path = raw_dir / f"yellow_tripdata_{period}.parquet"
        zone_path = raw_dir / "taxi_zone_lookup.csv"
    db_path = processed_dir / "tlc_analytics.duckdb"
    processed_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as con:
        con.read_parquet(str(trip_path)).create_view("raw_trips", replace=True)
        con.read_csv(str(zone_path), auto_detect=True).create_view("taxi_zones", replace=True)
        total_count = con.execute("SELECT COUNT(*) FROM raw_trips").fetchone()[0]
        valid_count, quarantine_count = validate_and_model(con, period, base_dir / "data" / "quarantine" / f"quarantine_{period}.parquet")
        metrics = calculate_metrics(con, period, total_count, valid_count)
        persist_metrics(con, metrics, period, processed_dir)
    logger.info("Completed %s: raw=%s valid=%s quarantined=%s", period, total_count, valid_count, quarantine_count)
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--base-dir", type=Path, default=Path("."))
    parser.add_argument("--no-download", action="store_true", help="Use already staged raw files")
    args = parser.parse_args()
    metrics = run(f"{args.year:04d}-{args.month:02d}", args.base_dir, retrieve=not args.no_download)
    print(metrics.to_string(index=False))


if __name__ == "__main__":
    main()