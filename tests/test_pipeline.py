from pathlib import Path

import duckdb
import pandas as pd

from run_pipeline import calculate_metrics, persist_metrics, validate_and_model


def test_validation_quarantines_bad_rows_and_metrics_are_idempotent(tmp_path: Path) -> None:
    con = duckdb.connect()
    con.register("trip_fixture", pd.DataFrame([
        {"VendorID": 1, "tpep_pickup_datetime": "2024-01-01 08:00:00", "tpep_dropoff_datetime": "2024-01-01 08:20:00", "PULocationID": 1, "DOLocationID": 2, "passenger_count": 1, "trip_distance": 5.0, "fare_amount": 20.0, "total_amount": 24.0},
        {"VendorID": 1, "tpep_pickup_datetime": "2024-01-01 08:00:00", "tpep_dropoff_datetime": "2024-01-01 08:00:30", "PULocationID": 1, "DOLocationID": 2, "passenger_count": 1, "trip_distance": 5.0, "fare_amount": 20.0, "total_amount": 24.0},
    ]))
    con.execute("""
        CREATE OR REPLACE VIEW raw_trips AS
        SELECT VendorID, CAST(tpep_pickup_datetime AS TIMESTAMP) AS tpep_pickup_datetime,
               CAST(tpep_dropoff_datetime AS TIMESTAMP) AS tpep_dropoff_datetime,
               PULocationID, DOLocationID, passenger_count, trip_distance,
               fare_amount, total_amount
        FROM trip_fixture
    """)
    con.execute("CREATE OR REPLACE VIEW taxi_zones AS SELECT * FROM (VALUES (1, 'Manhattan', 'A', 'Urban'), (2, 'Queens', 'B', 'Urban')) AS t(LocationID, Borough, Zone, service_zone)")
    valid, quarantined = validate_and_model(con, "2024-01", tmp_path / "quarantine.parquet")
    assert (valid, quarantined) == (1, 1)
    metrics = calculate_metrics(con, "2024-01", 2, valid)
    persist_metrics(con, metrics, "2024-01", tmp_path)
    persist_metrics(con, metrics, "2024-01", tmp_path)
    assert con.execute("SELECT COUNT(*) FROM monthly_operational_metrics").fetchone()[0] == 1
    con.close()