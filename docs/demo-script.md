# 4-Minute Demo Script

Read the quoted text aloud. Bracketed lines are screen actions, not spoken. This script uses the completed January 2024 run and its recorded results.

## 0:00-0:45 | The Problem

[Show `README.md` at the Operational Problem and Stakeholders sections.]

**Say:**

“Hello. This project takes NYC yellow taxi trip data from raw files to a small, auditable set of monthly operational metrics. The business question is: are recorded trips reliable, and how do trip durations vary across time and borough corridors?

The intended users are fleet operations, city transit planners, and the data operations team. The project KPI is operational trip efficiency and duration reliability. The output is meant to help teams identify patterns that deserve investigation. It is not intended to prove that a particular driver caused a delay.”

## 0:45-1:25 | Sources and Workflow

[Open `nyc-tlc-pipeline-source-mapping.md` and point to its source map and workflow diagram.]

**Say:**

“There are two actual inputs. The first is a monthly NYC TLC Parquet file, with one row per completed trip. The second is the TLC taxi-zone CSV, with one row per location. I join pickup and dropoff location IDs to the lookup to get borough names.

The trip is the central workflow event: pickup, travel, dropoff, and recorded fare. These sources do not include canceled trips, passenger search time, or the exact route taken. That limits what the metrics can tell us.”

## 1:25-2:20 | The FDE Judgement Call

[Open `run_pipeline.py` at `validate_and_model`; then show `data/quarantine/quarantine_2024-01.parquet` in the Explorer.]

**Say:**

“My most important judgement call was to quarantine questionable records instead of silently correcting or deleting them. A trip is accepted only if its duration is between one and 360 minutes, its distance is greater than zero and below 200 miles, its fare and total are nonnegative, both zones are known, and its calculated speed is no more than 80 miles per hour.

These thresholds are assumptions for this analysis, not ground truth about a trip. Quarantining preserves the original record and adds a reason so someone can audit it. For January 2024, the pipeline read 2,964,624 rows, accepted 2,831,758, and quarantined 132,866. The validity rate tells us how many rows pass these rules; it does not prove that every real trip was captured.”

## 2:20-3:20 | Run and Results

[Show the terminal command and output, or open `data/processed/metrics_summary_2024-01.csv`.]

**Say:**

“The pipeline runs with `python run_pipeline.py --year 2024 --month 1`. For this demonstration, I reused the already downloaded raw files with `--no-download`.

The January data-validity rate is 95.52 percent. The 95th percentile duration for trips from Manhattan to Queens is 66.65 minutes. Fare yield is 1.215 dollars per active trip minute. The peak-to-off-peak duration ratio is 1.23.

Manhattan-to-Queens is only a broad corridor proxy. The source does not identify which trips actually served an airport. The peak window is pickup hours eight through ten, compared with one through three in the morning.”

## 3:20-4:00 | Dependability and Decision

[In the VS Code PowerShell terminal, run this command. Do not try to open `.duckdb` or `.parquet` files as text; this reads them through DuckDB.]

```powershell
@'
import duckdb

con = duckdb.connect("data/processed/tlc_analytics.duckdb", read_only=True)
print("Monthly KPI rows in DuckDB:")
print(con.execute("SELECT * FROM monthly_operational_metrics ORDER BY reporting_period").df().to_string(index=False))
print("\nJanuary raw trip rows:")
print(con.execute("SELECT COUNT(*) FROM read_parquet('data/raw/yellow_tripdata_2024-01.parquet')").fetchone()[0])
print("\nJanuary quarantine reasons:")
print(con.execute("SELECT quarantine_reason, COUNT(*) AS records FROM read_parquet('data/quarantine/quarantine_2024-01.parquet') GROUP BY quarantine_reason ORDER BY records DESC").df().to_string(index=False))
con.close()
'@ | .\.venv312\Scripts\python.exe
```

[Then show `tests/test_pipeline.py` in the editor.]

**Say:**

“Raw downloads are first written to temporary files and then kept under `data/raw`. The pipeline writes monthly metrics to DuckDB and CSV, exports quarantined rows separately, and logs the row counts. Re-running a month replaces that month’s metric row instead of adding a duplicate. The test verifies that a bad fixture row is quarantined and that repeating persistence leaves one metric row.

The decision supported by this output is where to investigate monthly reliability and duration patterns next. Before acting on a corridor result, an operations team should compare it with traffic, weather, route, and airport-specific data. Thank you.”

## Before Recording

- Keep the README, source map, pipeline, terminal, and metrics CSV ready. Use the DuckDB command above to inspect database and Parquet files.
- Use the January 2024 outputs referenced above; do not claim these values apply to other months.
- If you rerun with a different month, update the spoken counts and metric values from that run before recording.