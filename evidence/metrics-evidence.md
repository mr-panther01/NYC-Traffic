# Metric Evidence and KUAL

Run the pipeline for the reporting month you will demonstrate. The final values are written to `data/processed/metrics_summary_YYYY-MM.csv`; the same row is stored in DuckDB. Do not enter illustrative values: copy the observed values from the actual run into this table or attach the generated CSV to the repository submission.

| Metric | Observed value | Definition / interpretation |
|---|---:|---|
| Data validity rate | 95.52% | Valid trip rows / all ingested raw trip rows, as a percentage. |
| Manhattan-to-Queens P95 duration | 66.65 minutes | 95th percentile of validated trip duration for Manhattan pickup and Queens dropoff. Proxy corridor, not airport-verified. |
| Revenue yield per active minute | $1.215/minute | Sum of fare amount / sum of validated trip duration in minutes. |
| Peak/off-peak duration ratio | 1.23x | Mean duration in pickup hours 08:00-10:59 divided by mean duration in 01:00-03:59. |

Observed run: January 2024; 2,964,624 raw trip rows, 2,831,758 valid rows, and 132,866 quarantined rows. Run command: `python run_pipeline.py --year 2024 --month 1 --no-download` using the staged raw files.

## Known / Unknown / Assumption / Limitation

| Category | Statement |
|---|---|
| Known | TLC trip records represent completed metered trips and zone IDs provide borough-level context. |
| Unknown | Canceled trips, passenger wait/search time, precise routes, traffic incidents, weather, and reasons for anomalous meter values are not represented by this pipeline's inputs. |
| Assumption | Trips shorter than 1 minute or longer than 360 minutes, with distance outside (0, 200) miles, negative fare/total, unknown zones 264/265, or estimated speed above 80 mph are quarantined as unsuitable for these metrics. These thresholds are operational rules, not verified ground truth. |
| Limitation | Zone-level geography cannot identify an airport trip or a street-level bottleneck. The Manhattan-to-Queens metric is only a broad corridor proxy. |

## Run Evidence Checklist

- [x] Record reporting month and command used.
- [x] Preserve `pipeline.log` showing retrieval and raw/valid/quarantined row counts.
- [x] Record the actual generated metric values in the table above.
- [x] Confirm the generated quarantine file and DuckDB output exist.
- [x] Record that the metric run used `--no-download` with previously downloaded raw inputs.