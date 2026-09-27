# NYC TLC Operational Analytics Pipeline — FDE Assignment 2 (Track B)

## 1. Operational Problem & Business Context

The NYC Taxi & Limousine Commission (TLC) processes millions of yellow taxi trip records monthly across fragmented data feeds. Raw operational logs contain significant data noise, including meter errors (zero-distance trips with high fares), corrupted duration records, and missing location metadata. 

Without a clean, workflow-oriented data pipeline, fleet operations and urban transit planners cannot reliably evaluate route performance, calculate operational yields, or isolate severe traffic congestion bottlenecks.

This project delivers a **dependable, automated, and repeatable operational data pipeline** that ingests raw TLC trip data and lookup tables, applies business validation, quarantines dirty records for operational auditing, and produces monthly operational metrics. The executable implementation is [run_pipeline.py](run_pipeline.py); the notebook is the narrated walkthrough.

---

## 2. Stakeholders & Decisions Supported

| Target Stakeholder | Primary Operational Need | Business Decision Supported |
| :--- | :--- | :--- |
| **TLC Fleet Operations** | Distinguish operational delay from driver inefficient routing. | Dynamic driver incentive allocation and route dispatch adjustments. |
| **City Transit Planners** | Monitor cross-borough corridor congestion patterns over time. | Infrastructure planning and congestion pricing impact evaluation. |
| **FDE / Data Operations Team** | Ensure pipeline health, auditability, and data trust. | Quarantine monitoring for meter hardware failure or software telemetry glitches. |

---

## 3. Project KPI & Operational Metrics

* **Primary Project KPI:** Operational Trip Efficiency & Duration Reliability across NYC Borough Corridors.

To evaluate this KPI, the pipeline automatically generates **4 key metrics** per monthly processing window:

1. **Data Validity Rate (%)**
   * **Formula:** $\frac{\text{Valid Trips}}{\text{Total Ingested Raw Trips}} \times 100$
   * **Purpose:** Measures incoming data quality hygiene and flags system-wide logging errors.
2. **P95 Airport Corridor Duration ($P_{95}$ Mins)**
   * **Formula:** $95\text{th percentile of } \text{trip\_duration\_minutes}$ for Manhattan $\rightarrow$ Queens routes.
   * **Purpose:** Quantifies extreme travel time variance along high-priority airport corridors (JFK/LGA).
3. **Revenue Yield per Active Minute ($\$/\text{Min}$)**
   * **Formula:** $\frac{\sum \text{fare\_amount}}{\sum \text{trip\_duration\_minutes}}$
   * **Purpose:** Evaluates operational yield and fare productivity across active trip time.
4. **Peak vs. Off-Peak Congestion Penalty Ratio**
   * **Formula:** $\frac{\text{Avg Duration (08:00–10:00)}}{\text{Avg Duration (01:00–03:00)}}$
   * **Purpose:** Isolates the exact travel time multiplier caused by peak urban traffic congestion.

---

## 4. Data Sources & Source Mapping

| System / Asset | Ingestion Mode | Format & Grain | Primary Keys | Key Gaps & Limitations |
| :--- | :--- | :--- | :--- | :--- |
| **NYC TLC Trip Records** | Remote HTTP Download (AWS S3 CloudFront) | Parquet File<br>*(1 row per completed trip)* | `VendorID`, `tpep_pickup_datetime`, `PULocationID` | No passenger wait/search time recorded; cancelled trips are absent. |
| **Taxi Zone Lookup** | Static HTTP CSV Ingestion | CSV File<br>*(1 row per TLC location zone)* | `LocationID` | Zone IDs aggregate entire neighborhoods; no exact GPS lat/long points. |

---

## 5. Pipeline Architecture & Dependability Features

The pipeline enforces a strict modular flow: **Ingest $\rightarrow$ Validate & Quarantine $\rightarrow$ Transform & Model $\rightarrow$ Idempotent Storage**.

```
[ Remote AWS S3 / HTTP CSV ]
             │
             ▼
[ Ingestion & Schema Profiling ]
             │
             ▼
[ Validation Suite ] ─────────────► [ Quarantine Storage ]
             │                       (data/quarantine/*.parquet)
             ▼
[ Relational Workflow Model ]
             │
             ▼
[ Monthly Operational Metrics ]
             │
             ▼
[ Atomic Database Persistence ]
  (data/processed/tlc_analytics.duckdb)
```

### Key Dependability Guarantee Mechanisms
* **Rerun Idempotency:** Executing `python run_pipeline.py --year 2024 --month 1` repeatedly safely replaces the existing `2024-01` row without duplicating it.
* **Audit-Safe Quarantining:** Invalid trips (e.g., negative duration, speed $> 80\text{ mph}$, location IDs $264/265$) are moved to `data/quarantine/` with explicit failure reason tags rather than being silently dropped or altered.
* **Complete Retrieval:** Downloads stream to a `.part` file, compare received bytes with `Content-Length` when provided, and are renamed only after completion. Existing non-empty raw files are reused.

---

## 6. Setup & Execution Instructions

### Prerequisites
* Python 3.12 recommended (use a stable Python release with prebuilt Windows wheels; Python 3.15 alpha may trigger unsupported dependency source builds).
* Recommended environment setup:

```bash
# Clone the repository
git clone https://github.com/your-username/nyc-tlc-fde-pipeline.git
cd nyc-tlc-fde-pipeline

# Create and activate virtual environment
py -3.12 -m venv .venv312
.venv312\Scripts\Activate.ps1  # Windows PowerShell

# Install dependencies
python -m pip install -r requirements.txt
```

### Running the Pipeline

Execute the pipeline for any target year and month via the CLI entry point:

```bash
# Process January 2024 data
python run_pipeline.py --year 2024 --month 1

# Reprocess files already staged in data/raw/
python run_pipeline.py --year 2024 --month 1 --no-download
```

For a local check after installing the requirements, run `pytest -q`. The fixture test verifies quarantine behavior and idempotent reruns.

### Output Artifacts
* **Persistent Database Store:** `data/processed/tlc_analytics.duckdb`
* **Exported Summary CSV:** `data/processed/metrics_summary_YYYY-MM.csv`
* **Quarantined Records Parquet:** `data/quarantine/quarantine_YYYY-MM.parquet`
* **Execution Log File:** `pipeline.log` (raw count, valid count, quarantine count, and download events)

## 7. Workflow/Data Model Evidence

The source ownership, grain, retrieval modes, lifecycle states, relational model, and pipeline flow are documented in [nyc-tlc-pipeline-source-mapping.md](nyc-tlc-pipeline-source-mapping.md). The workflow is modeled at trip grain: a completed trip is the operational event, zone lookup rows provide spatial dimensions, and quarantined rows remain available for audit.

## 8. Evidence and Demo

- Metric output and Known / Unknown / Assumption / Limitation evidence: [evidence/metrics-evidence.md](evidence/metrics-evidence.md)
- Full 2024 monthly KPI summary (one row per month): [evidence/annual-2024-monthly-summary.csv](evidence/annual-2024-monthly-summary.csv)
- 3-5 minute demo talk track: [docs/demo-script.md](docs/demo-script.md)
- Automated test: [tests/test_pipeline.py](tests/test_pipeline.py)

The metric evidence file intentionally has no fabricated results. Populate it from a real run; the pipeline writes the results to `data/processed/metrics_summary_YYYY-MM.csv`.

## 9. GitHub Submission

Repository URL: **Add the accessible GitHub URL before submission.** This workspace contains the project files but does not publish them to GitHub automatically.

---

## 10. Known / Unknown / Assumption / Limitation (KUAL) Framework

| Dimension | Description | Operational Impact |
| :--- | :--- | :--- |
| **Known** | Raw TLC datasets contain noisy telemetry artifacts including negative fares, zero trip distances, and unassigned location IDs ($264, 265$). | Handled by automated validation engine and directed into an explicit audit quarantine layer. |
| **Unknown** | Real-time traffic events, sudden weather disruptions, or driver route deviations between pickup and dropoff. | Unmeasured external factors may account for severe outliers in airport corridor $P_{95}$ duration. |
| **Assumption** | Trips with computed speed $> 80\text{ mph}$ or duration $< 1\text{ minute}$ represent faulty taximeter logging rather than physical driving trips. | These records are quarantined to prevent distortion of core duration and congestion metrics. |
| **Limitation** | Spatial resolution is restricted to TLC spatial zone polygons rather than block-level GPS street coordinates. | Spatial congestion bottlenecks can only be pinpointed to zone-to-zone corridors rather than specific intersections. |