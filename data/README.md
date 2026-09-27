# Data Directory

The pipeline creates or reuses these runtime locations:

- `raw/`: downloaded TLC monthly trip Parquet and zone lookup CSV. Raw inputs are not committed.
- `processed/`: DuckDB analytics store and monthly metrics CSV exports.
- `quarantine/`: monthly Parquet files for trips that fail validation.

The `.gitkeep` files retain the empty directory structure in a repository. Runtime data files remain ignored by Git.