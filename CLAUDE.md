# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Powerlifting ML Progress analyzes OpenPowerlifting data to estimate and predict powerlifting progress using XGBoost. It includes an automated ETL pipeline, ML training with Optuna hyperparameter optimization, MLflow experiment tracking, and a Streamlit dashboard deployed at https://powerlifting.streamlit.app.

## Commands

**Setup:**
```bash
make setup          # Create venv, install deps, configure pre-commit
```

**Run ETL steps individually:**
```bash
uv run python steps/01_extract.py   # Download OpenPowerlifting data
uv run python steps/02_load.py      # Upload reference tables to S3
uv run python steps/03_raw.py       # Raw layer transformations
uv run python steps/03_base.py      # Feature engineering
uv run python steps/05_train.py     # XGBoost training
uv run python steps/00_clean.py     # Cleanup local data folders
```

**MLflow/MinIO:**
```bash
make start          # Start MinIO container (artifact storage)
make stop           # Stop MinIO
make restart        # Restart MinIO
```

**Linting:**
```bash
uv run ruff check .          # Python linting
uv run ruff format .         # Python formatting
uv run sqlfluff lint sqls/   # SQL linting (DuckDB dialect)
uv run sqlfluff fix sqls/    # SQL auto-fix
```

**Streamlit app:**
```bash
uv run streamlit run about.py
```

## Architecture

### Data Pipeline (Medallion Pattern)

ETL runs as numbered scripts in `steps/`, executed sequentially:

1. **Extract** → downloads OpenPowerlifting ZIP, converts to parquet
2. **Load** → uploads reference CSVs from `reference-tables/` to S3
3. **Raw** → filters (SBD events, raw equipment, tested only), generates primary keys, type casting
4. **Base** → feature engineering (temporal features, progress metrics, meet classification)
5. **Train** → XGBoost with Optuna optimization, tracked via MLflow

Data flows: CSV → parquet (local `data/` dir) → S3 (`s3://powerlifting-ml-progress/`)

### Key Files

- `steps/conf.py` — Central configuration: enums (`OutputPathType`, `FileLocation`), constants, column definitions, path builders
- `steps/common_io.py` — S3 upload/download utilities
- `steps/03_raw.py` — Primary key generation logic: `{name}-{sex}-{birth_year}` with age tolerance (±2 years)
- `steps/03_base.py` — Feature engineering: temporal features, lift progress rates, meet type classification
- `about.py` — Streamlit homepage; `pages/` contains additional dashboard pages
- `sqls/` — DuckDB schema definitions (landing, raw, base, semantic layers)
- `infra/` — Terraform/OpenTofu for AWS resources (S3 bucket, IAM)

### Streamlit Dashboard

Multi-page app: `about.py` (homepage) + `pages/` directory. Loads data from S3 parquet with `@st.cache_data`. User data page shows Wilks score trends and lift progression via Altair charts.

### CI/CD

GitHub Actions (`.github/workflows/extract-transform-load.yml`) runs the full ETL pipeline on Tuesdays and Fridays at 14:15 UTC, auto-committing updated data files.

## Code Style

- **Python**: Ruff with 200-char line length. Rules: E, F, G, I, PT, PTH, RUF, SIM, T20.
- **SQL**: SQLFluff with DuckDB dialect, lowercase keywords.
- **Data processing**: Polars (not Pandas). DuckDB for SQL-based transformations.
- **Package management**: `uv` (not pip/poetry).
- **Pre-commit hooks** enforce Ruff and SQLFluff on commit.
