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
uv run python steps/03_base.py      # Feature engineering + Elo ratings
uv run python steps/04_analyses.py  # Advanced analytics (8 perspectives)
uv run python steps/05_train.py     # XGBoost v4 training
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
4. **Base** → feature engineering (temporal features, progress metrics, meet classification, Elo ratings)
4b. **Analyses** → 8 analytical perspectives (career trajectory, comp-indexed growth, percentile rank, rolling averages, lift ratios, Wilks trajectory, survival analysis, lifter archetypes)
5. **Train** → XGBoost v4 with Optuna optimization, tracked via MLflow

Data flows: CSV → parquet (local `data/` dir) → S3 (`s3://powerlifting-ml-progress/`)

### Key Files

- `steps/conf.py` — Central configuration: enums (`OutputPathType`, `FileLocation`), constants, column definitions, path builders
- `steps/common_io.py` — S3 upload/download utilities
- `steps/03_raw.py` — Primary key generation logic: `{name}-{sex}-{birth_year}` with age tolerance (±2 years)
- `steps/03_base.py` — Feature engineering: temporal features, lift progress rates, meet classification, Elo rating system, `prev_*` ML-safe lagged features
- `steps/04_analyses.py` — Advanced analytics: 8 analysis perspectives producing parquets for the dashboard
- `steps/05_train.py` — XGBoost v4 model: 28 leak-free input features, Optuna HPO (250 trials), MLflow tracking
- `about.py` — Streamlit homepage; `pages/` contains additional dashboard pages
- `sqls/` — DuckDB schema definitions (landing, raw, base, semantic layers)
- `infra/` — Terraform/OpenTofu for AWS resources (S3 bucket, IAM)

### ML Model (v4)

28 leak-free input features predicting `total_progress` (kg/year). Key design decisions:
- **Data leakage fix**: 10 features that used current-competition values replaced with `prev_*` lagged versions (e.g., `squat_ratio` → `prev_squat_ratio`). Original columns kept for analytics/dashboard.
- **Elo rating system**: Field-based Elo adapted from tennis. Lifters rated against meet segment peers (sex + weight class). Pre-meet `elo_rating` (no leakage), `elo_change` (momentum), `meet_field_elo` (field strength).
- **Elo constants** in `conf.py`: `ELO_INITIAL=1500`, `ELO_BASE_K=32`, meet-type K multipliers (international 1.5x → local 0.6x).

### Streamlit Dashboard

Multi-page app: `about.py` (homepage) + `pages/` directory. Loads data from S3 parquet with `@st.cache_data`.
- `01_machine_learning` — ML methodology documentation
- `02_user_data` — Individual lifter Wilks and SBD progression (Altair charts)
- `03_strength_progress` — Expected progress rates by weight class and experience
- `04_advanced_analytics` — 8 tabs: career trajectory, comp-indexed growth, percentile rank, rolling averages, lift ratios, Wilks trajectory, survival analysis, lifter archetypes

### CI/CD

GitHub Actions (`.github/workflows/extract-transform-load.yml`) runs the full ETL pipeline on Tuesdays and Fridays at 14:15 UTC, auto-committing updated data files.

## Code Style

- **Python**: Ruff with 200-char line length. Rules: E, F, G, I, PT, PTH, RUF, SIM, T20.
- **SQL**: SQLFluff with DuckDB dialect, lowercase keywords.
- **Data processing**: Polars (not Pandas). DuckDB for SQL-based transformations.
- **Package management**: `uv` (not pip/poetry).
- **Pre-commit hooks** enforce Ruff and SQLFluff on commit.
