# Evaluation: dbt+DuckDB vs Polars for this Pipeline

## Context

The pipeline currently uses Polars for all transformations across a medallion architecture (landing → raw → base → describe/analyses → train). DuckDB is installed but only used in exploratory notebooks. SQL schema files exist in `sqls/` but aren't executed in production. The question is whether migrating to dbt+DuckDB would be worth the effort.

## What each pipeline layer looks like in both approaches

| Layer | Current (Polars) | dbt+DuckDB feasibility |
|-------|-----------------|----------------------|
| `01_extract.py` — download ZIP, convert CSV→parquet | Python (requests, polars) | **Must stay Python** — HTTP + file I/O |
| `02_load.py` — upload reference tables to S3 | Python (boto3) | **Must stay Python** — S3 operations |
| `03_raw.py` — filter, type cast, primary key generation | Polars pipes | **Partially SQL** — filters and type casts map to SQL, but primary key generation uses Python dicts with age tolerance (±2yr) logic that's awkward in SQL |
| `03_base.py` — feature engineering | Polars pipes | **Mixed** — window functions (rolling avg, percentile rank, cumulative comps) map well to SQL. But `add_ipf_weight_class` uses Python `bisect` and a loop over all rows — requires a UDF or pre-computed lookup table in SQL |
| `04_describe.py` — group-by aggregations | Polars group_by + quantiles | **Perfect fit for SQL** — pure GROUP BY with PERCENTILE_CONT |
| `04_analyses.py` — 8 analysis queries | Polars group_by + quantiles | **7/8 fit SQL well**, but archetype clustering (KMeans) **cannot be done in SQL** |
| `05_train.py` — XGBoost + Optuna + MLflow | Python (sklearn, xgboost) | **Must stay Python** — ML training |

## dbt+DuckDB: what you'd gain

- **Built-in testing** — `schema.yml` tests for not_null, unique, accepted_values, relationships. Currently no data quality tests exist beyond the manual `data_quality_report()`.
- **Lineage graph** — `dbt docs generate` gives a DAG of model dependencies. Currently the dependency order is implicit (numbered filenames).
- **Declarative SQL** — The describe and analysis layers are essentially `SELECT ... GROUP BY` queries. SQL is arguably more readable for these than Polars `group_by().agg()` chains.
- **Incremental materializations** — Could avoid reprocessing unchanged data. Not critical at current scale (~3M rows) but useful if data grows.
- **Existing SQL groundwork** — `sqls/00_schemas.sql`, `01_landing.sql`, `02_raw.sql` already define the first two layers.

## dbt+DuckDB: what it would cost

- **New tooling** — Add `dbt-core` + `dbt-duckdb` dependencies, `profiles.yml`, `dbt_project.yml`, model directory structure, schema files.
- **Hybrid orchestration complexity** — Steps 1-2 must stay Python. Raw layer's primary key logic needs Python. Clustering needs Python. You'd end up with: `python → dbt run → python → dbt run → python`, which is harder to reason about than a single Python pipeline.
- **S3 integration overhead** — DuckDB reads parquet from S3 via `httpfs` extension. Needs `SET s3_region`, `SET s3_access_key_id`, etc. Different from the current boto3 approach and duplicates credential management.
- **Weight class classification** — The `add_ipf_weight_class` function uses `bisect.bisect_left` on IPF boundaries with special `inf` handling. In DuckDB you'd need a CASE WHEN with 8+ branches per sex, or a reference table join. Doable but less elegant.
- **Loss of pipe composability** — The `.pipe()` chain in `03_base.py` lets you add/remove/reorder transformations trivially. dbt models are more rigid (one SELECT per model, or CTEs).
- **Clustering stays Python anyway** — The archetype analysis (StandardScaler + KMeans) has no SQL equivalent, so you'd have a Python post-processing step regardless.

## Scale context

- OpenPowerlifting dataset: ~3M rows, ~50 columns → fits comfortably in memory
- Both Polars and DuckDB process this in seconds
- No performance pressure pushing toward one approach
- Pipeline runs 2x/week via GitHub Actions — execution time is not a bottleneck

## Recommendation: Stay with Polars

For this specific project, Polars is the better choice. The reasons:

1. **End-to-end Python** — The pipeline starts with HTTP downloads and ends with ML training. Polars keeps everything in one language, one runtime, one dependency chain. dbt would fragment this into Python → SQL → Python → SQL → Python.

2. **The hard parts can't be SQL** — Primary key generation (age tolerance logic), weight class classification (bisect), and clustering (KMeans) are Python-native operations. These represent ~40% of the transformation logic and would need Python UDFs or separate scripts regardless.

3. **Project scale doesn't justify the overhead** — dbt's strengths (testing, docs, lineage, incremental models) have diminishing returns on a solo project with <10 models. The existing numbered-script convention is clear enough.

4. **Already working** — The Polars pipeline is functional, well-structured, and handles the full medallion pattern. Migration effort would be non-trivial for marginal benefit.

### Where dbt would start making sense

- If the project grew to 20+ transformation models
- If non-Python contributors needed to write transformations
- If you wanted formal data contracts and automated testing
- If this became a shared data platform rather than a solo ML pipeline

## If you still want to try dbt (lightweight hybrid)

The lowest-cost way to introduce dbt would be to use it **only for the aggregation layers** (`04_describe`, `04_analyses`) since those are pure GROUP BY queries. The base layer and everything upstream stays Polars. This would look like:

```
steps/01_extract.py    → Python (unchanged)
steps/02_load.py       → Python (unchanged)
steps/03_raw.py        → Python/Polars (unchanged)
steps/03_base.py       → Python/Polars (unchanged, writes base.parquet)
dbt/models/analyses/   → SQL reads base.parquet, produces analysis parquets
steps/05_train.py      → Python (unchanged)
```

This would require `dbt-duckdb` with `external_root` pointed at `data/base/`, and ~10 SQL models replacing the Polars group_by code. Clustering would still be a separate Python step.
