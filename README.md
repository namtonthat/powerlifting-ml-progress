# 💪🏋️‍♂️ Powerlifting ML Progress

[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![python](https://img.shields.io/badge/Python-3.11-3776AB.svg?style=flat&logo=python&logoColor=white)](https://www.python.org)
[![extract-transform-load](https://github.com/namtonthat/powerlifting-ml-progress/actions/workflows/extract-transform-load.yml/badge.svg)](https://github.com/namtonthat/powerlifting-ml-progress/actions/workflows/extract-transform-load.yml)
[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://powerlifting.streamlit.app)

![Last Updated](https://img.shields.io/badge/Last%20Updated-2024--04--30-blue)
# :computer: Local Development

Refer to the `Makefile` using: `make` from the command line.
## :wrench: Setup
- `make setup` - creates a `virtualenv` and installs dependencies (via `pyenv` and `poetry` respectively) and then installs `precommit` for linting and code quality

# :gear: Data Model
Refer to `.github/workflows/*.yml` files.

Jobs are orchestrated by [`dagster`](https://github.com/dagster-io/dagster) with transformations done by `dbt`.

```mermaid
graph LR

    A[extract-transform-load.yml]
    B[01_extract.py]
    C[02_load.py]
    D[03_raw.py]
    E[03_base.py]
    F[`dbt` transformations]
    G[train machine learning model]

    A --> B
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
```

## 💡 Purpose

This repository analyzes publicly available data from the `OpenPowerlifting` database to estimate how much progress a powerlifter could make over time.

## 📊 Data

The data used in this repository is sourced from the `OpenPowerlifting` database, which contains information on powerlifting competitions, lifters, and their performances. You can download the necessary data from `s3://powerlifting-ml-progress/landing/openpowerlifting-latest.parquet` file.

##  References

- `OpenPowerlifting` database: [https://www.openpowerlifting.org/](https://www.openpowerlifting.org/)