# 💪🏋️‍♂️ Powerlifting ML Progress

[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![python](https://img.shields.io/badge/Python-3.11-3776AB.svg?style=flat&logo=python&logoColor=white)](https://www.python.org)
[![scrape](https://github.com/namtonthat/powerlifting-ml-progress/actions/workflows/scrape.yml/badge.svg)](https://github.com/namtonthat/powerlifting-ml-progress/actions/workflows/scrape.yml)
[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://powerlifting.streamlit.app)

![Last Updated](https://img.shields.io/badge/Last%20Updated-{{ last_updated }}-blue)
# :computer: Local Development

Refer to the `Makefile` using: `make` from the command line.
## :wrench: Setup
- `make setup` - creates a `virtualenv` and installs dependencies (via `pyenv` and `poetry` respectively) and then installs `precommit` for linting and code quality

# :gear: Data Model
Refer to `.github/workflows/scrape.yml` file.

Jobs are orchestrated by [`dagster`](https://github.com/dagster-io/dagster) with transformations done by `dbt`.

```mermaid
graph LR

    A[scrape.yml]
    B[01_load.py]
    C[`dbt` transformations]
    D[train machine learning model]

    A --> B
    B --> C
    C --> D
```

## 💡 Purpose

This repository analyzes publicly available data from the `OpenPowerlifting` database to estimate how much progress a powerlifter could make over time.

## 📊 Data

The data used in this repository is sourced from the `OpenPowerlifting` database, which contains information on powerlifting competitions, lifters, and their performances. You can download the necessary data from `s3://{{ bucket_name }}/{{ s3_key }}` file.

##  References

- `OpenPowerlifting` database: [https://www.openpowerlifting.org/](https://www.openpowerlifting.org/)
