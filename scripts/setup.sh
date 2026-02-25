#!/bin/bash

set -e

echo "==> Creating virtual environment"
uv venv --python 3.13.2

echo "==> Activating environment"
source .venv/bin/activate

echo "==> Installing dependencies"
uv sync --all-groups

echo "==> Installing pre-commit hooks"
pre-commit clean
pre-commit install

echo "==> Setup complete. Run 'source .venv/bin/activate' to activate."
