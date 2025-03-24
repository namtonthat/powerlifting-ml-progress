#!/bin/bash

set -e errexit

echo "create virtual env"
uv venv --python 3.13.2

echo "activating env"
source .venv/bin/activate

echo "syncing all groups"
uv sync --all-groups

echo "install precommit"
pre-commit clean
pre-commit install
