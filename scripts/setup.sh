#!/bin/bash
##
#   This script is used to setup the environment locally for this repository.
##

source .env/project.env

echo "🔄 Removing existing virtual environment..."
venv_name=${REPO_NAME}-${PYTHON_VERSION}
pyenv virtualenv-delete -f "$venv_name"

echo "🌍 Virtual environment setup"
pyenv virtualenv "${PYTHON_VERSION}" "$venv_name"
pyenv local "$venv_name"
pip install --upgrade pip setuptools wheel poetry

echo "📦 Install dependencies via poetry..."
poetry shell # this is needed to make sure that poetry respects the pyenv virtualenv
poetry install --no-root

echo "🔧 Install pre-commit hooks..."
pre-commit clean
pre-commit install

echo "🧹 Maintenance..."
pre-commit run --all-files

echo "✅ Setup complete!"
