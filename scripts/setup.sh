#!/bin/bash
##
#   This script is used to setup the environment locally for this repository.
##

# Detecting OS
OS_NAME=$(uname)
RELEASE_INFO=$(lsb_release -si 2>/dev/null || echo "Not Ubuntu")

source .env/project.env

echo "🔄 Removing existing virtual environment..."
pyenv virtualenv-delete -f ${REPO_NAME}-${PYTHON_VERSION}

if [[ "$OS_NAME" == "Darwin" ]]; then
	echo "⚙️  Creating arch prefix for mac OS"
	if [[ "$arch" == "x86_64" ]]; then
		ARCH_PREFIX="arch -x86_64"
	elif [[ "$arch" == "arm64" ]]; then
		ARCH_PREFIX="arch -arm64"
	else
		echo "Unsupported arch, defaults to x86_64, can also be arm64"
	fi
	# MacOS
	if [[ "$reinstall_python" == "true" ]]; then
		echo "⬇️  Installing python: ${PYTHON_VERSION}"
		$ARCH_PREFIX pyenv install ${PYTHON_VERSION}
	else
		echo "🔧  Using existing python: ${PYTHON_VERSION}"
	fi

	echo "🔧  Installing awscli..."
	$ARCH_PREFIX brew install awscli

elif [[ "$RELEASE_INFO" == "Ubuntu" ]]; then
	# Ubuntu 22.04
	echo "⬇️  Installing python: ${PYTHON_VERSION}"
	sudo apt update
	sudo apt install -y python${PYTHON_VERSION}

	echo "🔧  Installing awscli..."
	sudo apt install -y awscli

else
	echo "Unsupported OS or version."
fi

echo "🌍  Virtual environment setup"
pyenv virtualenv ${PYTHON_VERSION} $REPO_NAME-${PYTHON_VERSION}
pyenv local ${REPO_NAME}-${PYTHON_VERSION}
pip install --upgrade pip setuptools wheel poetry

echo "📦 Install dependencies via poetry..."
poetry install

echo "🔧 Install pre-commit hooks..."
pre-commit clean
pre-commit install

echo "🧹 Maintenance..."
pre-commit run --all-files

echo "✅ Setup complete!"
