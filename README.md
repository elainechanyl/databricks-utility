# Databricks utility

## Local setup

Install pyenv and poetry.
```
brew install pyenv poetry
```
Use pyenv to install Python.
```
pyenv install $(cat .python-version)
```
Create a virtual environment with installed python dependencies
```
poetry install
```

## Quick start
You must use AWS credential to run the utility.

Standalone run example:
```
aws-vault exec bar-dev-root -- python databricks_utility/__main__.py
```