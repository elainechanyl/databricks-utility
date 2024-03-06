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
If you get SSL error when running the utility, add environment variable as below
```agsl
export CURL_CA_BUNDLE="/usr/local/etc/ca-certificates/cert.pem"
```

## Quick start
You must use AWS credential to run the utility.

Standalone run example:
```
aws-vault exec bar-dev-root -- python databricks_utility/__main__.py dummy bar dev
```

You can run it with minimal arguments - utility name only (without domain and environment specified). In this case, domain and environment information will be retrieved from the environment variables `AWS_VAULT`
```
aws-vault exec bar-dev-root -- python databricks_utility/__main__.py {utility}
```

Utility names

| Utility | Descriptions                                                                                                                          |
|---------|---------------------------------------------------------------------------------------------------------------------------------------|
| dummy   | For testing connection to Databricks workspace and SQL warehouse with admin token. You can also use it as a template for development. |
|         |                                                                                                                                       |
