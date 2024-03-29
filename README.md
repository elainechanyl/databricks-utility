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

Example:
```
aws-vault exec bar-dev-root -- poetry run python databricks_utility/__main__.py dummy bar dev test-cdktf-data-product
```

Utility names

| Utility               | Descriptions                                                                                                                          |
|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| dummy                 | For testing connection to Databricks workspace and SQL warehouse with admin token. You can also use it as a template for development. |
| create_table          | Create Databricks unity table without column defined                                                                                  |
| delete_table          | Delete Databricks unity table                                                                                                         |
| get_table_permissions | Get the permissions granted to the Databricks unity table                                                                             |
