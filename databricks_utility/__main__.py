import os
import argparse

from databricks_sql_connection_template import test_connect

# Retrieve domain and environment from aws-vault
aws_vault_info = os.getenv('AWS_VAULT').split("-")
domain_from_aws_vault = aws_vault_info[0]
environment_from_aws_vault = aws_vault_info[1]

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("utility", help="name of utility to be run", type=str)
parser.add_argument("domain", nargs="?", default=domain_from_aws_vault, help="name of AWS domain", type=str)
parser.add_argument("environment", nargs="?", default=environment_from_aws_vault, help="name of environment", type=str, choices=["dev", "prd"])
args = parser.parse_args()
utility = args.utility
domain = args.domain
environment = args.environment

# Call utility
match utility:
    case "dummy":
        test_connect.ExampleSQLConnection(domain, environment)
    case _:
        print("Undefined utility.")