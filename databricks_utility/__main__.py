import os
import argparse

from databricks_sql_connection_template import test_connect

from unity_table import get_table_grant, create_table, delete_table

# Retrieve domain and environment from aws-vault
aws_vault_info = os.getenv('AWS_VAULT').split("-")
domain_from_aws_vault = aws_vault_info[0]
environment_from_aws_vault = aws_vault_info[1]

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("utility", help="name of utility to be run", type=str)
parser.add_argument("domain", nargs="?", default=domain_from_aws_vault, help="name of AWS domain", type=str)
parser.add_argument("environment", nargs="?", default=environment_from_aws_vault, help="name of environment", type=str, choices=["dev", "prd"])
parser.add_argument("data_product", nargs="?", default="", help="name of data product", type=str)
parser.add_argument("table_name", nargs="?", default="", help="name of unity table", type=str)
args = parser.parse_args()
utility = args.utility
domain = args.domain
environment = args.environment
data_product = args.data_product
table_name = args.table_name

# Call utility
match utility:
    case "dummy":
        test_connect.ExampleSQLConnection(domain, environment)
    case "get_table_grant":
        get_table_grant.GetTableGrant(domain, environment)
    case "create_table":
        if data_product != "" and table_name != "":
            create_table.CreateTable(domain, environment, data_product, table_name)
        else:
            print("Undefined data product and/or table name.")
    case "delete_table":
        if data_product != "" and table_name != "":
            delete_table.DeleteTable(domain, environment, data_product, table_name)
        else:
            print("Undefined data product and/or table name.")
    case _:
        print("Undefined utility.")