import argparse

from databricks_sql_connection_template import test_connect

from unity_table import get_table_grant, create_table, delete_table

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("utility", help="name of utility to be run", type=str)
parser.add_argument("domain", help="name of AWS domain", type=str)
parser.add_argument("environment", help="name of environment", type=str, choices=["dev", "prd"])
parser.add_argument("data_product", help="name of data product", type=str)
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
        test_connect.ExampleSQLConnection(domain, environment, data_product)
    case "get_table_permissions":
        if table_name != "":
            get_table_grant.GetTableGrant(domain, environment, data_product, table_name)
        else:
            print("Undefined table name.")
    case "create_table":
        if table_name != "":
            create_table.CreateTable(domain, environment, data_product, table_name)
        else:
            print("Undefined table name.")
    case "delete_table":
        if table_name != "":
            delete_table.DeleteTable(domain, environment, data_product, table_name)
        else:
            print("Undefined table name.")
    case _:
        print("Undefined utility.")