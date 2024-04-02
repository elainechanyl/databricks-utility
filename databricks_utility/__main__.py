import argparse

from databricks_sql_connection_template import test_connect
from unity_table import get_table_grant, create_table, delete_table
from identity import get_service_principal_id
from sql_statement import sql

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("utility", help="name of utility to be run", type=str)
parser.add_argument("domain", help="name of AWS domain", type=str)
parser.add_argument("environment", help="name of environment", type=str, choices=["dev", "prd"])
parser.add_argument("data_product", help="name of data product", type=str)
parser.add_argument("other_argument", nargs="?", default="", help="others: table name, sql statement, etc.", type=str)
args = parser.parse_args()
utility = args.utility
domain = args.domain
environment = args.environment
data_product = args.data_product
other_argument = args.other_argument

# Call utility
match utility:
    case "dummy":
        test_connect.ExampleSQLConnection(domain, environment, data_product)
    case "get_table_permissions":
        if other_argument != "":
            get_table_grant.GetTableGrant(domain, environment, data_product, other_argument)
        else:
            print("Undefined table name.")
    case "create_table":
        if other_argument != "":
            create_table.CreateTable(domain, environment, data_product, other_argument)
        else:
            print("Undefined table name.")
    case "delete_table":
        if other_argument != "":
            delete_table.DeleteTable(domain, environment, data_product, other_argument)
        else:
            print("Undefined table name.")
    case "get_sp_id":
        get_service_principal_id.GetSpId(environment, data_product)
    case "sql_statement":
        if other_argument != "":
            sql.SqlByDataProductSp(domain, environment, data_product, other_argument)
        else:
            print("Undefined SQL statement.")
    case _:
        print("Undefined utility.")