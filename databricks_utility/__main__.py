import argparse

from databricks_sql_connection_template import test_connect
from unity_table import create_table, delete_table
from databricks_utility.unity_grants import get_table_grant, get_external_location_grant
from identity import get_service_principal_id, service_principal_id_to_name
from cluster import create_cluster
from sql_statement import sql, get_data_sources

# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("utility", help="name of utility to be run", type=str)
parser.add_argument("domain", help="name of AWS domain", type=str)
parser.add_argument("environment", help="name of environment", type=str, choices=["dev", "prd", "uat"])
parser.add_argument("instance", help="name/id of instance e.g. data product, service principal, etc.", type=str)
parser.add_argument("other_argument", nargs="?", default="", help="others: table name, sql statement, etc.", type=str)
args = parser.parse_args()
utility = args.utility
domain = args.domain
environment = args.environment

# Call utility
match utility:
    case "dummy":
        data_product = args.instance
        test_connect.ExampleSQLConnection(domain, environment, data_product)
    case "get_table_permissions":
        data_product = args.instance
        table_name = args.other_argument
        get_table_grant.GetTableGrant(domain, environment, data_product, table_name)
    case "get_external_location_permissions":
        external_location_name = args.instance
        get_external_location_grant.GetExternalLocationGrant(domain, environment, external_location_name)
    case "create_table":
        data_product = args.instance
        table_name = args.other_argument
        create_table.CreateTable(domain, environment, data_product, table_name)
    case "delete_table":
        data_product = args.instance
        table_name = args.other_argument
        delete_table.DeleteTable(domain, environment, data_product, table_name)
    case "get_sp_id":
        data_product = args.instance
        get_service_principal_id.GetSpId(environment, data_product)
    case "get_sp_name":
        service_principal_id = args.instance
        service_principal_id_to_name.GetSpName(environment, service_principal_id)
    case "create_cluster":
        data_product = args.instance
        cluster_name = args.other_argument
        create_cluster.CreateCluster(domain, environment, data_product, cluster_name)
    case "sql_statement":
        data_product = args.instance
        sql_statement = args.other_argument
        if sql_statement != "":
            sql.SqlByDataProductSp(domain, environment, data_product, sql_statement)
        else:
            print("Undefined SQL statement.")
    case "get_data_source":
        _ = args.instance
        get_data_sources.GetAllDataSources(domain, environment)
    case _:
        print("Undefined utility.")