from databricks.sdk import WorkspaceClient, AccountClient
from databricks.sql import connect

import botocore
import botocore.session
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig


def get_databricks_host_url(domain, environment):
    return f"itv-{domain}-domain-{environment}.cloud.databricks.com"


def token_authentication(domain, environment, databricks_host, data_product, role):
    # get databricks token string from aws secrets manager
    aws_sm_client = botocore.session.get_session().create_client('secretsmanager')
    aws_sm_cache_config = SecretCacheConfig()
    aws_secret_cache = SecretCache(config=aws_sm_cache_config, client=aws_sm_client)

    match role:
        case "admin":
            token_secret_id = f"infra/{environment}/{environment}/databricks-platform-automation-token/"
        case "dataproduct":
            token_secret_id = f"infra/{environment}/{environment}/{domain}/{data_product}-db-sp-token".replace("data-product", "dp")
        case _:
            raise Exception(f"Incorrect token role [{role}]. Choose from [admin, dataproduct]")
    databricks_token = aws_secret_cache.get_secret_string(token_secret_id)

    # databricks workspace authentication
    databricks_client = WorkspaceClient(
        host=f"https://{databricks_host}",
        token=databricks_token
    )

    return databricks_token, databricks_client


def account_authentication(environment):
    # get databricks token string from aws secrets manager
    aws_sm_client = botocore.session.get_session().create_client('secretsmanager')
    aws_sm_cache_config = SecretCacheConfig()
    aws_secret_cache = SecretCache(config=aws_sm_cache_config, client=aws_sm_client)

    databricks_account_id = aws_secret_cache.get_secret_string(f"{environment}/databricks-id/")
    databricks_username = aws_secret_cache.get_secret_string(f"{environment}/databricks-username/")
    databricks_password = aws_secret_cache.get_secret_string(f"{environment}/databricks-pw/")

    databricks_account_client = AccountClient(
        host="https://accounts.cloud.databricks.com",
        account_id=databricks_account_id,
        username=databricks_username,
        password=databricks_password,
    )

    return databricks_account_client


def sql_warehouse_connection(domain, databricks_host, databricks_token, databricks_client, warehouse_type):

    # get SQL warehouse ID
    sql_warehouses = databricks_client.warehouses.list()

    match warehouse_type:
        case "default":
            warehouse_name = f"Default {domain} SQL warehouse"
        case "serverless":
            warehouse_name = f"Serverless {domain} SQL warehouse"
        case _:
            warehouse_name = f"Default {domain} SQL warehouse"
    sql_warehouse_id = ""
    for warehouse in sql_warehouses:
        if warehouse.name == warehouse_name:
            sql_warehouse_id = warehouse.id
    if sql_warehouse_id == "":
        raise Exception("SQL warehouse not found!")

    databricks_sql_endpoint = f"/sql/1.0/endpoints/{sql_warehouse_id}"

    databricks_sql_connection = connect(
        server_hostname=databricks_host,
        http_path=databricks_sql_endpoint,
        access_token=databricks_token,
    )

    return databricks_sql_connection
