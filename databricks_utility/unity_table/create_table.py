from databricks.sdk import WorkspaceClient
from databricks.sql import connect

import botocore
import botocore.session
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig


class CreateTable:

    def __init__(self, domain, environment, data_product, table_name):

        databricks_host = f"itv-{domain}-domain-{environment}.cloud.databricks.com"

        aws_sm_client = botocore.session.get_session().create_client('secretsmanager')
        aws_sm_cache_config = SecretCacheConfig()
        aws_secret_cache = SecretCache(config=aws_sm_cache_config, client=aws_sm_client)

        # Use data product service principal token to auth
        databricks_dp_token_secret_id = f"infra/{environment}/{environment}/{domain}/{data_product}-db-sp-token".replace("data-product", "dp")
        databricks_dp_token = aws_secret_cache.get_secret_string(databricks_dp_token_secret_id)

        databricks_client = WorkspaceClient(
            host=f"https://{databricks_host}",
            token=databricks_dp_token
        )

        # Get default SQL warehouse ID
        sql_warehouses = databricks_client.warehouses.list()

        sql_warehouse_id = ""
        for warehouse in sql_warehouses:
            if warehouse.name == f"Default {domain} SQL warehouse":
                sql_warehouse_id = warehouse.id
        if sql_warehouse_id == "":
            raise Exception("SQL warehouse not found!")

        # Connect to SQL warehouse
        databricks_sql_endpoint = f"/sql/1.0/endpoints/{sql_warehouse_id}"

        databricks_sql_connection = connect(
            server_hostname=databricks_host,
            http_path=databricks_sql_endpoint,
            access_token=databricks_dp_token
        )

        # Run queries
        databricks_sql_cursor = databricks_sql_connection.cursor()
        databricks_sql_cursor.execute(f"CREATE TABLE {domain}.`{data_product}`.{table_name} (id INT, name STRING)")
        result = databricks_sql_cursor.fetchall()
        print(result)

        # Close the SQL warehouse connection
        databricks_sql_cursor.close()
        databricks_sql_connection.close()