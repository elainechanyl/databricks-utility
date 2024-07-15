from databricks_utility.helper.databricks_connection import get_databricks_host_url, token_authentication, sql_warehouse_connection


class SqlByDataProductSp:

    def __init__(self, domain, environment, data_product, statement):
        databricks_host = get_databricks_host_url(domain, environment)
        # token_role = data_product
        token_role = "admin"
        databricks_token, databricks_client = token_authentication(domain, environment, databricks_host, token_role)

        warehouse_type = "serverless"
        databricks_sql_connection = sql_warehouse_connection(domain, databricks_host, databricks_token, databricks_client, warehouse_type)

        # Run queries
        databricks_sql_cursor = databricks_sql_connection.cursor()
        databricks_sql_cursor.execute(statement)
        result = databricks_sql_cursor.fetchall()
        print(result)

        # Close the SQL warehouse connection
        databricks_sql_cursor.close()
        databricks_sql_connection.close()
