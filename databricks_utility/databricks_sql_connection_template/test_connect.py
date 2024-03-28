from databricks_utility.helper.databricks_connection import token_authentication, sql_warehouse_connection


class ExampleSQLConnection:

    def __init__(self, domain, environment, data_product):
        databricks_host = f"itv-{domain}-domain-{environment}.cloud.databricks.com"
        token_role = "admin"
        databricks_token, databricks_client = token_authentication(domain, environment, databricks_host, data_product, token_role)

        warehouse_type = "default"
        databricks_sql_connection = sql_warehouse_connection(domain, databricks_host, databricks_token, databricks_client, warehouse_type)

        # Run queries
        databricks_sql_cursor = databricks_sql_connection.cursor()
        databricks_sql_cursor.execute("SELECT \"Hello World\"")
        result = databricks_sql_cursor.fetchall()
        print(result)

        # Close the SQL warehouse connection
        databricks_sql_cursor.close()
        databricks_sql_connection.close()