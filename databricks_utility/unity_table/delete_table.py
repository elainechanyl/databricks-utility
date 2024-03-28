from databricks_utility.helper.databricks_connection import token_authentication, sql_warehouse_connection


class DeleteTable:

    def __init__(self, domain, environment, data_product, table_name):
        databricks_host = f"itv-{domain}-domain-{environment}.cloud.databricks.com"
        token_role = "dataproduct"
        databricks_token, databricks_client = token_authentication(domain, environment, databricks_host, data_product, token_role)

        warehouse_type = "default"
        databricks_sql_connection = sql_warehouse_connection(domain, databricks_host, databricks_token, databricks_client, warehouse_type)

        # Run queries
        databricks_sql_cursor = databricks_sql_connection.cursor()
        databricks_sql_cursor.execute(f"DROP TABLE {domain}.`{data_product}`.`{table_name}`")
        # databricks_sql_cursor.execute(f"DROP VIEW {domain}.`{data_product}`.`{table_name}`")
        result = databricks_sql_cursor.fetchall()
        print(result)

        # Close the SQL warehouse connection
        databricks_sql_cursor.close()
        databricks_sql_connection.close()