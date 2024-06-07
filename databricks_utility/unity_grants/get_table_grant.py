from databricks.sdk.service import catalog

from databricks_utility.helper.databricks_connection import get_databricks_host_url, token_authentication


class GetTableGrant:

    def __init__(self, domain, environment, data_product, table_name):
        databricks_host = get_databricks_host_url(domain, environment)
        token_role = "admin"
        _, databricks_client = token_authentication(domain, environment, databricks_host, data_product, token_role)

        table_full_name = f"{domain}.{data_product}.{table_name}"
        grants = databricks_client.grants.get_effective(securable_type=catalog.SecurableType.TABLE, full_name=table_full_name)

        for permission in grants.privilege_assignments:
            print(permission)
