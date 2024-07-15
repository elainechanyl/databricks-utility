from databricks.sdk.service import catalog

from databricks_utility.helper.databricks_connection import get_databricks_host_url, token_authentication


class GetExternalLocationGrant:

    def __init__(self, domain, environment, external_location_name):
        databricks_host = get_databricks_host_url(domain, environment)
        token_role = "admin"
        _, databricks_client = token_authentication(domain, environment, databricks_host, token_role)

        grants = databricks_client.grants.get_effective(securable_type=catalog.SecurableType.EXTERNAL_LOCATION, full_name=external_location_name)

        for permission in grants.privilege_assignments:
            print(permission)
