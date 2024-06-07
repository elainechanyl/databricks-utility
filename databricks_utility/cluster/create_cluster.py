from databricks_utility.helper.databricks_connection import get_databricks_host_url, token_authentication


class CreateCluster:

    def __init__(self, domain, environment, data_product, cluster_name):
        databricks_host = get_databricks_host_url(domain, environment)
        token_role = "admin"
        _, databricks_client = token_authentication(domain, environment, databricks_host, data_product, token_role)

        cluster = databricks_client.clusters.create(
            cluster_name=cluster_name,
            policy_id="A9615E30AB0001C5",
            spark_version="13.3.x-scala2.12"
        )


