from databricks_utility.helper.databricks_connection import get_databricks_host_url, token_authentication


class GetAllDataSources:

    def __init__(self, domain, environment, data_product):
        databricks_host = get_databricks_host_url(domain, environment)
        token_role = "admin"
        _, databricks_client = token_authentication(domain, environment, databricks_host, data_product, token_role)

        data_sources = databricks_client.data_sources.list()

        for data_source in data_sources:
            print(f"{data_source.warehouse_id},{data_source.name},{domain},{environment}")
