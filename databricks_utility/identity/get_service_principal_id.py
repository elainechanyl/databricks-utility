from databricks_utility.helper.databricks_connection import account_authentication


class GetSpId:

    def __init__(self, environment, data_product):
        databricks_account_client = account_authentication(environment)

        sp_name = f"airflow-sp-{data_product}"
        sp_filter = f"displayName eq {sp_name}"
        sp_id_list = databricks_account_client.service_principals.list(filter=sp_filter)

        for sp_id in sp_id_list:
            print(sp_id)
