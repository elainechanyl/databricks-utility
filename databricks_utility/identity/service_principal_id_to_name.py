from databricks_utility.helper.databricks_connection import account_authentication


class GetSpName:

    def __init__(self, environment, service_principal_id):
        databricks_account_client = account_authentication(environment)

        sp_filter = f"applicationId eq {service_principal_id}"
        sp_name_list = databricks_account_client.service_principals.list(filter=sp_filter)

        for sp_name in sp_name_list:
            print(sp_name.display_name)
