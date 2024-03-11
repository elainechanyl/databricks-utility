from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog

import botocore
import botocore.session
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig


class GetTableGrant:

    def __init__(self, domain, environment):

        databricks_host = f"itv-{domain}-domain-{environment}.cloud.databricks.com"

        aws_sm_client = botocore.session.get_session().create_client('secretsmanager')
        aws_sm_cache_config = SecretCacheConfig()
        aws_secret_cache = SecretCache(config=aws_sm_cache_config, client=aws_sm_client)

        # Use automation admin token to auth
        databricks_admin_token_secret_id = f"infra/{environment}/{environment}/databricks-platform-automation-token/"
        databricks_admin_token = aws_secret_cache.get_secret_string(databricks_admin_token_secret_id)

        databricks_client = WorkspaceClient(
            host=f"https://{databricks_host}",
            token=databricks_admin_token
        )

        grants = databricks_client.grants.get_effective(securable_type=catalog.SecurableType.TABLE, full_name="bar.test-cdktf-data-product.employees")
        print(grants.privilege_assignments)

        # to be continued...
