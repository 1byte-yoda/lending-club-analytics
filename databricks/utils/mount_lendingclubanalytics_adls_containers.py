# Databricks notebook source
client_id = dbutils.secrets.get("lc-analytics-secret-scope", "lendingclubanalyticsdl-client-id")
client_secret = dbutils.secrets.get("lc-analytics-secret-scope", "lendingclubanalyticsdl-client-secret")
tenant_id = dbutils.secrets.get("lc-analytics-secret-scope", "lendingclubanalyticsdl-tenant-id")
directory_id = "44cab69f-4b33-45dd-8b14-344ed2949c2d"

# COMMAND ----------

def mount_container(container_name: str, env: str) -> bool:
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token"
    }
    storage_account = "lendingclubanalyticsdl"
    source = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/"
    mount_point = f"/mnt/{env}/{storage_account}/{container_name}"

    for mount_info in dbutils.fs.mounts():
        if mount_info.mountPoint == mount_point:
            return True
    
    return dbutils.fs.mount(
        source=source,
        mount_point=mount_point,
        extra_configs=configs
    )

# COMMAND ----------

ENV = "prod"

# COMMAND ----------

mount_container("bronze", env=ENV)

# COMMAND ----------

mount_container("silver", env=ENV)

# COMMAND ----------

mount_container("gold", env=ENV)
