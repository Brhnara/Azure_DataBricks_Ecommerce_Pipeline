# Databricks notebook source
# Storage Account ve Container Bilgileri
storage_account_name = "ecommercedl1"
containers = ["raw", "processed", "presentation", "temp"]
mount_point = f"/mnt/{container_name}"

# Service Principal Bilgileri
client_id = dbutils.secrets.get(scope="your-scope", key="") # Application (Client) ID
tenant_id = dbutils.secrets.get(scope="your-scope", key="")  # Directory (Tenant) ID
client_secret = dbutils.secrets.get(scope="your-scope", key="") # Secret Key

# OAuth Config
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}


# COMMAND ----------

# Mount Containers
for container_name in containers:
    mount_point = f"/mnt/{container_name}"
    
    try:
        dbutils.fs.mount(
            source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
            mount_point=mount_point,
            extra_configs=configs
        )
        print(f"✅ {container_name} mounted succesfully -> {mount_point}")
    except Exception as e:
        print(f"⚠️ {container_name} error occured: {str(e)}")

# COMMAND ----------


display(dbutils.fs.ls(mount_point))


display(dbutils.fs.mounts())
