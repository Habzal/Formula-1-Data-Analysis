# Databricks notebook source
# MAGIC %md 
# MAGIC #### Mount Azure Data Lake using Service Principal
# MAGIC  1. Get client_id, tenant_id and secret_value from key vault
# MAGIC  2. Set Spark config with App/Client ID, Directory/Tenant ID and Secret to access the storage account 
# MAGIC  3. Call file system utility mount to mount the storage
# MAGIC  4. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    #Get Secret from Key Vault
    client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
    secret_value = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-secret-value')

    #Set Spark configs
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": secret_value,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    #Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
        
    #Mount the storage account container
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount Raw Container
# MAGIC

# COMMAND ----------

mount_adls('formula1dl111111', 'raw')

# COMMAND ----------

mount_adls('formula1dl111111', 'processed')

# COMMAND ----------

mount_adls('formula1dl111111', 'presentation')
