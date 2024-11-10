# Databricks notebook source
# MAGIC %md 
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC  1. Register the Service Principal (also referred to as Azure AD Application).  
# MAGIC  2. Generate a secret/ password for the Application
# MAGIC  3. Set Spark Config with App/Client ID, Directory/Tenant ID and Secret to access the storage account 
# MAGIC  4. Assign Role Storage Blob Data Contributor to Data Lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-tenant-id')
secret_value = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-app-secret-value')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl111111.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl111111.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl111111.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl111111.dfs.core.windows.net", secret_value)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl111111.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl111111.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl111111.dfs.core.windows.net/circuits.csv"))
