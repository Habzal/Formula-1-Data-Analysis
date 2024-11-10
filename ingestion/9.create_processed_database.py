# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed
# MAGIC LOCATION "/mnt/formula1dl111111/processed"

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DATABASE f1_processed

# COMMAND ----------

# MAGIC %sql
# MAGIC USE f1_processed

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table pit_stops

# COMMAND ----------


spark.sql("SHOW TABLES").show()
