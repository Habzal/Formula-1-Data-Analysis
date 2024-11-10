# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_presentation
# MAGIC LOCATION "/mnt/formula1dl111111/presentation"

# COMMAND ----------

# MAGIC %sql
# MAGIC USE f1_presentation

# COMMAND ----------


spark.sql("SHOW TABLES").show()