# Databricks notebook source
# MAGIC %md
# MAGIC ### Drop all the tables

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_processed CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed
# MAGIC LOCATION "/mnt/formula1dl111111/processed"

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_presentation CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed
# MAGIC LOCATION "/mnt/formula1dl111111/presentation"