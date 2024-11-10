# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Ingest qualifying folder

# COMMAND ----------

# Create a parameter to pass in datasource at run time
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md 
# MAGIC ####  Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# Lets define the qualifying schema
qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("q1", StringType(), True),
                                    StructField("q2", StringType(), True),
                                    StructField("q3", StringType(), True),
                                   ])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option('multiline', True).json(f"/mnt/formula1dl111111/raw/{v_file_date}/qualifying")

# COMMAND ----------

qualifying_with_ingestion_date_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Rename columns and add new columns
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit
final_df = qualifying_with_ingestion_date_df.withColumnRenamed('qualifyId', 'qualify_id') \
                            .withColumnRenamed('driverId', 'driver_id') \
                            .withColumnRenamed('raceId', 'race_id') \
                            .withColumnRenamed('constructorId', 'constructor_id') \
    .withColumn("data_source", lit(v_data_source))  \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Write output to partquet file

# COMMAND ----------

# overwrite_partition(final_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.qualifying

# COMMAND ----------

dbutils.notebook.exit("Success")
