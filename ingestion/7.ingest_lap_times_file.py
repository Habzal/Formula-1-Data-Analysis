# Databricks notebook source
# MAGIC %md 
# MAGIC ## Ingest lap_times folder

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# Create a parameter to pass in datasource at run time
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md 
# MAGIC ####  Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# Lets define the lap_time schema
lap_times_schema = StructType(fields=[
                                    StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("lap", IntegerType(), False),
                                    StructField("position", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                   ])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f"/mnt/formula1dl111111/raw/{v_file_date}/lap_times")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Rename columns and add new columns
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit
final_df = lap_times_df.withColumnRenamed('driverId', 'driver_id') \
                                            .withColumnRenamed('raceId', 'race_id') \
                                            .withColumn('ingestion_date', current_timestamp()) \
                             .withColumn("data_source", lit(v_data_source)) \
                             .withColumn("file_date", lit(v_file_date))  

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Write output to partquet file

# COMMAND ----------

# overwrite_partition(final_df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.lap_times

# COMMAND ----------

dbutils.notebook.exit("Success")
