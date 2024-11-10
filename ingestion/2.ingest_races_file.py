# Databricks notebook source
# MAGIC %md 
# MAGIC ## Ingest RACES.csv file

# COMMAND ----------

# Create a parameter to pass in datasource at run time
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# Create a parameter to pass in file date at run time
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md 
# MAGIC ####  Read the CSV file using the spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/formula1dl111111/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType

race_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                     StructField('year', IntegerType(), True),
                                     StructField('round', IntegerType(), True),
                                     StructField('circuitId', IntegerType(), True),
                                     StructField('name', StringType(), True),
                                     StructField('date', DateType(), True),
                                     StructField('time', StringType(), True),
                                     StructField('url', StringType(), True)
                                     ])

# COMMAND ----------

races_df = spark.read.option('header', True).schema(race_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Select only the required columns

# COMMAND ----------

    from pyspark.sql.functions import col
    races_selected_df = races_df.select(col('raceId'), col('year'), col('round'), col('circuitId'), col('name'), col('date'), col('time'))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit
races_renamed_df = races_selected_df.withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('year', 'race_year') \
    .withColumnRenamed('circuitId', 'circuit_id') \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))
   

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, lit
races_timestamp_df = races_renamed_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Add ingestion date to the dataframe
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

races_final_df = add_ingestion_date(races_timestamp_df)

# COMMAND ----------

races_final_df = races_final_df.drop('date')
races_final_df = races_final_df.drop('time')

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Write data to datalake as parquet
# MAGIC

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races

# COMMAND ----------

dbutils.notebook.exit("Success")
