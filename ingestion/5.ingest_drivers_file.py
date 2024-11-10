# Databricks notebook source
# MAGIC %md 
# MAGIC ## Ingest drivers.json file

# COMMAND ----------

# Create a parameter to pass in datasource at run time
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# Create a parameter to pass in file date at run time
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md 
# MAGIC ####  Read the JSON file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

# Since we have a nested json, where name has two values, lets do a name schema first
name_schema = StructType(fields=[StructField('forename', StringType(), True),
                         StructField('surname', StringType(), True)])

# COMMAND ----------

# Now lets define the drivers schema
drivers_schema = StructType(fields=[StructField('driverId', IntegerType(), False),
                         StructField('driverRef', StringType(), True),
                         StructField('number', IntegerType(), True),
                         StructField('code', StringType(), True),
                         StructField('name', name_schema),
                         StructField('dob', DateType(), True),
                         StructField('nationality', StringType(), True),
                         StructField('url', StringType(), True)
                         ])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f"/mnt/formula1dl111111/raw/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Rename columns and add new columns
# MAGIC 1. driverID renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ingestion date added
# MAGIC 4. name added with concatenation of forename and surname
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit
drivers_with_columns_df = drivers_df.withColumnRenamed('driverId', 'driver_id') \
                                            .withColumnRenamed('driverRef', 'driver_ref') \
                                            .withColumn('ingestion_date', current_timestamp()) \
                                            .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname'))) \
                                            .withColumn("data_source", lit(v_data_source)) \
                                                .withColumn("file_date", lit(v_file_date))   

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Drop unwanted columns from the dataframe

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col('url'))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Write output to partquet file

# COMMAND ----------

drivers_final_df.write.mode('overwrite').format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers

# COMMAND ----------

dbutils.notebook.exit("Success")
