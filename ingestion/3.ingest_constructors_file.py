# Databricks notebook source
# MAGIC %md 
# MAGIC ## Ingest constructors.json file

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

# Lets use the DDL Style format to define our schema
constructors_schema = 'constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING' 

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema).json(f"/mnt/formula1dl111111/raw/{v_file_date}/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Drop unwanted columns from the dataframe

# COMMAND ----------

constrcutor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Rename columns and add ingestion date
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
constrcutor_final_df = constrcutor_dropped_df.withColumnRenamed('constructorId', 'constructor_id') \
                                            .withColumnRenamed('constrcutorRef', 'constructor_ref') \
                                            .withColumn('ingestion_date', current_timestamp()) \
                                            .withColumn("data_source", lit(v_data_source)) \
                                            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Write output to partquet file

# COMMAND ----------

constrcutor_final_df.write.mode('overwrite').format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors

# COMMAND ----------

dbutils.notebook.exit("Success")
