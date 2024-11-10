# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create circuits table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw_circuits;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw_circuits (
# MAGIC     circuitId INT,
# MAGIC     circuitRef STRING,
# MAGIC     name STRING,
# MAGIC     location STRING,
# MAGIC     country STRING,
# MAGIC     lat DOUBLE,
# MAGIC     lng DOUBLE,
# MAGIC     alt INT,
# MAGIC     url STRING
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC     path "/mnt/formula1dl111111/raw/circuits.csv", 
# MAGIC     header true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw_circuits

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create **races** table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw_races;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw_races (
# MAGIC     raceId INT,
# MAGIC     year INT,
# MAGIC     round INT,
# MAGIC     circuitId INT,
# MAGIC     name STRING,
# MAGIC     date DATE,
# MAGIC     time STRING,
# MAGIC     url STRING
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC     path "/mnt/formula1dl111111/raw/races.csv", 
# MAGIC     header true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw_races

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create constructors table
# MAGIC - Single line JSON

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw_constructors;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw_constructors (
# MAGIC    constructorId INT,
# MAGIC    constructorRef STRING,
# MAGIC    name STRING,
# MAGIC    nationality STRING,
# MAGIC    url STRING
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (
# MAGIC     path "/mnt/formula1dl111111/raw/constructors.json", 
# MAGIC     header true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw_constructors

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create `drivers` table
# MAGIC - Single line JSON

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw_drivers;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw_drivers (
# MAGIC    driverId INT,
# MAGIC    driverRef STRING,
# MAGIC    number INT,
# MAGIC    code STRING,
# MAGIC    name STRUCT<forename: STRING, surname STRING>
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (
# MAGIC     path "/mnt/formula1dl111111/raw/drivers.json", 
# MAGIC     header true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw_drivers

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create results table
# MAGIC - Single line JSON

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw_results;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw_results (
# MAGIC    resultId INT,
# MAGIC    raceId INT,
# MAGIC    driverId INT,
# MAGIC    constructorId INT,
# MAGIC    number INT, 
# MAGIC    grid INT,
# MAGIC    position INT,
# MAGIC    positionOrder INT,
# MAGIC    positionText STRING,
# MAGIC    points INT,
# MAGIC    laps INT,
# MAGIC    time STRING,
# MAGIC    milliseconds INT,
# MAGIC    fastestLap INT,
# MAGIC    rank INT,
# MAGIC    fastestLapTime STRING,
# MAGIC    fastestLapSpeed FLOAT,
# MAGIC    statusId STRING
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (
# MAGIC     path "/mnt/formula1dl111111/raw/results.json", 
# MAGIC     header true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw_results

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create pitstops table
# MAGIC - Multi line JSON

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw_pit_stops;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw_pit_stops (
# MAGIC    driverId INT,
# MAGIC    duration STRING,
# MAGIC    lap INT,
# MAGIC    milliseconds INT,
# MAGIC    raceId INT,
# MAGIC    stop INT,
# MAGIC    time STRING
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (
# MAGIC     path "/mnt/formula1dl111111/raw/pit_stops.json", multiLine true,
# MAGIC     header true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw_pit_stops

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create lap times table
# MAGIC - CSV File
# MAGIC - Multiple files

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw_lap_times;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw_lap_times (
# MAGIC    raceId INT,
# MAGIC    driverId INT,
# MAGIC    lap INT,
# MAGIC    position INT,
# MAGIC    time STRING,
# MAGIC    milliseconds INT
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC     path "/mnt/formula1dl111111/raw/lap_times",
# MAGIC     header true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw_lap_times

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create qualifying table
# MAGIC - JSON File
# MAGIC - MultiLine JSON
# MAGIC - Multiple files

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw_qualifying;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw_qualifying (
# MAGIC   constructorId INT,
# MAGIC    driverId INT,
# MAGIC    number INT,
# MAGIC    position INT,
# MAGIC    q1 STRING,
# MAGIC    q2 STRING,
# MAGIC    q3 STRING,
# MAGIC    qualifyId INT,
# MAGIC    raceId INT
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (
# MAGIC     path "/mnt/formula1dl111111/raw/qualifying", multiLine true,
# MAGIC     header true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw_qualifying
