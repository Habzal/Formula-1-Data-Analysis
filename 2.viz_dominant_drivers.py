# Databricks notebook source
html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers </h1>"""
displayHTML(html)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_dominant_drivers
# MAGIC AS
# MAGIC SELECT
# MAGIC   driver_name,
# MAGIC   COUNT(1) AS total_races,
# MAGIC   SUM(calculated_points) AS total_points,
# MAGIC   AVG(calculated_points) AS avg_points,
# MAGIC   RANK() OVER(order by AVG(calculated_points) DESC) AS driver_rank 
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC -- WHERE race_year BETWEEN 2011 AND 2020
# MAGIC GROUP BY driver_name
# MAGIC HAVING total_races >= 50
# MAGIC ORDER BY avg_points DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_dominant_drivers

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT
# MAGIC   race_year,
# MAGIC   driver_name,
# MAGIC   COUNT(1) AS total_races,
# MAGIC   SUM(calculated_points) AS total_points,
# MAGIC   AVG(calculated_points) AS avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
# MAGIC
# MAGIC GROUP BY race_year, driver_name
# MAGIC ORDER BY race_year, avg_points DESC;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT
# MAGIC   race_year,
# MAGIC   driver_name,
# MAGIC   COUNT(1) AS total_races,
# MAGIC   SUM(calculated_points) AS total_points,
# MAGIC   AVG(calculated_points) AS avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
# MAGIC
# MAGIC GROUP BY race_year, driver_name
# MAGIC ORDER BY race_year, avg_points DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   race_year,
# MAGIC   driver_name,
# MAGIC   COUNT(1) AS total_races,
# MAGIC   SUM(calculated_points) AS total_points,
# MAGIC   AVG(calculated_points) AS avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
# MAGIC GROUP BY race_year, driver_name
# MAGIC ORDER BY race_year, avg_points DESC;