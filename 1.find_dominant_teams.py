# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT
# MAGIC   team_name,
# MAGIC   COUNT(1) AS total_races,
# MAGIC   SUM(calculated_points) AS total_points,
# MAGIC   AVG(calculated_points) AS avg_points
# MAGIC FROM f1_presentation.calculated_race_results
# MAGIC WHERE race_year BETWEEN 2001 AND 2010
# MAGIC GROUP BY team_name
# MAGIC HAVING total_races >= 100
# MAGIC ORDER BY total_points DESC;