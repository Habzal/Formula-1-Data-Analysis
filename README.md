![f1](https://github.com/user-attachments/assets/3b4c1c91-2511-4ade-aad0-285aea689fc0)
# Formula-1-Data-Analysis

## Overview
Formula 1 Data Analysis project leveraging Spark on Azure Databricks and Delta Lake architecture. The project entails creating an ETL (Extract, Transform, Load) pipeline to retrieve Formula 1 racing data from the Ergast API, a platform dedicated to Formula 1 statistics, and subsequently processing and storing the data in Azure Data Lake Gen2 storage.

## Understanding Formula1:
- The Formula 1 season occurs annually, typically featuring around 20 races.
- Each race takes place over a weekend.
- Races are held at various circuits, with most hosting one race per year. However, due to COVID, some circuits have hosted multiple races in a season.
- Approximately 10 teams, or constructors, compete in each season.
- Each team has two primary drivers, each assigned their own car. While there are reserve drivers, our project will focus on the two main drivers for each team.
- A race weekend spans from Friday to Sunday, beginning with two practice sessions on Friday and a final practice session on Saturday morning. These practice sessions do not contribute to race points, so they won’t be covered in our analysis.
- The qualifying session takes place on Saturday afternoon and consists of three stages. The results of qualifying determine the starting grid for the race—the better the qualifying result, the higher the starting position.
- Unlike qualifying, which is a single-lap shootout, races consist of 50 to 70 laps, depending on the circuit length.
- Drivers make pit stops during the race for tire changes or to repair damaged cars.
- Race results determine the standings for both drivers and constructors. The driver with the most points at the end of the season wins the Drivers' Championship, while the team with the highest points total is awarded the Constructors' Championship.

## Data Model
![ergast_db](https://github.com/user-attachments/assets/07fcb8a7-e4f4-4f0d-a535-882918382abd)

## Tasks
### Data Ingestion
- Ingest all 8 files into Azure Data Lake.
- Ensure the same schema is applied to all ingested data.
- Include audit columns (e.g., created date, modified date) for data tracking.
- Store the ingested data in a columnar format (such as Parquet).
- Make the ingested data SQL-queryable for analysis.
- The ingestion logic must support incremental loading, ensuring new or updated data is captured over time.

### Data Transformation
- Join key information from different datasets to create a unified table for reporting purposes.
- Join relevant data to create a new table for analysis.
- Transformed tables must include audit columns to track data changes.
- The transformed data should be SQL-queryable to facilitate analysis.
- Store the transformed data in columnar format (e.g., Parquet).
- The transformation logic must handle incremental loading, ensuring that new data can be added or updated as needed.

### Data Analysis
- Identify Dominant Drivers—analyze the performance of drivers to determine who has been the most dominant over the course of the season.
- Identify Dominant Teams—analyze team performance to determine which teams have been the most successful.
- Visualize the Outputs—generate meaningful visualizations to present the data and analysis results.
- Create Databricks Dashboards—design interactive dashboards in Databricks for real-time monitoring and reporting.

## Dominant Drivers Dashboard
Link: https://adb-3243820008933182.2.azuredatabricks.net/editor/notebooks/110534886254747/dashboards/1d36c196-5e5e-4c97-a57a-0ca91019d2ba/present?o=3243820008933182
