## Project Overview
An automated, end-to-end Data Engineering pipeline that extracts live daily meteorological data from a REST API, transforms it using distributed cloud compute, and loads it into a serverless PostgreSQL data warehouse. 
This project demonstrates core Data Engineering principles including API integration, PySpark schema enforcement, handling strict SQL data types, and navigating cloud-native network security restrictions.
## Architecture Diagram
```text
  [Open-Meteo REST API]
           │
           ▼  HTTP GET (JSON)
  +-------------------------------------------------------------+
  |                   Databricks Workspace                      |
  |                                                             |
  |  1. Extraction (Python/Requests)                            |
  |     - Parse nested JSON arrays for target cities.           |
  |                                                             |
  |  2. Transformation (PySpark)                                |
  |     - Distributed memory processing.                        |
  |     - Explicit type casting (String to Date).               |
  |     - Handling Null/Missing values (.na.fill).              |
  |                                                             |
  |  3. The Workaround: Driver Node Fallback (Pandas)           |
  |     - Bypass Serverless DML constraints for external loads. |
  +-------------------------------------------------------------+
           │
           ▼  SQLAlchemy / psycopg2 (TCP/IP via SSL)
  [Neon Serverless PostgreSQL]
           │
           ▼  SQL Analytics (CTEs, Window Functions)
  [Downstream Analytics & BI]
Tech Stack
	• Language: Python, SQL
	• Compute & Orchestration: Databricks (Serverless Compute, Databricks Workflows)
	• Data Processing: PySpark, Pandas
	• Database: Neon (Serverless Cloud PostgreSQL)
	• API: Open-Meteo REST API
Pipeline Breakdown
1. Extraction (E)
The pipeline is triggered daily at 08:00 AM IST via Databricks Workflows. It connects to the Open-Meteo API using Python's requests library to fetch the previous day's weather data (max/min temperatures, precipitation, wind speed, sunrise/sunset) for target cities.
2. Transformation (T)
Raw JSON payloads are loaded into a PySpark DataFrame.
	• Data Quality: Explicit schema enforcement is applied. String dates are cast to native Date objects using to_date() to ensure downstream SQL Window Functions execute correctly.
	• Imputation: Null values for precipitation and wind speed are handled programmatically.
3. Loading (L) & Infrastructure Problem Solving
The Challenge: Databricks Serverless compute restricts external Data Manipulation Language (DML) operations (like JDBC INSERT statements) for security and performance reasons.
The Solution: I engineered a driver-node fallback solution. At the final stage, the PySpark DataFrame is collected back to standard Python memory (toPandas()) and routed securely into the Neon PostgreSQL database via SQLAlchemy and psycopg2 requiring SSL connections.
Downstream Analytics
Once the data lands in PostgreSQL, it is ready for analytical queries. Example SQL operations used on this dataset include:
	• CTEs to logically stage calculations.
	• Window Functions (AVG() OVER(PARTITION BY... ROWS BETWEEN)) to calculate 7-day rolling averages for temperatures.
	• Explicit type casting (::numeric) to handle strict PostgreSQL arithmetic functions.
Future Enhancements
	• [ ] Integrate Air Quality Index (AQI) data via additional API endpoints to model correlations between temperature and pollution.
	• [ ] Migrate local SQL transformations into dbt (Data Build Tool) for modular data modeling.
	• [ ] Connect a Looker Studio or Metabase BI dashboard directly to the Neon database for live visualization.
