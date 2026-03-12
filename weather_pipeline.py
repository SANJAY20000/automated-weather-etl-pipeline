# Databricks notebook source
# MAGIC %pip install psycopg2-binary sqlalchemy

# COMMAND ----------

import requests
from datetime import datetime, timedelta
from pyspark.sql.functions import col, to_date,to_timestamp

# --- STEP 1: EXTRACTION SETUP ---
# Pulling data for your selected cities
cities = {
    "Varanasi": {"lat": 25.3167, "lon": 83.0104},
    "Lucknow": {"lat": 26.8467, "lon": 80.9462},
    "Delhi": {"lat": 28.6139, "lon": 77.2090}
}
yesterday = datetime.now() - timedelta(days=1)
target_date = yesterday.strftime('%Y-%m-%d')
print(f"Starting ETL Extraction for date: {target_date}...\n")
weather_data_list = []
for city, coords in cities.items():
    lat = coords["lat"]
    lon = coords["lon"]
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=weather_code,temperature_2m_max,temperature_2m_min,sunrise,sunset,precipitation_sum,wind_speed_10m_max,daylight_duration&timezone=Asia%2FKolkata&start_date={target_date}&end_date={target_date}"
    
    try:
        response = requests.get(url)
        response.raise_for_status() 
        data = response.json()
        daily_data = data.get("daily", {})
        
        city_record = {
            "city": city,
            "date": daily_data.get("time", [None])[0],
            "weather_code": daily_data.get("weather_code", [None])[0],
            "temp_max_c": daily_data.get("temperature_2m_max", [None])[0],
            "temp_min_c": daily_data.get("temperature_2m_min", [None])[0],
            "sunrise": daily_data.get("sunrise", [None])[0],
            "sunset": daily_data.get("sunset", [None])[0],
            "precip_sum_mm": daily_data.get("precipitation_sum", [None])[0],
            "wind_speed_max_kmh": daily_data.get("wind_speed_10m_max", [None])[0]
        }
        
        weather_data_list.append(city_record)
        print(f"✅ Extracted: {city}")
        
    except Exception as e:
        print(f"❌ Failed: {city}. Error: {e}")
# --- STEP 2: TRANSFORMATION (PySpark) ---
print("\nTransforming data in Databricks...\n")
# Databricks automatically provides the 'spark' session object
spark_df = spark.createDataFrame(weather_data_list)
# 1. Cast strings to Dates (Crucial for SQL Window Functions later!)
spark_df = spark_df.withColumn("date", to_date(col("date"))) \
                   .withColumn("sunrise", to_timestamp(col("sunrise"))) \
                   .withColumn("sunset", to_timestamp(col("sunset")))
# 2. Handle missing values
spark_df = spark_df.na.fill(value=0, subset=["precip_sum_mm", "wind_speed_max_kmh"])
# Display the clean table in your Databricks Notebook
display(spark_df) 
import pandas as pd
from sqlalchemy import create_engine
# --- STEP 3: LOADING (Bypassing Serverless Restrictions) ---
print("\nLoading data to Neon Cloud PostgreSQL...\n")
# 1. Convert the PySpark DataFrame to a Pandas DataFrame
# (This pulls the data out of Spark's restricted engine into standard Python memory)
pandas_df = spark_df.toPandas()
# 2. Extract credentials from your Neon connection string
neon_host = "ep-lively-forest-ajhsge63-pooler.c-3.us-east-2.aws.neon.tech" 
port = "5432"
database_name = "neondb"
user = "neondb_owner"
password = "npg_lHAKk4uXTU5B"
# 3. Create a standard SQLAlchemy Connection String
# (Notice we keep sslmode=require for Neon's security)
db_url = f"postgresql+psycopg2://{user}:{password}@{neon_host}:{port}/{database_name}?sslmode=require"
try:
    # 4. Create the connection engine
    engine = create_engine(db_url)
    
    # 5. Push the DataFrame directly into the SQL table
    pandas_df.to_sql('daily_weather_recent', engine, if_exists='append', index=False)
    
    print("✅ Successfully loaded data into Neon Cloud PostgreSQL!")
except Exception as e:
    print(f"❌ Database load failed. Error: {e}")
