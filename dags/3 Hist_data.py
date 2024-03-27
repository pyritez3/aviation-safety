from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import random
import uuid

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('weather_data')

temperature_mean = 30  # Mean temperature for Chennai
temperature_variation = 5  # Variation around the mean temperature
temperature_min = 25
temperature_max = 35

humidity_min = 60
humidity_max = 90

def generate_historical_data(start_time, num_entries):
    historical_data = []
    for i in range(num_entries):
        timestamp = start_time - timedelta(minutes=i)
        temperature = random.normalvariate(temperature_mean, temperature_variation)
        temperature = max(temperature_min, min(temperature_max, temperature))  # Ensure temperature is within range
        humidity = random.randint(humidity_min, humidity_max)
        
        id_value = uuid.uuid4()
        
        historical_data.append((id_value, timestamp, temperature, temperature, temperature, temperature, random.randint(1000, 1010), humidity, random.randint(5000, 6000), random.uniform(0, 10), random.randint(0, 360), random.uniform(0, 10), "Chennai"))
    return historical_data

session.execute("""
   CREATE TABLE IF NOT EXISTS hist_data (
        id UUID,
        time TIMESTAMP,
        temp FLOAT,
        feels_like FLOAT,
        temp_min FLOAT,
        temp_max FLOAT,
        pressure INT,
        humidity INT,
        visibility INT,
        wind_speed FLOAT,
        wind_deg INT,
        wind_gust FLOAT,
        name TEXT,
        PRIMARY KEY ((id), time)
    ) WITH CLUSTERING ORDER BY (time DESC);
""")

# Use a default start time if no data exists
start_time = datetime.utcnow()

num_entries = 50

historical_data = generate_historical_data(start_time, num_entries)

for i, data in enumerate(historical_data):
    session.execute("""
        INSERT INTO hist_data (id, time, temp, feels_like, temp_min, temp_max, pressure, humidity, visibility, wind_speed, wind_deg, wind_gust, name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, data)
    print(f"Inserted entry {i+1} of {num_entries}")

print("Historical data inserted successfully into the hist_data table.")
