import datetime
import pandas as pd
from cassandra.cluster import Cluster
from datetime import timedelta
from datetime import datetime
import random
import uuid
import time





# Function to generate timestamps with a fixed interval in descending order
def generate_timestamps(start_timestamp, count, interval_seconds):
    timestamps = [start_timestamp]
    current_timestamp = start_timestamp
    for _ in range(count - 1):
        current_timestamp -= timedelta(seconds=interval_seconds)
        timestamps.append(current_timestamp)
    return timestamps


# Function to generate historical data based on the starting time
def generate_historical_data(num_entries):
    historical_data = []
    for i in range(num_entries):
        # timestamp = start_time - timedelta(minutes=i)
        temperature = random.normalvariate(temperature_mean, temperature_variation)
        temperature = max(temperature_min, min(temperature_max, temperature))  # Ensure temperature is within range
        humidity = random.randint(humidity_min, humidity_max)
        
        # Generate unique ID for each entry
        id_value = uuid.uuid4()
        
       
        historical_data.append((id_value,  temperature, temperature, temperature, temperature, random.randint(1000, 1010), humidity, random.randint(5000, 6000), random.uniform(0, 10), random.randint(0, 360), random.uniform(0, 10), "Chennai"))
    
    # Define column names for the DataFrame
    
    columns = ['id', 'temp', 'feels_like', 'temp_min', 'temp_max', 'pressure', 'humidity', 'visibility', 'wind_speed', 'wind_deg', 'wind_gust', 'name']
    
    # Create DataFrame from the list of data
    historical_data_df = pd.DataFrame(historical_data, columns=columns)

    return historical_data_df






# Cassandra connection setup
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('weather_data')

# Define the range for temperature data (in Celsius)
temperature_mean = 30  # Mean temperature for Chennai
temperature_variation = 5  # Variation around the mean temperature
temperature_min = 25
temperature_max = 35

# Define the range for humidity data (percentage)
humidity_min = 60
humidity_max = 90

# Get the earliest timestamp from the weather_table
result = session.execute("SELECT MIN(time) FROM weather_table")
start_time = result.one()[0]



# Example usage
start_timestamp = start_time  # Starting timestamp
count = 1000  # Number of timestamps to generate
interval_seconds = 60  # Interval between each timestamp in seconds

timestamps = generate_timestamps(start_timestamp, count, interval_seconds)

# Create DataFrame
df = pd.DataFrame({'time': timestamps})


# Define the number of entries to generate
num_entries = 50

# Generate historical data based on the starting time
historical_data_df = generate_historical_data(num_entries)



# Assuming historical_data_df is already defined

# Combine df with historical_data_df
combined_df = pd.concat([df, historical_data_df], axis=1)

combined_df = combined_df[[combined_df.columns[1], combined_df.columns[0]] + list(combined_df.columns[2:])]

# Display the combined dataframe and modified historical_data_df
print("Combined DataFrame:")
print(combined_df.head())

# print(combined_df.isnull().sum())
print(combined_df.dtypes)

# Create hist_data table if it doesn't exist
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
);

""")

# Sort DataFrame by time
combined_df.sort_values(by='time', inplace=True)

# Insert sorted data into Cassandra
# for _, row in combined_df.iterrows():
#     session.execute("""
#         INSERT INTO hist_data (id, time, temp, feels_like, temp_min, temp_max, pressure, humidity, visibility, wind_speed, wind_deg, wind_gust, name)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#     """, (row['id'], row['time'], row['temp'], row['feels_like'], row['temp_min'], row['temp_max'], row['pressure'], row['humidity'], row['visibility'], row['wind_speed'], row['wind_deg'], row['wind_gust'],row['name']))


import uuid

for i, row in combined_df.iterrows():
    # Convert the id value to a UUID object
    try:
    # Convert the id value to a UUID object
    id_value = uuid.UUID(str(row['id']))
except ValueError:
    print(f"Skipping row {i} due to invalid UUID string:", row['id'])
    continue
    
    # Extract other values from the row
    values = (
        id_value,  # Use the UUID object directly
        row['time'].strftime('%Y-%m-%d %H:%M:%S'), 
        row['temp'], 
        row['feels_like'], 
        row['temp_min'], 
        row['temp_max'], 
        row['pressure'], 
        row['humidity'], 
        row['visibility'], 
        row['wind_speed'], 
        row['wind_deg'], 
        row['wind_gust'], 
        row['name']
    )
    
    # Execute the CQL query with the extracted values
    session.execute("""
        INSERT INTO hist_data (id, time, temp, feels_like, temp_min, temp_max, pressure, humidity, visibility, wind_speed, wind_deg, wind_gust, name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, values)
    
    print(f"Inserted entry {i+1} of {len(combined_df)}")

print("Historical data inserted successfully into the hist_data table.")




