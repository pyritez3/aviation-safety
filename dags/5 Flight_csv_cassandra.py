import os
import pandas as pd
from cassandra.cluster import Cluster

# Establish connection to Cassandra
cluster = Cluster(['127.0.0.1'])  # Replace '127.0.0.1' with your Cassandra cluster's IP address
session = cluster.connect()

# Create keyspace and connect to it
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS flight_data
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' }
""")
session.set_keyspace('flight_data')

# Define Cassandra table schema
create_table_query = """
    CREATE TABLE IF NOT EXISTS flight_table (
        TotalFlightTime text,
        TotalMiles text,
        TotalTripTime text,
        DepartureDateTime text,
        DepartureTimeOffset text,
        DepartureCode text,
        DepartureName text,
        ArrivalDateTime text,
        ArrivalTimeOffset text,
        ArrivalCode text,
        ArrivalName text,
        FlightType text,
        FlightLegs text,
        FlightDays text,
        PRIMARY KEY (DepartureDateTime, ArrivalDateTime)
    )
"""
session.execute(create_table_query)

# Read DataFrame from CSV file
file_path = 'dags\\flight_details.csv'
if os.path.isfile(file_path) and os.stat(file_path).st_size != 0:
    df = pd.read_csv(file_path, header=None)  # Skip specifying column names
else:
    print("CSV file is empty or does not exist.")
    exit()

# Insert data into Cassandra table
for index, row in df.iterrows():
    insert_query = """
        INSERT INTO flight_table (
            TotalFlightTime, TotalMiles, TotalTripTime,
            DepartureDateTime, DepartureTimeOffset, DepartureCode, DepartureName,
            ArrivalDateTime, ArrivalTimeOffset, ArrivalCode, ArrivalName,
            FlightType, FlightLegs, FlightDays
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    session.execute(insert_query, (
        str(row[0]), str(row[1]), str(row[2]),
        str(row[3]), str(row[4]), str(row[5]), str(row[6]),
        str(row[7]), str(row[8]), str(row[9]), str(row[10]),
        str(row[11]), str(row[12]), str(row[13])
    ))

print("Data inserted into Cassandra table successfully.")
