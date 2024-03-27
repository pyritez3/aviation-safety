import json
import logging
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import uuid

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('weather_data')
session.execute("CREATE KEYSPACE IF NOT EXISTS weather_data WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
session.execute("""
    CREATE TABLE IF NOT EXISTS weather_table (
        id UUID PRIMARY KEY, 
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
        name TEXT
    )
""")

consumer = KafkaConsumer(
    'weather_data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def consume_data_store_in_cassandra():
    try:
        for message in consumer:
            data = message.value
            try:
                id_value = uuid.uuid4()
                session.execute("""
                    INSERT INTO weather_table (id, time, temp, feels_like, temp_min, temp_max, pressure, humidity, visibility, wind_speed, wind_deg, wind_gust, name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    id_value, 
                    data.get('Time'), 
                    data.get('temp'), 
                    data.get('feels_like'), 
                    data.get('temp_min'), 
                    data.get('temp_max'), 
                    data.get('pressure'), 
                    data.get('humidity'), 
                    data.get('visibility'), 
                    data['wind']['speed'],  
                    data['wind']['deg'],    
                    data['wind'].get('gust'),   
                    data.get('name')
                ))
            except Exception as e:
                logging.error(f'An error occurred while storing data in Cassandra: {e}')
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

consume_data_store_in_cassandra()