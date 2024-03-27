from confluent_kafka import Consumer, KafkaError
from cassandra.cluster import Cluster
import json

def write_to_cassandra(data):
    try:
        # Connect to Cassandra cluster
        cluster = Cluster(['localhost'])  # Replace 'localhost' with your Cassandra host
        session = cluster.connect()

        # Specify keyspace
        session.set_keyspace('spark_streams')  # Change 'spark_streams' to your keyspace name

        # Prepare your INSERT statement
        insert_query = """
            INSERT INTO weather_data (
                id, Time, temp, feels_like, temp_min, temp_max, pressure, humidity, visibility, wind, name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Execute INSERT statement
        session.execute(insert_query, (
            data['id'], data['Time'], data['temp'], data['feels_like'],
            data['temp_min'], data['temp_max'], data['pressure'],
            data['humidity'], data['visibility'], data['wind'], data['name']
        ))

        print("Data inserted successfully into Cassandra!")

    except Exception as e:
        print(f"Error inserting data into Cassandra: {e}")

    finally:
        # Close the Cassandra session and cluster connection
        session.shutdown()
        cluster.shutdown()

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker address
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}

def consume_and_process():
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['weather_data'])  # Subscribe to Kafka topic 'weather_data'

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('End of partition reached')
                else:
                    print(f'Kafka error: {msg.error().str()}')
                continue

            # Process the message
            data = json.loads(msg.value())  # Assuming the message value is JSON
            write_to_cassandra(data)

    except KeyboardInterrupt:
        consumer.close()

if __name__ == "__main__":
    consume_and_process()
