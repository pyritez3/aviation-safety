from cassandra.cluster import Cluster

# Cassandra connection setup
cluster = Cluster(['localhost'])
session = cluster.connect()
session.set_keyspace('spark_streams')

# Function to query and print data from Cassandra
def query_and_print_data():
    rows = session.execute("SELECT * FROM weather_data")
    for row in rows:
        print(row)

query_and_print_data()
