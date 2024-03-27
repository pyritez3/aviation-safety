import datetime
import subprocess
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import time




def weather_data():
    
    subprocess.run(["python", "dags\\weather_kafkaProducer.py"])

    proc = subprocess.Popen(["python", "dags\\kafkaConsumer_Cassandra.py"])
    time.sleep(10)
    proc.terminate()

weather_data()

""""
def predict():
    subprocess.run(["python", "dags\\flight_csv.py"])
    subprocess.run(["python", "dags\\Flight_csv_cassandra.py"])

    #Connect to Cassandra cluster
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('flight_data')

    # Get current time
    current_time = datetime.now()

    # Calculate one hour from now
    one_hour_later = current_time + timedelta(hours=1)
    # print("One hour later:", one_hour_later)

    # Format one hour later in the required format for Cassandra query
    one_hour_later_str = one_hour_later.strftime('%Y-%m-%dT%H:%M:%S')

    # Execute Cassandra query to retrieve flights scheduled to depart within the next one hour 
    departure_query = f"SELECT departurename, departuredatetime, arrivalname, arrivaldatetime FROM flight_table WHERE departuredatetime = '{one_hour_later_str}' ALLOW FILTERING"
    departure_result_set = session.execute(departure_query)

    # Execute Cassandra query to retrieve flights scheduled to arrive within the next one hour 
    arrival_query = f"SELECT departurename, departuredatetime, arrivalname, arrivaldatetime FROM flight_table WHERE arrivaldatetime = '{one_hour_later_str}' ALLOW FILTERING"
    arrival_result_set = session.execute(arrival_query)

    # Combine departure and arrival flights into a single list
    combined_result_set = []

    # Process departure flights
    for row in departure_result_set:
        combined_result_set.append((row.departurename, row.departuredatetime, "Departure"))

    # Process arrival flights
    for row in arrival_result_set:
        combined_result_set.append((row.arrivalname, row.arrivaldatetime, "Arrival"))

    # print("Combined result set:", combined_result_set)

    if combined_result_set:
        for name, time, flight_type in combined_result_set:
            if time == one_hour_later:
                if flight_type == "Departure":
                    print(f"Flight departing from {name} at {time}")
                elif flight_type == "Arrival":
                    print(f"Flight arriving at {name} at {time}")
    else:
        print("No data found")

    # Close Cassandra session and cluster connection
    session.shutdown()
    cluster.shutdown()


predict()

"""