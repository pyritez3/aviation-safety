import os
import requests
import xml.etree.ElementTree as ET
import pandas as pd

url = "https://timetable-lookup.p.rapidapi.com/TimeTable/MAA/IXM/20240327/"

headers = {
    "X-RapidAPI-Key": "API KEY",
    "X-RapidAPI-Host": "timetable-lookup.p.rapidapi.com"
}

response = requests.get(url, headers=headers)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    try:
        root = ET.fromstring(response.text)

        flight_details = []

        for flight_detail in root.findall('.//{http://www.opentravel.org/OTA/2003/05}FlightDetails'):
            flight_dict = {
                'TotalFlightTime': flight_detail.get('TotalFlightTime'),
                'TotalMiles': flight_detail.get('TotalMiles'),
                'TotalTripTime': flight_detail.get('TotalTripTime'),
                'DepartureDateTime': flight_detail.get('FLSDepartureDateTime'),
                'DepartureTimeOffset': flight_detail.get('FLSDepartureTimeOffset'),
                'DepartureCode': flight_detail.get('FLSDepartureCode'),
                'DepartureName': flight_detail.get('FLSDepartureName'),
                'ArrivalDateTime': flight_detail.get('FLSArrivalDateTime'),
                'ArrivalTimeOffset': flight_detail.get('FLSArrivalTimeOffset'),
                'ArrivalCode': flight_detail.get('FLSArrivalCode'),
                'ArrivalName': flight_detail.get('FLSArrivalName'),
                'FlightType': flight_detail.get('FLSFlightType'),
                'FlightLegs': flight_detail.get('FLSFlightLegs'),
                'FlightDays': flight_detail.get('FLSFlightDays'),
            }
            flight_details.append(flight_dict)

        df = pd.DataFrame(flight_details)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)

        file_path = 'dags\\flight_details.csv'
        if os.path.isfile(file_path) and os.stat(file_path).st_size != 0:
            df.to_csv(file_path, mode='a', index=False, header=False)
            print("Data appended to CSV file successfully.")
        else:
            df.to_csv(file_path, index=False)
            print("Data written to new CSV file successfully.")

    except Exception as e:
        print("Error:", e)
else:
    print("Request failed with status code:", response.status_code)
