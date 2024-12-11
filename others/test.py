# # import requests

# # import time

# # res = requests.get('https://api.openweathermap.org/data/2.5/weather?q=Chennai&appid=b227b30a1da87b503d72c82bd500b358&units=metric')
# # res = res.json()
# # print(time.time())
# # print(res)

# import datetime
# import pandas as pd

# # Function to generate timestamps with a fixed interval in descending order
# def generate_timestamps(start_timestamp, count, interval_seconds):
#     timestamps = [start_timestamp]
#     current_timestamp = start_timestamp
#     for _ in range(count - 1):
#         current_timestamp -= datetime.timedelta(seconds=interval_seconds)
#         timestamps.append(current_timestamp)
#     return timestamps

# # Example usage
# start_timestamp = datetime.datetime(2024, 3, 6, 10, 38, 38)  # Starting timestamp
# count = 1000  # Number of timestamps to generate
# interval_seconds = 60  # Interval between each timestamp in seconds

# timestamps = generate_timestamps(start_timestamp, count, interval_seconds)

# # Create DataFrame
# df = pd.DataFrame({'Timestamp': timestamps})

# # Display the DataFrame
# print(df.head())




import requests


url = "https://timetable-lookup.p.rapidapi.com/TimeTable/MAA/IXM/20240327/"

headers = {
    "X-RapidAPI-Key": "api key",
    "X-RapidAPI-Host": "timetable-lookup.p.rapidapi.com"
}

response = requests.get(url, headers=headers)

print(response.text)






