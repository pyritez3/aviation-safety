


import requests


url = "https://timetable-lookup.p.rapidapi.com/TimeTable/MAA/IXM/20240327/"

headers = {
    "X-RapidAPI-Key": "api key",
    "X-RapidAPI-Host": "timetable-lookup.p.rapidapi.com"
}

response = requests.get(url, headers=headers)

print(response.text)






