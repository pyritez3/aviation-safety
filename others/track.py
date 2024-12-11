import time
import requests
import json

LOG_FILE = "api_logs.json"
with open(LOG_FILE, "a") as f:
        log_entry = {"timestamp": time.time(), "Calls_per_Day": 0,"Calls_per_Minute":0}
        f.write(json.dumps(log_entry) + '\n')


calls_made_m = 0
calls_made_d = 0
start_time = time.time()

API_URL = f"https://api.openweathermap.org/data/2.5/weather?q=Chennai&appid=&units=metric"
MAX_CALLS_PER_MINUTE = 50
MAX_CALLS_PER_DAY = 800


def log_api_call(calls_made_d,calls_made_m):
    with open(LOG_FILE, "a") as f:
        log_entry = {"timestamp": time.time(), "Calls_per_Day": calls_made_d,"Calls_per_Minute":calls_made_m}
        f.write(json.dumps(log_entry) + '\n')
        
def api_call():
    

    while True:
        # Check if a minute has passed
        if time.time() - start_time >= 60:
            calls_made_m = 0
            start_time = time.time()

        # Make API call if within usage limit
        if calls_made_m < MAX_CALLS_PER_MINUTE and calls_made_d < MAX_CALLS_PER_DAY:
            data = requests.get(API_URL)
            data = data.json()
            calls_made_m += 1
            calls_made_d += 1
            print("API call made. Calls made this minute:", calls_made_m)
            log_api_call(calls_made_d,calls_made_m)

        else:
            print("Usage limit reached. Waiting...")
            break
