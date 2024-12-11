import uuid
import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
import requests


def get_data():
  
    API_URL = "https://api.openweathermap.org/data/2.5/weather?q=Chennai&appid=8&units=metric"
    data = requests.get(API_URL)
    data = data.json() 
    return data


def format_data(res):
    data = {}
    data['id'] = str(uuid.uuid4())
    current_time = datetime.now()
    data['Time'] = current_time.strftime("%Y-%m-%d %H:%M:%S")
    data['temp'] = res['main']['temp']
    data['feels_like'] = res['main']['feels_like']
    data['temp_min'] = res['main']['temp_min']
    data['temp_max'] = res['main']['temp_max']
    data['pressure'] = res['main']['pressure']
    data['humidity'] = res['main']['humidity']
    data['visibility'] = res['visibility']
    data['wind'] = res['wind']
    data['name'] = res['name']
    
    
    return data

def stream_data():
    
    producer = KafkaProducer(bootstrap_servers=['broker:9092'],api_version=(0,1,0), max_block_ms=12000)
    curr_time = time.time()

   
    while True:
        if time.time() > curr_time +  120 : #2 minute
            break
        try:
            res = get_data()
            res = format_data(res)
            
            producer.send('weather_data', json.dumps(res).encode('utf-8'))
            time.sleep(60)

            
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

stream_data()
