import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
from cassandra.cluster import Cluster
import tkinter as tk
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg


cluster = Cluster(['127.0.0.1'])
session = cluster.connect('weather_data')

# Function to retrieve historical data
def get_historical_data():
    result = session.execute("SELECT * FROM hist_data")
    data = [(row.time, row.temp, row.humidity, row.pressure, row.wind_speed) for row in result]
    df = pd.DataFrame(data, columns=['time', 'temperature', 'humidity', 'pressure', 'wind_speed'])
    return df.sort_values(by='time')


# Function to retrieve real-time data
def get_realtime_data():
    result = session.execute("SELECT * FROM weather_table")
    data = [(row.time, row.temp, row.humidity, row.pressure, row.wind_speed) for row in result]
    df = pd.DataFrame(data, columns=['time', 'temperature', 'humidity', 'pressure', 'wind_speed'])
    return df.sort_values(by='time')


def preprocess_data(df):
    # Convert timestamp to datetime
    df['time'] = pd.to_datetime(df['time'])
    df.set_index('time', inplace=True)
    df = df.dropna()
    return df

def forecast_weather(historical_data, realtime_data):
    predictions = {}
    variables = ['temperature', 'humidity', 'pressure', 'wind_speed']
    
    for var in variables:
        data = pd.concat([historical_data[var], realtime_data[var]])
        data = data.resample('min').mean()
        
        last_timestamp = data.index[-1]
        
        forecast_timestamps = pd.date_range(start=last_timestamp, periods=30, freq='60s')
        
        
        model = ARIMA(data, order=(5,1,0))
        model_fit = model.fit()
        predictions[var] = model_fit.forecast(steps=30, index=forecast_timestamps)
    
    return predictions

def check_safety(predictions):
    safety_boundaries = {
        'temperature': (10, 30),
        'humidity': (30, 70),
        'pressure': (900, 1100),
        'wind_speed': (0, 20),
    }
    
    safety_messages = {}
    for var, values in predictions.items():
        last_10_values = values[-10:]
        safety_messages[var] = []
        if var in safety_boundaries and safety_boundaries[var] is not None:
            lower_bound, upper_bound = safety_boundaries[var]
            for value in last_10_values:
                if lower_bound is not None and value < lower_bound:
                    safety_messages[var].append('below')
                elif upper_bound is not None and value > upper_bound:
                    safety_messages[var].append('above')
                else:
                    safety_messages[var].append('safe')
        else:
            safety_messages[var] = ['No specific safety check for this variable'] * len(last_10_values)
    return safety_messages


def main():
    historical_data = get_historical_data()
    historical_data = preprocess_data(historical_data)
    realtime_data = get_realtime_data()
    realtime_data = preprocess_data(realtime_data)
    predictions = forecast_weather(historical_data, realtime_data)
    safety_messages = check_safety(predictions)
    root = tk.Tk()
    root.title("Weather Dashboard")

    frame = tk.Frame(root)
    frame.pack()

    row_count = 0
    col_count = 0
    for var, values in predictions.items():
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.plot(values, label=f'Predicted {var.capitalize()}')
        ax.set_xlabel('Time')
        ax.set_ylabel(f'{var.capitalize()}')

        default_color = 'blue'
        safety_colors = [default_color] * len(values)
        
        last_10_values = values[-10:]
        for j, safety in enumerate(safety_messages[var][-10:]):
            if safety == 'above' or safety == 'below':
                safety_colors[-10 + j] = 'red'
            elif safety == 'safe':
                safety_colors[-10 + j] = 'green'
        
        ax.scatter(values.index, values, c=safety_colors, label='Safety Boundary')

        if var in ['temperature', 'humidity', 'pressure', 'wind_speed']:
            lower_bound, upper_bound = (10, 30) if var == 'temperature' else (30, 70) if var == 'humidity' else (900, 1100) if var == 'pressure' else (0, 20)
            ax.axhline(y=lower_bound, color='orange', linestyle='--', label='Lower Safety Boundary')
            ax.axhline(y=upper_bound, color='orange', linestyle='--', label='Upper Safety Boundary')

        ax.legend()

        conclusion = 'Unsafe' if 'above' in safety_messages[var] or 'below' in safety_messages[var] else 'Safe'
        conclusion_label = tk.Label(frame, text=f'{var.capitalize()} Conclusion: {conclusion}')
        conclusion_label.grid(row=row_count, column=col_count, padx=10, pady=10, sticky='w')

        canvas = FigureCanvasTkAgg(fig, master=frame)
        canvas.draw()
        canvas.get_tk_widget().grid(row=row_count, column=col_count + 1, padx=10, pady=10)

        col_count += 2
        if col_count == 4:
            col_count = 0
            row_count += 1

    root.mainloop()

if __name__ == "__main__":
    main()

