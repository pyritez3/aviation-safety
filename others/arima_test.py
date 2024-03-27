import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA
from cassandra.cluster import Cluster

# Cassandra connection setup
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('weather_data')

# Function to retrieve historical data from hist_data table
def get_historical_data():
    result = session.execute("SELECT * FROM hist_data")
    data = [(row.time, row.temp, row.humidity, row.pressure, row.wind_speed, row.wind_deg) for row in result]
    df = pd.DataFrame(data, columns=['time', 'temperature', 'humidity', 'pressure', 'wind_speed', 'wind_deg'])
    return df.sort_values(by='time')


# print(get_historical_data())
# Function to retrieve real-time data from weather_table
def get_realtime_data():
    result = session.execute("SELECT * FROM weather_table")
    data = [(row.time, row.temp, row.humidity, row.pressure, row.wind_speed, row.wind_deg) for row in result]
    df = pd.DataFrame(data, columns=['time', 'temperature', 'humidity', 'pressure', 'wind_speed', 'wind_deg'])
    return df.sort_values(by='time')

# Function to preprocess the data
def preprocess_data(df):
    # Convert timestamp to datetime
    df['time'] = pd.to_datetime(df['time'])
    # Set time as index
    df.set_index('time', inplace=True)
    # Handle missing values
    df = df.dropna()
    return df

# Function to train ARIMA models and make predictions for all variables
def forecast_weather(historical_data, realtime_data):
    predictions = {}
    variables = ['temperature', 'humidity', 'pressure', 'wind_speed', 'wind_deg']
    
    for var in variables:
        # Combine historical and real-time data
        data = pd.concat([historical_data[var], realtime_data[var]])
        # Resample data to minute frequency
        data = data.resample('min').mean()
        
        # Get the last timestamp in historical data
        last_timestamp = data.index[-1]
        
        # Add 30 minutes to the last timestamp
        forecast_timestamps = pd.date_range(start=last_timestamp, periods=60, freq='60s')
        
        # Train ARIMA model
        model = ARIMA(data, order=(5,1,0))
        model_fit = model.fit()
        # Make predictions
        predictions[var] = model_fit.forecast(steps=60, index=forecast_timestamps)
    
    return predictions

# Function to plot weather forecasts for all variables
# Function to plot weather forecasts for all variables in a 3x2 grid
# Function to plot weather forecasts for all variables in a 3x2 grid
# Function to plot weather forecasts for all variables in a 3x2 grid
def plot_weather_forecast(predictions, historical_data, realtime_data):
    variables = ['temperature', 'humidity', 'pressure', 'wind_speed', 'wind_deg']
    num_plots = len(variables)
    
    fig, axes = plt.subplots(3, 2, figsize=(15, 10))
    
    for i, var in enumerate(variables):
        row = i // 2
        col = i % 2
        ax = axes[row, col]
        
        # Plot historical data
        ax.plot(historical_data.index, historical_data[var], label=f'Historical {var.capitalize()}', color='gray', linestyle='-')
        
        # Plot ARIMA predictions
        ax.plot(predictions[var].index, predictions[var].values, label=f'Predicted {var.capitalize()}', color='blue')
        
        ax.set_xlabel('Time')
        ax.set_ylabel(f'{var.capitalize()}')
        ax.legend()
        
        # Plot safety boundary range (if required)
        if var == 'temperature':
            min_temperature = min(historical_data['temperature'].min(), realtime_data['temperature'].min())  # Minimum temperature requirement for flight
            max_temperature = max(historical_data['temperature'].max(), realtime_data['temperature'].max())  # Maximum temperature requirement for flight
            ax.axhline(y=min_temperature, color='red', linestyle='-', label='Min Temperature for Flight')
            ax.axhline(y=max_temperature, color='green', linestyle='-', label='Max Temperature for Flight')
        elif var == 'humidity':
            min_humidity = min(historical_data['humidity'].min(), realtime_data['humidity'].min())  # Minimum humidity requirement for flight
            max_humidity = max(historical_data['humidity'].max(), realtime_data['humidity'].max())  # Maximum humidity requirement for flight
            ax.axhline(y=min_humidity, color='red', linestyle='-', label='Min Humidity for Flight')
            ax.axhline(y=max_humidity, color='green', linestyle='-', label='Max Humidity for Flight')
        elif var == 'pressure':
            min_pressure = min(historical_data['pressure'].min(), realtime_data['pressure'].min())  # Minimum pressure requirement for flight
            max_pressure = max(historical_data['pressure'].max(), realtime_data['pressure'].max())  # Maximum pressure requirement for flight
            ax.axhline(y=min_pressure, color='red', linestyle='-', label='Min Pressure for Flight')
            ax.axhline(y=max_pressure, color='green', linestyle='-', label='Max Pressure for Flight')
        elif var == 'wind_speed':
            max_wind_speed = max(historical_data['wind_speed'].max(), realtime_data['wind_speed'].max())  # Maximum wind speed requirement for flight
            ax.axhline(y=max_wind_speed, color='green', linestyle='-', label='Max Wind Speed for Flight')
        # elif var == 'wind_deg':
        #     # No safety line for wind degree
            
        ax.legend()
    
    plt.suptitle('Weather Forecast for Various Variables')
    plt.tight_layout()
    plt.show()




# Main function
def main():
    # Retrieve historical data
    historical_data = get_historical_data()
    historical_data = preprocess_data(historical_data)
    # Retrieve real-time data
    realtime_data = get_realtime_data()
    realtime_data = preprocess_data(realtime_data)
    # Make predictions
    predictions = forecast_weather(historical_data, realtime_data)
    # Plot weather forecasts
    plot_weather_forecast(predictions, historical_data, realtime_data)

if __name__ == "__main__":
    main()

