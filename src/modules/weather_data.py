import requests
import pandas as pd
from datetime import datetime, timedelta
import time

# 1. Define your parameters
latitude = 51.458786  
longitude = -0.971828
start_date = datetime(2024, 12, 17)
end_date = datetime(2025, 12, 18)
# List of hourly variables you want. See full list in docs.
hourly_variables = ["temperature_2m", "precipitation", "rain", "snowfall", "snow_depth", "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high"]

# 2. Construct the API URL
url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": latitude,
    "longitude": longitude,
    "start_date": start_date,
    "end_date": end_date,
    "hourly": ",".join(hourly_variables),
    "timezone": "auto", # Automatically adjusts timestamps to local time.
    "wind_speed_unit": "mph"
}

# 3. Make the API request
print("Downloading weather data...")
batch_size = 7  # Number of days per batch
all_weather_data = []

current_date = start_date
while current_date <= end_date:
    # Calculate end of this batch
    batch_end = min(current_date + timedelta(days=batch_size - 1), end_date)
    params["start_date"] = current_date.strftime("%Y-%m-%d")
    params["end_date"] = batch_end.strftime("%Y-%m-%d")

    params = {
    "latitude": latitude,
    "longitude": longitude,
    "start_date": current_date.strftime("%Y-%m-%d"),
    "end_date": batch_end.strftime("%Y-%m-%d"),
    "hourly": ",".join(hourly_variables),
    "timezone": "auto", # Automatically adjusts timestamps to local time.
    "wind_speed_unit": "mph"
    }

    print(f"Downloading {current_date.date()} to {batch_end.date()}...")

    try:
        response = requests.get(url, params=params, timeout=180)  # Set a timeout for the request
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()  # Assuming the response is JSON
        data = pd.DataFrame(data=data["hourly"])
        data['Reading_date'] = pd.to_datetime(data['time']).dt.normalize()
        data['hour_of_day'] = pd.to_datetime(data['time']).dt.hour
        all_weather_data.append(data)
    except requests.RequestException as e:
        print(f"Error downloading data for {current_date.date()} to {batch_end.date()}: {e}")

    current_date = batch_end + timedelta(days=1)  # Move to the next batch  

    if current_date <= end_date:
        time.sleep(1)  # 1 second pause

# Combine all batches
if all_weather_data:
    weather_data = pd.concat(all_weather_data, ignore_index=True)
    print(f"Total: {len(weather_data)} weather records")
else:
    weather_data = pd.DataFrame()
    print("No data downloaded")