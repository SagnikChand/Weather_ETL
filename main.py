from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
import psycopg2
import requests
import os
import pandas as pd
from sqlalchemy import create_engine


@task
def fetch_and_insert_data():
    # Database Connection Details
 db_config = {
        "dbname": "weather_db",
        "user": "postgres",
        "password": "261819",
        "host": "localhost",
        "port": "5432"
    }

# API Key
API_KEY = '3154ef71a5f2bf8b3faa9121f09a1df0'

# List of Top 20 Economically Significant Cities
cities = [
    {"city": "New York", "lat": 40.7128, "lon": -74.0060},
    {"city": "London", "lat": 51.5074, "lon": -0.1278},
    {"city": "Tokyo", "lat": 35.6895, "lon": 139.6917},
    {"city": "Hong Kong", "lat": 22.3193, "lon": 114.1694},
    {"city": "Singapore", "lat": 1.3521, "lon": 103.8198},
    {"city": "Shanghai", "lat": 31.2304, "lon": 121.4737},
    {"city": "Paris", "lat": 48.8566, "lon": 2.3522},
    {"city": "Frankfurt", "lat": 50.1109, "lon": 8.6821},
    {"city": "Beijing", "lat": 39.9042, "lon": 116.4074},
    {"city": "San Francisco", "lat": 37.7749, "lon": -122.4194},
    {"city": "Dubai", "lat": 25.276987, "lon": 55.296249},
    {"city": "Los Angeles", "lat": 34.0522, "lon": -118.2437},
    {"city": "Sydney", "lat": -33.8688, "lon": 151.2093},
    {"city": "Chicago", "lat": 41.8781, "lon": -87.6298},
    {"city": "Seoul", "lat": 37.5665, "lon": 126.9780},
    {"city": "Toronto", "lat": 43.6532, "lon": -79.3832},
    {"city": "Mumbai", "lat": 19.0760, "lon": 72.8777},
    {"city": "Zurich", "lat": 47.3769, "lon": 8.5417},
    {"city": "Amsterdam", "lat": 52.3676, "lon": 4.9041},
    {"city": "Riyadh", "lat": 24.7136, "lon": 46.6753}
]

@task
def fetch_weather_data():
    """Fetch weather data for all cities in a single step."""
    logger = get_run_logger()
    all_weather_data = []
    for city in cities:
        try:
            url = f"https://api.openweathermap.org/data/2.5/weather?lat={city['lat']}&lon={city['lon']}&appid={API_KEY}&units=metric"
            response = requests.get(url)
            response.raise_for_status()
            weather_data = response.json()
            all_weather_data.append(weather_data)
            logger.info(f"Fetched weather data for {city['city']}.")
        except Exception as e:
            logger.error(f"Failed to fetch data for {city['city']}: {e}")
    return all_weather_data

@task
def parse_data(all_data):
    """Parse the weather API responses."""
    parsed_data_list = []
    for data in all_data:
        parsed_data = {
            "longitude": data["coord"].get("lon"),
        "latitude": data["coord"].get("lat"),
        "weather_id": data["weather"][0].get("id", 0),
        "weather_main": data["weather"][0].get("main", ""),
        "weather_description": data["weather"][0].get("description", ""),
        "weather_icon": data["weather"][0].get("icon", ""),
        "base": data.get("base", ""),
        "temp": data["main"].get("temp"),
        "feels_like": data["main"].get("feels_like"),
        "temp_min": data["main"].get("temp_min"),
        "temp_max": data["main"].get("temp_max"),
        "pressure": data["main"].get("pressure"),
        "humidity": data["main"].get("humidity"),
        "sea_level": data["main"].get("sea_level"),
        "grnd_level": data["main"].get("grnd_level"),
        "visibility": data.get("visibility"),
        "wind_speed": data["wind"].get("speed"),
        "wind_deg": data["wind"].get("deg"),
        "wind_gust": data["wind"].get("gust"),
        "rain_1h": data.get("rain", {}).get("1h", 0),
        "clouds_all": data["clouds"].get("all"),
        "dt": data.get("dt"),
        "sys_type": data["sys"].get("type"),
        "sys_id": data["sys"].get("id"),
        "country": data["sys"].get("country", ""),
        "sunrise": data["sys"].get("sunrise"),
        "sunset": data["sys"].get("sunset"),
        "timezone": data.get("timezone"),
        "location_id": data.get("id"),
        "city_name": data.get("name", ""),
        "cod": data.get("cod", 0)
        }
        parsed_data_list.append(parsed_data)
    return parsed_data_list

@task
def insert_data(parsed_data_list):
    logger = get_run_logger()
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        query = """
INSERT INTO weather_data (
    latitude, longitude, city_name, dt, temp, feels_like, temp_min,
    temp_max, pressure, humidity, visibility, wind_speed, wind_deg,
    wind_gust, rain_1h, clouds_all, country, sunrise, sunset, timezone
)
VALUES (
    %(latitude)s, %(longitude)s, %(city_name)s, %(dt)s, %(temp)s,
    %(feels_like)s, %(temp_min)s, %(temp_max)s, %(pressure)s,
    %(humidity)s, %(visibility)s, %(wind_speed)s, %(wind_deg)s,
    %(wind_gust)s, %(rain_1h)s, %(clouds_all)s, %(country)s,
    %(sunrise)s, %(sunset)s, %(timezone)s
)
ON CONFLICT (latitude, longitude, dt) DO UPDATE
SET
    temp = EXCLUDED.temp,
    feels_like = EXCLUDED.feels_like,
    temp_min = EXCLUDED.temp_min,
    temp_max = EXCLUDED.temp_max,
    pressure = EXCLUDED.pressure,
    humidity = EXCLUDED.humidity,
    visibility = EXCLUDED.visibility,
    wind_speed = EXCLUDED.wind_speed,
    wind_deg = EXCLUDED.wind_deg,
    wind_gust = EXCLUDED.wind_gust,
    rain_1h = EXCLUDED.rain_1h,
    clouds_all = EXCLUDED.clouds_all,
    country = EXCLUDED.country,
    sunrise = EXCLUDED.sunrise,
    sunset = EXCLUDED.sunset,
    timezone = EXCLUDED.timezone;
"""
        for parsed_data in parsed_data_list:
            cursor.execute(query, parsed_data)
        conn.commit()
        logger.info("All data inserted successfully!")
    except Exception as e:
        logger.error(f"Error inserting data: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@flow
def extract_etl():
    all_data = fetch_weather_data()
    parsed_data_list = parse_data(all_data)
    insert_data(parsed_data_list)

if __name__ == "__main__":
    extract_etl()
    pass


@task
def read_transform_and_save_data():
    
 @task
def fetch_data():
    # Update with your database credentials
        DATABASE_URI = "postgresql+psycopg2://postgres:261819@localhost:5432/weather_db"
        query = "SELECT * FROM weather_data"
    
        engine = create_engine(DATABASE_URI)
        df = pd.read_sql(query, engine)
    
        return df


@task
def transform_data(df):
    df = df[['longitude', 'latitude', 'temp', 'feels_like', 'temp_min', 'temp_max', 
             'pressure', 'humidity', 'visibility', 
             'wind_speed', 'wind_deg', 'clouds_all', 'dt', 'country',
             'sunrise', 'sunset', 'timezone', 'city_name']].copy()
    
    # Converting temperature from Kelvin to Celsius
    df['temp_celsius'] = df['temp'] - 273.15
    df['feels_like_celsius'] = df['feels_like'] - 273.15
    df['temp_min_celsius'] = df['temp_min'] - 273.15
    df['temp_max_celsius'] = df['temp_max'] - 273.15
    
    # Converting wind speed from m/s to km/h
    df['wind_speed_kmph'] = df['wind_speed'] * 3.6
    
    # Converting UNIX timestamps to readable datetime
    df['timestamp'] = pd.to_datetime(df['dt'], unit='s')
    df['sunrise_time'] = pd.to_datetime(df['sunrise'], unit='s')
    df['sunset_time'] = pd.to_datetime(df['sunset'], unit='s')
    
    # Adding additional derived columns
    df['day'] = df['timestamp'].dt.date  # Extracting date
    df['hour'] = df['timestamp'].dt.hour  # Extracting hour
    df['day_of_week'] = df['timestamp'].dt.day_name()  # Extracting weekday name
    
    # Dropping unnecessary columns and handling missing values
    df = df.dropna()  # Remove rows with null values (or you can use an imputation strategy)
    
    # Aggregating data (example: Average weather conditions per city per day)
    aggregated_df = df.groupby(['city_name', 'day'], as_index=False).agg({
        'temp_celsius': 'mean',
        'feels_like_celsius': 'mean',
        'temp_min_celsius': 'mean',
        'temp_max_celsius': 'mean',
        'pressure': 'mean',
        'humidity': 'mean',
        'wind_speed_kmph': 'mean',
        'clouds_all': 'mean',
        'visibility': 'mean'
    }).rename(columns={
        'temp_celsius': 'avg_temp_celsius',
        'feels_like_celsius': 'avg_feels_like_celsius',
        'temp_min_celsius': 'avg_temp_min_celsius',
        'temp_max_celsius': 'avg_temp_max_celsius',
        'pressure': 'avg_pressure',
        'humidity': 'avg_humidity',
        'wind_speed_kmph': 'avg_wind_speed_kmph',
        'clouds_all': 'avg_cloud_coverage',
        'visibility': 'avg_visibility'
    })
    
    return aggregated_df


@task
def update_postgres(df):
    DATABASE_URI = "postgresql+psycopg2://postgres:261819@localhost:5432/weather_db"
    engine = create_engine(DATABASE_URI)
    df.to_sql('transformed_weather_data', engine, if_exists='replace', index=False)


@flow
def transform_load():
    df = fetch_data()
    df_transformed = transform_data(df)
    update_postgres(df_transformed)

if __name__ == "__main__":
    transform_load()
    pass

# Define the Prefect Flow

@flow(schedule="0 6,18 * * *")  # Runs twice a day: 6:00 AM and 6:00 PM
def weather_pipeline():
    fetch_and_insert_data()
    read_transform_and_save_data()

if __name__ == "__main__":
    weather_pipeline()