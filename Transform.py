

from prefect import flow, task
import pandas as pd
from sqlalchemy import create_engine

@task
def fetch_data():
    # Update with your database credentials
    DATABASE_URI = "postgresql+psycopg2://user_name:password@localhost:5432/weather_db"
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
    DATABASE_URI = "postgresql+psycopg2://user_name:password@localhost:5432/weather_db"
    engine = create_engine(DATABASE_URI)
    df.to_sql('transformed_weather_data', engine, if_exists='replace', index=False)


@flow
def transform_load():
    df = fetch_data()
    df_transformed = transform_data(df)
    update_postgres(df_transformed)

if __name__ == "__main__":
    transform_load()











