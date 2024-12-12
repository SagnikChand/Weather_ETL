# Weather Data Pipeline
This repository houses my weather data pipeline, along with it's required scripts and files.

## Overview

This project automates the process of fetching weather data from the OpenWeather API for the top 20 financial capitals, transforming the data, and storing it in a PostgreSQL database. The transformed data is then displayed on a Tableau dashboard for visualization. The entire pipeline is managed and automated using Prefect to ensure smooth execution and maintainability.

## Data Source

- **Source**: OpenWeather API
- **Data**: Weather data for the top 20 financial capitals around the world.
- **Fields**: Includes weather conditions, temperature, humidity, wind speed, and other related data.
  
The data is fetched from the API and processed regularly to provide up-to-date weather statistics for these cities.

## Transformation Steps

1. **Data Extraction**: The data is retrieved from the OpenWeather API. The relevant weather information for the top 20 financial capitals is collected.
2. **Data Cleaning**: Any missing or invalid data is handled. The data is cleaned to remove irrelevant or incorrect entries.
3. **Aggregation**: The data is aggregated to provide relevant statistics such as average temperature, wind speed, and humidity over specific time intervals.
4. **Data Transformation**: The raw data is transformed into a more useful format:
   - Temperature is converted from Kelvin to Celsius.
   - Wind speed is converted from meters per second to kilometers per hour.
   - Timestamps are converted to a human-readable date format.
5. **Storage**: The transformed data is stored in a PostgreSQL database for easy retrieval and further analysis.

## Destination of Data

- **PostgreSQL Database**: The cleaned and aggregated data is stored in a PostgreSQL database, ensuring secure and scalable storage. The database is hosted locally for development purposes.
- **Tableau**: The transformed data is loaded into Tableau for interactive visualizations. This enables users to monitor weather patterns and trends across the top financial capitals in real-time.

## Automation with Prefect

- **Prefect Workflow**: The entire pipeline is automated using **Prefect**. 
  - **Fetching Data**: Data is retrieved from the OpenWeather API at regular intervals.
  - **Transformation**: The fetched data is cleaned, transformed, and stored in the PostgreSQL database.
  - **Scheduling**: The Prefect flow is scheduled to run twice daily, ensuring that the data is regularly updated.
  - **Error Handling**: Prefect ensures that if any part of the flow fails, appropriate actions (e.g., retries or notifications) are taken to maintain the flow's reliability.

### Directory Structure

```bash
.
├── data_pipeline/
│   ├── Extract.py         # Fetch data from OpenWeather API
│   ├── Transform.py     # Data transformation logic
│   ├── main.py       # Prefect flow to orchestrate tasks
├── requirements.txt          # List of Python dependencies
└── README.md                 # Project overview and documentation
```

### Requirements

To run the project, install the required dependencies:

```bash
pip install -r requirements.txt
```

