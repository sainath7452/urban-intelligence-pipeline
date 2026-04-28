# weather_ingestion.py
# Fetches historical weather data for New York City from Open-Meteo API
# and lands it in GCS raw bucket as partitioned JSON files
# Open-Meteo is free, no API key required

import io
import json
import warnings
from datetime import datetime, timedelta

import pandas as pd
import requests
from google.cloud import storage

warnings.filterwarnings("ignore")

# Configuration
PROJECT_ID = "urban-intelligence-pipeline-sv"
RAW_BUCKET = "urban-intelligence-pipeline-sv-raw"
INGESTION_DATE = datetime.now().strftime("%Y-%m-%d")

# New York City coordinates
NYC_LATITUDE = 40.7128
NYC_LONGITUDE = -74.0060
NYC_TIMEZONE = "America/New_York"

# Initialize GCS client
storage_client = storage.Client(project=PROJECT_ID)


def fetch_weather_data(
    start_date: str,
    end_date: str,
    latitude: float = NYC_LATITUDE,
    longitude: float = NYC_LONGITUDE,
) -> dict:
    """
    Fetch historical hourly weather data from Open-Meteo API.
    Free API, no authentication required.
    Returns raw JSON response.
    """
    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_10m",
            "weather_code",
            "visibility",
        ],
        "timezone": NYC_TIMEZONE,
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
    }

    print(f"Fetching weather data from {start_date} to {end_date}...")
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    print(f"Weather data fetched successfully")
    return data


def parse_weather_response(raw_data: dict) -> pd.DataFrame:
    """
    Parse the Open-Meteo API response into a clean DataFrame.
    Each row represents one hour of weather data.
    """
    hourly = raw_data["hourly"]

    df = pd.DataFrame({
        "datetime": pd.to_datetime(hourly["time"]),
        "temperature_f": hourly["temperature_2m"],
        "humidity_pct": hourly["relative_humidity_2m"],
        "precipitation_inch": hourly["precipitation"],
        "wind_speed_mph": hourly["wind_speed_10m"],
        "weather_code": hourly["weather_code"],
        "visibility": hourly["visibility"],
    })

    # Add derived columns useful for joining with taxi data
    df["date"] = df["datetime"].dt.date.astype(str)
    df["hour"] = df["datetime"].dt.hour
    df["year"] = df["datetime"].dt.year
    df["month"] = df["datetime"].dt.month
    df["day"] = df["datetime"].dt.day

    # Add weather condition labels based on WMO weather codes
    df["weather_condition"] = df["weather_code"].apply(classify_weather)

    # Add ingestion metadata
    df["ingestion_date"] = INGESTION_DATE
    df["ingestion_timestamp"] = datetime.now().isoformat()
    df["source_system"] = "open_meteo_api"
    df["latitude"] = raw_data["latitude"]
    df["longitude"] = raw_data["longitude"]

    print(f"Parsed {len(df):,} hourly weather records")
    return df


def classify_weather(code: int) -> str:
    """
    Convert WMO weather interpretation codes to readable labels.
    Reference: https://open-meteo.com/en/docs
    """
    if code == 0:
        return "clear_sky"
    elif code in [1, 2, 3]:
        return "partly_cloudy"
    elif code in [45, 48]:
        return "foggy"
    elif code in [51, 53, 55, 61, 63, 65]:
        return "rainy"
    elif code in [71, 73, 75, 77]:
        return "snowy"
    elif code in [80, 81, 82]:
        return "rain_showers"
    elif code in [95, 96, 99]:
        return "thunderstorm"
    else:
        return "other"


def upload_to_gcs(df: pd.DataFrame, bucket_name: str, gcs_path: str) -> str:
    """
    Upload weather DataFrame to GCS as Parquet.
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    blob.upload_from_file(buffer, content_type="application/octet-stream")
    return f"gs://{bucket_name}/{gcs_path}"


def run_weather_ingestion(start_date: str, end_date: str):
    """
    Main ingestion function — fetch, parse, and land weather data.
    Designed to be called by Airflow DAG in production.
    """
    print(f"Starting weather ingestion")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Ingestion date: {INGESTION_DATE}")

    # Fetch from API
    raw_data = fetch_weather_data(start_date, end_date)

    # Parse into DataFrame
    df = parse_weather_response(raw_data)

    # Land in GCS
    gcs_path = (
        f"weather/ingestion_date={INGESTION_DATE}/"
        f"nyc_weather_{start_date}_{end_date}.parquet"
    )
    uploaded_path = upload_to_gcs(df, RAW_BUCKET, gcs_path)

    print(f"Ingestion complete")
    print(f"Location: {uploaded_path}")
    print(f"Records: {len(df):,}")
    return uploaded_path


if __name__ == "__main__":
    # Fetch January 2022 weather to match our taxi data
    run_weather_ingestion(
        start_date="2022-01-01",
        end_date="2022-01-31"
    )