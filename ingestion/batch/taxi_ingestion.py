# taxi_ingestion.py
# Extracts NYC Yellow Taxi data from BigQuery public dataset
# and lands it in GCS raw bucket as partitioned Parquet files

import io
import warnings
from datetime import datetime

import pandas as pd
from google.cloud import bigquery, storage

warnings.filterwarnings("ignore")

# Configuration
PROJECT_ID = "urban-intelligence-pipeline-sv"
RAW_BUCKET = "urban-intelligence-pipeline-sv-raw"
INGESTION_DATE = datetime.now().strftime("%Y-%m-%d")

# Initialize GCP clients
storage_client = storage.Client(project=PROJECT_ID)
bq_client = bigquery.Client(project=PROJECT_ID)


def extract_taxi_data(month: int = 1, year: int = 2022, limit: int = 500000) -> pd.DataFrame:
    """
    Extract NYC Yellow Taxi trip data from BigQuery public dataset.
    Filters out invalid records and adds derived columns for analysis.
    """
    query = f"""
    SELECT
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        rate_code,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        imp_surcharge,
        airport_fee,
        total_amount,
        pickup_location_id,
        dropoff_location_id,
        data_file_year,
        data_file_month,
        DATE(pickup_datetime)                   AS trip_date,
        EXTRACT(HOUR FROM pickup_datetime)      AS pickup_hour,
        EXTRACT(DAYOFWEEK FROM pickup_datetime) AS day_of_week,
        TIMESTAMP_DIFF(
            dropoff_datetime,
            pickup_datetime,
            MINUTE
        )                                       AS trip_duration_minutes
    FROM
        `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_{year}`
    WHERE
        pickup_datetime IS NOT NULL
        AND dropoff_datetime IS NOT NULL
        AND trip_distance > 0
        AND fare_amount > 0
        AND total_amount > 0
        AND passenger_count > 0
        AND passenger_count <= 6
        AND data_file_month = {month}
    LIMIT {limit}
    """

    print(f"Extracting taxi data for {year}-{month:02d}...")
    df = bq_client.query(query).to_dataframe()
    print(f"Extracted {len(df):,} records")
    return df


def clean_taxi_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply basic cleaning rules and add ingestion metadata.
    Raw zone cleaning is minimal — heavy transforms happen in Dataproc.
    """
    # Convert datetime columns
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], utc=True)
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], utc=True)
    df["trip_date"] = pd.to_datetime(df["trip_date"])

    # Remove records that slipped through the SQL filter
    df = df[df["fare_amount"] > 0]
    df = df[df["trip_distance"] > 0]
    df = df[df["trip_duration_minutes"] > 0]
    df = df[df["trip_duration_minutes"] < 300]

    # Add ingestion metadata for lineage tracking
    df["ingestion_date"] = INGESTION_DATE
    df["ingestion_timestamp"] = datetime.now().isoformat()
    df["source_system"] = "bigquery_public_data"
    df["source_table"] = "tlc_yellow_trips_2022"

    print(f"Records after cleaning: {len(df):,}")
    return df


def upload_to_gcs(df: pd.DataFrame, bucket_name: str, gcs_path: str) -> str:
    """
    Upload DataFrame to GCS as Parquet using in-memory buffer.
    Partitioned by ingestion_date for incremental processing.
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    blob.upload_from_file(buffer, content_type="application/octet-stream")
    return f"gs://{bucket_name}/{gcs_path}"


def run_taxi_ingestion(month: int = 1, year: int = 2022):
    """
    Main ingestion function — extract, clean, and land taxi data.
    Designed to be called by Airflow DAG in production.
    """
    print(f"Starting taxi ingestion — {year}-{month:02d}")
    print(f"Ingestion date: {INGESTION_DATE}")

    # Extract
    df = extract_taxi_data(month=month, year=year)

    # Clean
    df = clean_taxi_data(df)

    # Load to GCS
    gcs_path = (
        f"taxi/ingestion_date={INGESTION_DATE}/"
        f"yellow_trips_{year}_{month:02d}.parquet"
    )
    uploaded_path = upload_to_gcs(df, RAW_BUCKET, gcs_path)

    print(f"Ingestion complete")
    print(f"Location: {uploaded_path}")
    print(f"Records: {len(df):,}")
    print(f"Columns: {df.shape[1]}")
    return uploaded_path


if __name__ == "__main__":
    run_taxi_ingestion(month=1, year=2022)