# load_to_bigquery.py
# Loads raw Parquet files from GCS into BigQuery raw dataset
# Creates external tables pointing to GCS — no data duplication

from google.cloud import bigquery

# Configuration
PROJECT_ID = "urban-intelligence-pipeline-sv"
DATASET_RAW = "raw"
GCS_RAW_BUCKET = "urban-intelligence-pipeline-sv-raw"
INGESTION_DATE = "2026-04-28"

bq_client = bigquery.Client(project=PROJECT_ID)


def create_external_table(
    table_name: str,
    gcs_uri: str,
    schema: list
) -> None:
    """
    Create a BigQuery external table pointing to GCS Parquet files.
    External tables read directly from GCS — no storage duplication.
    This is the standard pattern for raw zone tables in a data lake.
    """
    table_id = f"{PROJECT_ID}.{DATASET_RAW}.{table_name}"

    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [gcs_uri]
    external_config.autodetect = True

    table = bigquery.Table(table_id)
    table.external_data_configuration = external_config

    # Delete if exists and recreate
    bq_client.delete_table(table_id, not_found_ok=True)
    bq_client.create_table(table)

    print(f"External table created: {table_id}")
    print(f"Source: {gcs_uri}")


def load_native_table(
    table_name: str,
    gcs_uri: str,
    write_disposition: str = "WRITE_TRUNCATE"
) -> None:
    """
    Load Parquet from GCS into a native BigQuery table.
    Native tables are faster to query than external tables.
    Used for staging and marts layers.
    """
    table_id = f"{PROJECT_ID}.{DATASET_RAW}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        autodetect=True
    )

    load_job = bq_client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config
    )

    load_job.result()  # Wait for job to complete

    table = bq_client.get_table(table_id)
    print(f"Loaded {table.num_rows:,} rows into {table_id}")


def run_raw_load():
    """
    Load all raw data from GCS into BigQuery raw dataset.
    Creates native tables for both taxi and weather data.
    """
    print("Starting raw data load to BigQuery...")
    print(f"Project: {PROJECT_ID}")
    print(f"Dataset: {DATASET_RAW}")
    print(f"Ingestion date: {INGESTION_DATE}")

    # Load taxi data
    print("\nLoading taxi data...")
    taxi_gcs_uri = (
        f"gs://{GCS_RAW_BUCKET}/taxi/"
        f"ingestion_date={INGESTION_DATE}/*.parquet"
    )
    load_native_table(
        table_name="taxi_raw",
        gcs_uri=taxi_gcs_uri
    )

    # Load weather data
    print("\nLoading weather data...")
    weather_gcs_uri = (
        f"gs://{GCS_RAW_BUCKET}/weather/"
        f"ingestion_date={INGESTION_DATE}/*.parquet"
    )
    load_native_table(
        table_name="weather_raw",
        gcs_uri=weather_gcs_uri
    )

    print("\nRaw load complete")
    print("Tables available in BigQuery:")
    print(f"  {PROJECT_ID}.raw.taxi_raw")
    print(f"  {PROJECT_ID}.raw.weather_raw")


if __name__ == "__main__":
    run_raw_load()