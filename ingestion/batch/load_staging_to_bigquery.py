# load_staging_to_bigquery.py
# Loads transformed Parquet files from GCS staging into BigQuery staging dataset

from google.cloud import bigquery

PROJECT_ID = "urban-intelligence-pipeline-sv"
GCS_STAGING_BUCKET = "urban-intelligence-pipeline-sv-staging"
INGESTION_DATE = "2026-04-28"

bq_client = bigquery.Client(project=PROJECT_ID)


def load_native_table(table_id: str, gcs_uri: str) -> None:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )
    load_job = bq_client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config
    )
    load_job.result()
    table = bq_client.get_table(table_id)
    print(f"Loaded {table.num_rows:,} rows into {table_id}")


def run():
    print("Loading staging data into BigQuery...")

    table_id = f"{PROJECT_ID}.staging.taxi_weather_staging"
    gcs_uri = (
        f"gs://{GCS_STAGING_BUCKET}/taxi_weather/"
        f"ingestion_date={INGESTION_DATE}/*.parquet"
    )

    load_native_table(table_id, gcs_uri)
    print("Staging load complete")
    print(f"Table: {table_id}")


if __name__ == "__main__":
    run()