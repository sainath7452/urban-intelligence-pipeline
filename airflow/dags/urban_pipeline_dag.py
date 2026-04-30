# urban_pipeline_dag.py
# Master orchestration DAG for the Urban Intelligence Pipeline
# Runs daily at 2am UTC
# Orchestrates: ingestion → transformation → dbt → monitoring

import sys
sys.path.insert(0, "C:/Users/saina/Desktop/urban-intelligence-pipeline")
from monitoring.pipeline_monitor import run_monitoring
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import subprocess
import os

# ---------------------------------------------------------------------------
# Project paths — override via env vars for portability across environments
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.getenv("URBAN_PROJECT_ROOT", "C:/Users/saina/Desktop/urban-intelligence-pipeline")
PYTHON_PATH  = os.getenv("URBAN_PYTHON",       "C:/Users/saina/anaconda3/python.exe")
DBT_PATH     = os.getenv("URBAN_DBT",          "C:/Users/saina/anaconda3/Scripts/dbt.exe")
DBT_PROJECT  = os.path.join(PROJECT_ROOT, "dbt", "urban_intelligence")
DBT_PROFILES = os.path.join(PROJECT_ROOT, "dbt")   # folder containing profiles.yml

# ---------------------------------------------------------------------------
# GCP Configuration
# ---------------------------------------------------------------------------
PROJECT_ID     = "urban-intelligence-pipeline-sv"
REGION         = "us-central1"
CLUSTER_NAME   = "urban-pipeline-cluster"
SCRIPTS_BUCKET = "urban-intelligence-pipeline-sv-scripts"

# ---------------------------------------------------------------------------
# Default arguments
# FIX: use a fixed start_date, NOT days_ago(1).
# days_ago(1) triggers an immediate backfill run the moment the DAG is
# switched on. A fixed past date with catchup=False is always safer.
# ---------------------------------------------------------------------------
default_args = {
    "owner":            "sainath_panga",
    "depends_on_past":  False,
    "start_date":       datetime(2025, 4, 29),   # fixed date — not days_ago()
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def run_python_script(script_path: str, description: str) -> None:
    """Run a Python script as an Airflow task; stream stdout and raise on error."""
    print(f"Starting : {description}")
    print(f"Script   : {script_path}")

    result = subprocess.run(
        [PYTHON_PATH, script_path],
        capture_output=True,
        text=True,
    )

    if result.stdout:
        print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Script failed: {description}\n{result.stderr}")

    print(f"Completed: {description}")


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------
def ingest_taxi_data(**context) -> None:
    """Task 1 — Ingest NYC taxi data from BigQuery public dataset to GCS."""
    run_python_script(
        script_path=os.path.join(PROJECT_ROOT, "ingestion", "batch", "taxi_ingestion.py"),
        description="NYC Taxi batch ingestion",
    )


def ingest_weather_data(**context) -> None:
    """Task 2 — Ingest NYC weather data from Open-Meteo API to GCS."""
    run_python_script(
        script_path=os.path.join(PROJECT_ROOT, "ingestion", "batch", "weather_ingestion.py"),
        description="NYC Weather API ingestion",
    )


def load_raw_to_bigquery(**context) -> None:
    """Task 3 — Load raw Parquet files from GCS into BigQuery raw dataset."""
    run_python_script(
        script_path=os.path.join(PROJECT_ROOT, "ingestion", "batch", "load_to_bigquery.py"),
        description="Load raw data to BigQuery",
    )


def create_dataproc_cluster(**context) -> None:
    """Task 4 — Create Dataproc cluster for PySpark job."""
    print("Creating Dataproc cluster...")
    result = subprocess.run([
        "gcloud", "dataproc", "clusters", "create", CLUSTER_NAME,
        f"--region={REGION}",
        "--zone=us-central1-a",
        "--master-machine-type=n1-standard-2",
        "--master-boot-disk-size=50",
        "--num-workers=2",
        "--worker-machine-type=n1-standard-2",
        "--worker-boot-disk-size=50",
        "--image-version=2.1-debian11",
        f"--project={PROJECT_ID}",
    ], capture_output=True, text=True)

    if result.returncode != 0:
        if "ALREADY_EXISTS" in result.stderr:
            print("Cluster already exists — continuing.")
        else:
            raise Exception(f"Failed to create cluster:\n{result.stderr}")

    print("Dataproc cluster ready.")


def run_pyspark_transform(**context) -> None:
    """Task 5 — Upload PySpark script to GCS and submit job to Dataproc."""
    spark_script = os.path.join(PROJECT_ROOT, "transform", "dataproc", "taxi_weather_transform.py")

    print("Uploading PySpark script to GCS...")
    subprocess.run(
        ["gsutil", "cp", spark_script, f"gs://{SCRIPTS_BUCKET}/"],
        check=True,
    )

    print("Submitting PySpark job to Dataproc...")
    result = subprocess.run([
        "gcloud", "dataproc", "jobs", "submit", "pyspark",
        f"gs://{SCRIPTS_BUCKET}/taxi_weather_transform.py",
        f"--cluster={CLUSTER_NAME}",
        f"--region={REGION}",
        f"--project={PROJECT_ID}",
    ], capture_output=True, text=True)

    if result.returncode != 0:
        raise Exception(f"PySpark job failed:\n{result.stderr}")

    print("PySpark transformation complete.")


def delete_dataproc_cluster(**context) -> None:
    """
    Task 6 — Delete Dataproc cluster to save costs.

    Uses trigger_rule=ALL_DONE (set on the operator below) so this task runs
    even when t5_pyspark fails — preventing an orphaned, billing cluster.
    """
    print("Deleting Dataproc cluster...")
    subprocess.run([
        "gcloud", "dataproc", "clusters", "delete", CLUSTER_NAME,
        f"--region={REGION}",
        f"--project={PROJECT_ID}",
        "--quiet",
    ], capture_output=True, text=True)
    print("Cluster deleted — costs saved.")


def load_staging_to_bigquery(**context) -> None:
    """Task 7 — Load transformed data from GCS staging into BigQuery."""
    run_python_script(
        script_path=os.path.join(PROJECT_ROOT, "ingestion", "batch", "load_staging_to_bigquery.py"),
        description="Load staging data to BigQuery",
    )


def run_dbt_models(**context) -> None:
    """
    Task 8 — Run dbt models then dbt tests.

    FIX: --profiles-dir points dbt at the repo's profiles.yml instead of
    searching ~/.dbt/, which may not exist on the Airflow worker.
    """
    print("Running dbt models...")
    run_result = subprocess.run(
        [
            DBT_PATH, "run",
            "--project-dir", DBT_PROJECT,
            "--profiles-dir", DBT_PROFILES,
        ],
        capture_output=True,
        text=True,
    )
    print(run_result.stdout)
    if run_result.returncode != 0:
        raise Exception(f"dbt run failed:\n{run_result.stderr}")

    print("Running dbt tests...")
    test_result = subprocess.run(
        [
            DBT_PATH, "test",
            "--project-dir", DBT_PROJECT,
            "--profiles-dir", DBT_PROFILES,
        ],
        capture_output=True,
        text=True,
    )
    print(test_result.stdout)
    if test_result.returncode != 0:
        raise Exception(f"dbt tests failed:\n{test_result.stderr}")

    print("dbt models and tests complete.")



# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="urban_intelligence_pipeline",
    default_args=default_args,
    description="End-to-end Urban Intelligence data pipeline",
    schedule_interval="0 2 * * *",   # daily at 02:00 UTC
    catchup=False,
    tags=["urban_intelligence", "production"],
) as dag:

    t1_ingest_taxi = PythonOperator(
        task_id="ingest_taxi_data",
        python_callable=ingest_taxi_data,
        doc_md="Ingests NYC Yellow Taxi data from BigQuery public dataset to GCS raw bucket.",
    )

    t2_ingest_weather = PythonOperator(
        task_id="ingest_weather_data",
        python_callable=ingest_weather_data,
        doc_md="Fetches hourly NYC weather data from Open-Meteo API to GCS raw bucket.",
    )

    t3_load_raw = PythonOperator(
        task_id="load_raw_to_bigquery",
        python_callable=load_raw_to_bigquery,
        doc_md="Loads raw Parquet files from GCS into BigQuery raw dataset.",
    )

    t4_create_cluster = PythonOperator(
        task_id="create_dataproc_cluster",
        python_callable=create_dataproc_cluster,
        doc_md="Provisions Dataproc cluster for PySpark transformation.",
    )

    t5_pyspark = PythonOperator(
        task_id="run_pyspark_transform",
        python_callable=run_pyspark_transform,
        doc_md="Joins taxi and weather data using PySpark on Dataproc.",
    )

    # FIX: trigger_rule=ALL_DONE ensures the cluster is deleted even when
    # t5_pyspark fails — without this, a failed Spark job leaves the cluster
    # running and you continue to be billed for it.
    t6_delete_cluster = PythonOperator(
        task_id="delete_dataproc_cluster",
        python_callable=delete_dataproc_cluster,
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="Deletes Dataproc cluster after job completion to minimise costs.",
    )

    t7_load_staging = PythonOperator(
        task_id="load_staging_to_bigquery",
        python_callable=load_staging_to_bigquery,
        doc_md="Loads transformed data from GCS staging into BigQuery staging dataset.",
    )

    t8_dbt = PythonOperator(
        task_id="run_dbt_models",
        python_callable=run_dbt_models,
        doc_md="Runs dbt models and data quality tests.",
    )

    t9_monitoring = PythonOperator(
        task_id="run_pipeline_monitoring",
        python_callable=run_monitoring,
        doc_md="Runs Claude AI anomaly detection and generates pipeline health report.",
    )

    # ---------------------------------------------------------------------------
    # Task dependencies
    # Taxi + weather ingestion run in parallel; everything else is sequential.
    # ---------------------------------------------------------------------------
    [t1_ingest_taxi, t2_ingest_weather] >> t3_load_raw
    t3_load_raw >> t4_create_cluster >> t5_pyspark >> t6_delete_cluster
    t6_delete_cluster >> t7_load_staging >> t8_dbt >> t9_monitoring