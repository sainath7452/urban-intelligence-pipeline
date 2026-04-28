# taxi_weather_transform.py
# PySpark transformation job that runs on GCP Dataproc
# Reads raw taxi and weather data from GCS
# Joins them on date and hour
# Writes clean analytical dataset back to GCS staging zone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    DoubleType, TimestampType
)
import sys

# Configuration
PROJECT_ID = "urban-intelligence-pipeline-sv"
GCS_RAW_BUCKET = "gs://urban-intelligence-pipeline-sv-raw"
GCS_STAGING_BUCKET = "gs://urban-intelligence-pipeline-sv-staging"
INGESTION_DATE = "2026-04-28"
BQ_DATASET_STAGING = "staging"


def create_spark_session() -> SparkSession:
    """
    Initialize Spark session with BigQuery connector.
    The BigQuery connector is pre-installed on Dataproc clusters.
    """
    spark = (
        SparkSession.builder
        .appName("TaxiWeatherTransformation")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("Spark session created successfully")
    print(f"Spark version: {spark.version}")
    return spark


def read_taxi_data(spark: SparkSession) -> "DataFrame":
    """
    Read raw taxi Parquet data from GCS.
    Uses wildcard to read all partitions under ingestion_date.
    """
    path = f"{GCS_RAW_BUCKET}/taxi/ingestion_date={INGESTION_DATE}/*.parquet"
    print(f"Reading taxi data from: {path}")

    df = spark.read.parquet(path)
    print(f"Taxi records loaded: {df.count():,}")
    return df


def read_weather_data(spark: SparkSession) -> "DataFrame":
    """
    Read raw weather Parquet data from GCS.
    """
    path = f"{GCS_RAW_BUCKET}/weather/ingestion_date={INGESTION_DATE}/*.parquet"
    print(f"Reading weather data from: {path}")

    df = spark.read.parquet(path)
    print(f"Weather records loaded: {df.count():,}")
    return df


def clean_taxi_data(df: "DataFrame") -> "DataFrame":
    """
    Apply transformation rules to taxi data.
    Adds derived columns and standardizes data types.
    """
    return (
        df
        # Cast columns to correct types
        .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
        .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
        .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
        .withColumn("tip_amount", F.col("tip_amount").cast(DoubleType()))

        # Add derived columns for analysis
        .withColumn(
            "fare_per_mile",
            F.when(F.col("trip_distance") > 0,
                F.round(F.col("fare_amount") / F.col("trip_distance"), 2)
            ).otherwise(None)
        )
        .withColumn(
            "tip_percentage",
            F.when(F.col("fare_amount") > 0,
                F.round((F.col("tip_amount") / F.col("fare_amount")) * 100, 2)
            ).otherwise(0.0)
        )
        .withColumn(
            "is_airport_trip",
            F.when(
                (F.col("pickup_location_id").isin(["1", "132", "138"])) |
                (F.col("dropoff_location_id").isin(["1", "132", "138"])),
                True
            ).otherwise(False)
        )
        .withColumn(
            "trip_category",
            F.when(F.col("trip_distance") < 1, "short")
            .when(F.col("trip_distance") < 5, "medium")
            .when(F.col("trip_distance") < 15, "long")
            .otherwise("very_long")
        )

        # Extract join keys - date and hour to match with weather
        .withColumn("trip_date_str", F.col("trip_date").cast(StringType()))
        .withColumn("pickup_hour_int", F.col("pickup_hour").cast(IntegerType()))

        # Remove outliers
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("fare_amount") < 500)
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("trip_distance") < 100)
        .filter(F.col("trip_duration_minutes") > 0)
        .filter(F.col("trip_duration_minutes") < 300)
    )


def clean_weather_data(df: "DataFrame") -> "DataFrame":
    """
    Prepare weather data for joining with taxi data.
    Creates join keys matching taxi data structure.
    """
    return (
        df
        .withColumn("weather_date_str", F.col("date").cast(StringType()))
        .withColumn("weather_hour_int", F.col("hour").cast(IntegerType()))
        .withColumn(
            "is_raining",
            F.when(F.col("precipitation_inch") > 0.01, True).otherwise(False)
        )
        .withColumn(
            "is_cold",
            F.when(F.col("temperature_f") < 32, True).otherwise(False)
        )
        .withColumn(
            "temp_category",
            F.when(F.col("temperature_f") < 20, "freezing")
            .when(F.col("temperature_f") < 32, "very_cold")
            .when(F.col("temperature_f") < 50, "cold")
            .when(F.col("temperature_f") < 65, "mild")
            .when(F.col("temperature_f") < 80, "warm")
            .otherwise("hot")
        )
        # Select only columns needed for join
        .select(
            "weather_date_str",
            "weather_hour_int",
            "temperature_f",
            "humidity_pct",
            "precipitation_inch",
            "wind_speed_mph",
            "weather_code",
            "weather_condition",
            "is_raining",
            "is_cold",
            "temp_category"
        )
    )


def join_taxi_weather(
    taxi_df: "DataFrame",
    weather_df: "DataFrame"
) -> "DataFrame":
    """
    Join taxi trips with weather conditions on date and hour.
    Left join ensures we keep all taxi trips even if weather
    data is missing for some hours.
    """
    print("Joining taxi and weather data...")

    joined = taxi_df.join(
        weather_df,
        on=[
            taxi_df["trip_date_str"] == weather_df["weather_date_str"],
            taxi_df["pickup_hour_int"] == weather_df["weather_hour_int"]
        ],
        how="left"
    )

    # Drop duplicate join key columns
    joined = joined.drop("weather_date_str", "weather_hour_int")

    print(f"Joined records: {joined.count():,}")
    return joined


def write_to_staging(df: "DataFrame") -> None:
    """
    Write transformed and joined data to GCS staging bucket as Parquet.
    Also write directly to BigQuery staging dataset.
    """
    # Write to GCS staging
    staging_path = (
        f"{GCS_STAGING_BUCKET}/taxi_weather/"
        f"ingestion_date={INGESTION_DATE}"
    )
    print(f"Writing to GCS staging: {staging_path}")

    df.write.mode("overwrite").parquet(staging_path)
    print("GCS staging write complete")

    # Write to BigQuery staging table
    print("Writing to BigQuery staging table...")
    (
        df.write.format("bigquery")
        .option("table", f"{PROJECT_ID}:{BQ_DATASET_STAGING}.taxi_weather_staging")
        .option("temporaryGcsBucket", "urban-intelligence-pipeline-sv-staging")
        .option("createDisposition", "CREATE_IF_NEEDED")
        .option("writeDisposition", "WRITE_TRUNCATE")
        .save()
    )
    print("BigQuery staging write complete")


def run_transformation():
    """
    Main transformation pipeline.
    Orchestrates read, clean, join, and write steps.
    """
    print("Starting taxi-weather transformation job")
    print(f"Ingestion date: {INGESTION_DATE}")

    spark = create_spark_session()

    # Read raw data
    taxi_df = read_taxi_data(spark)
    weather_df = read_weather_data(spark)

    # Clean
    taxi_clean = clean_taxi_data(taxi_df)
    weather_clean = clean_weather_data(weather_df)

    print(f"Taxi records after cleaning: {taxi_clean.count():,}")

    # Join
    joined_df = join_taxi_weather(taxi_clean, weather_clean)

    # Write
    write_to_staging(joined_df)

    print("\nTransformation complete")
    print(f"Output: {GCS_STAGING_BUCKET}/taxi_weather/ingestion_date={INGESTION_DATE}")

    spark.stop()


if __name__ == "__main__":
    run_transformation()