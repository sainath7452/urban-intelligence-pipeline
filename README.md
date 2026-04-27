# 🏙️ Urban Intelligence Pipeline

> End-to-end GCP data engineering platform analyzing NYC Taxi demand patterns correlated with weather and crime data — with Claude AI automation.

![GCP](https://img.shields.io/badge/GCP-BigQuery%20%7C%20Dataproc%20%7C%20Composer-blue)
![Terraform](https://img.shields.io/badge/IaC-Terraform-purple)
![dbt](https://img.shields.io/badge/Transform-dbt-orange)
![Airflow](https://img.shields.io/badge/Orchestration-Airflow-red)
![Python](https://img.shields.io/badge/Python-3.13-green)
![Status](https://img.shields.io/badge/Status-Active%20Development-brightgreen)

---

## 🎯 Business Problem

How do weather conditions and neighborhood crime rates affect NYC taxi demand and pricing across time? This platform answers that question at 1.5 billion row scale — combining three real-world public datasets, a live streaming layer, and an AI-powered monitoring system into a single fully automated pipeline.

---

## 🏗️ Architecture

    Raw Sources
         │
         ▼
    GCS Raw Landing Zone  (partitioned Parquet + JSON)
         │
         ▼
    GCP Dataproc + PySpark  (distributed joins at billion-row scale)
         │
         ▼
    BigQuery Data Warehouse  (raw → staging → marts · star schema)
         │
         ├──── Looker Studio Dashboards  (live KPI reporting)
         │
         ├──── Airflow (Cloud Composer)  (orchestration · retries · SLA alerts)
         │
         └──── Claude AI Automation  (anomaly detection · health reports)

---

## 🛠️ Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Cloud Platform | Google Cloud Platform | Cloud-agnostic depth beyond Azure experience |
| Data Lake | Google Cloud Storage | Partitioned Parquet · versioned · lifecycle-managed |
| Data Warehouse | BigQuery | Serverless columnar · 1.5B+ rows at low cost |
| Batch Processing | PySpark on Dataproc | Distributed joins across billion-row datasets |
| Stream Ingestion | Pub/Sub + Dataflow | Real-time ride event processing with exactly-once semantics |
| SQL Transformation | dbt Core | Modular · tested · auto-documented SQL layers |
| Orchestration | Apache Airflow via Cloud Composer | Industry-standard DAG scheduling with retries and SLAs |
| AI Automation | Claude API (Anthropic) | Anomaly detection · plain-English pipeline health reports |
| Data Quality | Great Expectations | Automated validation gates between every pipeline layer |
| Infrastructure | Terraform | Entire environment reproducible from a single command |
| CI/CD | GitHub Actions | Automated testing and deployment on every merge |
| Visualization | Looker Studio | Live BigQuery-native dashboards |
| Languages | Python · SQL · PySpark · HCL · YAML | Full-stack data engineering |

---

## 📦 Data Sources

| Dataset | Source | Volume | Role in Pipeline |
|---|---|---|---|
| NYC Taxi Trips | bigquery-public-data.new_york_taxi_trips | 1.5B+ rows | Core fact table — demand, fares, distance, timing |
| Chicago Crime | bigquery-public-data.chicago_crime | 7M+ records | Neighborhood safety enrichment via spatial join |
| NOAA Weather | REST API — Python ingestion | Daily per city | Demand correlation — temperature, precipitation, wind |
| Synthetic Ride Events | Python generator → Pub/Sub | Real-time stream | Streaming layer demonstration |

---

## 📁 Repository Structure

    urban-intelligence-pipeline/
    ├── terraform/                   # All GCP infrastructure as code
    │   ├── main.tf                  # Terraform backend + GCP provider
    │   ├── gcs.tf                   # Raw, staging, scripts buckets
    │   ├── bigquery.tf              # Warehouse datasets (raw, staging, marts)
    │   ├── iam.tf                   # Service account + least-privilege roles
    │   └── variables.tf             # Environment variables
    ├── ingestion/
    │   ├── batch/                   # BigQuery public data → GCS Parquet
    │   └── streaming/               # Pub/Sub publisher + Dataflow consumer
    ├── transform/
    │   └── dataproc/                # PySpark jobs for billion-row joins
    ├── dbt/
    │   ├── models/staging/          # Cleaned, typed, deduplicated tables
    │   ├── models/marts/            # Star schema — facts and dimensions
    │   └── tests/                   # dbt data quality tests
    ├── dags/
    │   └── urban_pipeline_dag.py    # Master Airflow orchestration DAG
    ├── automation/
    │   ├── monitor.py               # Claude API anomaly detection
    │   └── reporter.py              # Automated pipeline health reports
    ├── dashboards/                  # Looker Studio screenshots and config
    ├── tests/                       # pytest unit and integration tests
    └── docs/
        └── ADR-001-platform-choices.md   # Architecture decision records

---

## 💡 Key Engineering Decisions

**Why partition GCS raw zone by `ingestion_date` rather than `trip_date`?**
Late-arriving data would land in incorrect partitions and silently corrupt incremental dbt model runs. Partitioning by `ingestion_date` makes every pipeline run idempotent — safe to rerun without duplicating or missing records.

**Why PySpark on Dataproc for batch rather than Dataflow?**
The core workload is a multi-way join across 1.5B taxi rows, 7M crime records, and daily weather data. Spark's in-memory shuffle handles large-join batch workloads more efficiently than Dataflow's Apache Beam model. Dataflow is used exclusively for the streaming layer where its exactly-once semantics matter.

**Why dbt rather than raw BigQuery SQL scripts?**
dbt enforces the `raw → staging → marts` contract, auto-generates data lineage documentation, and executes quality tests on every transformation run. Raw SQL scripts offer none of these guarantees and become unmaintainable as model count grows.

**Why Claude API rather than standard Cloud Monitoring alerts?**
Cloud Monitoring tells you a threshold was breached. Claude explains what likely caused it, which upstream table to investigate first, and generates a plain-English daily summary that non-technical stakeholders can act on without opening a dashboard.

---

## 📐 Architecture Decision Records

All major technology choices are documented with context, reasoning, tradeoffs, and consequences:

- [ADR-001 — Core platform and tool choices](docs/ADR-001-platform-choices.md)

---

## 👤 Author

**Sainath Reddy Panga** — Data Engineer


