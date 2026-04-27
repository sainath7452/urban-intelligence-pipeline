# 🏙️ Urban Intelligence Pipeline

> End-to-end GCP data engineering platform analyzing NYC Taxi demand patterns correlated with weather and crime data — with Claude AI automation.

![GCP](https://img.shields.io/badge/GCP-BigQuery%20%7C%20Dataproc%20%7C%20Composer-blue)
![Terraform](https://img.shields.io/badge/IaC-Terraform-purple)
![dbt](https://img.shields.io/badge/Transform-dbt-orange)
![Airflow](https://img.shields.io/badge/Orchestration-Airflow-red)
![Python](https://img.shields.io/badge/Python-3.13-green)

## 🎯 Business Problem

How do weather conditions and neighborhood crime rates affect NYC taxi demand and pricing across time? This pipeline answers that question at 1.5 billion row scale.

## 🏗️ Architecture

Raw Sources → GCS Landing Zone → Dataproc PySpark → BigQuery (raw → staging → marts) → Looker Studio

Orchestration: Apache Airflow (Cloud Composer) manages every step end-to-end

AI Layer: Claude API monitors pipeline health, detects anomalies, generates reports