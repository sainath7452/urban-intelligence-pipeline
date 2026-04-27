# ADR-001: Core Platform and Tool Choices

**Date:** 2026-04-27
**Status:** Accepted
**Author:** Sainath Reddy Panga

## Context
Building a senior-level data engineering portfolio project processing
1.5B+ NYC taxi records correlated with weather and crime data.
The goal is to demonstrate production-grade engineering practices
across ingestion, transformation, orchestration, and AI automation.

## Decisions and Reasoning

### GCP over AWS/Azure
I already have Azure experience from Mercedes-Benz, BASF, and
Heidelberg Materials. GCP was chosen specifically to demonstrate
cloud-agnostic capability. GCP also offers the richest set of public
BigQuery datasets, eliminating the need for external data sources
in the raw ingestion layer.

### BigQuery as the Warehouse
Serverless, columnar, and natively integrated with GCS and Looker
Studio. Partitioning and clustering support means we can query
1.5B+ rows efficiently without pre-aggregating. Cost scales with
query volume, not cluster uptime — critical for a dev environment.

### Terraform for IaC
All infrastructure defined as code from Day 1. The entire environment
can be torn down and rebuilt in under 10 minutes — critical for cost
control during development and for demonstrating reproducibility.
State is stored remotely in GCS so it is never lost.

### dbt for SQL Transformation
Modular, testable, documented SQL. dbt enforces the
raw → staging → marts layering pattern which is now industry
standard. Tests and documentation are built into the tool, not
bolted on afterwards.

### Airflow via Cloud Composer for Orchestration
Industry standard for batch pipeline scheduling. Chosen over Cloud
Workflows because Airflow experience is explicitly required in 80%+
of senior DE job descriptions. The complexity cost is worth the
resume signal.

### Claude API for Automation Layer
AI-powered monitoring that auto-detects anomalies, generates
plain-English pipeline health reports, and answers natural language
questions about data. This is the differentiator no other portfolio
project has.

## Consequences
- Cloud Composer is the most expensive component — will be
  provisioned last and torn down between sessions to manage costs.
- Terraform state bucket was created manually as a one-time
  bootstrap step before all other infrastructure was managed by
  Terraform.
- Project ID uses suffix "-sv" due to global uniqueness constraint
  on GCP project IDs.