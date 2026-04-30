"""
pipeline_monitor.py
====================
Day 6 — Urban Intelligence Pipeline Monitoring

Checks:
  1. Row count drop vs previous day          (>20% drop = anomaly)
  2. Null/missing values in key columns      (>5% nulls = anomaly)
  3. Weather data freshness                  (no weather data today = anomaly)

On anomaly:
  - Prints detailed report to Airflow logs
  - Sends alert email via SMTP

Usage (standalone):
  python pipeline_monitor.py

Usage (from Airflow DAG — replaces t9_monitoring placeholder):
  from monitoring.pipeline_monitor import run_monitoring
  PythonOperator(task_id="run_pipeline_monitoring", python_callable=run_monitoring)

GCP Project : urban-intelligence-pipeline-sv
BQ Dataset  : urban_raw
Table       : fct_taxi_trips
"""

from __future__ import annotations

import os
import smtplib
import logging
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Configuration — override via environment variables
# ---------------------------------------------------------------------------
GCP_PROJECT      = os.getenv("GCP_PROJECT",      "urban-intelligence-pipeline-sv")
BQ_DATASET       = os.getenv("BQ_DATASET",       "urban_raw")
TAXI_TABLE       = os.getenv("TAXI_TABLE",        "fct_taxi_trips")
WEATHER_TABLE    = os.getenv("WEATHER_TABLE",     "weather_data")   # adjust if different

# Email config — set these as Airflow Variables or env vars
ALERT_EMAIL_TO   = os.getenv("ALERT_EMAIL_TO",   "sainath@example.com")
ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM",  "alerts@urban-pipeline.com")
SMTP_HOST        = os.getenv("SMTP_HOST",         "smtp.gmail.com")
SMTP_PORT        = int(os.getenv("SMTP_PORT",     "587"))
SMTP_USER        = os.getenv("SMTP_USER",         "")   # set in env
SMTP_PASSWORD    = os.getenv("SMTP_PASSWORD",     "")   # set in env

# Anomaly thresholds
ROW_COUNT_DROP_THRESHOLD  = float(os.getenv("ROW_COUNT_DROP_THRESHOLD",  "0.20"))  # 20% drop
NULL_RATE_THRESHOLD       = float(os.getenv("NULL_RATE_THRESHOLD",        "0.05"))  # 5% nulls

# Key columns to check for nulls
KEY_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "pickup_location_id",
    "dropoff_location_id",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class CheckResult:
    check_name: str
    status: str          # "PASS" | "WARN" | "FAIL"
    metric_value: Any
    threshold: Any
    message: str
    details: dict = field(default_factory=dict)


@dataclass
class MonitoringReport:
    run_date: str
    pipeline: str
    checks: list[CheckResult] = field(default_factory=list)
    overall_status: str = "PASS"

    def add_check(self, result: CheckResult) -> None:
        self.checks.append(result)
        if result.status == "FAIL":
            self.overall_status = "FAIL"
        elif result.status == "WARN" and self.overall_status != "FAIL":
            self.overall_status = "WARN"

    def has_anomalies(self) -> bool:
        return self.overall_status in ("FAIL", "WARN")

    def summary_lines(self) -> list[str]:
        lines = [
            "=" * 70,
            f"  URBAN INTELLIGENCE PIPELINE — MONITORING REPORT",
            f"  Run date : {self.run_date}",
            f"  Status   : {self.overall_status}",
            "=" * 70,
        ]
        for c in self.checks:
            icon = {"PASS": "✓", "WARN": "⚠", "FAIL": "✗"}.get(c.status, "?")
            lines.append(f"  {icon} [{c.status}] {c.check_name}")
            lines.append(f"       {c.message}")
            if c.details:
                for k, v in c.details.items():
                    lines.append(f"       {k}: {v}")
        lines.append("=" * 70)
        return lines


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------
def _bq_client() -> bigquery.Client:
    return bigquery.Client(project=GCP_PROJECT)


def _run_query(client: bigquery.Client, sql: str) -> list[dict]:
    logger.debug("Running BQ query:\n%s", sql)
    rows = list(client.query(sql).result())
    return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Check 1 — Row count drop vs previous day
# ---------------------------------------------------------------------------
def check_row_count_drop(client: bigquery.Client, today: str, yesterday: str) -> CheckResult:
    sql = f"""
    SELECT
      DATE(pickup_datetime) AS trip_date,
      COUNT(*)              AS row_count
    FROM `{GCP_PROJECT}.{BQ_DATASET}.{TAXI_TABLE}`
    WHERE DATE(pickup_datetime) IN ('{today}', '{yesterday}')
    GROUP BY 1
    ORDER BY 1
    """
    try:
        rows = _run_query(client, sql)
        counts = {r["trip_date"].strftime("%Y-%m-%d") if hasattr(r["trip_date"], "strftime") else str(r["trip_date"]): r["row_count"] for r in rows}

        today_count     = counts.get(today, 0)
        yesterday_count = counts.get(yesterday, 0)

        if yesterday_count == 0:
            return CheckResult(
                check_name="Row count drop vs previous day",
                status="WARN",
                metric_value=today_count,
                threshold=ROW_COUNT_DROP_THRESHOLD,
                message="No data found for yesterday — cannot compute drop rate.",
                details={"today": today, "today_count": today_count, "yesterday": yesterday, "yesterday_count": 0},
            )

        drop_rate = (yesterday_count - today_count) / yesterday_count
        if drop_rate > ROW_COUNT_DROP_THRESHOLD:
            status = "FAIL"
            message = f"Row count dropped {drop_rate:.1%} vs yesterday (threshold: {ROW_COUNT_DROP_THRESHOLD:.0%})."
        else:
            status = "PASS"
            message = f"Row count is healthy — dropped only {max(drop_rate, 0):.1%} vs yesterday."

        return CheckResult(
            check_name="Row count drop vs previous day",
            status=status,
            metric_value=drop_rate,
            threshold=ROW_COUNT_DROP_THRESHOLD,
            message=message,
            details={
                "today": today,         "today_count": today_count,
                "yesterday": yesterday, "yesterday_count": yesterday_count,
                "drop_rate": f"{drop_rate:.2%}",
            },
        )

    except Exception as exc:
        return CheckResult(
            check_name="Row count drop vs previous day",
            status="FAIL",
            metric_value=None,
            threshold=ROW_COUNT_DROP_THRESHOLD,
            message=f"Query failed: {exc}",
        )


# ---------------------------------------------------------------------------
# Check 2 — Null/missing values in key columns
# ---------------------------------------------------------------------------
def check_null_rates(client: bigquery.Client, today: str) -> CheckResult:
    null_selects = "\n      ,".join(
        [f"COUNTIF({col} IS NULL) AS null_{col}" for col in KEY_COLUMNS]
    )
    sql = f"""
    SELECT
      COUNT(*) AS total_rows,
      {null_selects}
    FROM `{GCP_PROJECT}.{BQ_DATASET}.{TAXI_TABLE}`
    WHERE DATE(pickup_datetime) = '{today}'
    """
    try:
        rows = _run_query(client, sql)
        if not rows:
            return CheckResult(
                check_name="Null/missing values in key columns",
                status="WARN",
                metric_value=None,
                threshold=NULL_RATE_THRESHOLD,
                message=f"No rows found for {today} — cannot check nulls.",
            )

        row         = rows[0]
        total_rows  = row["total_rows"] or 1
        violations  = {}

        for col in KEY_COLUMNS:
            null_count = row.get(f"null_{col}", 0) or 0
            null_rate  = null_count / total_rows
            if null_rate > NULL_RATE_THRESHOLD:
                violations[col] = f"{null_rate:.2%} nulls ({null_count:,} rows)"

        if violations:
            status  = "FAIL"
            message = f"{len(violations)} column(s) exceed {NULL_RATE_THRESHOLD:.0%} null threshold."
        else:
            status  = "PASS"
            message = f"All {len(KEY_COLUMNS)} key columns are within null threshold on {today}."

        return CheckResult(
            check_name="Null/missing values in key columns",
            status=status,
            metric_value=violations,
            threshold=NULL_RATE_THRESHOLD,
            message=message,
            details={"total_rows": total_rows, "violations": violations or "none"},
        )

    except Exception as exc:
        return CheckResult(
            check_name="Null/missing values in key columns",
            status="FAIL",
            metric_value=None,
            threshold=NULL_RATE_THRESHOLD,
            message=f"Query failed: {exc}",
        )


# ---------------------------------------------------------------------------
# Check 3 — Weather data freshness
# ---------------------------------------------------------------------------
def check_weather_freshness(client: bigquery.Client, today: str) -> CheckResult:
    sql = f"""
    SELECT COUNT(*) AS weather_rows
    FROM `{GCP_PROJECT}.{BQ_DATASET}.{WEATHER_TABLE}`
    WHERE DATE(timestamp) = '{today}'
    """
    try:
        rows         = _run_query(client, sql)
        weather_rows = rows[0]["weather_rows"] if rows else 0

        if weather_rows == 0:
            status  = "FAIL"
            message = f"No weather data found for {today} — ingestion may have failed."
        elif weather_rows < 20:   # expect at least 24 hourly records
            status  = "WARN"
            message = f"Only {weather_rows} weather records for {today} — expected 24 hourly records."
        else:
            status  = "PASS"
            message = f"Weather data is fresh — {weather_rows} records found for {today}."

        return CheckResult(
            check_name="Weather data freshness",
            status=status,
            metric_value=weather_rows,
            threshold="≥ 20 records",
            message=message,
            details={"date": today, "weather_rows": weather_rows},
        )

    except Exception as exc:
        return CheckResult(
            check_name="Weather data freshness",
            status="FAIL",
            metric_value=None,
            threshold="≥ 20 records",
            message=f"Query failed: {exc}",
        )


# ---------------------------------------------------------------------------
# Email alert
# ---------------------------------------------------------------------------
def send_alert_email(report: MonitoringReport) -> None:
    if not SMTP_USER or not SMTP_PASSWORD:
        logger.warning("SMTP credentials not configured — skipping email alert.")
        logger.warning("Set SMTP_USER and SMTP_PASSWORD env vars to enable email alerts.")
        return

    subject = f"[{report.overall_status}] Urban Intelligence Pipeline — {report.run_date}"

    # Plain text body
    text_body = "\n".join(report.summary_lines())

    # HTML body
    rows_html = ""
    for c in report.checks:
        color = {"PASS": "#2e7d32", "WARN": "#f57c00", "FAIL": "#c62828"}.get(c.status, "#333")
        rows_html += f"""
        <tr>
          <td style="padding:8px;border:1px solid #ddd">{c.check_name}</td>
          <td style="padding:8px;border:1px solid #ddd;color:{color};font-weight:bold">{c.status}</td>
          <td style="padding:8px;border:1px solid #ddd">{c.message}</td>
        </tr>"""

    status_color = {"PASS": "#2e7d32", "WARN": "#f57c00", "FAIL": "#c62828"}.get(report.overall_status, "#333")
    html_body = f"""
    <html><body style="font-family:Arial,sans-serif;max-width:700px;margin:auto">
      <h2 style="color:{status_color}">Urban Intelligence Pipeline — {report.overall_status}</h2>
      <p><strong>Run date:</strong> {report.run_date}</p>
      <table style="border-collapse:collapse;width:100%">
        <thead>
          <tr style="background:#f5f5f5">
            <th style="padding:8px;border:1px solid #ddd;text-align:left">Check</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left">Status</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left">Message</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
      <p style="color:#888;font-size:12px;margin-top:20px">
        Urban Intelligence Pipeline · GCP Project: {GCP_PROJECT}
      </p>
    </body></html>
    """

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = ALERT_EMAIL_FROM
        msg["To"]      = ALERT_EMAIL_TO
        msg.attach(MIMEText(text_body, "plain"))
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(ALERT_EMAIL_FROM, ALERT_EMAIL_TO, msg.as_string())

        logger.info("Alert email sent to %s", ALERT_EMAIL_TO)

    except Exception as exc:
        logger.error("Failed to send alert email: %s", exc)


# ---------------------------------------------------------------------------
# Main monitoring runner — called by Airflow t9_monitoring
# ---------------------------------------------------------------------------
def run_monitoring(**context) -> None:
    """
    Entry point for Airflow PythonOperator.
    Can also be called standalone: python pipeline_monitor.py
    """
    today     = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    yesterday = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")

    # Support Airflow context ds (execution date) if available
    if context:
        ds = context.get("ds")   # Airflow logical date string YYYY-MM-DD
        if ds:
            today     = ds
            yesterday = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info("Starting pipeline monitoring for date: %s", today)

    report = MonitoringReport(
        run_date=today,
        pipeline="urban-intelligence-pipeline",
    )

    client = _bq_client()

    # Run all checks
    logger.info("Running check 1/3: Row count drop...")
    report.add_check(check_row_count_drop(client, today, yesterday))

    logger.info("Running check 2/3: Null rates in key columns...")
    report.add_check(check_null_rates(client, today))

    logger.info("Running check 3/3: Weather data freshness...")
    report.add_check(check_weather_freshness(client, today))

    # Print full report to logs
    for line in report.summary_lines():
        logger.info(line)

    # Send email if anomaly detected
    if report.has_anomalies():
        logger.warning("Anomalies detected — sending alert email...")
        send_alert_email(report)
    else:
        logger.info("All checks passed — no alert needed.")

    # Fail the Airflow task if any check is FAIL (not just WARN)
    failed_checks = [c for c in report.checks if c.status == "FAIL"]
    if failed_checks:
        raise Exception(
            f"Pipeline monitoring failed — {len(failed_checks)} check(s) failed: "
            + ", ".join(c.check_name for c in failed_checks)
        )

    logger.info("Pipeline monitoring complete. Overall status: %s", report.overall_status)


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run_monitoring()