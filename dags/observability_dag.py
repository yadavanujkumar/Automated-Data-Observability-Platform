"""
Data Observability DAG for monitoring data quality metrics.

This DAG runs hourly observability checks on a simulated PostgreSQL database
and pushes metrics to a Prometheus Push Gateway.

Metrics tracked:
- Freshness: Time elapsed (in hours) since the newest record was created
- Volume: Total row count for the day
- Quality/Nulls: Percentage of NULL values in the amount column
"""

from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import requests  # noqa: F401 - Used in production for Push Gateway integration
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    "owner": "data-observability",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def get_database_connection() -> dict[str, Any]:
    """
    Simulate a connection to a PostgreSQL database.

    In production, this would use psycopg2 to connect to the actual database.
    For this simulation, we return connection parameters.

    Returns:
        dict: Database connection parameters
    """
    # Simulated connection parameters for observability_target database
    connection_params = {
        "host": "localhost",
        "port": 5432,
        "database": "observability_target",
        "user": "observability_user",
        "password": "observability_pass",
    }

    # Simulate connection using psycopg2
    # In production:
    # import psycopg2
    # conn = psycopg2.connect(**connection_params)
    # return conn

    print(f"Simulating connection to database: {connection_params['database']}")
    return connection_params


def get_simulated_sales_data() -> pd.DataFrame:
    """
    Simulate the sales_data table from the PostgreSQL database.

    The table contains:
    - id: Unique identifier
    - amount: Sale amount (may contain NULL values)
    - created_at: Timestamp when the record was created

    Returns:
        pd.DataFrame: Simulated sales data
    """
    # Get current time for simulation
    now = datetime.now()

    # Create simulated data with some NULL values in amount column
    # Generate records spread throughout the current day (every 15 minutes)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    hours_elapsed = (now - today_start).total_seconds() / 3600

    data = {
        "id": list(range(1, 101)),
        "amount": [
            100.50,
            None,
            250.00,
            75.25,
            None,
            300.00,
            150.75,
            None,
            200.00,
            50.00,
        ]
        * 10,
        "created_at": [
            now - timedelta(hours=i * hours_elapsed / 100) for i in range(100)
        ],
    }

    df = pd.DataFrame(data)

    # Filter to only include records from today
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    df_today = df[df["created_at"] >= today_start].copy()

    return df_today


def run_observability_checks() -> dict[str, float]:
    """
    Run observability checks on the sales_data table.

    Calculates three key metrics:
    1. Freshness: Hours since the newest record was created
    2. Volume: Total row count for the day
    3. Quality/Nulls: Percentage of NULL values in the amount column

    Returns:
        dict: Dictionary containing the three observability metrics (all as floats)
    """
    # Get database connection (simulated)
    get_database_connection()

    # Get sales data (simulated)
    df = get_simulated_sales_data()

    # Current time for calculations
    now = datetime.now()

    # Metric 1: Freshness - hours since newest record
    if len(df) > 0:
        newest_record_time = df["created_at"].max()
        freshness_hours = (now - newest_record_time).total_seconds() / 3600
    else:
        freshness_hours = float("inf")

    # Metric 2: Volume - total row count for the day
    volume_rows = len(df)

    # Metric 3: Quality/Nulls - percentage of NULL values in amount column
    if len(df) > 0:
        null_count = df["amount"].isna().sum()
        null_percentage = (null_count / len(df)) * 100
    else:
        null_percentage = 0.0

    metrics = {
        "data_freshness_hours": round(freshness_hours, 2),
        "data_volume_rows": float(volume_rows),
        "data_null_percentage": round(null_percentage, 2),
    }

    return metrics


def format_metrics_for_prometheus(metrics: dict[str, float]) -> str:
    """
    Format metrics into Prometheus exposition format.

    Converts the metrics dictionary into a string format compatible
    with Prometheus Push Gateway.

    Args:
        metrics: Dictionary of metric names and values

    Returns:
        str: Prometheus-formatted metrics string
    """
    prometheus_lines = []

    for metric_name, value in metrics.items():
        # Format: metric_name value
        prometheus_lines.append(f"{metric_name} {value}")

    return "\n".join(prometheus_lines)


def push_metrics_to_prometheus(
    metrics: str,
    pushgateway_url: str = "http://localhost:9091",
    job_name: str = "data_observability"
) -> bool:
    """
    Simulate pushing metrics to Prometheus Push Gateway.

    In production, this would make an actual HTTP POST request
    to the Prometheus Push Gateway.

    Args:
        metrics: Prometheus-formatted metrics string
        pushgateway_url: URL of the Prometheus Push Gateway
        job_name: Name of the job for grouping metrics

    Returns:
        bool: True if push was successful, False otherwise
    """
    url = f"{pushgateway_url}/metrics/job/{job_name}"

    print(f"Simulating push to Prometheus Push Gateway: {url}")
    print(f"Metrics being pushed:\n{metrics}")

    # Simulate the HTTP POST request
    # In production:
    # try:
    #     response = requests.post(
    #         url,
    #         data=metrics,
    #         headers={"Content-Type": "text/plain"}
    #     )
    #     return response.status_code == 200
    # except requests.RequestException as e:
    #     print(f"Error pushing metrics: {e}")
    #     return False

    return True


def run_observability_task(**kwargs) -> None:
    """
    Main task function that runs observability checks and pushes metrics.

    This function:
    1. Runs the observability checks
    2. Formats metrics for Prometheus
    3. Logs the formatted output
    4. Simulates pushing to Prometheus Push Gateway
    """
    print("Starting data observability checks...")

    # Run observability checks
    metrics = run_observability_checks()
    print(f"Calculated metrics: {metrics}")

    # Format for Prometheus
    prometheus_metrics = format_metrics_for_prometheus(metrics)

    # Create key-value string for logging/debugging
    key_value_string = ", ".join(
        [f"{k}={v}" for k, v in metrics.items()]
    )
    print(f"Metrics summary: {key_value_string}")

    # Push to Prometheus Push Gateway (simulated)
    success = push_metrics_to_prometheus(prometheus_metrics)

    if success:
        print("Successfully pushed metrics to Prometheus Push Gateway")
    else:
        print("Failed to push metrics to Prometheus Push Gateway")

    # Store metrics in XCom for downstream tasks if needed
    task_instance = kwargs.get("ti")
    if task_instance:
        task_instance.xcom_push(key="observability_metrics", value=metrics)
        task_instance.xcom_push(
            key="prometheus_metrics", value=prometheus_metrics
        )


# Define the DAG
with DAG(
    dag_id="data_observability_dag",
    default_args=default_args,
    description="Hourly data observability checks for monitoring data quality",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["observability", "data-quality", "monitoring"],
) as dag:

    # Single task to run observability checks
    observability_task = PythonOperator(
        task_id="run_observability_checks",
        python_callable=run_observability_task,
        provide_context=True,
    )
