# Automated Data Observability Platform

A comprehensive Data Observability solution that monitors data quality metrics using Apache Airflow. The system runs hourly checks on a simulated PostgreSQL database and pushes metrics to a Prometheus Push Gateway.

## Features

- **Automated Hourly Checks**: Airflow DAG runs every hour to collect observability metrics
- **Three Key Metrics**:
  - **Freshness**: Time elapsed (in hours) since the newest record was created
  - **Volume**: Total row count for the day
  - **Quality/Nulls**: Percentage of NULL values in the amount column
- **Prometheus Integration**: Metrics formatted for Prometheus Push Gateway consumption
- **Simulated Database**: Includes functions to simulate PostgreSQL database connections

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌────────────────────┐
│   Airflow DAG   │────▶│  Observability   │────▶│  Prometheus Push   │
│  (Hourly Run)   │     │     Checks       │     │     Gateway        │
└─────────────────┘     └──────────────────┘     └────────────────────┘
                               │
                               ▼
                    ┌──────────────────┐
                    │   PostgreSQL     │
                    │ (observability_  │
                    │     target)      │
                    └──────────────────┘
```

## Requirements

- Python 3.8+
- Apache Airflow 2.6+
- pandas
- psycopg2-binary
- requests

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yadavanujkumar/Automated-Data-Observability-Platform.git
cd Automated-Data-Observability-Platform
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Initialize Airflow (if not already done):
```bash
airflow db init
```

4. Copy the DAG to your Airflow dags folder:
```bash
cp dags/observability_dag.py $AIRFLOW_HOME/dags/
```

## Usage

### Running the DAG

The DAG will automatically run every hour once Airflow is started. You can also trigger it manually:

```bash
airflow dags trigger data_observability_dag
```

### Prometheus Metrics Format

The metrics are formatted in Prometheus exposition format:

```
data_freshness_hours 0.5
data_volume_rows 48
data_null_percentage 30.0
```

### Key-Value Output Format

For logging and debugging, metrics are also output as key-value pairs:

```
data_freshness_hours=0.5, data_volume_rows=48, data_null_percentage=30.0
```

## Database Schema

The simulated `sales_data` table has the following structure:

| Column     | Type      | Description                    |
|------------|-----------|--------------------------------|
| id         | INTEGER   | Unique identifier              |
| amount     | DECIMAL   | Sale amount (may contain NULL) |
| created_at | TIMESTAMP | Record creation timestamp      |

## Prometheus Push Gateway Integration

To push metrics to a real Prometheus Push Gateway:

1. Start Prometheus Push Gateway:
```bash
docker run -d -p 9091:9091 prom/pushgateway
```

2. The DAG will automatically push metrics to `http://localhost:9091/metrics/job/data_observability`

## Configuration

Default DAG configuration can be modified in `dags/observability_dag.py`:

- `schedule_interval`: Default is `@hourly`
- `pushgateway_url`: Default is `http://localhost:9091`
- `job_name`: Default is `data_observability`

## License

MIT License - see [LICENSE](LICENSE) for details