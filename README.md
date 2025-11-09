# Telecom Data Processing Pipeline

A comprehensive real-time and batch data processing system for telecom events, built with Apache Spark, Kafka, PostgreSQL, and Apache Airflow.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Components](#components)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Usage](#usage)
- [Data Flow](#data-flow)
- [Database Schema](#database-schema)
- [Configuration](#configuration)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)

## 🎯 Overview

This project implements a complete data pipeline for processing telecom events in real-time and generating daily aggregated statistics. The system handles various event types including calls, SMS, data sessions, and balance recharges, with support for anomaly detection and regional analytics.

### Key Features

- **Real-time Stream Processing**: Processes telecom events as they arrive using Apache Spark Structured Streaming
- **Batch Processing**: Daily aggregation jobs for historical analysis
- **Anomaly Detection**: Identifies and stores invalid or malformed events
- **Regional Analytics**: Tracks metrics by region (Almaty, Astana, Pavlodar, Taraz)
- **Event Type Aggregation**: Supports multiple event types with different metrics
- **Orchestration**: Automated daily batch jobs via Apache Airflow
- **Data Validation**: Comprehensive validation rules for incoming events

## 🏗️ Architecture

```
┌─────────────┐
│   Producer  │ (Generates test events)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    Kafka    │ (Event streaming)
└──────┬──────┘
       │
       ▼
┌─────────────────────┐
│ Spark Stream         │ (Real-time processing)
│ Processor           │
└──────┬──────────────┘
       │
       ├──────────────┐
       ▼              ▼
┌─────────────┐  ┌─────────────┐
│ PostgreSQL  │  │ PostgreSQL  │
│ real_time_  │  │ anomalies   │
│ metrics     │  │             │
└──────┬──────┘  └─────────────┘
       │
       ▼
┌─────────────────────┐
│ Airflow DAG         │ (Daily scheduler)
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│ Spark Batch         │ (Daily aggregation)
│ Processor           │
└──────┬──────────────┘
       │
       ▼
┌─────────────┐
│ PostgreSQL  │
│ daily_stats │
└─────────────┘
```

## 🔧 Components

### 1. **Producer** (`producer/producer.py`)
- Generates synthetic telecom events for testing
- Event types: `data_session`, `call`, `sms`, `balance_recharge`
- Publishes events to Kafka topic `telecom_events`
- Uses Faker library for realistic data generation

### 2. **Stream Processor** (`spark_jobs/stream_processor.py`)
- Real-time processing of Kafka events using Spark Structured Streaming
- Features:
  - **Event Type Aggregation**: 5-minute windows grouped by event type and subtype
  - **Regional Aggregation**: 10-minute windows grouped by region
  - **Anomaly Detection**: Filters invalid events based on validation rules
  - **Watermarking**: 1-minute watermark for late data handling
- Writes to:
  - `real_time_metrics` table (aggregated metrics)
  - `anomalies` table (invalid events)

### 3. **Batch Processor** (`spark_jobs/batch_processor.py`)
- Daily batch job for historical data aggregation
- Processes data from `real_time_metrics` for a specific date
- Generates:
  - Total events count across all regions
  - Region-specific event counts
- Writes results to `daily_stats` table
- Deletes existing data for the date before inserting new results

### 4. **Airflow DAG** (`dags/telecom_batch_dag.py`)
- Orchestrates daily batch processing
- Schedule: `@daily` (runs at midnight)
- Executes batch processor with execution date as parameter
- Uses Airflow's `{{ ds }}` template variable for date handling

### 5. **Database Schema** (`airflowTables.sql`)
- Defines three main tables:
  - `real_time_metrics`: Stores streaming aggregation results
  - `anomalies`: Stores invalid events with reasons
  - `daily_stats`: Partitioned table for daily aggregated statistics

## 📦 Prerequisites

- **Docker** and **Docker Compose** installed
- **Python 3.8+** (for running producer locally)
- **PostgreSQL JDBC Driver** (automatically downloaded by Spark)
- **Kafka Python Client** (for producer)
- **Faker** library (for test data generation)

## 🚀 Setup

### 1. Environment Variables

Create a `.env` file in the project root (optional, defaults are used if not set):

```env
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
```

### 2. Initialize Database

First, start PostgreSQL and initialize the schema:

```bash
# Start only PostgreSQL
docker-compose up -d postgres

# Wait for PostgreSQL to be ready, then run:
docker-compose exec postgres psql -U airflow -d airflow -f /path/to/airflowTables.sql
```

Or manually execute the SQL from `airflowTables.sql`:

```bash
docker-compose exec postgres psql -U airflow -d airflow
# Then paste the SQL commands from airflowTables.sql
```

### 3. Start All Services

```bash
docker-compose up -d
```

This will start:
- **Zookeeper** (port 2181)
- **Kafka** (ports 9092, 29092)
- **PostgreSQL** (port 5432)
- **Airflow Web Server** (port 8080)
- **Airflow Scheduler**
- **Spark Master** (ports 7077, 8081)
- **Spark Worker**

### 4. Access Services

- **Airflow UI**: http://localhost:8080
  - Username: `admin`
  - Password: `admin`
- **Spark Master UI**: http://localhost:8081
- **Kafka**: `localhost:9092` (external), `kafka:29092` (internal)

### 5. Install Producer Dependencies

For running the producer locally:

```bash
pip install kafka-python faker
```

## 💻 Usage

### Running the Stream Processor

Start the Spark streaming job to process events in real-time:

```bash
docker compose exec -u 0 spark-master spark-submit `  --master spark://spark-master:7077 `  --conf spark.jars.ivy=/tmp/.ivy2 `  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,org.postgresql:postgresql:42.6.0 `  /opt/bitnamilegacy/spark/jobs/stream_processor.py
```

### Running the Producer

Generate and send test events to Kafka:

```bash
cd producer
python producer.py
```

The producer will continuously generate events and send them to the `telecom_events` topic.

### Running Batch Processing Manually

Process data for a specific date:

```bash
# PowerShell
$TODAY = (Get-Date).ToString("yyyy-MM-dd")

docker-compose exec -u root spark-master spark-submit `  --master spark://spark-master:7077 `  --packages org.postgresql:postgresql:42.6.0 `  /opt/bitnamilegacy/spark/jobs/batch_processor.py $TODAY
``` 

### Automated Batch Processing via Airflow

1. Access Airflow UI at http://localhost:8080
2. Enable the `telecom_daily_batch_job` DAG
3. The DAG will run daily at midnight and process the previous day's data

## 🔄 Data Flow

### Real-time Processing Flow

1. **Producer** generates telecom events and publishes to Kafka topic `telecom_events`
2. **Stream Processor** reads from Kafka using Spark Structured Streaming
3. Events are parsed and validated:
   - Valid events: MSISDN present, valid event type, non-negative numeric fields
   - Invalid events: Stored in `anomalies` table
4. Valid events are aggregated in two streams:
   - **Event Type Aggregation** (5-minute windows):
     - Groups by event type and subtype
     - Calculates: event count, total data (MB), total amount
     - Converts data_session data to GB (divides by 1024)
   - **Regional Aggregation** (10-minute windows):
     - Groups by region
     - Calculates: event count per region
5. Aggregated metrics are written to `real_time_metrics` table

### Batch Processing Flow

1. **Airflow DAG** triggers daily at midnight
2. **Batch Processor** reads from `real_time_metrics` for the execution date
3. Aggregations performed:
   - Total events count (sum of all region_agg metrics)
   - Region-specific event counts
4. Existing data for the date is deleted from `daily_stats`
5. New aggregated results are written to `daily_stats` table

## 🗄️ Database Schema

### `real_time_metrics`
Stores real-time aggregated metrics from stream processing.

| Column | Type | Description |
|--------|------|-------------|
| `window_start` | TIMESTAMP | Start of aggregation window |
| `window_end` | TIMESTAMP | End of aggregation window |
| `metric_type` | VARCHAR(50) | Type: `event_type_agg` or `region_agg` |
| `metric_key` | VARCHAR(100) | Key: event type/subtype or region name |
| `metric_value` | DOUBLE PRECISION | Aggregated value (count, data GB, or amount) |
| `created_at` | TIMESTAMP | Record creation timestamp |

### `anomalies`
Stores invalid events detected during stream processing.

| Column | Type | Description |
|--------|------|-------------|
| `event_data` | JSONB | Full event data as JSON |
| `reason` | VARCHAR(255) | Reason for anomaly (e.g., "invalid_data") |
| `created_at` | TIMESTAMP | Record creation timestamp |

### `daily_stats`
Partitioned table for daily aggregated statistics.

| Column | Type | Description |
|--------|------|-------------|
| `stat_date` | DATE | Date of statistics |
| `metric_type` | VARCHAR(50) | Type: `total_events_count` or `region_total_events` |
| `metric_key` | VARCHAR(100) | Key: `all_regions` or region name |
| `metric_value` | DOUBLE PRECISION | Aggregated value |

**Partitioning**: Table is partitioned by `stat_date` (example partition: `daily_stats_2025_11`)

## ⚙️ Configuration

### Stream Processor Configuration

- **Kafka Topic**: `telecom_events`
- **Kafka Server**: `kafka:29092` (internal), `localhost:9092` (external)
- **Processing Triggers**:
  - Valid events: 30 seconds
  - Anomalies: 1 minute
- **Watermark**: 1 minute
- **Window Sizes**:
  - Event type aggregation: 5 minutes
  - Regional aggregation: 10 minutes

### Batch Processor Configuration

- **Date Format**: `YYYY-MM-DD`
- **Spark Master**: `spark://spark-master:7077`
- **PostgreSQL Connection**: Configured via environment variables

### Event Types

Supported event types:
- `call`: Voice calls with duration
- `sms`: Text messages
- `data_session`: Data usage sessions with data volume
- `balance_recharge`: Account recharges with amount

### Validation Rules

Events are considered valid if:
- `msisdn` is not null and not empty
- `event_type` is one of: `call`, `sms`, `data_session`, `balance_recharge`
- `duration_seconds` is null or >= 0
- `data_mb` is null or >= 0
- `amount` is null or >= 0

## 📁 Project Structure

```
beelinetask/
├── dags/
│   └── telecom_batch_dag.py      # Airflow DAG for daily batch jobs
├── producer/
│   └── producer.py                # Kafka event producer (test data generator)
├── spark_jobs/
│   ├── batch_processor.py         # Daily batch aggregation job
│   └── stream_processor.py        # Real-time stream processing job
├── airflowTables.sql              # Database schema definitions
├── docker-compose.yml             # Docker services configuration
└── README.md                      # This file
```

## 🔍 Troubleshooting

### Stream Processor Not Processing Events

1. **Check Kafka connectivity**:
   ```bash
   docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
   ```

2. **Verify topic exists**:
   ```bash
   docker-compose exec kafka kafka-topics --describe --topic telecom_events --bootstrap-server localhost:9092
   ```

3. **Check Spark logs**:
   ```bash
   docker-compose logs spark-master
   ```


check sql table for a data: 

docker-compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM real_time_metrics ORDER BY created_at DESC LIMIT 10;"

docker-compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM anomalies LIMIT 10;"

docker-compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM daily_stats ORDER BY stat_date DESC, metric_key;"

### Batch Processor Fails

1. **Verify date format**: Must be `YYYY-MM-DD`
2. **Check PostgreSQL connection**: Ensure database is accessible
3. **Verify data exists**: Check if `real_time_metrics` has data for the specified date
4. **Check Spark logs** for detailed error messages

### Database Connection Issues

1. **Verify PostgreSQL is running**:
   ```bash
   docker-compose ps postgres
   ```

2. **Check environment variables**:
   ```bash
   docker-compose exec postgres env | grep POSTGRES
   ```

3. **Test connection**:
   ```bash
   docker-compose exec postgres psql -U airflow -d airflow -c "SELECT COUNT(*) FROM real_time_metrics;"
   ```

### Airflow DAG Not Running

1. **Check DAG is enabled** in Airflow UI
2. **Verify scheduler is running**:
   ```bash
   docker-compose logs airflow-scheduler
   ```

3. **Check DAG syntax**:
   ```bash
   docker-compose exec airflow-scheduler airflow dags list
   ```

### Producer Not Sending Events

1. **Verify Kafka is accessible** at `localhost:9092`
2. **Check producer logs** for connection errors
3. **Test Kafka connectivity**:
   ```bash
   docker-compose exec kafka kafka-console-producer --topic telecom_events --bootstrap-server localhost:9092
   ```

## 📝 Notes

- The stream processor uses `startingOffsets: latest`, so it only processes new events after startup
- Batch processor deletes existing data for a date before inserting new results (idempotent operation)
- Daily stats table uses PostgreSQL partitioning for better query performance
- All timestamps are stored in UTC
- The producer generates events with weighted probabilities (60% data_session, 25% call, 10% SMS, 5% balance_recharge)

## 🔐 Security Considerations

- Default credentials are used for development. **Change passwords in production**
- PostgreSQL credentials are passed via environment variables
- Consider using secrets management for production deployments
- Network isolation between services is handled by Docker Compose

## 📄 License

This project is provided as-is for demonstration purposes.

