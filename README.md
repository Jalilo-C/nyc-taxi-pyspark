# NYC Taxi Analytics — PySpark Data Engineering Project

## Overview

An end-to-end Data Engineering project built with **Apache Spark (PySpark)** to process and analyze New York City Yellow Taxi trip records.

The project implements a **Medallion Architecture (Bronze, Silver, Gold)** to ingest, transform, validate, and aggregate real-world taxi data.

The objective is to build a structured and testable data pipeline while applying core Data Engineering concepts such as distributed processing, data quality checks, Parquet storage, Spark SQL, and automated testing.

## Architecture

```text
                 NYC TLC Yellow Taxi Dataset
                            |
                            v
                    +---------------+
                    |    BRONZE     |
                    | Raw Parquet   |
                    +---------------+
                            |
                            v
                    +---------------+
                    |    SILVER     |
                    | Data Quality  |
                    | Transformations|
                    +---------------+
                            |
                            v
                    +---------------+
                    |     GOLD      |
                    | Daily Metrics |
                    | Location      |
                    | Payment       |
                    +---------------+
                            |
                            v
                    +---------------+
                    |  Parquet Data |
                    |   Analytics   |
                    +---------------+
```

### Bronze — Raw Data Ingestion

- Reads the original Yellow Taxi Parquet dataset.
- Preserves the source data without modifying its records.
- Stores the raw dataset for downstream processing.

### Silver — Data Transformation and Quality

Applies data quality checks and creates additional validation columns:

| Column | Description |
|---|---|
| `is_valid_duration` | Identifies trips with a valid pickup/dropoff time sequence. |
| `is_valid_distance` | Identifies trips with a positive distance. |
| `has_missing_passengers` | Identifies missing passenger counts. |
| `has_non_positive_passengers` | Identifies zero or negative passenger counts. |
| `is_disputed` | Identifies trips with payment type 4 (Dispute). |
| `has_negative_fare` | Identifies trips with negative fare amounts. |

Potentially problematic records are flagged rather than automatically deleted, preserving the original information for further analysis.

### Gold — Analytical Data

Generates three analytical datasets.

**1. Daily Metrics — `daily_metrics`**

Aggregates taxi activity by pickup date.

- Total number of trips.
- Total and average recorded trip amounts.
- Average valid trip distance.
- Number of trips with valid duration.

**2. Location Analytics — `location_analytics`**

Aggregates activity by taxi zone.

- Number of pickups.
- Number of drop-offs.
- Sum of recorded trip amounts for pickups.

Uses a full outer join to retain zones appearing only as pickup or drop-off locations.

**3. Payment Analytics — `payment_analytics`**

Analyzes trip activity by payment type.

- Total number of trips.
- Total and average recorded trip amounts.
- Percentage of trips by payment type.

> Note: The amount metrics represent recorded `total_amount` values, not necessarily money actually collected.

## Dataset

**Source:** [NYC Taxi & Limousine Commission (TLC) — Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

**Dataset:** Yellow Taxi Trip Records — January 2025

**Format:** Parquet

**Download:**
[Yellow Taxi January 2025 Dataset](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet)

The downloaded dataset contains approximately 3.47 million records.

The Gold layer explicitly filters pickup timestamps to the January 2025 interval:

`2025-01-01 <= pickup_datetime < 2025-02-01`

This ensures that trips outside January are excluded from the monthly analytical datasets.

## Tech Stack

| Technology | Purpose |
|---|---|
| Python 3.12 | Main programming language |
| Apache Spark / PySpark 4.3 | Distributed data processing |
| Java 17 | Spark runtime |
| Parquet | Columnar data storage |
| Pytest | Automated testing |
| Git & GitHub | Version control |

## Project Structure

```text
NYC-Taxi-PySpark/
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── src/
│   ├── __init__.py
│   ├── main.py
│   ├── pipeline.py
│   └── gold.py
│
├── tests/
│   ├── conftest.py
│   ├── test_gold.py
│   ├── test_location.py
│   └── test_payment.py
│
├── .gitignore
├── README.md
└── requirements.txt
```

Generated datasets are stored locally in the `data/` directory and excluded from Git.

## Installation and Setup

### Prerequisites

- Python 3.12
- Java 17
- Git

### 1. Clone the repository

```bash
git clone https://github.com/Jalilo-C/nyc-taxi-pyspark.git
cd NYC-Taxi-PySpark
```

### 2. Create a virtual environment

```bash
python -m venv .venv
```

Activate it on Windows:

```powershell
.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

Make sure `pytest` is included in your development dependencies.

### 4. Download the dataset

Download the January 2025 Yellow Taxi Parquet file from the TLC source and place it here:

```text
data/bronze/yellow_tripdata_2025-01.parquet
```

## Running the Pipeline

Run the entire pipeline from the project root:

```bash
python -m src.main
```

The pipeline automatically executes:

1. Bronze ingestion.
2. Silver transformations and data quality checks.
3. Gold daily analytics.
4. Gold location analytics.
5. Gold payment analytics.

The same SparkSession is shared across all processing stages and stopped after execution.

## Running Tests

After generating the Silver and Gold datasets, execute:

```bash
pytest -v
```

The automated tests validate:

- Daily metrics row count and date uniqueness.
- Presence of expected analytical columns.
- Consistency between Silver trip counts and Gold aggregates.
- Consistency of pickup and drop-off counts.
- Payment percentages summing approximately to 100%.

## Spark Configuration

The project uses local Spark execution:

```python
.master("local[*]")
```

The shuffle partition count is configured to 20:

```python
.config("spark.sql.shuffle.partitions", "20")
```

This value was tested experimentally instead of relying on Spark's default of 200 partitions. No formal performance benchmark was conducted, so this configuration should not be interpreted as a proven performance improvement.

Adaptive Query Execution (AQE) remains enabled using Spark's default configuration.

## Key Learning Outcomes

Through this project, I applied and practiced:

- PySpark DataFrame API and Spark SQL.
- Distributed transformations and aggregations.
- Lazy evaluation and execution plans.
- Shuffle operations and join strategies.
- Data quality validation.
- Parquet storage and column pruning.
- Medallion Architecture.
- Automated testing with Pytest.
- End-to-end pipeline orchestration.

## Future Improvements

Potential extensions include:

- Integrating Apache Airflow for scheduled pipeline orchestration.
- Implementing incremental data processing.
- Adding more analytical datasets and visualizations.
- Deploying the pipeline using Microsoft Fabric.
- Exploring cloud-based data storage and processing.

---

**Project:** NYC Taxi Analytics  
**Focus:** Data Engineering | PySpark | Apache Spark | Data Quality | Medallion Architecture