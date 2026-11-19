# NYC Taxi Analytics with PySpark

## Overview

End-to-end Data Engineering project using Apache Spark and PySpark to process and analyze real-world New York City Yellow Taxi trip records.

## Dataset

Source: [NYC Taxi & Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

Dataset: Yellow Taxi Trip Records — January 2025.

Format: Parquet.

## Architecture

The project follows the Medallion Architecture:

- **Bronze:** Raw data ingestion.
- **Silver:** Data cleaning, validation and transformation.
- **Gold:** Business aggregations and analytics.

## Tech Stack

- Python 3.12
- Apache Spark / PySpark 4.3
- Java 17
- Parquet
- Git / GitHub

## Project Structure

```text
NYC-Taxi-PySpark/
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── src/
│   └── pipeline.py
├── tests/
├── README.md
└── requirements.txt
```

## Objectives

- Build an end-to-end PySpark ETL pipeline.
- Apply data quality checks.
- Implement Bronze, Silver and Gold layers.
- Optimize Spark transformations.
- Store analytical datasets in partitioned Parquet files.

## Setup

```bash
pip install -r requirements.txt
```

Execution instructions will be added as the pipeline is implemented.