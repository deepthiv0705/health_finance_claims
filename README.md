# Health Finance Claims Analytics Platform

## 1.Overview

The **Health Finance Claims Analytics Platform** is an end-to-end, production-style data engineering project that simulates how healthcare insurance claims data is ingested, processed, validated, transformed, and served for analytics and reporting.

This project is designed to closely mirror **real-world product company data platforms**, covering the full lifecycle from raw data generation to analytics-ready fact and dimension tables.

The pipeline integrates **Healthcare + Finance domain concepts**, making it relevant for organizations working on claims processing, risk analysis, cost optimization, and fraud detection.

## 2.Business Problem Statement

Healthcare organizations deal with large volumes of claims data originating from multiple providers and members.

This data must:

\- Be ingested reliably - Be cleaned and standardized - Support historical tracking - Be transformed into analytics-friendly models - Meet data quality and governance standards

This project addresses these challenges by implementing a modern, cloud-based data engineering solution.

## 3.High-Level Architecture

**Pipeline Flow:**

→Python (Local)

→ Amazon S3 (Raw CSV)

→ AWS Glue (PySpark: CSV → Parquet + Cleansing)

→ Amazon S3 (Curated Parquet)

→ AWS Glue (Parquet → Snowflake)

→ Snowflake (Raw / Staging)

→ dbt (Core & Mart Models)

**Orchestration:** Apache Airflow controls the end-to-end execution; ensuring reliable and sequential execution.

## 4.Technology Stack

| Layer | Tools & Technologies |
| --- | --- |
| Orchestration | Apache Airflow |
| Programming | Python, PySpark |
| Cloud Storage | Amazon S3 |
| Processing | AWS Glue |
| Data Warehouse | Snowflake |
| Transformations<br><br>Modeling | dbt Core, dbt Cloud<br><br>Star Schema (Facts & Dimensions) |
| OS & Infra | Ubuntu (WSL), Linux |

## 5.Data Sources

Synthetic datasets are generated to simulate real production data:

### 5.1. Members

- Member demographics
- Location details

### 5.2. Providers

- Provider information
- Medical specialties

### 5.3. Claims

- Claim transactions
- Diagnosis codes
- Financial amounts
- Claim status lifecycle

## 6.Pipeline Breakdown

### 6.1. Data Generation (Python)

- Custom Python scripts generate realistic dummy data using Faker and Pandas &NumPy
- Outputs both CSV and Parquet formats
- Ensures deterministic outputs using random seeds

**Outcome:** Reproducible datasets for development, testing, and demos.

### 6.2. Data Ingestion to S3

S3 is used as the **data lake** layer.

### Folder Structure

s3://&lt;bucket-name&gt;/  
├── raw/csv/  
├── processed/parquet/

- CSV used as raw source
- Parquet used for optimized analytics and downstream processing

### 6.3. Data Processing with AWS Glue

#### Glue Job 1: CSV → Parquet + Cleaning

- Reads raw CSV data from S3
- Performs:
  - Schema enforcement and cleaning
  - Data type standardization and new column derivation
  - Null handling
- Writes optimized Parquet files

#### Glue Job 2: Parquet → Snowflake

- Reads curated Parquet data
- Loads into Snowflake staging tables
- Uses IAM roles and secure credentials

**Why Glue?** - Serverless - Scalable - PySpark-based distributed processing

### 6.4. Data Transformation with dbt

### Layered Modeling

| Layer | Purpose |
| --- | --- |
| Staging | Raw loaded data from Glue |
| Core | Cleaned, conformed entities |
| Mart | Analytics-ready facts & dimensions |

### Optimization

- Columnar storage
- Partition-aware loading
- Analytics-friendly schemas

### Features Implemented

- **Models:** staging → core → marts
- **Snapshots:** historical tracking of changes
- **Macros:** reusable SQL logic
- **Tests:**
  - Not null
  - Unique
  - Referential integrity

### Benefits

- Version-controlled transformations
- Modular, maintainable SQL
- Built-in data quality checks

#### Staging Models

- Standardize column names and data types
- Apply basic transformations

#### Snapshots

- Track historical changes in dimension data
- Enable Slowly Changing Dimensions (SCD)

#### Core Models

- Build clean, reusable business entities

#### Analytics Marts

- Fact and dimension tables optimized for reporting

### 6.5. Orchestration with Airflow

Task Flow:

- Generate dummy data (PythonOperator)
- Upload data to S3 (PythonOperator)
- Glue job - CSV → Parquet
- Glue job - Parquet → Snowflake
- dbt build (BashOperator)
- End

### Key Capabilities

- Sequential dependency handling
- Retry logic & failure handling
- Centralized logging
- Manual & scheduled triggers

## 7.Data Modeling

### Fact Tables

- **fact_claims**: Central fact capturing claim-level transactions

### Dimension Tables

- **dim_members**
- **dim_providers**

The star schema design ensures optimal query performance and reporting flexibility.

## 8.Data Quality & Testing

Implemented using dbt:

\- Not-null tests - Uniqueness constraints - Referential integrity checks - Snapshot validation

These checks ensure trust and reliability of analytics data.

## 9.Key Learnings & Skills Demonstrated

- Designing production-grade data pipelines
- Python automation, data generation, and transformation
- Cloud storage (S3) and IAM access configuration
- Cloud-native processing using AWS Glue
- Snowflake data warehousing and staging/marts setup
- Analytics engineering and data modelling using dbt
- Airflow DAG orchestration and monitoring
- End-to-end ownership of data workflows

## 10. Environment & Platform Setup (Overview)

This pipeline was built and tested in a local Linux-based development environment to closely mirror production behaviour.

Key setup decisions:

- **Ubuntu 22.04 (WSL2)** used to ensure Linux parity with cloud runtimes
- **Apache Airflow (Local Executor)** for workflow orchestration and retries
- **AWS IAM Roles** for secure Glue and S3 access
- **Snowflake role-based access** aligned with transformation responsibilities
- **dbt Core** used locally for controlled, versioned transform

## 11.How to Run (High Level)

- Clone the repository
- Set up Python virtual environment
- Configure AWS credentials and Snowflake connections
- Start Airflow and trigger the DAG
- Run dbt models and tests

## 12. Future Enhancements

- Add Power BI dashboards
- Add data freshness checks
- CI/CD for airflow

## 13. Documentation

Detailed technical documentation is available in the /docs folder:

- docs/architecture.md - System design & flow
- docs/airflow_orchestration.md - DAG & task design
- docs/data_model.md - Fact & dimension modelling
- docs/dbt_strategy.md - Transformations, snapshots, macros
- docs/setup_and_installation.md - Installation setups

## 14. Production-Readiness Considerations

- Idempotent pipeline execution
- Retry and failure handling
- Partitioned data storage
- Data quality enforcement
- Modular, maintainable codebase

## 15. Resume Value & Industry Alignment

This project demonstrates:

- End-to-end ownership of data pipelines
- Strong cloud-native architecture skills
- Hands-on orchestration and automation
- Analytics engineering best practices

## Author

**Deepthi V** - Data Engineer

This project is intended for portfolio and learning purposes and demonstrates real-world data engineering practices.

**Status:** ✅ Successfully Implemented & Automated