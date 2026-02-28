# Module 5: End-to-End Data Pipeline with Bruin

A production-ready ELT (Extract, Load, Transform) pipeline demonstrating modern data engineering practices for NYC taxi trip data processing and analytics.

## Overview

This project showcases a complete data platform built with [Bruin](https://getbruin.com/), an orchestration tool for data ingestion, transformation, and governance. The pipeline ingests real-world public data, applies data quality checks, performs incremental transformations, and generates business-ready analytics.

**Key Features:**
- Python-based data ingestion from public TLC APIs
- Incremental data loading with deduplication strategies
- SQL transformations using DuckDB
- Built-in data quality checks and validation
- Scheduled daily orchestration
- Reference data management (payment type lookup)
- Aggregated analytics with time-window optimization

## Architecture

The pipeline follows a standard three-layer ELT pattern:

```
ingestion/ → staging/ → reports/
   (Raw)   → (Clean)  → (Analytics)
```

### Layer 1: Ingestion (`ingestion/`)
Fetches raw NYC taxi trip data from TLC public endpoints using Python. Each ingestion task:
- Extracts data for a configured date range
- Handles multiple taxi types (yellow, green, etc.) via pipeline variables
- Appends data to maintain historical records
- Uses runtime environment variables for date filtering

**Key files:**
- `trips.py` - Fetches trip data from TLC API with support for date ranges and taxi type filtering
- `payment_lookup.csv` - Reference data for payment type descriptions

### Layer 2: Staging (`staging/`)
Cleans, deduplicates, and standardizes raw data with comprehensive data quality checks:
- Removes duplicates using composite keys
- Validates critical fields (not null checks)
- Casts and transforms data types
- Implements referential integrity with lookup tables
- Uses `create+replace` strategy for deterministic rebuilds

**Key files:**
- `trips.sql` - Core staging transformation with primary key constraints and quality checks

### Layer 3: Reports (`reports/`)
Generates time-window optimized analytics for business insights:
- Aggregates metrics by date and payment type
- Calculates trip counts, fare totals, distance, and passenger metrics
- Uses time-interval materialization for efficient incremental updates

**Key files:**
- `trips_report.sql` - Daily reporting aggregations

## Tech Stack

| Component | Technology |
|-----------|-----------|
| **Orchestration** | [Bruin](https://getbruin.com/) |
| **Data Warehouse** | DuckDB (local) / BigQuery (cloud-ready) |
| **Ingestion** | Python 3.11 with pandas |
| **Transformation** | SQL |
| **Scheduling** | Bruin daily schedule |
| **Configuration** | YAML |

## Getting Started

### Prerequisites
- Python 3.11+
- Bruin CLI installed
- DuckDB (included with Bruin)

### Installation

1. **Install dependencies:**
   ```bash
   pip install -r my-pipeline/pipeline/assets/ingestion/requirements.txt
   ```

2. **Navigate to the pipeline directory:**
   ```bash
   cd my-pipeline
   ```

### Running the Pipeline

**Execute a single run:**
```bash
bruin run
```

**Check data lineage:**
```bash
bruin lineage my-pipeline/pipeline/assets/ingestion/trips.py
```

**Validate the pipeline structure:**
```bash
bruin validate
```

## Data Quality & Testing

The pipeline implements comprehensive data quality checks at each layer:

### Staging Layer Validations
- **Primary Key Integrity:** Composite keys on `(tpep_pickup_datetime, tpep_dropoff_datetime)` ensure no duplicate trips
- **Non-Null Checks:** Critical timestamp and ID fields must be populated
- **Referential Integrity:** Payment types validated against lookup table
- **Schema Validation:** Automatic column type casting and validation

### Quality Check Patterns
Each column includes:
```yaml
columns:
  - name: column_name
    type: data_type
    description: Clear, documented purpose
    checks:
      - name: not_null  # Built-in validation
```

This declarative approach ensures:
- Data contracts are enforced at ingestion time
- Schema changes are explicit and documented
- Quality issues surface early in the pipeline

## Configuration

The pipeline is configured via `pipeline.yml`:

```yaml
name: nyc_taxi
schedule: daily                    # Runs every day
start_date: "2022-01-01"          # Historical start date
default_connections:
  duckdb: duckdb-default          # Local data warehouse

variables:
  taxi_types:                      # Configurable taxi types
    type: array
    items:
      type: string
    default: ["yellow"]
```

This approach enables:
- Easy switching between taxi types (yellow, green, etc.)
- Date range filtering at runtime via `BRUIN_START_DATE` / `BRUIN_END_DATE`
- Reusable pipeline definitions across environments

## Clean Code Principles Demonstrated

### 1. Separation of Concerns
- **Ingestion:** Raw data fetching (I)
- **Staging:** Data cleaning (E/L)
- **Reports:** Business logic (T)

Each layer has a single responsibility, making the pipeline easy to test and maintain.

### 2. Documentation & Clarity
- Comprehensive docstrings explaining purpose and behavior
- Column descriptions for data lineage and metadata
- Clear asset naming conventions following `layer.entity` pattern
- YAML comments explaining configuration choices

### 3. Dependency Management
Each asset declares explicit dependencies:
```yaml
depends:
  - ingestion.trips
  - ingestion.payment_lookup
```
This enables Bruin to:
- Validate the DAG (directed acyclic graph)
- Optimize execution order
- Provide clear lineage tracking

### 4. Incremental Processing
- Staging uses `create+replace` for safe rebuilds
- Ingestion uses `append` to maintain history
- Reports use time-window strategies for efficiency
- Reduces compute costs and execution time

### 5. Type Safety & Validation
- Explicit column types (timestamp, string, integer, etc.)
- Primary key constraints prevent duplicates
- Pre-defined checks catch data quality issues early
- Schema violations fail loudly before downstream impact

## Data Flow Example

For a sample run on **2024-01-15**:

1. **Ingestion** (2024-01-15 data)
   ```
   TLC API → Fetch yellow taxi trips → DuckDB table
   Records: ~15,000 trips
   ```

2. **Staging** (Deduplication & validation)
   ```
   Raw trips → Remove duplicates → Validate schemas → Cleaned trips
   Quality checks: No nulls, Valid payment types, Unique keys
   ```

3. **Reports** (Aggregation)
   ```
   Cleaned trips → Group by date + payment type → Metrics
   Output: 5-7 rows (one per payment type)
   Metrics: trip count, avg fare, total distance, passenger count
   ```

## Monitoring & Logs

Pipeline execution logs are stored in:
```
logs/runs/nyc_taxi/YYYY_MM_DD_HH_MM_SS.json
```

Example log structure:
```json
{
  "run_id": "...",
  "timestamp": "2026-02-28T10:50:32Z",
  "status": "success",
  "assets_processed": 3,
  "duration_seconds": 45
}
```

## Future Enhancements

- **Cloud Deployment:** Deploy to BigQuery with automated schema migration
- **Advanced Metrics:** Add predictive analytics and anomaly detection
- **Data Governance:** Implement data lineage tracking and PII masking
- **Extended Coverage:** Support fhv (for-hire vehicle) and green taxi data simultaneously
- **CI/CD Integration:** Automated pipeline validation on pull requests

## Learning Resources

This project implements concepts from the Data Engineering Zoomcamp:
- Modern data stack architecture
- ELT vs ETL patterns
- Incremental processing strategies
- Data quality frameworks
- Orchestration best practices

## Project Structure

```
Module5/
├── README.md (this file)
└── my-pipeline/
    ├── README.md (tutorial guide)
    └── pipeline/
        ├── pipeline.yml (configuration)
        └── assets/
            ├── ingestion/          # Raw data extraction
            │   ├── trips.py
            │   ├── payment_lookup.csv
            │   └── requirements.txt
            ├── staging/            # Data cleaning & validation
            │   └── trips.sql
            └── reports/            # Analytics & aggregations
                └── trips_report.sql
```

## Key Takeaways

**Well-Architected Pipeline**
- Clear separation of concerns across ingestion, transformation, reporting
- Explicit dependency management enables reliable orchestration

**Data Quality First**
- Built-in validation catches issues early
- Declarative schema enforcement prevents silent data corruption

**Scalable Design**
- Incremental processing strategies reduce compute waste
- Modular structure enables easy feature additions

**Production-Ready Code**
- Comprehensive documentation for team collaboration
- Type safety and validation throughout the pipeline

## Questions or Feedback?

For implementation details, refer to the [Bruin Documentation](https://getbruin.com/docs) or the [tutorial guide](my-pipeline/README.md).

--- This README was generated by my AI agent but read and edited by me to ensure it accurately reflects the project and provides clear guidance for users.