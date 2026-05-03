# NYC Taxi Operations Analytics

## Project Goal

Simulation of a analytics engineering workflow for operational taxi data. It ingests raw NYC taxi trip records, transforms them with dbt into tested analytics marts, orchestrates the workflow with Airflow, and exposes business-ready KPIs in a dashboard.

## Business Questions

1. How does taxi demand change by day, hour, and zone?
2. Which pickup zones generate the most revenue?
3. What are the strongest peak-hour demand patterns?
4. How much data is excluded due to quality issues?
5. Which zones or periods show abnormal trip behavior?

## Tech Stack

- Python
- Postgres
- dbt
- Airflow
- Metabase
- Docker Compose

## Data Model

Raw → Staging → Intermediate → Marts

## Outputs

- dbt models
- dbt tests
- dbt documentation
- Airflow DAG
- Metabase dashboard