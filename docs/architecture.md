## Data Sources

### NYC TLC Yellow Taxi Trip Records

The main dataset consists of monthly Yellow Taxi trip record files published by the NYC Taxi & Limousine Commission.

Each source file represents one month of taxi trip records. The raw trip data is loaded into the warehouse without business filtering. Validation and business logic are applied later in dbt.

Default data range:

- January 2025
- February 2025
- March 2025

### Taxi Zone Lookup Table

The Taxi Zone Lookup Table maps location IDs to boroughs, zones, and service zones. It is loaded as a raw lookup table and transformed into `dim_taxi_zone`.

### Source Data Principle

Raw data should remain as close as possible to the original source. Cleaning, filtering, and business definitions are handled in dbt models, not in ingestion scripts.