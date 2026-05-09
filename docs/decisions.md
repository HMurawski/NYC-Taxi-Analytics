## ADR-001: Use NYC TLC Yellow Taxi Trip Records as the primary data source

The project uses official NYC Taxi & Limousine Commission Yellow Taxi Trip Records as the primary source dataset.

Yellow Taxi data was selected because it provides a realistic operational event dataset with pickup and drop-off timestamps, locations, trip distances, fare components, passenger counts, and payment information. This makes it suitable for building demand, revenue, operational performance, and data quality analytics.

The default project scope uses Q1 2025 data:

- yellow_tripdata_2025-01.parquet
- yellow_tripdata_2025-02.parquet
- yellow_tripdata_2025-03.parquet

The Taxi Zone Lookup Table is used as a dimension source to enrich pickup and drop-off location IDs with borough, zone, and service zone information.

Green Taxi, FHV, and High Volume FHV datasets are intentionally excluded from the first version to keep the project focused and avoid unnecessary schema complexity.

## ADR-002: Keep raw trip data unfiltered

Raw trip data should preserve all source records from the TLC Parquet files. Business validation should not happen in Python ingestion scripts.

Invalid, suspicious, or extreme records are retained in the raw layer and classified later in dbt.

This decision allows the project to:

- separate ingestion from business logic
- make data quality issues transparent
- build a dedicated data quality dashboard
- keep raw row counts reconcilable with source files

## ADR-003: Use deterministic source metadata for raw trip records

The TLC Yellow Taxi data does not provide a natural unique trip identifier.

To create a stable technical identifier, the ingestion process adds:

- `source_file`
- `source_row_number`
- `raw_trip_id`

The pair `source_file + source_row_number` represents the original row position inside a monthly source file.