## ADR-001: Use NYC TLC Yellow Taxi Trip Records as the primary data source

The project uses official NYC Taxi & Limousine Commission Yellow Taxi Trip Records as the primary source dataset.

Yellow Taxi data was selected because it provides a realistic operational event dataset with pickup and drop-off timestamps, locations, trip distances, fare components, passenger counts, and payment information. This makes it suitable for building demand, revenue, operational performance, and data quality analytics.

The default project scope uses Q1 2025 data:

- yellow_tripdata_2025-01.parquet
- yellow_tripdata_2025-02.parquet
- yellow_tripdata_2025-03.parquet

The Taxi Zone Lookup Table is used as a dimension source to enrich pickup and drop-off location IDs with borough, zone, and service zone information.

Green Taxi, FHV, and High Volume FHV datasets are intentionally excluded from the first version to keep the project focused and avoid unnecessary schema complexity.