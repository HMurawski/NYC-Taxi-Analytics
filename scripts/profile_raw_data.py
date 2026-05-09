#run with python scripts/profile_raw_data.py
import duckdb

TRIPS_PATH = "data/raw/yellow_tripdata_2025-*.parquet"
ZONES_PATH = "data/raw/taxi_zone_lookup.csv"

con = duckdb.connect()

print("\n=== Yellow Taxi schema ===")
con.sql(f"""
DESCRIBE
SELECT *
FROM read_parquet('{TRIPS_PATH}');
""").show(max_rows=100)

print("\n=== Sample trip records ===")
con.sql(f"""
SELECT *
FROM read_parquet('{TRIPS_PATH}')
LIMIT 10;
""").show(max_rows=10)

print("\n=== Row count by source file ===")
con.sql(f"""
SELECT
    filename,
    COUNT(*) AS row_count
FROM read_parquet('{TRIPS_PATH}', filename = true)
GROUP BY filename
ORDER BY filename;
""").show(max_rows=20)

print("\n=== Pickup/dropoff datetime range ===")
con.sql(f"""
SELECT
    MIN(tpep_pickup_datetime) AS min_pickup_datetime,
    MAX(tpep_pickup_datetime) AS max_pickup_datetime,
    MIN(tpep_dropoff_datetime) AS min_dropoff_datetime,
    MAX(tpep_dropoff_datetime) AS max_dropoff_datetime
FROM read_parquet('{TRIPS_PATH}');
""").show()

print("\n=== Basic numeric ranges ===")
con.sql(f"""
SELECT
    MIN(passenger_count) AS min_passenger_count,
    MAX(passenger_count) AS max_passenger_count,
    MIN(trip_distance) AS min_trip_distance,
    MAX(trip_distance) AS max_trip_distance,
    MIN(fare_amount) AS min_fare_amount,
    MAX(fare_amount) AS max_fare_amount,
    MIN(total_amount) AS min_total_amount,
    MAX(total_amount) AS max_total_amount
FROM read_parquet('{TRIPS_PATH}');
""").show()

print("\n=== Potential data quality issues ===")
con.sql(f"""
SELECT
    COUNT(*) AS raw_rows,

    SUM(
        CASE 
            WHEN tpep_dropoff_datetime <= tpep_pickup_datetime 
            THEN 1 ELSE 0 
        END
    ) AS non_positive_duration_rows,

    SUM(
        CASE 
            WHEN trip_distance <= 0 
            THEN 1 ELSE 0 
        END
    ) AS non_positive_distance_rows,

    SUM(
        CASE 
            WHEN total_amount < 0 
            THEN 1 ELSE 0 
        END
    ) AS negative_total_amount_rows,

    SUM(
        CASE 
            WHEN PULocationID IS NULL OR DOLocationID IS NULL
            THEN 1 ELSE 0
        END
    ) AS missing_location_rows

FROM read_parquet('{TRIPS_PATH}');
""").show()

print("\n=== Taxi zone lookup sample ===")
con.sql(f"""
SELECT *
FROM read_csv_auto('{ZONES_PATH}')
LIMIT 10;
""").show(max_rows=10)

print("\n=== Taxi zone lookup row count ===")
con.sql(f"""
SELECT COUNT(*) AS zone_count
FROM read_csv_auto('{ZONES_PATH}');
""").show()

