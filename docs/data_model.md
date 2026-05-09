# Data Model

## Raw Layer

### raw.yellow_taxi_trips

**Grain:** one row per source trip record from a TLC monthly Yellow Taxi Parquet file.

The raw trip table preserves all source records, including records with invalid dates, zero or negative distances, negative fare amounts, and out-of-period timestamps. No business filtering is applied during ingestion.

Additional ingestion metadata is added:

- `raw_trip_id`
- `source_file`
- `source_year`
- `source_month`
- `source_row_number`
- `loaded_at`

### raw.taxi_zone_lookup

**Grain:** one row per TLC `LocationID`.

The zone lookup table maps taxi location IDs to borough, zone, and service zone.

## Staging Layer

### stg_taxi_trips

**Grain:** one row per raw trip record.

Purpose:

- standardize column names
- cast data types
- expose ingestion metadata
- create basic derived fields such as pickup date, pickup hour, and trip duration

### stg_taxi_zones

**Grain:** one row per taxi location ID.

Purpose:

- standardize zone lookup column names
- provide clean dimensional attributes for pickup and dropoff enrichment

## Intermediate Layer

### int_trip_validations

**Grain:** one row per raw trip record.

Purpose:

- calculate validation flags
- classify records as valid or invalid
- keep invalid records explainable for data quality reporting

Example validation flags:

- `invalid_duration_flag`
- `invalid_distance_flag`
- `invalid_amount_flag`
- `invalid_pickup_month_flag`
- `invalid_location_flag`
- `is_valid_trip`

## Mart Layer

### fct_taxi_trips

**Grain:** one row per valid taxi trip.

Purpose:

- business-ready fact table for taxi trip analytics
- excludes records that fail the core validity checks

### dim_taxi_zone

**Grain:** one row per taxi location ID.

Purpose:

- dimension table for pickup and dropoff zone reporting

### mart_daily_operations

**Grain:** one row per pickup date.

### mart_hourly_demand

**Grain:** one row per pickup date and pickup hour.

### mart_zone_performance

**Grain:** one row per pickup zone and reporting period.

### mart_data_quality_summary

**Grain:** one row per source month and validation issue type.