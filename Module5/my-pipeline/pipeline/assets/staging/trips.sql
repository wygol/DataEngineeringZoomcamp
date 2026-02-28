/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: create+replace

columns:
  - name: tpep_pickup_datetime
    type: timestamp
    description: Trip start time
    primary_key: true
    checks:
      - name: not_null
  - name: tpep_dropoff_datetime
    type: timestamp
    description: Trip end time
    primary_key: true
    checks:
      - name: not_null
  - name: pu_location_id
    type: integer
    description: NYC taxi zone ID for pickup location
    primary_key: true
    checks:
      - name: not_null
  - name: do_location_id
    type: integer
    description: NYC taxi zone ID for dropoff location
    primary_key: true
    checks:
      - name: not_null
  - name: fare_amount
    type: double
    description: Trip fare in USD
    primary_key: true
    checks:
      - name: non_negative
  - name: trip_distance
    type: double
    description: Trip distance in miles
    checks:
      - name: non_negative
  - name: passenger_count
    type: double
    description: Number of passengers
    checks:
      - name: non_negative
  - name: payment_type
    type: integer
    description: Payment type ID
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Human-readable payment type (from lookup table)
    checks:
      - name: not_null

custom_checks:
  - name: no_duplicates
    description: Verify no duplicate composite keys exist (deduplication is working)
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT tpep_pickup_datetime, tpep_dropoff_datetime, pu_location_id, do_location_id, fare_amount
        FROM staging.trips
        GROUP BY tpep_pickup_datetime, tpep_dropoff_datetime, pu_location_id, do_location_id, fare_amount
        HAVING COUNT(*) > 1
      ) t
    value: 0

@bruin */

WITH raw_data AS (
  -- Fetch raw trips within the time window
  SELECT 
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    pu_location_id,
    do_location_id,
    fare_amount,
    trip_distance,
    payment_type,
    passenger_count,
    extracted_at,
    -- ROW_NUMBER for deduplication: keep only the most recently extracted record per composite key
    ROW_NUMBER() OVER (
      PARTITION BY tpep_pickup_datetime, tpep_dropoff_datetime, pu_location_id, do_location_id, fare_amount
      ORDER BY extracted_at DESC
    ) as rn
  FROM ingestion.trips
  WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
    AND tpep_pickup_datetime < '{{ end_datetime }}'
    AND fare_amount >= 0  -- Filter out invalid records with negative fares
),
deduplicated AS (
  -- Keep only the first row (most recent extraction) for each composite key
  SELECT 
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    pu_location_id,
    do_location_id,
    fare_amount,
    trip_distance,
    payment_type,
    passenger_count,
    extracted_at
  FROM raw_data
  WHERE rn = 1
),
enriched AS (
  -- Join with payment lookup to add payment_type_name
  SELECT 
    d.tpep_pickup_datetime,
    d.tpep_dropoff_datetime,
    d.pu_location_id,
    d.do_location_id,
    d.fare_amount,
    d.trip_distance,
    d.passenger_count,
    d.payment_type,
    COALESCE(p.payment_type_name, 'Unknown') as payment_type_name
  FROM deduplicated d
  LEFT JOIN ingestion.payment_lookup p
    ON d.payment_type = p.payment_type_id
)
SELECT *
FROM enriched
