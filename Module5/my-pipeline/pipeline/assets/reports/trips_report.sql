/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

# Reports use time_interval to rebuild only the relevant time window
# This must use the same incremental_key as staging for consistency
materialization:
  type: table
  strategy: create+replace

# Primary keys: report_date + payment_type for uniqueness
# Metrics: trip_count, total_fare, avg_distance, avg_passenger_count
columns:
  - name: report_date
    type: date
    description: Date of trips (from pickup datetime)
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Payment method (cash, card, etc.)
    primary_key: true
    checks:
      - name: not_null
  - name: trip_count
    type: bigint
    description: Number of trips
    checks:
      - name: non_negative
  - name: total_fare_amount
    type: double
    description: Total fare revenue in USD
    checks:
      - name: non_negative
  - name: avg_fare_amount
    type: double
    description: Average fare per trip
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: double
    description: Average trip distance in miles
    checks:
      - name: non_negative
  - name: avg_passenger_count
    type: double
    description: Average number of passengers per trip
    checks:
      - name: non_negative

@bruin */

-- Aggregate staging trips data by date and payment type for dashboards
-- Metrics: trip volume, revenue, distance, and passenger patterns
SELECT
  DATE(tpep_pickup_datetime) as report_date,
  payment_type_name,
  COUNT(*) as trip_count,
  SUM(fare_amount) as total_fare_amount,
  AVG(fare_amount) as avg_fare_amount,
  AVG(trip_distance) as avg_trip_distance,
  AVG(passenger_count) as avg_passenger_count
FROM staging.trips
WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
  AND tpep_pickup_datetime < '{{ end_datetime }}'
GROUP BY
  DATE(tpep_pickup_datetime),
  payment_type_name
ORDER BY
  report_date DESC,
  trip_count DESC
