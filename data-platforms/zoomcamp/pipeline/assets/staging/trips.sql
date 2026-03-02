/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the trip started"
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "When the trip ended"
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Type of taxi (yellow or green)"
    primary_key: true
    checks:
      - name: not_null
  - name: vendor_id
    type: integer
    description: "TPEP/LPEP provider code"
  - name: passenger_count
    type: integer
    description: "Number of passengers in the vehicle"
  - name: trip_distance
    type: float
    description: "Elapsed trip distance in miles"
    checks:
      - name: non_negative
  - name: rate_code_id
    type: integer
    description: "Final rate code in effect at the end of the trip"
  - name: pickup_location_id
    type: integer
    description: "TLC Taxi Zone where the trip started"
    primary_key: true
  - name: dropoff_location_id
    type: integer
    description: "TLC Taxi Zone where the trip ended"
    primary_key: true
  - name: payment_type_id
    type: integer
    description: "Numeric payment method code"
  - name: payment_type_name
    type: string
    description: "Human-readable payment method from lookup table"
  - name: fare_amount
    type: float
    description: "Base fare in USD"
    primary_key: true
    checks:
      - name: non_negative
  - name: tip_amount
    type: float
    description: "Tip amount in USD"
    checks:
      - name: non_negative
  - name: tolls_amount
    type: float
    description: "Total tolls paid in USD"
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    description: "Total amount charged to passengers in USD"
  - name: extracted_at
    type: timestamp
    description: "UTC timestamp when this row was ingested"

custom_checks:
  - name: row_count_positive
    description: Ensures the table is not empty
    query: SELECT COUNT(*) > 0 FROM staging.trips
    value: 1

@bruin */

WITH deduplicated AS (
    SELECT
        COALESCE(tpep_pickup_datetime, lpep_pickup_datetime)   AS pickup_datetime,
        COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) AS dropoff_datetime,
        taxi_type,
        VendorID        AS vendor_id,
        passenger_count,
        trip_distance,
        RatecodeID      AS rate_code_id,
        PULocationID    AS pickup_location_id,
        DOLocationID    AS dropoff_location_id,
        payment_type    AS payment_type_id,
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,
        extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY
                COALESCE(tpep_pickup_datetime, lpep_pickup_datetime),
                COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime),
                PULocationID,
                DOLocationID,
                fare_amount
            ORDER BY extracted_at DESC
        ) AS rn
    FROM ingestion.trips
    WHERE COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) >= '{{ start_datetime }}'
      AND COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) < '{{ end_datetime }}'
)
SELECT
    d.pickup_datetime,
    d.dropoff_datetime,
    d.taxi_type,
    d.vendor_id,
    d.passenger_count,
    d.trip_distance,
    d.rate_code_id,
    d.pickup_location_id,
    d.dropoff_location_id,
    d.payment_type_id,
    p.payment_type_name,
    d.fare_amount,
    d.tip_amount,
    d.tolls_amount,
    d.total_amount,
    d.extracted_at
FROM deduplicated d
LEFT JOIN ingestion.payment_lookup p
    ON d.payment_type_id = p.payment_type_id
WHERE d.rn = 1
  AND d.pickup_datetime IS NOT NULL
  AND d.dropoff_datetime IS NOT NULL
