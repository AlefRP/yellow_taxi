{{ config(materialized='table') }}

WITH trips AS (
    SELECT
        vendor_id,
        passenger_count
    FROM {{ ref('stg_yellow_taxi') }}
)

SELECT
    vendor_id,
    SUM(passenger_count) AS total_passengers,
    COUNT(*)             AS total_trips
FROM trips
GROUP BY vendor_id
HAVING SUM(passenger_count) > 0
