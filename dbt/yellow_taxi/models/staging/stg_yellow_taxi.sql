{{ config(
    materialized='view'
) }}

WITH bronze AS (
    SELECT *
    FROM {{ ref('src_yellow_taxi') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['vendor_id', 'pickup_datetime']) }} AS id,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_date,
    passenger_count,
    store_and_fwd_flag
FROM bronze
