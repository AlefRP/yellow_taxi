{{ config(
    materialized='table',
    partition_by={
      "field": "pickup_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["vendor_id"]
) }}

WITH raw AS (
    SELECT *
    FROM {{ ref('src_yellow_taxi') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['VendorID', 'tpep_pickup_datetime']) }} AS id,
    VendorID                                AS vendor_id,
    tpep_pickup_datetime                    AS pickup_datetime,
    tpep_dropoff_datetime                   AS dropoff_datetime,
    DATE(tpep_pickup_datetime)              AS pickup_date,
    passenger_count,
    store_and_fwd_flag
FROM raw
