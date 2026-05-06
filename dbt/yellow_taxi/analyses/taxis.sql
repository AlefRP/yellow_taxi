WITH taxi AS (
    SELECT * FROM {{ source('dbtbigq', 'raw_yellow_taxi') }}
)
SELECT
    SUM(fare_amount) AS total_fare
FROM taxi