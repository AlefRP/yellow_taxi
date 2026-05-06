-- Falha se houver linhas com passenger_count negativo na bronze.
SELECT *
FROM {{ source('bronze', 'bronze_yellow_taxi') }}
WHERE passenger_count < 0
