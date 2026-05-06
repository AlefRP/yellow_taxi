-- Falha se houver linhas com passenger_count negativo no source.
SELECT *
FROM {{ ref('src_yellow_taxi') }}
WHERE passenger_count < 0
