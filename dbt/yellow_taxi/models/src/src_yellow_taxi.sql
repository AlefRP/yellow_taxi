SELECT *
FROM {{ source('dbtbigq', 'raw_yellow_taxi') }}
