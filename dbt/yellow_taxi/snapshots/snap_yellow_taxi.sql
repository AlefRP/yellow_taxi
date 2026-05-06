{% snapshot snap_yellow_taxi %}

{{
   config(
       target_schema='taxis_ny',
       strategy='timestamp',
       unique_key=['VendorID', 'tpep_pickup_datetime'],
       updated_at='tpep_pickup_datetime',
       invalidate_hard_deletes=True
   )
}}

SELECT *
FROM {{ source('dbtbigq', 'raw_yellow_taxi') }}

{% endsnapshot %}
