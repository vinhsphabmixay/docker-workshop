-- depends_on: {{ source('staging', 'green_tripdata_20192020') }}
{{
    config(
        materialized='view'
    )
}}
with tripdata as
(
    SELECT
        *
        ,ROW_NUMBER() OVER(PARTITION BY vendorid, lpep_pickup_datetime) AS rn
    FROM {{ source('staging', 'green_tripdata_20192020') }}
    WHERE vendorid IS NOT NULL
)
SELECT
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['vendorid', 'lpep_pickup_datetime'])}} as tripid
    , cast(vendorid AS INTEGER) as vendorid
    , cast(ratecodeid as integer) as ratecodeid
    , cast(pulocationid as integer) as pickup_locationid
    , cast(dolocationid as integer) as dropoff_locationid

    -- timestamps
    , cast(lpep_pickup_datetime as timestamp) as pickup_datetime
    , cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime

    -- trip info
    , store_and_fwd_flag
    , cast(passenger_count as integer) as passenger_count
    , cast(trip_distance as numeric) as trip_distance
    , cast(trip_type as integer) as trip_type

    -- payment info
    , cast(fare_amount as numeric) as fare_amount
    , cast(extra as numeric) as extra
    , cast(mta_tax as numeric) as mta_tax
    , cast(tip_amount as numeric) as tip_amount
    , cast(tolls_amount as numeric) as tolls_amount
    , cast(ehail_fee as numeric) as ehail_fee
    , cast(improvement_surcharge as numeric) as improvement_surcharge
    , cast(total_amount as numeric) as total_amount
    , coalesce(cast(payment_type as integer), 0) as payment_type
    , {{taxi_rides_ny.get_payment_type_description('payment_type')}} as payment_type_description
FROM tripdata
WHERE rn = 1

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

    limit 100
{% endif %}
