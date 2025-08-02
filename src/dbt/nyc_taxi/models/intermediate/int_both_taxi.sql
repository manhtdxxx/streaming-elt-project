{{ 
    config(
        schema = 'intermediate',
        alias = 'int_both_taxi',
        materialized = 'incremental',
        unique_key = 'trip_id',
        incremental_strategy = 'merge'
    ) 
}}

WITH green_source AS (
    SELECT * FROM {{ ref('stg_green_taxi') }}
    {% if is_incremental() %}
        WHERE loaded_at > (SELECT max(loaded_at) FROM {{ this }})
    {% endif %}
),

yellow_source AS (
    SELECT * FROM {{ ref('stg_yellow_taxi') }}
    {% if is_incremental() %}
        WHERE loaded_at > (SELECT max(loaded_at) FROM {{ this }})
    {% endif %}
),

after_union AS (
    SELECT 
        trip_id, vendor_id,
        pickup_datetime,
        dropoff_datetime,
        pu_location_id,
        do_location_id,
        ratecode_id_clean AS ratecode_id,
        payment_id_clean AS payment_id,
        passenger_count,
        passenger_count_flag,
        trip_distance,
        trip_distance_flag,
        fare_amount,
        fare_amount_flag,
        extra,
        extra_flag,
        mta_tax,
        mta_tax_flag,
        tip_amount,
        tip_amount_flag,
        tolls_amount,
        tolls_amount_flag,
        improvement_surcharge,
        improvement_surcharge_flag,
        congestion_surcharge,
        congestion_surcharge_flag,
        total_amount,
        total_amount_flag,
        'green' AS taxi_type,
        trip_type,
        trip_type_flag,
        ehail_fee,
        ehail_fee_flag,
        0 AS airport_fee,
        'not_applicable' AS airport_fee_flag,
        loaded_at
    FROM green_source

    UNION ALL

    SELECT
        trip_id,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        pu_location_id,
        do_location_id,
        ratecode_id_clean AS ratecode_id,
        payment_id_clean AS payment_id,
        passenger_count,
        passenger_count_flag,
        trip_distance,
        trip_distance_flag,
        fare_amount,
        fare_amount_flag,
        extra,
        extra_flag,
        mta_tax,
        mta_tax_flag,
        tip_amount,
        tip_amount_flag,
        tolls_amount,
        tolls_amount_flag,
        improvement_surcharge,
        improvement_surcharge_flag,
        congestion_surcharge,
        congestion_surcharge_flag,
        total_amount,
        total_amount_flag,
        'yellow' AS taxi_type,
        1 AS trip_type, -- because yellow is street-hail (1), not including dispatch like green (2)
        'not_applicable' AS trip_type_flag,
        0 AS ehail_fee, -- fee when using dispatch
        'not_applicable' AS ehail_fee_flag,
        airport_fee,
        airport_fee_flag,
        loaded_at
    FROM yellow_source
),

after_filter AS (
    SELECT * 
    FROM after_union
    WHERE
        passenger_count_flag NOT IN ('missing', 'invalid')
        AND trip_distance_flag NOT IN ('missing', 'invalid')
        AND fare_amount_flag NOT IN ('missing', 'invalid')
        AND extra_flag NOT IN ('missing', 'invalid')
        AND mta_tax_flag NOT IN ('missing', 'invalid')
        AND tip_amount_flag NOT IN ('missing', 'invalid')
        AND tolls_amount_flag NOT IN ('missing', 'invalid')
        AND improvement_surcharge_flag NOT IN ('missing', 'invalid')
        AND congestion_surcharge_flag NOT IN ('missing', 'invalid')
        AND total_amount_flag NOT IN ('missing', 'invalid')
        AND ehail_fee_flag NOT IN ('missing', 'invalid')
        AND airport_fee_flag NOT IN ('missing', 'invalid')
        AND trip_type_flag NOT IN ('missing', 'invalid')
),

final AS (
    SELECT
        trip_id,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        pu_location_id,
        do_location_id,
        ratecode_id,
        payment_id,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        ehail_fee,
        airport_fee,
        total_amount,
        trip_type,
        taxi_type,
        loaded_at
    FROM after_filter
)

SELECT * FROM final
