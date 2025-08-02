{{ 
    config(
        schema = 'mart',
        alias = 'fact_trip',
        materialized = 'incremental',
        unique_key = 'trip_id',
        incremental_strategy = 'merge'
    ) 
}}

WITH source AS (
    SELECT * FROM {{ ref('int_metrics_added') }}
    {% if is_incremental() %}
        WHERE loaded_at > (SELECT max(loaded_at) FROM {{ this }})
    {% endif %}
),

final AS (
    SELECT
        trip_id,
        vendor_id,
        trip_type_id,
        pickup_datetime,
        dropoff_datetime,
        pu_location_id,
        do_location_id,
        ratecode_id,
        payment_id,
        passenger_count,
        trip_distance,
        trip_duration,
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
        night_flag,
        weekend_flag,
        loaded_at
    FROM source
    ORDER BY pickup_datetime DESC
)

SELECT * FROM final