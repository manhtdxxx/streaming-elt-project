{{ 
    config(
        schema = 'intermediate',
        alias = 'int_metrics_added',
        materialized = 'incremental',
        unique_key = 'trip_id',
        incremental_strategy = 'merge'
    ) 
}}

WITH source AS (
    SELECT * FROM {{ ref('int_both_taxi') }}
    {% if is_incremental() %}
        WHERE loaded_at > (SELECT max(loaded_at) FROM {{ this }})
    {% endif %}
),

after_metrics AS (
    SELECT
        *,
        -- thời lượng chuyến đi (theo minutes)
        ROUND(EXTRACT(epoch FROM (dropoff_datetime - pickup_datetime)) / 60, 0) AS trip_duration,

        -- chuyến đi đêm (pickup từ 22h -> 6h)
        CASE 
            WHEN EXTRACT(hour FROM pickup_datetime) >= 22 OR EXTRACT(hour FROM pickup_datetime) < 6 THEN 1
            ELSE 0
        END AS night_flag,

        -- chuyến đi cuối tuần (0: sunday, 6: saturday)
        CASE 
            WHEN EXTRACT(dow FROM pickup_datetime) IN (0,6) THEN 1
            ELSE 0
        END AS weekend_flag

    FROM source
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
        trip_type AS trip_type_id,
        taxi_type,
        night_flag,
        weekend_flag,
        loaded_at
    FROM after_metrics
)

SELECT * FROM final