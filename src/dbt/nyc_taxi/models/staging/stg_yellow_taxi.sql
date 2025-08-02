    {{ 
        config(
            schema = 'staging',
            alias = 'stg_yellow_taxi',
            materialized = 'incremental',
            unique_key = 'trip_id',
            incremental_strategy = 'merge'
        ) 
    }}

    WITH source AS (
        SELECT * 
        FROM {{ source('raw_taxi', 'yellow_taxi') }}
        {% if is_incremental() %}
            WHERE loaded_at > (SELECT max(loaded_at) FROM {{ this }})
        {% endif %}
    ),

    after_dropped AS (
        SELECT * 
        FROM source
        WHERE 
            trip_id IS NOT NULL
            AND VendorID IS NOT NULL
            AND tpep_pickup_datetime IS NOT NULL
            AND tpep_dropoff_datetime IS NOT NULL
            AND PULocationID IS NOT NULL
            AND DOLocationID IS NOT NULL
            AND tpep_dropoff_datetime > tpep_pickup_datetime
    ),

    after_unique AS (
        SELECT DISTINCT ON (trip_id) *
        FROM after_dropped
        ORDER BY trip_id, loaded_at DESC -- should use updated_at but since i don't have that col
    ),

    after_renamed_and_ordered AS (
        SELECT
            trip_id,
            VendorID AS vendor_id,
            tpep_pickup_datetime AS pickup_datetime,
            tpep_dropoff_datetime AS dropoff_datetime,
            PULocationID AS pu_location_id,
            DOLocationID AS do_location_id,
            RatecodeID AS ratecode_id,
            payment_type AS payment_id,
            passenger_count,
            trip_distance,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            congestion_surcharge,
            Airport_fee AS airport_fee,
            total_amount,
            loaded_at
        FROM after_unique
    ),

    after_flagged AS (
        SELECT *,
            -- ratecode_id clean
            CASE
                WHEN ratecode_id IS NULL THEN -1
                WHEN ratecode_id NOT IN (1,2,3,4,5,6) THEN -1
                ELSE ratecode_id
            END AS ratecode_id_clean,

            -- payment_id clean
            CASE
                WHEN payment_id IS NULL THEN -1
                WHEN payment_id NOT IN (1,2,3,4,5,6) THEN -1
                ELSE payment_id
            END AS payment_id_clean,

            -- passenger_count flag
            CASE
                WHEN passenger_count IS NULL THEN 'missing'
                WHEN passenger_count <= 0 THEN 'invalid'
                ELSE 'valid'
            END AS passenger_count_flag,

            -- trip_distance flag
            CASE
                WHEN trip_distance IS NULL THEN 'missing'
                WHEN trip_distance <= 0 THEN 'invalid'
                ELSE 'valid'
            END AS trip_distance_flag,

            -- fare_amount flag
            CASE
                WHEN fare_amount IS NULL THEN 'missing'
                WHEN fare_amount < 0 THEN 'invalid'
                WHEN fare_amount = 0 THEN 'zero'
                ELSE 'valid'
            END AS fare_amount_flag,

            -- extra flag
            CASE
                WHEN extra IS NULL THEN 'missing'
                WHEN extra < 0 THEN 'invalid'
                WHEN extra = 0 THEN 'zero'
                ELSE 'valid'
            END AS extra_flag,

            -- mta_tax flag
            CASE
                WHEN mta_tax IS NULL THEN 'missing'
                WHEN mta_tax < 0 THEN 'invalid'
                WHEN mta_tax = 0 THEN 'zero'
                ELSE 'valid'
            END AS mta_tax_flag,

            -- tip_amount flag
            CASE
                WHEN tip_amount IS NULL THEN 'missing'
                WHEN tip_amount < 0 THEN 'invalid'
                WHEN tip_amount = 0 THEN 'zero'
                ELSE 'valid'
            END AS tip_amount_flag,

            -- tolls_amount flag
            CASE
                WHEN tolls_amount IS NULL THEN 'missing'
                WHEN tolls_amount < 0 THEN 'invalid'
                WHEN tolls_amount = 0 THEN 'zero'
                ELSE 'valid'
            END AS tolls_amount_flag,

            -- improvement_surcharge flag
            CASE
                WHEN improvement_surcharge IS NULL THEN 'missing'
                WHEN improvement_surcharge < 0 THEN 'invalid'
                WHEN improvement_surcharge = 0 THEN 'zero'
                ELSE 'valid'
            END AS improvement_surcharge_flag,

            -- congestion_surcharge flag
            CASE
                WHEN congestion_surcharge IS NULL THEN 'missing'
                WHEN congestion_surcharge < 0 THEN 'invalid'
                WHEN congestion_surcharge = 0 THEN 'zero'
                ELSE 'valid'
            END AS congestion_surcharge_flag,

            -- airport_fee flag (yellow taxi only)
            CASE
                WHEN airport_fee IS NULL THEN 'missing'
                WHEN airport_fee < 0 THEN 'invalid'
                WHEN airport_fee = 0 THEN 'zero'
                ELSE 'valid'
            END AS airport_fee_flag,

            -- total_amount flag
            CASE
                WHEN total_amount IS NULL THEN 'missing'
                WHEN total_amount < 0 THEN 'invalid'
                WHEN total_amount = 0 THEN 'zero'
                ELSE 'valid'
            END AS total_amount_flag

        FROM after_renamed_and_ordered
    )

    -- final
    SELECT * FROM after_flagged
