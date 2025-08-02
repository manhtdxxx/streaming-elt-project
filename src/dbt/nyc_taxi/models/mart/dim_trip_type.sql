{{
    config(
        schema='mart',
        alias='dim_trip_type',
        materialized='incremental',
        unique_key='trip_type_id',
        incremental_strategy='merge'
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
        trip_type_id,
        CASE
            WHEN trip_type_id = 1 THEN 'Street-hail'
            WHEN trip_type_id = 2 THEN 'Dispatch'
            ELSE 'Unknown Trip Type'
        END AS trip_type_name,
        MAX(loaded_at) AS loaded_at
    FROM source
    GROUP BY trip_type_id
    ORDER BY trip_type_id ASC
)

SELECT * FROM final
