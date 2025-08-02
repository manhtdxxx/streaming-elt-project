{{
    config(
        schema='mart',
        alias='dim_ratecode',
        materialized='incremental',
        unique_key='ratecode_id',
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
        ratecode_id,
        CASE
            WHEN ratecode_id = 1 THEN 'Standard rate'
            WHEN ratecode_id = 2 THEN 'JFK'
            WHEN ratecode_id = 3 THEN 'Newark'
            WHEN ratecode_id = 4 THEN 'Nassau or Westchester'
            WHEN ratecode_id = 5 THEN 'Negotiated fare'
            WHEN ratecode_id = 6 THEN 'Group ride'
            ELSE 'Unknown Ratecode'
        END AS ratecode_name,
        MAX(loaded_at) AS loaded_at
    FROM source
    GROUP BY ratecode_id
    ORDER BY ratecode_id ASC
)

SELECT * FROM final
