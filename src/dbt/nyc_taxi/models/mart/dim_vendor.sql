{{
    config(
        schema='mart',
        alias='dim_vendor',
        materialized='incremental',
        unique_key='vendor_id',
        incremental_strategy='merge'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('int_metrics_added') }}
    {% if is_incremental() %}
        WHERE loaded_at > (SELECT max(loaded_at) FROM {{ this }})
    {% endif %}
),

final as (
    SELECT
        vendor_id,
        CASE
            WHEN vendor_id = 1 THEN 'Vendor One'
            WHEN vendor_id = 2 THEN 'Vendor Two'
            ELSE 'Unknown Vendor'
        END AS vendor_name,
        MAX(loaded_at) AS loaded_at
    FROM source
    GROUP BY vendor_id
    ORDER BY vendor_id ASC
)

SELECT * FROM final
