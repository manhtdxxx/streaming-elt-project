{{
    config(
        schema='mart',
        alias='dim_payment',
        materialized='incremental',
        unique_key='payment_id',
        incremental_strategy='merge'
    )
}}

WITH source AS (
    SELECT * 
    FROM {{ ref('int_metrics_added') }}
    {% if is_incremental() %}
        WHERE loaded_at > (SELECT max(loaded_at) FROM {{ this }})
    {% endif %}
),

final AS (
    SELECT
        payment_id,
        CASE
            WHEN payment_id = 1 THEN 'Credit card'
            WHEN payment_id = 2 THEN 'Cash'
            WHEN payment_id = 3 THEN 'No charge'
            WHEN payment_id = 4 THEN 'Dispute'
            WHEN payment_id = 5 THEN 'Unknown'
            WHEN payment_id = 6 THEN 'Voided trip'
            ELSE 'Other'
        END AS payment_name,
        MAX(loaded_at) AS loaded_at
    FROM source
    GROUP BY payment_id
    ORDER BY payment_id ASC
)

SELECT * FROM final
