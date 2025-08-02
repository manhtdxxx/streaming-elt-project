{{ 
    config(
        schema = 'mart',
        alias = 'dim_location',
        materialized = 'incremental',
        unique_key = 'location_id',
        incremental_strategy = 'merge'
    ) 
}}


WITH source AS (
    SELECT 
        pu_location_id AS location_id,
        loaded_at
    FROM {{ ref('int_metrics_added') }}
    {% if is_incremental() %}
        WHERE loaded_at > (SELECT max(loaded_at) FROM {{ this }})
    {% endif %}

    UNION ALL

    SELECT 
        do_location_id AS location_id,
        loaded_at
    FROM {{ ref('int_metrics_added') }}
    {% if is_incremental() %}
        WHERE loaded_at > (SELECT max(loaded_at) FROM {{ this }})
    {% endif %}
),


final AS (
    SELECT
        location_id,
        'Location ' || LPAD(CAST(location_id AS TEXT), 3, '0') AS location_name,
        MAX(loaded_at) AS loaded_at
    FROM source
    GROUP BY location_id
    ORDER BY location_id ASC
)

SELECT * FROM final