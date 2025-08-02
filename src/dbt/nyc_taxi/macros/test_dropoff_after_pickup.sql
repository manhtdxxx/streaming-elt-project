{% test dropoff_after_pickup(model) %}
    select *
    from {{ model }}
    where dropoff_datetime <= pickup_datetime
{% endtest %}

-- if returns 0, then True