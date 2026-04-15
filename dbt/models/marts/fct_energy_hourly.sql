{{
    config(
        materialized='incremental',
        unique_key=['device_id', 'stat_hour'],
        on_schema_change='fail',
    )
}}

WITH source AS (
    SELECT
        s.device_id,
        d.name,
        d.category,
        s.stat_hour,
        s.energy_kwh
    FROM {{ ref('stg_energy_hourly') }} s
    LEFT JOIN {{ ref('dim_devices') }} d USING (device_id)
)

SELECT *
FROM source

{% if is_incremental() %}
WHERE stat_hour > (SELECT MAX(stat_hour) FROM {{ this }})
{% endif %}
