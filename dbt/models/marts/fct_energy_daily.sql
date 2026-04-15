{{
    config(
        materialized='incremental',
        unique_key=['device_id', 'stat_date'],
        on_schema_change='fail',
    )
}}

WITH source AS (
    SELECT
        s.device_id,
        d.name,
        d.category,
        s.stat_date,
        s.energy_kwh
    FROM {{ ref('stg_energy_daily') }} s
    LEFT JOIN {{ ref('dim_devices') }} d USING (device_id)
)

SELECT *
FROM source

{% if is_incremental() %}
WHERE stat_date > (SELECT MAX(stat_date) FROM {{ this }})
{% endif %}
