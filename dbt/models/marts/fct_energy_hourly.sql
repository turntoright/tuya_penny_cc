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
-- Use a 2-hour lookback so the current in-progress hour gets updated on each run.
-- The unique_key MERGE handles deduplication of existing rows.
WHERE stat_hour >= TIMESTAMP_SUB((SELECT MAX(stat_hour) FROM {{ this }}), INTERVAL 2 HOUR)
{% endif %}
