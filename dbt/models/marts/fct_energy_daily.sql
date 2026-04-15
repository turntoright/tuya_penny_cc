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
-- Use a 2-day lookback so the current in-progress day gets updated on each run.
-- The unique_key MERGE handles deduplication of existing rows.
WHERE stat_date >= DATE_SUB((SELECT MAX(stat_date) FROM {{ this }}), INTERVAL 2 DAY)
{% endif %}
