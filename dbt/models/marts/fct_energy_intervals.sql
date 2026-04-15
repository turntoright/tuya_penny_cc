{{
    config(
        materialized='incremental',
        unique_key=['device_id', 'event_ts'],
        on_schema_change='fail',
    )
}}

WITH source AS (
    SELECT
        s.device_id,
        d.name,
        d.category,
        s.event_ts,
        s.prev_event_ts,
        TIMESTAMP_DIFF(s.event_ts, s.prev_event_ts, MINUTE) AS interval_minutes,
        s.interval_kwh,
        s.dp_value / 100.0                                   AS cumulative_kwh,
        s.interval_kwh < 0                                   AS is_counter_reset
    FROM {{ ref('stg_energy_dp_log') }} s
    LEFT JOIN {{ ref('dim_devices') }} d USING (device_id)
    WHERE s.interval_kwh IS NOT NULL
)

SELECT *
FROM source

{% if is_incremental() %}
WHERE event_ts > (SELECT MAX(event_ts) FROM {{ this }})
{% endif %}
