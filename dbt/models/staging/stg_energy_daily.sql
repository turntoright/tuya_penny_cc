-- Daily energy derived from DP event log.
-- See stg_energy_hourly for rationale.
WITH add_ele AS (
    SELECT
        device_id,
        category,
        event_ts,
        interval_kwh
    FROM {{ ref('stg_energy_dp_log') }}
    WHERE interval_kwh > 0   -- exclude counter resets
)

SELECT
    device_id,
    category,
    DATE(event_ts) AS stat_date,
    SUM(interval_kwh) AS energy_kwh
FROM add_ele
GROUP BY 1, 2, 3
