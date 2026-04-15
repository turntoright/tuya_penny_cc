-- Hourly energy derived from DP event log.
-- The Tuya statistics-month endpoint is not available on this account,
-- so we aggregate add_ele intervals from stg_energy_dp_log.
WITH add_ele AS (
    SELECT
        device_id,
        category,
        event_ts,
        interval_kwh
    FROM {{ ref('stg_energy_dp_log') }}
    WHERE interval_kwh > 0   -- exclude counter resets (negative deltas)
)

SELECT
    device_id,
    category,
    TIMESTAMP_TRUNC(event_ts, HOUR) AS stat_hour,
    SUM(interval_kwh)               AS energy_kwh
FROM add_ele
GROUP BY 1, 2, 3
