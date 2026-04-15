-- One row per DP event per device.
-- Deduplicates raw rows by taking the latest ingest_ts per (device_id, log_date).
-- Unnests the payload JSON array into individual DP events.
-- Computes add_ele interval_kwh via LAG window function.
-- Negative interval_kwh indicates a counter reset (flag preserved, not dropped).

WITH deduped AS (
    SELECT
        device_id,
        category,
        log_date,
        payload,
        ROW_NUMBER() OVER (
            PARTITION BY device_id, log_date ORDER BY ingest_ts DESC
        ) AS rn
    FROM {{ source('tuya_raw', 'raw_energy_dp_log') }}
),

events AS (
    SELECT
        device_id,
        category,
        log_date,
        JSON_VALUE(event, '$.code')                                  AS dp_code,
        CAST(JSON_VALUE(event, '$.value') AS FLOAT64)                AS dp_value,
        TIMESTAMP_MILLIS(
            CAST(JSON_VALUE(event, '$.event_time') AS INT64)
        )                                                             AS event_ts
    FROM deduped,
    UNNEST(JSON_QUERY_ARRAY(payload)) AS event
    WHERE rn = 1
      AND JSON_VALUE(event, '$.code') IN (
          'add_ele', 'cur_power', 'cur_current', 'cur_voltage'
      )
),

with_lag AS (
    SELECT
        device_id,
        category,
        dp_code,
        dp_value,
        LAG(dp_value) OVER (
            PARTITION BY device_id, dp_code ORDER BY event_ts
        )                                                             AS prev_value,
        event_ts,
        LAG(event_ts) OVER (
            PARTITION BY device_id, dp_code ORDER BY event_ts
        )                                                             AS prev_event_ts,
        log_date
    FROM events
)

SELECT
    device_id,
    category,
    dp_code,
    dp_value,
    prev_value,
    event_ts,
    prev_event_ts,
    -- interval_kwh: only meaningful for add_ele (unit: 0.01 kWh → divide by 100)
    -- Negative value indicates counter reset; preserved so marts can flag it.
    CASE
        WHEN dp_code = 'add_ele' AND prev_value IS NOT NULL
            THEN (dp_value - prev_value) / 100.0
        ELSE NULL
    END AS interval_kwh,
    log_date
FROM with_lag
