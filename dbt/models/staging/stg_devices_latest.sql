WITH ranked AS (
    SELECT
        device_id,
        JSON_VALUE(payload, '$.customName')                              AS name,
        JSON_VALUE(payload, '$.category')                                AS category,
        JSON_VALUE(payload, '$.productName')                             AS product_name,
        CAST(JSON_VALUE(payload, '$.isOnline') AS BOOL)                  AS is_online,
        TIMESTAMP_SECONDS(
            CAST(JSON_VALUE(payload, '$.activeTime') AS INT64)
        )                                                                 AS active_time,
        ingest_ts,
        ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY ingest_ts DESC) AS rn
    FROM {{ source('tuya_raw', 'raw_devices') }}
)

SELECT
    device_id,
    name,
    category,
    product_name,
    is_online,
    active_time,
    ingest_ts
FROM ranked
WHERE rn = 1
