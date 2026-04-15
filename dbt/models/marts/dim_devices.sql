SELECT
    d.device_id,
    d.name,
    d.category,
    d.product_name,
    d.is_online,
    d.active_time,
    r.parent_device_id
FROM {{ ref('stg_devices_latest') }} d
LEFT JOIN {{ ref('device_relationships') }} r
    ON d.device_id = r.child_device_id
