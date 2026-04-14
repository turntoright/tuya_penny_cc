---
name: Tuya OpenAPI — findings and gotchas
description: Practical notes on API endpoint selection, pagination, authentication, and BigQuery loading for this project
type: reference
---

# Tuya OpenAPI — findings and gotchas

Collected during initial bring-up (2026-04-14).

## 1. Device-list endpoint

### Use `/v2.0/cloud/thing/device`, not `/v1.3/iot-03/devices`

| Endpoint | Behaviour on this account |
|---|---|
| `GET /v1.3/iot-03/devices` | Returns `200 OK` with `total: 0`, empty `list` — silently wrong |
| `GET /v1.1/iot-03/devices` | Returns `28841106 No permissions. This API is not subscribed.` |
| `GET /v2.0/cloud/thing/device` | Returns all 17 devices correctly |

The v1.3 endpoint appears to need a project-level subscription or a different scope that this project's credentials do not have. The v2.0 endpoint works out of the box.

### v2 response structure

```jsonc
{
  "success": true,
  "result": [          // flat list, NOT result.list
    {
      "id": "eb06bb8cb363e2e6da4dv4",
      "name": "WIFI Smart Meter Pro W 5",
      "category": "dlq",
      "isOnline": false,
      "activeTime": 1757384263,
      "createTime": 1757384263,
      "updateTime": 1757384312,
      "productId": "cx6evwvj8f46rhie",
      "productName": "W 1P40 1P63 保护款",
      "customName": "楼上客厅空调",
      "uuid": "0b2861c88abbbf78",
      "localKey": "...",
      "ip": "...",
      "lat": "-43.5",
      "lon": "172.58",
      "timeZone": "+12:00",
      "bindSpaceId": "261066752",
      "sub": false,
      "model": "",
      "icon": "smart/icon/..."
    },
    ...
  ],
  "t": 1776151690370,
  "tid": "..."
  // no "total", no "has_more"
}
```

Note: field names are **camelCase** (unlike the v1.3 endpoint which uses snake_case).

### Pagination

The v2 endpoint uses **page-number** pagination (`page_no`, `page_no`), not cursor-based (`last_row_key`).

- Query params: `page_size` (int), `page_no` (int, 1-indexed)
- **`page_size` max is 20** — passing 100 returns `40000904 param size too much`
- There is no `has_more` flag; infer end-of-data when `len(result) < page_size`
- There is no top-level `total` count in the response

## 2. Authentication

Token is fetched via `GET /v1.0/token?grant_type=1`. The response includes a `uid` field:

```json
{ "uid": "bay1755554604433hMSM" }
```

This is the platform UID, not the short value in `TUYA_USER_UID` in `.env`. The short value (`6FUT1X5K`) appears to be an internal reference; **it is not used as a query filter** by the current implementation. The device list is scoped to the project's credentials automatically.

The access token expires after ~6700 seconds (~1 h 52 m). The client refreshes automatically when less than 5 minutes remain.

## 3. BigQuery setup

The dataset must be created manually before the first run:

```bash
bq mk --dataset --location=US <GCP_PROJECT_ID>:<BQ_DATASET_RAW>
# e.g.
bq mk --dataset --location=US penny-rent:tuya_raw
```

### JSON column storage

The `payload` column is `JSON` type. Pass the device dict **directly** — do **not** call `json.dumps()` first.

```python
# Wrong: double-encodes as a JSON string, breaking JSON_VALUE() queries
"payload": json.dumps(device)

# Correct: BigQuery stores it as a JSON object
"payload": device
```

With the correct approach, field extraction in SQL works as expected:

```sql
SELECT
  device_id,
  JSON_VALUE(payload, '$.name')     AS name,
  JSON_VALUE(payload, '$.category') AS category,
  JSON_VALUE(payload, '$.isOnline') AS is_online
FROM `<project>.<dataset>.raw_devices`
```

## 4. Devices in this project (snapshot 2026-04-14)

| Category code | Product | Count | Online |
|---|---|---|---|
| `znjdq` | A5 智能继电器开关 (smart relay switch) | 12 | 12 |
| `dlq` | WIFI Smart Meter Pro W (power meter) | 5 | 0 |

All devices share `bindSpaceId: 261066752` and are located near Christchurch, NZ (`lat: -43.5, lon: 172.58, timeZone: +12:00`).
