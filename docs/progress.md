# 项目进展记录

> 本文档记录 tuya_penny_cc 项目的设计决策、实施进展和后续计划。

---

## 项目背景

本项目为一个 Tuya 开发者账号下的 IoT 能耗监控管道。
目标是从 Tuya OpenAPI 抓取设备元数据与能耗数据，存入 BigQuery，
并通过 dbt 转换为可分析的数据集。

设备规模：约 100 台以内，每天采集 1 到数次。
设备类型：
- 12 台 `znjdq` A5 智能继电器开关（均在线）
- 5 台 `dlq` WIFI Smart Meter Pro W 智能电表（均离线）

所有设备位于新西兰基督城（`lat: -43.5, lon: 172.58, timeZone: +12:00`），
共享同一个 `bindSpaceId: 261066752`。

---

## 整体架构决策

### 运行环境

选择 **Cloud Run Job**（非 Cloud Functions、非 App Engine）：
- 任务型工作负载，不需要常驻进程
- 支持长时间运行（回填场景）
- 单镜像多 `--task` 参数分发，降低维护成本
- 调度由 Cloud Scheduler 驱动

### 数据存储

BigQuery append-only 原始表（`tuya_raw` dataset）：
- 每次写入都保留完整历史，永不修改原始数据
- 配套 lineage 字段：`ingest_ts`、`ingest_run_id`、`source_endpoint`、`ingest_date`
- `payload` 列使用 `JSON` 类型存储完整设备对象

### 设备拓扑

设备之间存在级联关系（父设备 → 子设备）：
- 拓扑关系通过 `dbt/seeds/device_relationships.csv` 管理（Git 版本控制）
- 变更历史通过 dbt SCD Type 2 快照（`snap_device_relationships`）追踪
- 父设备净负载公式：`父实测电量 ≈ 子设备总和 + 父自身负载`（在 marts 层计算）

### 设备名称变更追踪

- `stg_devices_latest` 视图：每台设备取最新一次 `device_sync` 记录
- `snap_devices` 快照：对 `name`、`category`、`productName` 字段做 SCD Type 2 追踪
- 每次名称变更都会生成新行并关闭旧行，保留完整历史

---

## 实施计划拆分

| 计划 | 描述 | 状态 |
|---|---|---|
| **A — Ingestion MVP** | device_sync 作业、BQ 写入器、CLI 入口、21 个单元测试 | ✅ 已完成 |
| **B — 能耗作业** | energy_realtime、hourly、daily、历史回填（含 backfill CLI） | ✅ 已完成 |
| **C — dbt 项目** | staging、快照（SCD2）、marts、拓扑层 | ⬜ 未开始 |
| **D — 云部署** | Cloud Run、Scheduler、Secret Manager、CI/CD | ⬜ 未开始 |

---

## Plan A — Ingestion MVP 完成情况

### 实施方式

采用 subagent 驱动开发（subagent-driven-development skill）：
- 每个任务派发独立 subagent 实现，避免上下文污染
- 每个任务完成后做两阶段 review：先检查 spec 符合性，再检查代码质量
- 全程 TDD：先写失败测试，再写最小实现，最后通过并提交

### 9 个任务清单

| # | 内容 | 状态 |
|---|---|---|
| 1 | 项目脚手架（pyproject.toml、目录结构、ruff 配置） | ✅ |
| 2 | Settings 配置（pydantic-settings + .env 加载） | ✅ |
| 3 | Tuya Auth 签名模块（HMAC-SHA256，纯函数） | ✅ |
| 4 | TuyaClient（token 缓存、分页请求、tenacity 重试） | ✅ |
| 5 | `list_devices()` 分页实现 | ✅ |
| 6 | BQ schemas（RAW_DEVICES_SCHEMA） | ✅ |
| 7 | BigQueryWriter（load_table_from_json，append-only） | ✅ |
| 8 | CLI 入口（typer + StrEnum task 分发） | ✅ |
| 9 | device_sync 作业（protocol 协议、lineage 字段写入） | ✅ |

最终测试结果：**21 个单元测试全部通过**，ruff 无告警。

### 技术栈

- **Python 3.11+**，包管理器 **uv**
- **httpx** HTTP 客户端，**tenacity** 重试
- **pydantic-settings** 配置管理，**SecretStr** 保护凭据
- **typer** CLI，**StrEnum** 任务分发
- **google-cloud-bigquery** BQ 客户端
- **pytest + respx** 单元测试（HTTP mock）
- **ruff** 代码质量（E/F/I/B/UP/SIM，line-length=100）

---

## Smoke Run 发现的问题

完成 Plan A 实现后，对真实账号进行了冒烟测试，发现并修复了两个关键问题：

### 问题 1：API 端点选错

**现象：** 原实现使用 `/v1.3/iot-03/devices`，返回 `total: 0`，设备列表为空，且没有任何错误提示。

**根因：** 该端点在本账号下没有订阅权限，静默返回空结果。

**修复：** 切换到 `/v2.0/cloud/thing/device`，成功返回全部 17 台设备。

**v2.0 端点差异：**
- 响应结构：`result` 是平铺列表（非 `result.list`）
- 字段命名：camelCase（`isOnline`、`customName`、`activeTime`）
- 分页方式：`page_no`（1-indexed）+ `page_size`（最大 **20**，超过报 `40000904`）
- 结束判断：`len(result) < page_size`（无 `has_more` 字段）

**影响：** 3 个单元测试需同步更新端点和响应结构，修改后全部通过。

### 问题 2：BigQuery JSON 列双重编码

**现象：** 原实现对 device dict 调用了 `json.dumps()` 再存入 BQ `JSON` 列，导致 SQL 中 `JSON_VALUE()` 查询失败（值被当作字符串而非 JSON 对象）。

**修复：** 直接传入 dict，不调用 `json.dumps()`。

```python
# 错误（BQ 会把整个字符串当作 JSON 值）
"payload": json.dumps(device)

# 正确（BQ 直接存储为 JSON 对象）
"payload": device
```

完整记录见：`ingestion/docs/tuya_api_notes.md`

---

## 代码提交历史（Plan A）

| Commit | 内容 |
|---|---|
| `init` | 初始仓库结构 |
| 脚手架 | pyproject.toml、src 布局、ruff 配置 |
| auth | HMAC-SHA256 签名模块 + 6 个测试 |
| client | TuyaClient token 缓存 + ruff 修复（line-length） |
| list_devices | 分页实现（yield from 优化） |
| schemas | RAW_DEVICES_SCHEMA |
| writer | BigQueryWriter + UTC 修复（UP017） |
| cli | main.py typer + StrEnum（B008 noqa） |
| device_sync | 作业模块 + protocol 协议 |
| api-fix | 切换 v2.0 端点 + BQ JSON 修复 + 3 个测试更新 |
| docs | tuya_api_notes.md（API 发现记录） |

---

## Plan B — 能耗作业完成情况

| 作业 | 状态 |
|---|---|
| `energy_realtime` | ✅ 已完成（2026-04-14） |
| `energy_hourly` | ✅ 已完成（2026-04-14） |
| `energy_daily` | ✅ 已完成（2026-04-14） |
| 历史回填（backfill CLI） | ✅ 已完成（集成至 hourly/daily，2026-04-14） |

### B.1 — energy_realtime 完成情况

#### 实施方式

采用与 Plan A 相同的 subagent 驱动开发 + 两阶段 review（spec 符合性 + 代码质量）。

#### 新增组件

| 文件 | 内容 |
|---|---|
| `tuya/client.py` | 新增 `get_device_dps(device_id)` 方法，调用 `/v1.0/iot-03/devices/{id}/status` |
| `bq/schemas.py` | 新增 `RAW_ENERGY_REALTIME_SCHEMA`（含 `category` 列） |
| `jobs/energy_realtime.py` | 能耗实时作业：过滤在线设备 → 拉取 DP → 写入 BQ |
| `main.py` | `Task.energy_realtime` 加入 StrEnum，`match/case` 分发 |

#### 关键设计决策

- **只采集在线设备**：`isOnline == False` 的设备静默跳过，不写入任何行
- **`payload` 存 DP 列表**（`list[dict]`），不做 `json.dumps()`，与 device_sync 一致
- **`source_endpoint` 按设备展开**：每行记录实际调用的完整路径（含 device_id）
- **`DPS_PATH` 单一定义**：`energy_realtime.py` 直接引用 `TuyaClient.DPS_PATH`，避免路径常量重复
- **`category` 字段强制存在**：使用 `device["category"]`（非 `.get()`），缺失时快速失败

#### 测试结果

**27 个单元测试全部通过**，ruff 无告警。新增 6 个测试（2 个 client + 4 个 job）。

#### 提交历史

| Commit | 内容 |
|---|---|
| `b2ac063` | feat(tuya): add get_device_dps() to TuyaClient |
| `cc622d6` | fix(tuya): narrow retry docstring; url-encode device_id |
| `801cf5a` | feat(bq): add RAW_ENERGY_REALTIME_SCHEMA |
| `a0dbc1b` | feat(jobs): add energy_realtime job |
| `932f4c8` | fix(jobs): import DPS_PATH from client; harden category field |
| `4bcd93d` | feat(cli): wire energy_realtime task into CLI dispatcher |
| `658accb` | fix(cli): replace dead case fallback with assert_never |

---

## Plan B — B.2 energy_hourly / energy_daily 完成情况

### 实施方式

与 B.1 相同：subagent 驱动开发 + 两阶段 review（spec 符合性 + 代码质量），每个 task 独立提交。

### 新增组件

| 文件 | 内容 |
|---|---|
| `tuya/client.py` | 新增 `get_energy_stats(device_id, granularity, start_ts_ms, end_ts_ms)` |
| `bq/schemas.py` | 新增 `RAW_ENERGY_HOURLY_SCHEMA`、`RAW_ENERGY_DAILY_SCHEMA` |
| `jobs/energy_hourly.py` | 小时聚合作业，默认采集上一个完整小时 |
| `jobs/energy_daily.py` | 日聚合作业，默认采集昨天 |
| `main.py` | 新增 `Task.energy_hourly`、`Task.energy_daily`；新增 `--date`、`--start-date`、`--end-date` 参数 |

### 关键设计决策

- **端点（待 smoke test 确认）：** `GET /v1.0/iot-03/devices/{device_id}/statistics-month`
  - 查询参数：`type`（`"hour"` / `"day"`）、`start_time`（ms）、`end_time`（ms）
  - 路径和参数名为 Tuya OpenAPI 惯例推断，首次实际运行时需对照响应做确认
- **时间窗口（全部 UTC）：**
  - `energy_hourly`：默认 = 上一个完整小时；`--date` → 24 个窗口；`--start-date/--end-date` → 逐天展开 × 24
  - `energy_daily`：默认 = 昨天；`--date` → 1 个窗口；`--start-date/--end-date` → 逐天展开 × 1
- **`end_ms` 精确公式：** 排他性上界 `int(next_boundary.timestamp() * 1000) - 1`，无毫秒遗漏
- **backfill 集成进 CLI：** `--date` 与 `--start-date/--end-date` 互斥；单独传其中一个校验报错
- **只采集在线设备：** `isOnline == False` 静默跳过（与 `energy_realtime` 一致）
- **`payload` 存完整聚合列表：** `list[dict]`，直接传入，不做 `json.dumps()`

### 测试结果

**43 个单元测试全部通过**，ruff 无告警。新增 16 个测试（3 client + 6 hourly + 6 daily + 1 main）。

### 提交历史

| Commit | 内容 |
|---|---|
| `23bbbe6` | docs: add energy_hourly & energy_daily job design spec (Plan B) |
| `39fec3c` | docs: add energy_hourly & energy_daily implementation plan (Plan B) |
| `6bee5a0` | feat(tuya): add get_energy_stats() to TuyaClient |
| `12938f2` | fix(tuya): validate granularity in get_energy_stats |
| `5cccc13` | feat(bq): add RAW_ENERGY_HOURLY_SCHEMA and RAW_ENERGY_DAILY_SCHEMA |
| `4342ebe` | feat(jobs): add energy_hourly job |
| `df8b6be` | fix(jobs): remove page_size param; use get() for isOnline |
| `9a90cb0` | fix(jobs): guard partial date range; add boundary tests |
| `1fe0a55` | feat(jobs): add energy_daily job |
| `cdc7ecf` | fix(tests): remove dead list_devices setup in partial date range test |
| `7b89eb2` | fix(jobs): use exclusive end_ms for energy_daily day window |
| `bcb8251` | feat(cli): add energy_hourly and energy_daily tasks with date params |
| `07e0bb6` | style(cli): wrap long typer.Option lines under 100 chars |

---

## 下一步计划

### Plan C — dbt 项目

- `tuya_staging` dataset：设备去重、类型转换、字段重命名（camelCase → snake_case）
- `snap_devices` SCD2 快照：追踪名称和分类变更
- `snap_device_relationships` SCD2 快照：追踪拓扑变更
- `tuya_marts` dataset：设备维度表、能耗事实表、拓扑计算（父净负载）

### Plan D — 云部署

- Artifact Registry 镜像构建
- Cloud Run Job 部署（每个 `--task` 对应一个 Scheduler job）
- Secret Manager 管理 Tuya 凭据
- GitHub Actions CI/CD（测试 + lint + 镜像构建）
