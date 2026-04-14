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
| **B — 能耗作业** | energy_realtime、hourly、daily、历史回填 | ⬜ 未开始 |
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

## 下一步计划

### Plan B — 能耗作业

需要实现三类能耗数据采集：

| 作业 | 端点（待确认） | 采集频率 |
|---|---|---|
| `energy_realtime` | DP 实时读数（cur_power, add_ele 等） | 每天 1–6 次 |
| `energy_hourly` | Tuya 小时聚合 | 每天 1 次 |
| `energy_daily` | Tuya 日聚合 | 每天 1 次 |

注意事项：
- 两类设备（relay switch `znjdq` vs power meter `dlq`）的 DP 字段不同，需分别处理
- 历史回填作业（backfill）需支持日期范围参数
- 端点选择需通过实际 API 测试验证（参考 v2.0 经验）

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
