# Tuya IoT 系统设计 — 中文说明 / 决策过程

> 本文档是对 `2026-04-14-tuya-iot-design.md` 这份正式 spec 的中文说明，
> 记录脑暴过程中的关键决策、选项对比和取舍逻辑。
> 正式 spec 是英文的"做什么、怎么做"，本文档是中文的"为什么这么做"。

## 1. 项目背景

用户拥有 Tuya developer 账号，账号下挂着约 100 台 IoT 设备（主要是智能继电器开关）。
需求：

- 读取设备元数据（名称、ID、类别等），追踪设备名变化
- 读取详细电量消耗数据（每天 1 到数次）
- 全部数据存进 BigQuery 供分析
- 系统要可扩展

工作目录 `D:\git\dbt_projects\tuya_penny_cc`，启动时是空仓库（仅 `.gitignore`）。

## 2. 关键决策一览

| # | 决策点 | 用户选择 | 备选 | 取舍逻辑 |
|---|---|---|---|---|
| 1 | 运行环境 | GCP 全托管 | 本地 cron / 容器化 | 与 BigQuery 同生态，免运维 |
| 2 | 电量数据颗粒度 | 全都要（实时累计 + 小时 + 日） | 单一颗粒度 | 数据冗余但分析灵活，规模小不心疼 |
| 3 | 规模 | 100 台以内 / 1–N 次每天 | — | 决定了用 Cloud Run Job 而非 Function，无需担心 API 配额 |
| 4 | 仓库职责划分 | B：单仓库 + 内部分层（ingestion + dbt） | A：拆两个仓库 / C：不用 dbt | 单人维护 + 路径已暗示用 dbt + dbt snapshot 完美解决设备名追踪 |
| 5 | 设备拓扑 | dbt seed CSV + dbt snapshot | BQ 表 / Google Sheet | 版本控制、CI 可校验、变化即 git commit |
| 6 | 拓扑关系 | 邻接表 + 多层级 + 多 relationship_type | 单亲单层 | 预留扩展性 |

## 3. 重要架构原则

### 3.1 三层关注点分离

```
ingestion (Python)        →   "原样搬运"（不做业务逻辑）
        │
        ▼
tuya_raw (BQ, append-only) →   不可变记录，可重放
        │
        ▼
dbt staging/marts (SQL)    →   所有清洗、聚合、建模
```

任何业务理解（怎么解析 DP、怎么 dedup、怎么递归拓扑）都属于 dbt 层。
ingestion 的职责被严格限制在"调 API → 写 raw"，将来换数据源/换仓库代价最小。

### 3.2 Raw 层的 `payload JSON` 列

**不要把 Tuya 返回的每个字段都拍平成强类型列**，理由：

- Tuya 偶尔会加新 DP 字段，强类型 schema 会被打破
- 不同设备品类（继电器/灯/传感器）字段差别很大，强类型很臃肿
- 解析逻辑放 dbt staging，改起来是 SQL PR，不需要重新部署 ingestion

代价：查询时需要 `JSON_EXTRACT`，但在 staging 层做一次解析就能消除。

### 3.3 `ingest_*` 三件套（血缘列）

每条 raw 记录都带：

- `ingest_ts` — 写入时间
- `ingest_run_id` — 哪次 Job 运行写的
- `source_endpoint` — 调的哪个 Tuya 接口

任何数据质量问题都能 SQL 一查就追溯到具体一次运行。

### 3.4 设备 ID 是不变的，名字会变

- 所有 join 用 `device_id`
- `name` 通过 dbt snapshot 追踪 SCD Type 2
- 历史分析用 `dim_devices_history`，当前看板用 `dim_devices`

### 3.5 拓扑变化也用 SCD2

设备 1 下游接 2 和 3 这种关系是用户私有元数据，Tuya 不会给。
解决方案：

- 写在 `dbt/seeds/device_relationships.csv`
- `dbt snapshot` 在它上面建 SCD2
- 变拓扑 = 改 CSV + git commit + 下次 dbt 跑自动入历史

电量公式：`parent_measured ≈ Σ(children_measured) + parent_own_load`。
"父扣子"派生口径属于 mart 层后续工作，v1 不实现。

## 4. 为什么是 Cloud Run Job 而不是 Cloud Function

- 回填可能要跑很久（拉一年小时数据），Function 有时长上限风险
- Job 天生支持重试、并发控制、超时配置
- 本地 Docker 跑通 = 上 Cloud 跑通，调试体验好
- 同一镜像通过 `--task=xxx` 复用，新增任务不增加部署负担

## 5. 为什么 Scheduler 多个、Job 一个

- Job 镜像只有一份，所有 task 共用代码（`tuya/client.py`、`bq/writer.py` 等）
- 每个 Scheduler 用不同 `--task` 参数触发同一个 Job
- 加新任务：写一个 `jobs/<new>.py` + 加一条 Scheduler，不动其他

## 6. 为什么 batch load 不用 streaming insert

| 维度 | batch load (`load_table_from_json`) | streaming insert |
|---|---|---|
| 费用 | 免费 | 按行收费 |
| 延迟 | 分钟级 | 秒级 |
| Streaming buffer 副作用 | 无 | 影响 update/delete、partition 写入 |
| 100 台 / 数次每天 | 完全够用 | 用不上 |

## 7. 为什么有 `ops_runs` 表

虽然 Cloud Logging 也记录每次运行，但：

- SQL 比 Log Query 友好得多
- 可以和 dbt mart 直接 join（"今天采集了多少行 / 平均耗时多少"）
- 数据新鲜度告警可以一条 SQL 解决

## 8. 文档语言约定

- **正式 spec / README / 代码注释 / commit message / PR 描述**：英文
- **说明性文档（如本文）/ 与用户对话 / 内部 notes**：中文

## 9. 后续步骤

1. 用户审阅本设计与正式 spec
2. 进入 implementation plan 阶段（`writing-plans` skill）
3. 按 plan 分阶段实现
4. v1 的明确范围：见 spec §2 表格（"In scope" 列）
5. 明确 deferred 的工作：见 spec §18

## 10. 待确认事项（来自 spec §19）

- GCP project ID 与 BigQuery region
- dbt 实际跑在哪里（dbt Cloud / Cloud Run / GitHub Actions）
- 告警接收方（邮箱、Slack webhook）
- 拓扑 CSV 初始数据
