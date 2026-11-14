# 04-changecast（变投影）

Changecast — 变更日志投影：源变更流 → 按序投影 → 乱序/晚到策略钩子 → 快照可查互证。

与 Feedstitch（多源 Feed 归并）、Batchgate（作业依赖编排）、Syncledger（双端对账）主链路不同。夹具全本地，不连活库。

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://localhost:5400 |
| API | http://localhost:10400 |

## 主链

配置投影流 → 读取 CDC 夹具当前阶段并按 seq 应用到投影表 → 世代围栏处置旧世代迟到事件 → 批次计数与快照互证

## 世代围栏（本题目）

每条投影流维护一个 **当前源 epoch**（采集世代号）。事件自带 `epoch`（或由夹具级 `declared_epoch` 宣告）。
当事件 epoch **小于**当前流 epoch 时，按流级策略互斥处理：

| 策略 | 行为 |
| --- | --- |
| `reject` | 拒绝该事件并计入 `epoch_rejected_count`；**不改主投影、不改水位** |
| `bypass` | 进入**旧世代旁路桶** `epoch_bypass`：可查、可手工重放；默认**不写主投影** |

epoch 大于当前流、但尚未被宣告提升对方的事件，同样被围栏拒绝（`rejected_ahead_epoch`），
等待夹具/配置宣告提升。

### 宣告提升

夹具按阶段推进（`fixture_stage`），某阶段的 `declared_epoch` 大于当前 epoch 时**原子切换**：

| 缓冲处置 | 行为 |
| --- | --- |
| `migrate_bypass` | 提升时把旧 epoch 未完成缓冲（`ooo_buffer` 中 `open`）一并迁入旁路桶，标记 `migrated_on_bump` |
| `reject_buffer` | 提升时拒绝一切旧 epoch 未完成缓冲，标记 `rejected_on_bump`，不入旁路 |

提升本身写 `epoch_changes` 一行，记录 from/to、来源（夹具宣告 / 手动）、缓冲处置与计数。
页面「世代围栏」可查看当前 epoch、手动触发提升、以及演示旧世代拒绝或旁路。

### 手工重放

旁路桶重放走独立接口 `POST /api/epoch/replay`。默认拦截两类会造成回退的重放：

- 目标实体已有**当前世代**写入的主投影行 → `newer_generation_already_projected`
- 目标实体已应用的 seq 不早于旁路行的 seq → `older_seq_would_rollback`

两类拦截都计入 `epoch_replay_blocked_count` 并写重放日志。须显式
`allow_overwrite_newer=true` 才可强压，且日志记为 `manual_replay_override`。
旁路重放**永不自动**压过新世代已写实体。

### 互证

页面与 API 同屏对照：当前 epoch / 提升次数、拒绝计数、旁路入桶计数、主投影行（存活+已删）、
旁路行数、事件留痕数、水位 seq，以及提升记录中「当时存活」与累计计数。

### 页面演示路径

1. 打开「世代围栏」，看到三条流的当前 epoch 均为 1。
2. 去「投影台」对 `member_profile` 连跑两趟：第一趟 epoch 1 全量；第二趟夹具宣告提升到 2，
   两个 epoch 2 事件写入，两个 epoch 1 迟到事件被拒；M1002 保持已删、M1001 停在 platinum。
3. 在「世代围栏」把 `store_archive` 的旧世代策略切成「进入旁路桶」，再跑两趟：
   旁路桶出现 `sa-003-late`；主投影 S01 仍是 epoch 1 写入的「城东旗舰店」未被旧世代更名覆盖，
   S02 则是新世代的「滨江新店」。
4. 对该旁路行点「重放」→ 被拦（S01 已被当前世代写过）；点「强压重放」→ 覆盖并记日志。
5. 对 `channel_attr` 点「挂入旧世代缓冲」再「提升 epoch」，选择两种缓冲处置可对比：
   迁入旁路（`migrated_on_bump`）或全部拒绝（`rejected_on_bump`）。

### 夹具

三个流各有两阶段：阶段 1 为第一世代；阶段 2 宣告 epoch 2，并混入旧世代迟到事件
（例如 `mp-005-late` 迟到 upsert 不得把已软删的 M1002 复活）。每跑一趟推进一阶段；
跑完后重跑最后阶段按 event_id 幂等跳过。

## 种子投影流

- **member_profile**：会员档案变更（`member_id`，字段 `display_name` / `tier` / `region`）
- **store_archive**：门店档案变更（`store_id`，字段 `name` / `status` / `city`）
- **channel_attr**：渠道属性变更（`channel_code`，字段 `label` / `active`）

底座默认按 **事件 seq 升序** 投影；乱序缓冲表空挂，深逻辑留给后题。对外文案不用「工单」「CRM」。

## 技术栈

Python 3.12 + FastAPI + SQLite；Vue 3 + Vite + Nginx。

## 0-1 切口（未实现，见出题规划）

`arrival_vs_event_order` / `ooo_window` / `event_idempotency` / `tombstone_policy` / `watermark_rewind` / …

本题目刻意**不做**：字段级 Schema/契约漂移门禁、多源权威序归并。
