# API Version Gateway（Python / FastAPI）

一个**纯进程内**实现的对外 API 服务样板，聚焦三件让分布式调用结果“可预期、可解释”的事：

1. **版本协商**：调用方按请求选择 API 版本；未声明 / 未知 / 已下线 / 已弃用的处理互不相同。
2. **幂等写入**：同一幂等键的重复提交不产生重复副作用；并发重复请求最多一个生效；支持整批原子生效的批量下单。
3. **配额限流**：按调用方 + 滑动时间窗口计量（批量按实际条数扣减）；超限返回可用于重试的信息，并发不透支、窗口边缘不翻倍。

不依赖任何外部服务（无 Redis / 数据库 / 消息队列）或真实凭据；所有状态都在单进程内存中，适合本地验证与测试。

---

## 1. 本地运行与验证

```bash
# 依赖由 uv 管理（见 pyproject.toml / uv.lock）
uv sync

# 运行测试（-v 可看到每个用例的名称与结果；测试日志经 caplog 断言判定依据）
uv run pytest
uv run pytest -v
# 只跑批量下单相关用例
uv run pytest tests/test_batch.py -v

# 启动服务
uv run uvicorn main:app --reload
# 交互式文档：http://127.0.0.1:8000/docs
```

公开端点（无需版本声明、不计配额）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 服务信息 |
| GET | `/health` | 健康检查 |
| GET | `/versions` | 版本目录（版本、状态、下线时间、后继版本） |

业务端点：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/orders` | 创建订单（写请求，**必须**带 `Idempotency-Key`） |
| POST | `/orders/batch` | 批量创建订单（整批原子生效，配额按条数计） |
| GET | `/orders/{id}` | 查询订单 |

### curl 示例

```bash
# 正常写入（受支持版本）
curl -i -X POST http://127.0.0.1:8000/orders \
  -H 'X-API-Version: 2024-01-01' \
  -H 'X-Caller-Id: alice' \
  -H 'Idempotency-Key: order-20260923-001' \
  -H 'Content-Type: application/json' \
  -d '{"name":"book","quantity":2,"amount":10}'

# 用同一个 Idempotency-Key + 同样的请求体重放 → 201，Idempotent-Replay: true，不重复写
# 同一个 Key + 不同请求体 → 409 idempotency_conflict

# 已弃用版本 → 201，但响应头带 Deprecation / Sunset / X-API-Successor / Warning: 299
curl -i -X POST http://127.0.0.1:8000/orders \
  -H 'X-API-Version: 2023-01-01' -H 'Idempotency-Key: k-2' \
  -H 'Content-Type: application/json' -d '{"name":"pen"}'

# 批量下单：整批原子生效，返回 applied 为实际生效条数
curl -i -X POST http://127.0.0.1:8000/orders/batch \
  -H 'X-API-Version: 2024-01-01' \
  -H 'X-Caller-Id: alice' \
  -H 'Idempotency-Key: batch-20261003-001' \
  -H 'Content-Type: application/json' \
  -d '{"orders":[{"name":"book","quantity":2},{"name":"pen"},{"name":"ruler"}]}'
# → 201 {"orders":[...3 条...], "applied":3, ...}，本批占用 3 条配额
# 同一 Idempotency-Key + 同样请求体重放 → 201 同一份订单列表，Idempotent-Replay: true，
#   不重复写、不重复扣额度
# 其中任一条不合法（如 {"name":123}、{"name":""}、{"name":"x","quantity":0}）
#   → 400 invalid_request，details.errors 的 loc 指出是第几条
#   （如 ["orders",1,"name"]），整批一条都不写
# 剩余额度暂时不足整批（条数 ≤ 窗口上限）→ 429 quota_exceeded，details 含
#   needed/remaining/retry_after_seconds 与 retryable=true；等窗口恢复后
#   用【同一个】Idempotency-Key + 同一批内容重试 → 重新判定，额度够则 201
# 单批条数超过整段窗口的配额上限 → 422 batch_too_large，不带 Retry-After，
#   details.remediation=split_into_smaller_batches：必须拆成更小批次并换新键
```

---

## 2. 版本协商规则

- 每个业务请求必须通过 **`X-API-Version`** 头声明版本（缺头或空串视为未声明）。
- 版本目录由 `gateway/config.py` 静态声明，每个版本有明确的生命周期状态：

| 版本 | 状态 | 行为 |
| --- | --- | --- |
| `2024-01-01` | `supported` | 正常服务 |
| `2023-01-01` | `deprecated` | **仍可用**，响应头给出弃用提示与停止支持时间 `2027-06-30T00:00:00Z`，并指向后继版本 `2024-01-01` |
| `2022-01-01` | `sunset` | **直接拒绝（410）**，下线时间 `2025-12-31T00:00:00Z` |

- **字段与语义一致性**：所有在线版本（supported / deprecated）对同一资源返回完全相同的字段集合与含义（见 `Order`：`id/name/quantity/amount/created_by`），差异只体现在弃用提示头上。
- 三类拒绝原因使用不同的 HTTP 状态码与错误码，调用方可以精确区分：

| 场景 | HTTP | error.code | 关键 details |
| --- | --- | --- | --- |
| 未声明版本 | 400 | `version_undeclared` | `header`、`available`（全部已知版本） |
| 未知版本（注册表中不存在） | 404 | `version_unknown` | `requested_version`、`supported` |
| 已下线版本 | 410 | `version_sunset` | `sunset_at`、`successor` |

弃用提示响应头（deprecated 版本的所有业务响应都会带）：

- `Deprecation: true`、`X-API-Deprecated: true`
- `Sunset: <停止支持时间>`
- `X-API-Successor: <建议迁移版本>`
- `Warning: 299 - "... retired on <date>; please migrate to <successor>."`（HTTP 头只能用 ASCII，中文说明见响应体与日志）
- `X-API-Version: <实际协商版本>`（所有业务响应都回显）

管道顺序为 **版本协商 → 配额限流 → 幂等写入**：例如配额已耗尽时，若请求本身版本不合法，仍然先返回版本错误，避免错误原因被掩盖。唯一的例外是 `POST /orders/batch`：批内条数要解析请求体后才知道，且重放不应重复扣额度，因此批量端点在路由内按“幂等占位 → 赢家按条数扣减配额 → 整批写入”执行（详见第 4 节）。

---

## 3. 幂等写入语义

- 所有 `POST /orders` 与 `POST /orders/batch` 请求必须携带 **`Idempotency-Key`** 头，缺失返回 `400 idempotency_key_missing`。
- 服务端对请求体计算规范化指纹（JSON 键排序后 SHA-256）：
  - **同键 + 同指纹**：重复提交，回放首次结论，不再次执行业务。
  - **同键 + 不同指纹**：`409 idempotency_conflict`，调用方键用错了，必须换键。
- 每个键在内存中是一条状态记录：`inflight → completed / failed`；另有一类特殊的 `retryable`（可重试失败），仅用于“失败由可恢复的外部条件造成且本次无任何副作用”的场景（当前为批量端点因窗口额度暂时不足返回的 429）：
  - `completed / failed`：结论永久有效（TTL 内），同键提交只回放；
  - `retryable`：**不锁定结论**。之后同键同体提交会在占位阶段重新建立 `inflight` 记录、重新执行完整业务判定——条件已恢复（如额度已释放）则正常 `201` 生效并转为 `completed`；仍不满足则再次得到新的失败结论。指纹校验在重占位时依旧生效，同键不同内容仍是 `409`。

### 并发重复请求（最多一个生效）

并发到达的同键同体请求：

1. 第一个请求在锁内原子占位成功（`acquired`），执行**唯一一次**真实写入；
2. 其余请求得到 `inflight`，阻塞等待首个请求落定（默认 10 秒）：
   - 首个请求成功 → 等待者与之后的重放都得到 **同一资源、同一状态码 201**，响应头 `Idempotent-Replay: true`；
   - 首个请求失败 → 等待者得到失败回放 `idempotent_replay_<原错误码>`，明确告知**未产生副作用**；普通失败（写入异常、永久性的 `batch_too_large` 等）需要更换新键重试；唯独“可重试失败”（批量 429，窗口额度暂时不足）不回放旧结论，见第 4 节，窗口恢复后用原键即可重新判定；
   - 等待超时（首个请求仍未结束）→ `409 idempotency_inflight`，明确说明“本次未产生副作用，请稍后携同键重试”，**不会静默丢弃、也不会让第二个请求生效**。
3. 首执行响应带 `Idempotent-Replay: false`；所有回放响应带 `Idempotent-Replay: true`。

记录在定论后保留 TTL（默认 86400 秒，可在 `Settings` 配置）；`inflight` 记录不参与 TTL 回收，避免同进程内仍在执行的请求被新请求顶替。并发安全由占位/计数路径上的互斥锁保证（见 `tests/test_idempotency.py` 的多线程用例）。

### 调用方之间的隔离

- 幂等记录**按调用方隔离**：存储键 internally 为 `(caller, Idempotency-Key)`。两个调用方碰巧使用同一个幂等键时，各自独立占位、独立生效、各自重放各自的结果，不会串到对方（见 `tests/test_batch.py::test_same_key_different_callers_independent`）。
- 配额同样只算在各自头上：一个调用方超限被拒，不影响其他调用方的额度（见 `test_quota_isolated_per_caller`）。

---

## 4. 批量下单语义

`POST /orders/batch` 一次提交多条订单，请求体为 `{"orders": [...]}`（1–100 条）：

- **原子性与条目校验**：整批要么全部生效、要么一条都不生效。所有条目先整体校验，任一条不合法 → `400 invalid_request`，`details.errors` 的 `loc` 指出具体是哪一条（如 `["orders", 1, "name"]` 表示第 2 条的 `name` 字段），此时一条都不会写入、一分额度都不扣；写入在同一把锁内一次完成，不存在“只写进去一半”的中间态。校验边界（单笔 `POST /orders` 同样适用）：
  - `name`：必须是非空字符串，空串或纯空白（`""`、`"   "`、`"\t\n"` 等）一律拒绝；类型不是字符串也拒绝；
  - `quantity`：必须是 **正整数（> 0）**，`0` 与负数一律拒绝；
  - 批次数本身：`orders` 必须为 1–100 条（空批次或超过 100 条同样 400）。
- **返回**：`201` 时响应体为 `{"orders": [...], "applied": N, "api_version": ...}`，`applied` 即本批实际生效条数；被判非法或被配额拒绝的批次不会出现在任何成功返回中。
- **幂等**：整个请求体（含全部条目）计算一个指纹。同一幂等键 + 同一批内容重复提交 → 回放首次结论（同一份订单列表、同一个 `applied`），**不重复写、也不重复扣配额**；同键不同内容 → `409 idempotency_conflict`；并发同键请求同样最多一个生效。
- **配额按条数计**：本批的配额占用 = 批内条数（`cost = len(orders)`），不是按请求次数。判定与扣减在限流器的同一把锁内原子完成，拒绝分两种，处理方式刻意不同：

  | 情形 | 状态码 / error.code | 含义与调用方动作 |
  | --- | --- | --- |
  | 窗口内剩余额度**暂时**不足整批（批内条数 ≤ 配额上限） | `429 quota_exceeded` | 暂时性失败：一条不写、一分不扣；`details` 含 `needed / remaining / limit / window_seconds / retry_after_seconds`，并带 `Retry-After` 头与 `"retryable": true, "retry_with_same_idempotency_key": true`。请在窗口释放出足够额度后，**用同一个幂等键和同一批内容重试**，服务端会重新走完整判定：额度已恢复则整批 `201` 生效；仍不足则继续 `429`。 |
  | 单批条数本身 **超过整段窗口的配额上限** | `422 batch_too_large` | 永久性失败：无论等待多久、窗口怎么滚动都不可能整批放行。**不带 `Retry-After`**；`details` 含 `needed / limit / max_batch_size / "retryable": false / "remediation": "split_into_smaller_batches"`。调用方必须**把批次拆成每批不超过上限的多个小批次，并为每个小批次使用新的幂等键**分别提交，不要按“马上重试”反复打。 |

  - 多个批量请求并发提交时，原子扣减保证成功占用合计不超过配额（见 `test_concurrent_batches_never_exceed_quota`）。
- **配额暂时不足（429）不锁死幂等结论**：该失败被记为幂等键的“可重试失败（`retryable`）”——它不产生任何副作用，也不会像普通失败那样被永久回放。窗口恢复后同键同体重试会重新占位、重新执行配额判定与写入（见 `test_batch_quota_rejection_retried_same_key_after_window_succeeds`）；窗口未恢复时同键重试则再次真实判定、仍返回新的 `429`（见 `test_batch_quota_rejection_retried_same_key_before_window_still_denied`）。一旦某次重试成功，之后同键重放回到标准幂等语义（回放同一批订单、不重复写不重复扣费）。
- **超限批次（422）是永久性结论**：与普通失败一致地被记为 `failed`，同键重放得到 `idempotent_replay_batch_too_large`；换窗口、等再久也无济于事，唯一出路是拆小批次 + 使用新幂等键（见 `test_batch_too_large_replay_keeps_decision_and_same_key_never_helps`）。

### 4.1 本地验证这些行为

自动化测试（推荐，虚拟时钟可精确推进窗口）：

```bash
uv run pytest -v                                  # 全量：逐用例结果 + 业务分组 + 覆盖汇总
uv run pytest tests/test_batch.py -v              # 只看批量下单（20 个用例）
uv run pytest -k "quota_rejection or too_large" -v   # 只看同键重试 / 超限拆批相关
```

关键用例与被钉住的行为：

| 用例 | 钉住的行为 |
| --- | --- |
| `test_batch_quota_rejection_retried_same_key_after_window_succeeds` | 被 429 拒绝后推进虚拟时钟越过窗口，**同一幂等键 + 同一批内容**重试返回 201、真正多写 3 条，之后再重放仍是同一批订单 |
| `test_batch_quota_rejection_retried_same_key_before_window_still_denied` | 窗口未恢复时同键重试得到的是**重新判定的新 429**（非 `idempotent_replay_*`），仍不写入 |
| `test_batch_with_empty_name_rejected_entirely` / `test_batch_with_non_positive_quantity_rejected_entirely` | 空名字（含纯空白）、数量 0 / 负数整批 400，`loc` 指出条目下标，零写入零扣额 |
| `test_batch_too_large_for_window_distinguished_from_temporary_shortage` | 条数超整窗上限 → 422 `batch_too_large`，`retryable=false`、无 `Retry-After`、给拆批指引 |
| `test_batch_too_large_replay_keeps_decision_and_same_key_never_helps` | 超限批次同键重放仍 422；拆小并换新键后才 201 |

手工 curl 验证（真实时钟，trial 调用方窗口为 60s / 10 条）：

```bash
# 1) 用同一幂等键先触发 429（可先把窗口额度用满），记录返回的 retry_after_seconds
curl -i -X POST http://127.0.0.1:8000/orders/batch \
  -H 'X-API-Version: 2024-01-01' -H 'X-Caller-Id: trial' \
  -H 'Idempotency-Key: batch-retry-demo' -H 'Content-Type: application/json' \
  -d '{"orders":[{"name":"a"},{"name":"b"}]}'
# 2) 等待 Retry-After 秒后，原样重发同一条命令（键与请求体都不变）→ 201 applied=2
# 3) 非法条目：任一 name 为空串或 quantity<=0 → 400，loc 指出第几条
curl -i -X POST http://127.0.0.1:8000/orders/batch \
  -H 'X-API-Version: 2024-01-01' -H 'X-Caller-Id: trial' \
  -H 'Idempotency-Key: batch-invalid-demo' -H 'Content-Type: application/json' \
  -d '{"orders":[{"name":"ok"},{"name":"","quantity":-1}]}'
# 4) 超过整窗上限：一次提交 11 条（trial 上限 10）→ 422 batch_too_large，
#    无 Retry-After，details.remediation=split_into_smaller_batches
```

---

## 5. 限流语义

- 调用方身份取自 **`X-Caller-Id`** 头；缺省为 `anonymous`。
- 规则按调用方在 `config.py` 配置，**滑动时间窗口**：

| 调用方 | 窗口配额 | 窗口长度 |
| --- | --- | --- |
| `premium` | 1000 | 60s |
| `trial` | 10（测试夹具中为 3） | 60s |
| 其他具名调用方（默认） | 100 | 60s |
| `anonymous` | 20 | 60s |

- 计量单位为**成功占用的额度**：普通端点每次请求计 1；`POST /orders/batch` 按批内实际生效条数计（见第 4 节）。判定与记账在同一把锁内**原子**完成：高并发下不会把额度用超（100 并发、配额 50 与 20 并发 × 每批 3 条的对拍见测试）。
- **只有成功占用的请求才记账**：被限流拒绝的请求不消耗额度——超限后的重试既刷不出额外名额，也不会污染窗口内已有的占用。
- 窗口为滑动窗口 `(now - window_seconds, now]`：只统计窗口内的成功占用，因此**任意一段 window_seconds 长度的时间内，占用合计都不超过配额**——窗口边缘的密集请求也无法像固定窗口那样借“切窗”瞬间翻倍（见 `test_sliding_window_edge_no_double_spend` 与 `test_window_edge_burst_cannot_exceed_quota`）。
- 超限返回 `429 quota_exceeded`，并附可用于重试的信息：
  - 响应头 `Retry-After: <秒>`；
  - 错误体 `details` 中含 `caller / limit / window_seconds / retry_after_seconds`（批量场景另有 `needed / remaining`）；
  - 所有业务响应都带 `X-RateLimit-Limit`、`X-RateLimit-Remaining`、`X-RateLimit-Reset`（距下一笔占用释放的秒数）。

---

## 6. 错误响应格式

统一为：

```json
{
  "error": {
    "code": "quota_exceeded",
    "message": "调用方 trial 已超出 60s 窗口配额（3 次），请在窗口重置后重试。",
    "details": { "caller": "trial", "limit": 3, "window_seconds": 60.0,
                 "retry_after_seconds": 60 }
  }
}
```

错误码目录：`version_undeclared` / `version_unknown` / `version_sunset` /
`idempotency_key_missing` / `idempotency_conflict` / `idempotency_inflight` /
`idempotent_replay_*` / `quota_exceeded`（429，窗口额度暂时不足，可同键重试）/
`batch_too_large`（422，单批超过整窗配额上限，必须拆批换新键）/
`invalid_request`（含空名字、非正数量等条目校验）/ `resource_not_found`。

## 7. 日志（判定依据可解释）

`api_gateway` logger 输出单行结构化日志（`event=... key=value`），关键事件：

- `event=version_decision`：`result=supported|deprecated|rejected`，拒绝时带 `reason=...`，并打印 `requested/sunset_at/successor`；
- `event=idempotency_decision`：打印 `idempotency_key`、请求体指纹前缀与 `outcome=acquired|completed|waiting|replay|failed`（批量请求另带 `batch_size`）；
- `event=quota_decision`：打印 `caller / used / limit / window_seconds / allowed / reset_in_seconds`（批量请求另带 `cost`，即本批条数）；
- `event=batch_decision`：批量请求的结论——`result=applied` 时带 `applied=<生效条数>`；`result=rejected` 时带 `reason=quota_exceeded`（暂时性，带 `needed / remaining / retry_after_seconds / retryable=true`）或 `reason=batch_too_large`（永久性超限，带 `needed / limit`，无 `retry_after`）；同键重试在额度恢复后重新占位时，幂等日志为 `outcome=acquired`（重新执行），而非 `outcome=replay`。

测试运行时的判定日志同样可被 `caplog` 捕获并断言（见 `tests/test_api.py`、`tests/test_batch.py` 的日志用例）；`uv run pytest -v` 会逐个列出用例名与结果，并在每个业务模块首次出现时打印“▶ 覆盖业务: …”分组标题，结尾给出各业务模块的用例数汇总。

## 8. 代码结构

```
gateway/
  config.py       # 版本注册表、配额规则、Settings（纯数据）
  errors.py       # 按拒绝原因区分的错误类型（状态码 + 错误码）
  versioning.py   # 版本协商
  idempotency.py  # 幂等记录状态机（原子占位 + 等待回放 + TTL，按调用方隔离；
                  #   failed 永久回放，retryable 失败允许同键重新判定）
  ratelimit.py    # 滑动窗口计量（按条数原子扣减、边缘不翻倍、拒绝不记账、
                  #   exceeds_limit 区分“永远放不下”与“暂时不够”）
  resources.py    # Order 资源与写入服务（单笔/批量原子写，side_effects 计真实副作用）
  app.py          # FastAPI 装配：中间件管道 + 单笔/批量路由 + 统一错误体 + 日志
main.py           # uvicorn 入口（main:app）
tests/            # 版本协商 / 幂等（含并发）/ 限流（滑动窗口+按条计费+并发）/
                  # 批量（原子性+配额+并发+隔离+窗口边缘）/ 端到端与日志
```
