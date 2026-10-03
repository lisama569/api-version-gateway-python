# API Version Gateway（Python / FastAPI）

一个**纯进程内**实现的对外 API 服务样板，聚焦四件让分布式调用结果”可预期、可解释”的事：

1. **版本协商**：调用方按请求选择 API 版本；未声明 / 未知 / 已下线 / 已弃用的处理互不相同。
2. **幂等写入**：同一（调用方，幂等键）的重复提交不产生重复副作用；并发重复请求最多一个生效；不同调用方即使同键也各自独立。
3. **配额限流**：按调用方 + **滑动时间窗口**计量；单笔按请求计数、批量按实际占用条数预留；超限返回可用于重试的信息，并发不漏计、**任意窗口长度内都不透支、窗口边缘无突发空档**。
4. **批量下单**：一个请求带多条订单，整批原子生效或整批拒绝；同键重放复用首批结果；配额按条数预留，余额不足整批拒绝。

不依赖任何外部服务（无 Redis / 数据库 / 消息队列）或真实凭据；所有状态都在单进程内存中，适合本地验证与测试。

---

## 1. 本地运行与验证

```bash
# 依赖由 uv 管理（见 pyproject.toml / uv.lock）
uv sync

# 运行测试
uv run pytest

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
| POST | `/orders` | 创建单条订单（写请求，**必须**带 `Idempotency-Key`） |
| POST | `/orders/batch` | 批量创建订单（整批原子，**必须**带 `Idempotency-Key`） |
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

# 批量下单：orders 数组，整批要么全生效要么一条不生效
curl -i -X POST http://127.0.0.1:8000/orders/batch \
  -H 'X-API-Version: 2024-01-01' \
  -H 'X-Caller-Id: alice' \
  -H 'Idempotency-Key: batch-20261003-001' \
  -H 'Content-Type: application/json' \
  -d '{"orders":[{"name":"book","quantity":1},{"name":"pen","quantity":2}]}'
# 201 → {"orders":[...], "created_count":2, "batch_size":2, "api_version":"2024-01-01"}

# 用同一个 Idempotency-Key + 同样的请求体重放 → 201，Idempotent-Replay: true，不重复写
# 同一个 Key + 不同请求体 → 409 idempotency_conflict

# 已弃用版本 → 201，但响应头带 Deprecation / Sunset / X-API-Successor / Warning: 299
curl -i -X POST http://127.0.0.1:8000/orders \
  -H 'X-API-Version: 2023-01-01' -H 'Idempotency-Key: k-2' \
  -H 'Content-Type: application/json' -d '{"name":"pen"}'
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

管道顺序为 **版本协商 → 配额限流 → 幂等写入**：例如配额已耗尽时，若请求本身版本不合法，仍然先返回版本错误，避免错误原因被掩盖。

---

## 3. 幂等写入语义

- 所有 `POST /orders` 请求必须携带 **`Idempotency-Key`** 头，缺失返回 `400 idempotency_key_missing`。
- 服务端对请求体计算规范化指纹（JSON 键排序后 SHA-256）：
  - **同键 + 同指纹**：重复提交，回放首次结论，不再次执行业务。
  - **同键 + 不同指纹**：`409 idempotency_conflict`，调用方键用错了，必须换键。
- 幂等记录按**调用方隔离**：记录的复合键是 `(X-Caller-Id, Idempotency-Key)`。
  两个不同调用方即使使用完全相同的幂等键与请求体，也是各自独立的状态机，
  互不回放、互不冲突（详见 3.5 节隔离规则）。
- 每个键在内存中是一条状态记录：`inflight → completed / failed`。

### 并发重复请求（最多一个生效）

并发到达的同键同体请求：

1. 第一个请求在锁内原子占位成功（`acquired`），执行**唯一一次**真实写入；
2. 其余请求得到 `inflight`，阻塞等待首个请求落定（默认 10 秒）：
   - 首个请求成功 → 等待者与之后的重放都得到 **同一资源、同一状态码 201**，响应头 `Idempotent-Replay: true`；
   - 首个请求失败 → 等待者得到失败回放 `idempotent_replay_<原错误码>`，明确告知**未产生副作用**，需要时请更换新键重试；
   - 等待超时（首个请求仍未结束）→ `409 idempotency_inflight`，明确说明“本次未产生副作用，请稍后携同键重试”，**不会静默丢弃、也不会让第二个请求生效**。
3. 首执行响应带 `Idempotent-Replay: false`；所有回放响应带 `Idempotent-Replay: true`。

记录在定论后保留 TTL（默认 86400 秒，可在 `Settings` 配置）；`inflight` 记录不参与 TTL 回收，避免同进程内仍在执行的请求被新请求顶替。并发安全由占位/计数路径上的互斥锁保证（见 `tests/test_idempotency.py` 的多线程用例）。

---

## 3.5 批量下单语义（`POST /orders/batch`）

请求体为 `{"orders": [ <订单字段>, ... ]}`，每个元素与单笔下单字段一致（`name` 必填、`quantity` 正整数、`amount` 非负整数默认 0）。写请求同样**必须**携带 `Idempotency-Key`。

### 原子性：整批生效或整批拒绝

- 全部条目通过校验且配额足够时，**整批一次性提交**（先在临界区外构建全部订单对象，再在同一把锁内顺序提交，不存在"写进去一半"的中间态）。
- 任一条目不合法（如空 `name`、`quantity <= 0`）→ `400 batch_item_invalid`，错误体 `details.index` 指出**是第几条（从 0 开始）**及字段/原因，整批一条都不写入。
- `orders` 为空数组 → `400 batch_item_invalid`（`details.reason=empty_batch`）。
- 成功响应：

  ```json
  {
    "orders": [{"id": "...", "name": "book", ...}, ...],
    "created_count": 2,
    "batch_size": 2,
    "api_version": "2024-01-01"
  }
  ```

  `created_count` 即这批实际生效的条数（成功时恒等于 `batch_size`，重放时为首批条数）。

### 批量幂等：同键同体重放首批结果

- 同一（调用方，幂等键）+ 同一批内容指纹重复提交 → 回放首批的**整份订单列表与状态码**，`Idempotent-Replay: true`，不再写任何一条、不再占用任何额度。
- 同键但批内容不同 → `409 idempotency_conflict`。
- 并发的同键同体批次与单笔规则一致：恰好一个请求首执行，其余阻塞等待后回放同一批结果。
- **配额拒绝不占住幂等键**：首批在产生副作用前因额度不足被拒时，服务端释放该键的 inflight 占位（`release`），等待者/后续携同键请求可重新占位；窗口恢复后用**同一个键**即可成功，而不会拿到永久的"处理中"或被迫换键。

### 配额计量：按这批实际占用的条数

- 批量请求**不按请求次数计费**，而是在写入前按 `cost = len(orders)` **原子预留（reserve）**：只有"当前滑动窗口占用 + 本批条数 ≤ 上限"时才预留成功并继续写入。
- 余额不足以覆盖整批 → `429 quota_exceeded`，**先判定后写入，绝不先写一部分再报超限**；被拒批次不预留任何额度（紧接着更小的批次仍可成功）。错误体说清三件事：
  - `required`：本批需要多少条；
  - `remaining` / `used`：当前窗口还剩 / 已占多少条；
  - `retry_after_seconds` 与响应头 `Retry-After`：**最早什么时候重试能凑够额度**（取最早占用滑出窗口、释放出足够名额的时刻）。
- 批次数本身超过窗口上限（如上限 3 却一次提交 6 条）→ `retry_after_seconds=null` 且不返回 `Retry-After`：任何窗口都装不下，响应明确提示需拆批。
- 预留成功后若写入环节异常，服务端**退还预留（refund）**并记录幂等失败结论，额度视同从未占用。
- 幂等重放的批次不重复计费。

### 调用方隔离规则

- 幂等记录的复合键是 `(X-Caller-Id, Idempotency-Key)`：两个调用方碰巧使用**同一个幂等键**，也各自首执行、各自回放自己的结果，订单 ID 与 `created_by` 互不相同、互不串用。
- 配额按调用方各自维护独立的滑动窗口日志：一个调用方超限 / 窗口恢复 / 重试时间都与其他调用方无关。
- 缺省 `X-Caller-Id` 统一归入 `anonymous`，其配额与结果同样独立于具名调用方。

---

## 4. 限流语义

- 调用方身份取自 **`X-Caller-Id`** 头；缺省为 `anonymous`。
- 规则按调用方在 `config.py` 配置，**滑动时间窗口**（默认窗口长度 60s）：

| 调用方 | 窗口配额 | 窗口长度 |
| --- | --- | --- |
| `premium` | 1000 | 60s |
| `trial` | 10（测试夹具中为 3） | 60s |
| 其他具名调用方（默认） | 100 | 60s |
| `anonymous` | 20 | 60s |

- **滑动窗口日志**：每个调用方在内存中维护一条带时间戳的占用事件列表。任意时刻 T，只统计窗口 `(T-window, T]` 内的占用成本之和；额度随旧占用**逐条滑出**而逐步恢复，而不是整点一次性全部恢复。
- **任意窗口长度都不透支**：固定窗口在切换边界两侧各允许一份完整配额（边界瞬间可出现 2x 突发）；滑动日志不存在该空档——落在窗口边缘的密集请求仍与上一窗口内未滑出的占用合并判定。对拍用例见 `test_window_edge_no_burst_over_sliding_window` 与 `test_dense_requests_on_window_edge_do_not_exceed_quota`。
- **两种计量方式**：
  - 单笔请求 / 读请求（`check`）：每请求计 1，计数与判定在同一把锁内原子完成；被业务拒绝、被限流、幂等重放的请求**同样计数**，无法靠刷失败/重试刷出额外名额。
  - 批量请求（`reserve` / `refund`）：按本批实际占用条数**预留**，整批容纳不下则不记任何账；业务异常时按预留凭据退还。见第 3.5 节。
- 高并发下计数/预留不漏计、不超卖（100 并发单笔对拍、40 个 2 条批量并发对拍见测试）。
- 超限返回 `429 quota_exceeded`，并附可用于重试的信息：
  - 响应头 `Retry-After: <秒>`（批量且批次超过上限时不返回——重试无意义）；
  - 错误体 `details` 中含 `caller / limit / window_seconds / retry_after_seconds`，批量还含 `required / remaining / used`；
  - 所有业务响应都带 `X-RateLimit-Limit`、`X-RateLimit-Remaining`、`X-RateLimit-Reset`（按当前窗口内最早占用估算的重置秒数）。

---

## 5. 错误响应格式

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
`idempotent_replay_*` / `quota_exceeded` / `batch_item_invalid` /
`invalid_request` / `resource_not_found`。

## 6. 日志（判定依据可解释）

`api_gateway` logger 输出单行结构化日志（`event=... key=value`），关键事件：

- `event=version_decision`：`result=supported|deprecated|rejected`，拒绝时带 `reason=...`，并打印 `requested/sunset_at/successor`；
- `event=idempotency_decision`：打印 `caller / idempotency_key`、请求体指纹前缀与 `outcome=acquired|completed|waiting|replay|failed`；
- `event=batch_idempotency_decision`：批量版本，额外带 `batch_size / created_count`，以及配额拒绝占位释放后的 `reacquired_after_release`；
- `event=quota_decision`：单笔路径，打印 `caller / used / limit / window_seconds / allowed / reset_in_seconds`；
- `event=batch_quota_decision`：批量路径，打印 `caller / required / remaining / used / limit / allowed / retry_after_seconds`。

## 7. 代码结构

```
gateway/
  config.py       # 版本注册表、配额规则、Settings（纯数据）
  errors.py       # 按拒绝原因区分的错误类型（状态码 + 错误码，含 batch_item_invalid）
  versioning.py   # 版本协商
  idempotency.py  # 幂等记录状态机（调用方命名空间 + 原子占位 + 等待回放 + release + TTL）
  ratelimit.py    # 滑动窗口占用日志（check 按次 / reserve 按条数预留 / refund / snapshot）
  resources.py    # Order 资源与写入服务（单笔 + create_orders_batch 原子批量，side_effects 计数）
  app.py          # FastAPI 装配：中间件管道 + 路由（含 /orders/batch）+ 统一错误体 + 日志
main.py           # uvicorn 入口（main:app）
tests/
  test_versioning.py  # 版本协商
  test_idempotency.py # 幂等（含并发）
  test_ratelimit.py   # 滑动窗口边界/切窗/预留退还/并发不超卖/按调用方隔离
  test_batch.py       # 批量原子性/幂等/配额计量/并发批量/调用方隔离/窗口边缘密集请求
  test_api.py         # 端到端管道、错误归类、判定日志
```

## 8. 本地验证批量新行为

```bash
uv sync
uv run pytest          # 结束后打印「业务覆盖」分组：每个用例按业务域归类并标注结果
```

重点可以关注以下新增用例（`tests/test_batch.py` 与 `tests/test_ratelimit.py`）：

| 业务行为 | 对应用例 |
| --- | --- |
| 批量原子性：整批生效 / 非法条目指出下标并整批拒绝 | `test_batch_success_all_or_nothing_count_in_response`、`test_batch_invalid_item_rejects_whole_batch_with_index` |
| 同键重放复用首批结果、零重复写入 | `test_batch_replay_reuses_first_result_with_zero_new_writes` |
| 配额按条数计量；不足时整批拒绝并告知需要/剩余/重试时间 | `test_quota_charged_per_item_not_per_request`、`test_insufficient_quota_rejects_whole_batch_with_requirements` |
| 并发批量不透支额度 | `test_concurrent_batches_never_overspend_quota`、`test_concurrent_reserve_never_overcommits` |
| 不同调用方同键互不干扰、配额互不连累 | `test_distinct_callers_same_key_are_independent`、`test_one_callers_quota_exhaustion_does_not_block_another` |
| 窗口边缘密集请求不超额 | `test_dense_requests_on_window_edge_do_not_exceed_quota`、`test_window_edge_no_burst_over_sliding_window` |
| 配额拒绝后同键在窗口恢复时可成功 | `test_batch_quota_rejection_does_not_pin_key` |

也可启动服务后用第 1 节的 curl 手工验证：观察 201 响应的 `created_count`、`Idempotent-Replay` 头、`X-RateLimit-Remaining` 头，以及额度不足时 429 响应体里的 `required/remaining/retry_after_seconds`。
