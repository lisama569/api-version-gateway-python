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
# 加 --log-cli-level=INFO：实时看到每个用例覆盖的业务判定日志与用例结果行
uv run pytest -v --log-cli-level=INFO
# 只跑批量下单相关用例（含拒绝/重试/校验/超批的回归用例）
uv run pytest tests/test_batch.py tests/test_batch_rejection.py -v

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
# 其中任一条不合法（如 {"name":""}、{"name":"x","quantity":0}）→ 400 invalid_request，
#   details.errors 的 loc 指出是第几条（如 ["orders",1,"name"]），整批一条都不写；
#   修正内容后可用同一幂等键重新提交
# 剩余额度不足整批 → 429 quota_exceeded，details 含 needed/remaining/retry_after_seconds
#   与 retryable:true；窗口恢复后用同一幂等键 + 同一批内容重试会重新判定，
#   额度够就 201 生效，不会回放旧的拒绝结论
# 单批条数超过整段窗口配额 → 400 batch_exceeds_quota_limit（无 Retry-After），
#   details 含 action:"split_batch" 与 max_batch_size，请拆成更小批次再提交
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
- 每个键在内存中是一条状态记录：`inflight → completed / failed`；另外批量端点的**可重试拒绝**（配额不足、单批超配额上限）会走 `release` 直接释放占位、不留记录，让同键重试能重新判定（见第 4 节）。

### 并发重复请求（最多一个生效）

并发到达的同键同体请求：

1. 第一个请求在锁内原子占位成功（`acquired`），执行**唯一一次**真实写入；
2. 其余请求得到 `inflight`，阻塞等待首个请求落定（默认 10 秒）：
   - 首个请求成功 → 等待者与之后的重放都得到 **同一资源、同一状态码 201**，响应头 `Idempotent-Replay: true`；
   - 首个请求失败 → 等待者得到失败回放 `idempotent_replay_<原错误码>`，明确告知**未产生副作用**，需要时请更换新键重试；
   - 等待超时（首个请求仍未结束）→ `409 idempotency_inflight`，明确说明“本次未产生副作用，请稍后携同键重试”，**不会静默丢弃、也不会让第二个请求生效**。
3. 首执行响应带 `Idempotent-Replay: false`；所有回放响应带 `Idempotent-Replay: true`。

记录在定论后保留 TTL（默认 86400 秒，可在 `Settings` 配置）；`inflight` 记录不参与 TTL 回收，避免同进程内仍在执行的请求被新请求顶替。并发安全由占位/计数路径上的互斥锁保证（见 `tests/test_idempotency.py` 的多线程用例）。

### 调用方之间的隔离

- 幂等记录**按调用方隔离**：存储键 internally 为 `(caller, Idempotency-Key)`。两个调用方碰巧使用同一个幂等键时，各自独立占位、独立生效、各自重放各自的结果，不会串到对方（见 `tests/test_batch.py::test_same_key_different_callers_independent`）。
- 配额同样只算在各自头上：一个调用方超限被拒，不影响其他调用方的额度（见 `test_quota_isolated_per_caller`）。

---

## 4. 批量下单语义

`POST /orders/batch` 一次提交多条订单，请求体为 `{"orders": [...]}`（1–100 条）：

- **原子性**：整批要么全部生效、要么一条都不生效。所有条目先整体校验，任一条不合法 → `400 invalid_request`，`details.errors` 的 `loc` 指出具体是哪一条（如 `["orders", 1, "name"]` 表示第 2 条的 `name` 字段），此时一条都不会写入；写入在同一把锁内一次完成，不存在"只写进去一半"的中间态。
- **条目校验边界**：每条订单的 `name` 必须是非空且非纯空白的字符串，`quantity` 必须是 ≥ 1 的整数（`amount` 不限）。空名字、纯空白名字、数量为 0 或负数都属于非法条目，会让整批被 `400` 拒绝、一条不写；校验发生在幂等占位**之前**，因此被拒后修正内容、用**同一幂等键**重新提交是允许的（不会触发 `409 idempotency_conflict`）。单笔下单 `POST /orders` 使用同一套校验规则。
- **返回**：`201` 时响应体为 `{"orders": [...], "applied": N, "api_version": ...}`，`applied` 即本批实际生效条数（被整批拒绝的批次不会出现在成功返回里）。
- **幂等**：整个请求体（含全部条目）计算一个指纹。同一幂等键 + 同一批内容重复提交 → 回放首次结论（同一份订单列表、同一个 `applied`），**不重复写、也不重复扣配额**；同键不同内容 → `409 idempotency_conflict`；并发同键请求同样最多一个生效。
- **配额按条数计**：本批的配额占用 = 批内条数（`cost = len(orders)`），不是按请求次数。判定与扣减在限流器的同一把锁内原子完成：
  - 剩余额度 ≥ 批内条数 → 整批放行并一次扣减；
  - 剩余额度不足（但整批条数 ≤ 窗口配额上限）→ **整批拒绝**（`429 quota_exceeded`），一条不写、一分额度不扣；错误体 `details` 说明 `needed`（本次需要多少条）、`remaining`（当前还剩多少条）、`limit` / `window_seconds`、`retry_after_seconds`（预计多久后可重试，按"累计释放足够条数"的最早时刻估算）与 `retryable: true`，并带 `Retry-After` 头。**这是可重试的拒绝**：配额拒绝不会固化成幂等失败记录，窗口恢复后携带**同一幂等键 + 同一批内容**重试会重新走一遍完整判定——额度够就正常生效（`201`），仍不够才继续 `429`；生效之后同键重试才走幂等回放；
  - 单批条数 > 窗口配额上限本身 → `400 batch_exceeds_quota_limit`：这种情况等多久都不可能整批放下，与"暂时不够"明确区分——**不带 `Retry-After` 头**，`details` 含 `retryable: false`、`action: "split_batch"` 与 `max_batch_size`（即窗口配额上限），请把订单拆成不超过该上限的更小批次再提交（可继续使用同一幂等键，超批拒绝同样不占键）；
  - 多个批量请求并发提交时，原子扣减保证成功占用合计不超过配额（见 `test_concurrent_batches_never_exceed_quota`）。

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
`idempotent_replay_*` / `quota_exceeded` / `batch_exceeds_quota_limit` /
`invalid_request` / `resource_not_found`。

## 7. 日志（判定依据可解释）

`api_gateway` logger 输出单行结构化日志（`event=... key=value`），关键事件：

- `event=version_decision`：`result=supported|deprecated|rejected`，拒绝时带 `reason=...`，并打印 `requested/sunset_at/successor`；
- `event=idempotency_decision`：打印 `idempotency_key`、请求体指纹前缀与 `outcome=acquired|completed|waiting|replay|failed`（批量请求另带 `batch_size`）；配额类拒绝走 `release`（释放占位、不留记录），因此窗口恢复后的同键重试会再次出现 `outcome=acquired` 而不是 `outcome=replay`；
- `event=quota_decision`：打印 `caller / used / limit / window_seconds / allowed / reset_in_seconds`（批量请求另带 `cost`，即本批条数）；
- `event=batch_decision`：批量请求的结论——`result=applied` 时带 `applied=<生效条数>`；`result=rejected` 时带 `reason=quota_exceeded`（暂时不够，可稍后同键重试）或 `reason=batch_exceeds_quota_limit`（单批超过整段配额，需拆批），并附 `needed / remaining / retry_after_seconds` 等判定依据。

测试运行时的判定日志同样可被 `caplog` 捕获并断言（见 `tests/test_api.py`、`tests/test_batch.py`、`tests/test_batch_rejection.py` 的日志用例）；`uv run pytest -v` 会逐个列出用例名与结果，加 `--log-cli-level=INFO` 还能在输出中实时看到每个用例覆盖的业务判定日志，以及每个用例的结果行（`event=test_case case=<用例名> result=passed|failed|skipped`）。

## 8. 代码结构

```
gateway/
  config.py       # 版本注册表、配额规则、Settings（纯数据）
  errors.py       # 按拒绝原因区分的错误类型（状态码 + 错误码）
  versioning.py   # 版本协商
  idempotency.py  # 幂等记录状态机（原子占位 + 等待回放 + TTL，按调用方隔离）
  ratelimit.py    # 滑动窗口计量（按条数原子扣减、边缘不翻倍、拒绝不记账）
  resources.py    # Order 资源与写入服务（单笔/批量原子写，side_effects 计真实副作用）
  app.py          # FastAPI 装配：中间件管道 + 单笔/批量路由 + 统一错误体 + 日志
main.py           # uvicorn 入口（main:app）
tests/            # 版本协商 / 幂等（含并发）/ 限流（滑动窗口+按条计费+并发）/
                  # 批量（原子性+配额+并发+隔离+窗口边缘）/
                  # 批量拒绝回归（同键重试语义+条目校验+超批指引）/ 端到端与日志
```
