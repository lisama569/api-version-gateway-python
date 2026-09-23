# API Version Gateway（Python / FastAPI）

一个**纯进程内**实现的对外 API 服务样板，聚焦三件让分布式调用结果“可预期、可解释”的事：

1. **版本协商**：调用方按请求选择 API 版本；未声明 / 未知 / 已下线 / 已弃用的处理互不相同。
2. **幂等写入**：同一幂等键的重复提交不产生重复副作用；并发重复请求最多一个生效。
3. **配额限流**：按调用方 + 固定时间窗口计数；超限返回可用于重试的信息，并发不漏计、窗口切换无空档。

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
| POST | `/orders` | 创建订单（写请求，**必须**带 `Idempotency-Key`） |
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

## 4. 限流语义

- 调用方身份取自 **`X-Caller-Id`** 头；缺省为 `anonymous`。
- 规则按调用方在 `config.py` 配置，固定时间窗口：

| 调用方 | 窗口配额 | 窗口长度 |
| --- | --- | --- |
| `premium` | 1000 | 60s |
| `trial` | 10（测试夹具中为 3） | 60s |
| 其他具名调用方（默认） | 100 | 60s |
| `anonymous` | 20 | 60s |

- 计数与放行判定在同一把锁内**原子**完成：高并发下不会漏计/低估（100 并发、配额 50 的对拍见测试）。
- **每个请求都计数**，包括被业务拒绝的请求、被限流的请求与幂等重放请求——无法用“故意打失败请求”或“超限后重试”刷出额外名额。
- 窗口为左闭右开区间；落入新窗口的第一个请求原子滚动计数桶，旧窗口立即失效，不存在“新旧窗口同时放行”的切换空档。
- 超限返回 `429 quota_exceeded`，并附可用于重试的信息：
  - 响应头 `Retry-After: <秒>`；
  - 错误体 `details` 中含 `caller / limit / window_seconds / retry_after_seconds`；
  - 所有业务响应都带 `X-RateLimit-Limit`、`X-RateLimit-Remaining`、`X-RateLimit-Reset`（距窗口重置秒数）。

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
`idempotent_replay_*` / `quota_exceeded` / `invalid_request` / `resource_not_found`。

## 6. 日志（判定依据可解释）

`api_gateway` logger 输出单行结构化日志（`event=... key=value`），关键事件：

- `event=version_decision`：`result=supported|deprecated|rejected`，拒绝时带 `reason=...`，并打印 `requested/sunset_at/successor`；
- `event=idempotency_decision`：打印 `idempotency_key`、请求体指纹前缀与 `outcome=acquired|completed|waiting|replay|failed`；
- `event=quota_decision`：打印 `caller / used / limit / window_seconds / allowed / reset_in_seconds`。

## 7. 代码结构

```
gateway/
  config.py       # 版本注册表、配额规则、Settings（纯数据）
  errors.py       # 按拒绝原因区分的错误类型（状态码 + 错误码）
  versioning.py   # 版本协商
  idempotency.py  # 幂等记录状态机（原子占位 + 等待回放 + TTL）
  ratelimit.py    # 固定窗口计数（原子判定、窗口滚动）
  resources.py    # Order 资源与写入服务（side_effects 计数体现真实副作用）
  app.py          # FastAPI 装配：中间件管道 + 路由 + 统一错误体 + 日志
main.py           # uvicorn 入口（main:app）
tests/            # 版本协商 / 幂等（含并发）/ 限流（边界+并发+切窗）/ 端到端与日志
```
