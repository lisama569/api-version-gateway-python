# API Version Gateway（Python / FastAPI）

一个**对外 API 服务**，在纯内存、无外部服务、无真实凭据的前提下，提供三类可预期、可解释的能力：

1. **API 版本协商**：调用方按请求选择版本；未知 / 已下线 / 未声明三类拒绝原因可区分；已弃用版本给出明确弃用提示与停止支持时间。
2. **幂等写入**：同一幂等键的重复提交不产生重复副作用；并发到达的重复请求最多一个生效，其余得到可解释结论。
3. **配额限流**：按调用方 + 滑动时间窗口计数；超限拒绝并返回可用于重试的信息；并发不漏计，窗口切换无空档。

技术栈沿用仓库现有方式：**FastAPI + uvicorn，依赖由 [uv](https://docs.astral.sh/uv/) 管理**（见 `pyproject.toml` / `uv.lock`）。

---

## 一、快速开始（本地验证）

```bash
uv sync                       # 安装依赖（含 dev：pytest、httpx）
uv run pytest -q              # 运行全部测试（24 项）
uv run uvicorn main:app --reload   # 启动服务： http://127.0.0.1:8000
```

服务自带交互式文档：`http://127.0.0.1:8000/docs`。

相关请求头：

| 请求头 | 说明 |
| --- | --- |
| `X-API-Version` | **必填**。选择 API 版本，如 `2025-06-01` |
| `Idempotency-Key` | **写入必填**。调用方生成的幂等键（如 UUID） |
| `X-Caller-Id` | 可选。调用方标识，缺省为 `anonymous`；限流与幂等均按其隔离 |

创建资源示例：

```bash
curl -s -X POST http://127.0.0.1:8000/resources \
  -H 'X-API-Version: 2025-06-01' \
  -H 'X-Caller-Id: alice' \
  -H 'Idempotency-Key: 7b2c...' \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo","data":{"k":"v"}}'
```

读取资源：

```bash
curl -s http://127.0.0.1:8000/resources/res-000001 \
  -H 'X-API-Version: 2025-06-01' -H 'X-Caller-Id: alice'
```

默认配额可通过环境变量调整（无需改代码）：`QUOTA_LIMIT`（默认 100）、`QUOTA_WINDOW_SECONDS`（默认 60）。

---

## 二、版本兼容与弃用规则

### 版本注册表

| 版本 | 状态 | 停止支持时间（Sunset） | 建议升级到 |
| --- | --- | --- | --- |
| `2025-06-01` | current（当前） | — | — |
| `2024-01-01` | deprecated（已弃用，仍可用） | **2026-12-31** | `2025-06-01` |
| `2023-01-01` | retired（已下线，拒绝） | 2024-06-01 | `2024-01-01` |

### 兼容性承诺

- 同一资源在各**可用**版本下的字段与语义保持一致：响应模型固定为
  `{ "id", "name", "data", "api_version" }`，不因版本而增删字段或改变含义。
- 版本通过 `X-API-Version` 请求头选择；协商到的版本会回填到响应体 `api_version`。

### 拒绝原因（三类必须可区分）

| 场景 | HTTP 状态码 | 错误码 `error.code` | 含义 |
| --- | --- | --- | --- |
| 缺少 `X-API-Version` 头 | 400 | `VERSION_UNDECLARED` | 请求**未声明**版本 |
| 填了系统从未发布的版本 | 404 | `VERSION_UNKNOWN` | 版本**未知/无法识别** |
| 填了曾发布但已下线的版本 | 410 | `VERSION_RETIRED` | 版本**已下线**，并给出日落时间与继任版本 |

### 弃用提示

使用已弃用版本（`2024-01-01`）的请求仍会成功，但每个响应都带：

- `Deprecation: true`
- `Sunset: <RFC7231 HTTP-date>`（停止支持时间）
- `X-Api-Sunset: 2026-12-31`
- `Link: </>; rel="successor-version"; version="2025-06-01"`
- `X-Api-Successor-Version: 2025-06-01`

> 注：HTTP 响应头只能使用 Latin-1 字符，因此头里的提示为 ASCII；完整中文说明见错误响应体 `error.message/details` 与服务端日志。

---

## 三、幂等写入语义

- 所有 `POST /resources` 都**必须**携带 `Idempotency-Key`，否则返回
  `400 IDEMPOTENCY_KEY_MISSING`。
- 幂等记录按 `(X-Caller-Id, Idempotency-Key)` 隔离；请求内容用规范化 JSON 的
  SHA-256 指纹比对。
- 判定规则：

| 情况 | 结果 |
| --- | --- |
| 首次请求 | 真正执行副作用，返回 `201` |
| 同键 + 同体的后续重复提交 | **重放**首次结果，状态码/响应体与首次完全一致，响应头带 `Idempotency-Replayed: true`，**不再产生副作用** |
| 同键但请求体不同 | `422 IDEMPOTENCY_KEY_CONFLICT`（键被复用为不同内容） |
| 首个请求仍在处理时，并发到达的同键请求 | `409 IDEMPOTENCY_IN_PROGRESS`，并带 `Retry-After`（明确“尚未重复执行，可稍后重试”，不静默丢弃） |

- **并发“最多一个生效”**：首个请求完成前持记录锁；并发重复请求要么拿到
  `409 IN_PROGRESS`，要么在首个完成后重放，绝不可能两个都真正创建资源。
- 幂等判定先于配额：**重复重放不消耗配额**；取得执行资格但被限流/处理失败时，
  会释放其 `in_progress` 记录，调用方可用同一幂等键安全重试，不会被误判为冲突。
- 内存实现：进程重启后记录清空；这对“不依赖外部服务”的本地服务是预期行为。

---

## 四、限流（配额）语义

- 维度：按 `X-Caller-Id` 计数，窗口为**滑动窗口**（默认每调用方 `100` 次 / `60` 秒）。
- 算法：**滑动窗口日志（sliding-window log）**。每次请求在同一把锁内完成
  “裁剪过期记录 → 判剩余 → 占用”，因此：
  - 高并发下计数不会漏计或低估（并发压测：50 并发 / 限额 10，精确放行 10）；
  - 窗口随时间连续滑动，不存在固定窗口在边界处“配额瞬间重置、可被反复利用”的空档。
- 超限响应：

  - HTTP `429`，错误码 `QUOTA_EXCEEDED`；
  - `Retry-After: <秒>`：最早一条记录何时滑出窗口，即建议重试时刻；
  - `X-RateLimit-Limit`、`X-RateLimit-Remaining`、`X-RateLimit-Window-Seconds`
    说明配额依据；响应体 `error.details` 同步给出 `limit/window/retry_after_seconds`。
- 可通过 `create_app(caller_overrides={"vip": 100})` 为特定调用方配置独立配额
  （测试夹具中 `vip=100`）。

---

## 五、错误响应与可观测性

所有业务错误统一为如下结构，`code` 与 `reason` 一致，便于程序化归类：

```json
{
  "error": {
    "code": "QUOTA_EXCEEDED",
    "reason": "QUOTA_EXCEEDED",
    "message": "……人类可读说明……",
    "details": { "caller_id": "alice", "limit": 3, "window_seconds": 10.0, "retry_after_seconds": 10 }
  }
}
```

服务端以 JSON 行打印三类决策依据（logger 名 `api_gateway`）：

- `version_decision`：请求版本、协商结果、版本状态、Sunset、拒绝原因；
- `idempotency`：调用方、幂等键、`acquired/completed/replay/conflict/in_progress`、资源 ID；
- `quota_decision`：调用方、上限、已用、剩余、窗口、`retry_after`、判定原因。

---

## 六、项目结构

```
app/
  errors.py       # 错误码、统一异常
  versioning.py   # 版本注册表与协商（未知/下线/弃用）
  idempotency.py  # 幂等存储与并发去重
  ratelimit.py    # 滑动窗口配额限流
  resources.py    # 内存资源存储
  schemas.py      # 请求/响应模型
  applog.py       # 结构化日志
  main.py         # FastAPI 装配（版本协商 → 幂等 → 配额）
main.py           # 本地启动入口（uvicorn main:app）
tests/            # pytest：版本协商 / 幂等(含并发) / 限流边界 / 日志与错误归类
```

## 七、本地自检清单

```bash
uv run pytest -q                 # 全部单测
# 版本协商
curl -i -X POST localhost:8000/resources -H 'X-API-Version: 2023-01-01' \
  -H 'Idempotency-Key: k' -H 'Content-Type: application/json' -d '{"name":"a"}'   # 410 RETIRED
# 弃用提示
curl -i -X POST localhost:8000/resources -H 'X-API-Version: 2024-01-01' \
  -H 'Idempotency-Key: k' -H 'Content-Type: application/json' -d '{"name":"a"}'   # 201 + Sunset
# 重复提交（观察 Idempotency-Replayed 与相同资源 ID）
curl -i -X POST localhost:8000/resources -H 'X-API-Version: 2025-06-01' \
  -H 'Idempotency-Key: fixed-key' -H 'Content-Type: application/json' -d '{"name":"a"}'
curl -i -X POST localhost:8000/resources -H 'X-API-Version: 2025-06-01' \
  -H 'Idempotency-Key: fixed-key' -H 'Content-Type: application/json' -d '{"name":"a"}'
```
