# NapCatQQ-RS API 文档

本文档基于 `napcat-rs/crates/api` 的当前实现。

## 概览

`napcat-api` 提供 HTTP 与 WebSocket 双通道：

- HTTP 使用 `axum::Router`，路径前缀为根路径。
- WebSocket 使用 `/ws` 进行事件订阅。
- 所有 HTTP 成功响应遵循统一返回体：

```json
{
  "status": "ok",
  "retcode": 0,
  "data": "...",
  "message": null
}
```

`status = ok` 且 `retcode = 0` 表示成功，`status = failed` 且 `retcode = -1` 表示失败。

## 路由列表

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/health` | 服务健康检查，返回 `napcat api ready`。
| `GET` | `/login/status` | 查询运行态登录状态。
| `GET` | `/get_status` | OneBot 兼容登录状态别名。
| `GET` | `/get_login_info` | OneBot 兼容登录信息（`online` 状态、`user_id`、`nickname`）。
| `POST` | `/message/send` | 发送统一消息模型（`NapMessage`）。
| `POST` | `/send_msg` | OneBot 兼容发送接口（`message_type` + `user_id/group_id`）。
| `POST` | `/send_private_msg` | OneBot 私聊发送。
| `POST` | `/send_group_msg` | OneBot 群聊发送。 |
| `GET` | `/message/listen` | 长轮询获取事件（参数见下）。 |
| `GET` | `/groups` | 群列表查询；内存为空时回退默认群。 |
| `GET` | `/users` | 用户列表查询；内存为空时回退默认用户。 |
| `GET` | `/ws` | WebSocket 消息订阅（推送 `ProtocolEvent` JSON）。 |

## 数据模型

### `LoginStatusData`

- `online: bool`：运行时是否处于运行状态。
- `message: String`：状态描述。

### `LoginInfoData`

- `user_id: String`
- `nickname: String`
- `online: bool`

### `GroupInfo` / `UserInfo`

- `group_id` / `user_id`
- `group_name` / `nickname`

### `SendRequest`

```rust
pub struct SendRequest {
    pub message: NapMessage,
}
```

`NapMessage` 与 `NapMessageRecipient` 定义于 `napcat-message`：

- `id`: 消息ID（字符串）
- `sender_id`: 发送方
- `recipient`: 私聊/群聊目标
- `elements`: 文本/图片/文件/@/回复/JSON 片段数组

### OneBot 兼容请求

- `CompatSendRequest`：包含 `message_type`, `user_id`, `group_id`, `message`
- `SendPrivateRequest`: `{ user_id, message }`
- `SendGroupRequest`: `{ group_id, message }`

## 监听接口

### `GET /message/listen`

查询参数：

- `timeout_ms?: u64`（默认 `200`）
- `max_events?: usize`（默认 `8`，最大 `32`）

响应 `data` 字段为字符串数组，每条为序列化后的事件 JSON。

### `GET /ws`

订阅方式：握手升级后，服务端持续将 `ProtocolEvent` 序列化后按行发送。

支持的事件类型（`ProtocolEvent`）：

- `connected`
- `disconnected`
- `message_received`
- `warning`

## 响应与错误

- `ApiEnvelope<T>` 所有成功响应统一外层结构。
- `ApiError::InvalidRequest` 返回 `400 Bad Request`。
- `ApiError::EventDispatch` 返回 `503 Service Unavailable`。

## 服务器启动

```rust
use napcat_api;

#[tokio::main]
async fn main() -> napcat_protocol::ProtocolResult<()> {
    napcat_api::run("127.0.0.1:3000").await
}
```

`run` 内部创建 `ApiState`、路由并在给定地址监听。

## 与核心/协议的交互

- 外部 `ProtocolBackend` 在调用侧可通过 `ProtocolEvent::MessageReceived` 投递消息。
- `ApiState` 维护一个广播通道用于事件分发：HTTP 拉取 (`/message/listen`) 与 WebSocket (`/ws`) 共享该广播。
- 发送接口会把消息转为 `ProtocolEvent::MessageReceived` 并广播，供上层协议模块监听。
