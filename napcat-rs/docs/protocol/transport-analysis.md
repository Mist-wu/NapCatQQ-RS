# NapCatQQ-RS 协议层草稿

## 目标

- 建立真实 QQ 协议接入前的统一抽象边界。
- 记录协议适配器、认证、消息收发与状态同步的数据结构。
- 为后续真实实现提供最小可验证的测试样例。

## 当前实现状态

- `crates/qq-client`: 已提供 `QQClient` 抽象和 mock 实现。
- `crates/qq-client`: 已新增 `TcpQQClient` 实现，支持 JSON-line socket 会话、登录握手与收发骨架，可用于真实端点接入替代。
- `crates/protocol`: 已提供 `ProtocolBackend` 与 OneBot HTTP 适配器。
- 统一事件总线：`EventBus`（`crates/event`）已作为 API 层事件分发载体。

## 数据路径（v1）

1. API 层产生发送事件 -> `napcat_api::push_send_event`
2. 通过 `dispatch` 队列异步提交
3. 事件总线发布 `ProtocolEvent::MessageReceived`
4. HTTP 轮询/WebSocket 订阅者消费

## 待完成（阶段 3）

- 对接真实 QQ 客户端连接器
  - 登录流程
  - 数据包编解码
  - 心跳与重连
  - TCP 线协议连接与登录框架（阶段性）
  - 服务端状态机
- 协议分层打包解析
  - 会话建立/鉴权帧
  - 普通消息帧
  - 群聊控制帧
- 连接故障和风控边界处理

## 测试项

- [ ] `qq-client` 真实连接器构建测试
- [ ] `protocol` 数据包编码/解码单元测试
- [ ] `api` 与真实协议端到端事件回路 smoke 测试
