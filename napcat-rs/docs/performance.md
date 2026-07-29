# NapCatQQ-RS 性能优化记录

## 目标
降低运行时与 API 热路径开销，减少不必要的克隆与锁持有时间，提升高并发场景的吞吐和关闭延迟。

## 已完成优化

- runtime
  - 使用 `tokio::task::JoinSet` 在关闭阶段并发等待任务退出，减少顺序等待导致的尾延迟。
  - `Runtime` 关闭前通过 `mem::take` 一次性取出全部任务表，缩短持锁窗口，降低停机时的并发竞争。
  - 增加服务注册/任务注册去重防护，避免重复服务名导致的运行态异常。
- API
  - 引入事件转发专用 `mpsc` 队列，把 `emit_event` 从直接 `broadcast::send` 改为异步转发，避免 WebSocket 与 HTTP 路径直接共享 `send` 瓶颈。
  - `ApiState` 的群组与用户缓存改为 `Arc<Vec<_>>`，在读路径返回克隆后的数据，减少重复分配和写锁持有。
  - `list_groups`、`list_users` 改为从缓存读取快照，去掉缺省值回填的额外构造逻辑。
- 消息与协议
  - 将 `MessageHandler::handle` 与 `forward_to_handler` 接口改为按引用传递 `&Message` 与 `&ProtocolEvent`，避免频繁大对象复制。

## 基准测试新增

- `runtime_lifecycle`：保持 1 个服务注册+关闭流程，用于回归基础开销。
- `register_and_shutdown_runtime_with_8_services`：覆盖 8 个服务并发注册后关闭，观察缩放下的开销。

## 待测与下一步

- 增加任务停止耗时分位点统计（P50/P95/P99）到 CI 基准报告中。
- 结合 `tracing` spans 补充分层事件计时，识别 `emit_event` 与 `list_*` 端点的热点。
