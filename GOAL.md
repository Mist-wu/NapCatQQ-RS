# NapCatQQ-RS Project Goal

## 总目标

将 [NapCatQQ](https://github.com/NapNeko/NapCatQQ) 重写为可实际运行的 Rust 项目 `NapCatQQ-RS`，提供真实 QQ 登录、消息收发、OneBot 11 兼容 API、插件能力、持久化、部署与完整测试体系。

本项目的目标是可替代原项目的生产实现，不是演示、脚手架或 Mock 集合。所有代码在本机开发、提交并推送；允许在本机构建、测试和运行，同时必须在服务器 `root@152.42.241.53` 完成 Linux 环境、部署及真实 QQ/E2E 验证。

## 阶段目标

### 1. 原项目分析

- 分析原 NapCatQQ 的目录结构、技术栈、启动方式和核心模块。
- 分析 QQ 交互、Hook、登录、消息流、API、插件和配置机制。
- 在 `docs/original-analysis.md` 记录可追踪的迁移映射和兼容要求。

### 2. Rust Workspace

- 使用 Rust edition 2024 创建 workspace。
- 建立 `core`、`protocol`、`qq-client`、`message`、`event`、`api`、`plugin`、`storage`、`config`、`cli` 等职责清晰的 crate。
- 使用 Tokio、Axum、Serde、Tracing、SQLx 和 Clap。
- 在服务器通过 workspace 级格式检查、Clippy、构建和测试。

### 3. 核心架构

- `core`：生命周期、服务管理、异步任务、取消、优雅关闭和错误传播。
- `config`：文件配置、环境变量覆盖、默认值、校验和敏感字段保护。
- `message`：私聊、群聊、文本、图片、文件、At、回复和 JSON 序列化的统一模型。
- `message`：提供可扩展的 `MessageHandler` trait 和可靠的消息分发机制。
- `protocol`：以 trait 抽象 QQ 协议能力，业务代码不得直接依赖协议实现。
- `qq-client`：实现真实 QQ 网络连接、鉴权、登录态维护、心跳、重连、收包和发包；Mock 仅用于测试。

### 4. API 与 OneBot 兼容

- 提供 HTTP API 和 WebSocket API。
- 支持登录状态、消息发送、消息监听、群管理和用户管理。
- 兼容原 NapCatQQ 使用的 OneBot 11 请求、响应、事件和错误语义。
- 编写 `docs/api.md`，记录端点、动作、事件、字段、鉴权和兼容差异。
- 在服务器使用真实 QQ 账号完成关键接口 E2E 验证。

### 5. 插件系统

- 定义稳定、具备生命周期和错误隔离能力的 `Plugin` trait。
- 支持 Rust、WASM 和 HTTP 插件。
- 支持插件发现、加载、配置、启停、卸载、权限限制和失败隔离。
- 动态加载不得破坏主进程安全和稳定性。

### 6. 存储与测试体系

- 使用 SQLx 实现必要的状态、配置和消息数据持久化。
- 建立单元测试、集成测试、E2E 测试和 benchmark。
- 覆盖生命周期、配置优先级、消息模型、协议抽象、API、插件和存储迁移。
- 生成 `docs/test-report.md`，记录服务器环境、命令、结果和已知限制。
- 在服务器通过 `cargo test` 和 `cargo bench`。

### 7. 性能优化

- 分析 CPU、内存和 IO 行为。
- 优化异步并发、数据复制、clone、channel、背压和连接管理。
- 对关键路径建立可重复 benchmark，优化前后数据必须可对比。
- 在 `docs/performance.md` 记录方法、结果和取舍。

### 8. 安全审计

- 审查 `unsafe`、文件权限、秘密管理、输入校验和网络攻击面。
- 检查 HTTP/WebSocket 鉴权、请求大小、速率限制、路径与命令注入风险。
- 检查插件边界、WASM 能力限制和动态加载风险。
- 在 `docs/security-review.md` 记录发现、修复和剩余风险。

### 9. CI/CD 与交付

- 配置 GitHub Actions 自动执行 `cargo fmt --check`、`cargo clippy -- -D warnings` 和 `cargo test`。
- 提供可重复的构建、发布和部署流程。
- 编写架构文档、API 文档和 `docs/deploy.md`。
- 保持 `CHANGELOG.md` 与每个独立提交同步。
- GitHub 公开仓库必须包含清晰、可回滚的完整提交历史。

## 必需交付物

- 可实际运行的完整 Rust 重写版本。
- GitHub 公开仓库 `NapCatQQ-RS` 和完整提交历史。
- 原项目分析与迁移文档。
- 架构文档和 public Rust API 文档。
- OneBot/HTTP/WebSocket API 文档。
- 插件开发与安全模型文档。
- 部署文档。
- 性能报告、安全审计报告和测试报告。
- 自动化 CI/CD 工作流。

## Definition of Done

只有同时满足以下条件，才可以宣布“重写完成”：

- 生产路径使用真实 QQ 协议客户端，不使用 `MockQQClient`、空实现或占位后端。
- 在服务器上能够完成真实 QQ 登录并稳定维持会话。
- 私聊和群聊的文本、图片、文件、At、回复能够真实收发。
- 登录状态、消息、群管理和用户管理的 HTTP/WebSocket API 与目标 OneBot 11 行为兼容。
- Rust、WASM 和 HTTP 插件均有可运行实现与测试。
- 服务器上的 `cargo fmt --check` 通过。
- 服务器上的 `cargo clippy -- -D warnings` 通过。
- 服务器上的 `cargo test` 全部通过。
- 服务器上的 `cargo bench` 可成功执行，并已记录基准结果。
- 真实 QQ/OneBot E2E 测试通过，结果写入测试报告。
- 文档、CI/CD、CHANGELOG 和部署流程完整。
- 所有完成内容均以单一目的 Conventional Commit 提交并推送到 GitHub `main`。

若服务器不可达或任何验证尚未执行，只能报告当前实现进度与阻塞项，不得把目标标记为完成。
