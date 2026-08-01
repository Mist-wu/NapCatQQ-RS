# NapCatQQ-RS Agent Instructions

本仓库按标准 Rust workspace 项目方式开发。默认在本机完成开发、构建和测试，通过 GitHub CI 做持续验证，并在需要时使用云服务器完成部署及真实 QQ/E2E 验证。

## 开发环境

开发机为当前 Mac，运行服务器为：

```text
root@152.42.241.53
```

- 本机是主要开发环境，可进行代码编辑、依赖安装、构建、测试、benchmark、运行和 Git/GitHub 操作。
- GitHub Actions 是统一的持续集成环境。
- 云服务器用于 Linux 部署验证、长期运行、真实 QQ 登录和 E2E 测试，也可用于问题复现和性能测试。
- 如确需在服务器修复环境相关问题，应将修改正常提交回 Git 仓库，避免服务器出现不可追踪的长期差异。
- 不得把密码、令牌、QQ 凭据、Cookie、设备信息或其他秘密写入仓库、提交记录、日志和文档。

## 服务器连接与工作目录

使用 SSH 连接：

```bash
ssh root@152.42.241.53
```

服务器项目根目录统一为：

```text
/root/projects/NapCatQQ-RS
```

需要使用服务器时检查：

```bash
rustc --version
cargo --version
git --version
gh --version
```

本机和服务器可分别安装各自环境缺少的工具。服务器仓库不存在时，在 `/root/projects` 下从 GitHub 克隆；已存在时使用：

```bash
cd /root/projects/NapCatQQ-RS
git pull --ff-only origin main
```

仅当交付内容要求服务器部署或真实 QQ/E2E 验证时，SSH 不可达才构成对应验证的阻塞；普通 Rust 单元开发可继续进行。

## GitHub 与版本管理

- 使用本机已登录的 `gh` CLI 管理 GitHub 仓库。
- 开始 GitHub 操作前检查 `gh auth status`；未登录时停止并提示用户登录。
- 默认分支为 `main`，远程仓库为公开仓库 `NapCatQQ-RS`。
- 每个提交只能包含一个清晰、可独立回滚的目的。
- 提交信息使用 Conventional Commits，例如 `feat(core): implement runtime shutdown`。
- 面向用户的功能、修复和兼容性变化应更新 `CHANGELOG.md`；纯内部重构可按实际影响判断。
- 推送后由 GitHub Actions 执行标准检查；涉及部署、QQ 协议或平台差异时再同步服务器验证。
- 验证失败时，用独立的修复提交处理；不得把多个无关修复压入同一个提交。
- 不得覆盖、重置或删除用户已有的未提交改动。

## 标准开发流程

1. 查看工作区状态并阅读相关代码。
2. 完成单一目的的修改和对应测试。
3. 在 workspace 根目录依次运行 `cargo fmt --all -- --check`、`cargo clippy --workspace --all-targets -- -D warnings` 和 `cargo test --workspace --all-targets`。
4. 需要发布可执行文件时运行 `cargo build --workspace --release`；性能相关修改运行对应 benchmark。
5. 按实际影响更新文档和 `CHANGELOG.md`。
6. 创建清晰、可回滚的 Conventional Commit，并推送 GitHub。
7. 确认 CI 通过。涉及部署或真实 QQ 行为时，在服务器补充部署与 E2E 验证。

## Rust 代码规范

- Rust edition 使用 2024。
- 异步运行时使用 Tokio，HTTP/WebSocket 使用 Axum，序列化使用 Serde，日志使用 Tracing，存储使用 SQLx，CLI 使用 Clap。
- 禁止在生产代码中使用 `unwrap()`、`expect()`、`todo!()` 和无说明的 `unsafe`。
- 可失败操作返回 `Result`，错误必须带有足够上下文。
- 所有 public 接口必须包含 `///` 文档。
- 保持模块职责单一，禁止巨型文件和跨层直接依赖。
- 业务层只能依赖协议抽象，不得直接依赖具体 QQ 协议实现。
- 并发路径必须考虑取消、关闭、背压、资源释放和错误传播。
- 所有代码必须通过 `cargo fmt --all -- --check`、`cargo clippy --workspace --all-targets -- -D warnings` 和 `cargo test --workspace --all-targets`。

## 完成判定

- Mock、空实现、占位符、仅接口设计或仅能编译的骨架不算完成。
- 单元测试通过但真实 QQ 登录、消息收发或 OneBot E2E 未验证，不算对应功能完成。
- 未 push 到 GitHub 的本机修改不算交付。
- 普通 Rust 功能以代码、测试和 CI 结果判定；部署、QQ 协议和 OneBot 集成功能还必须有相应服务器/E2E 结果。
- 只有满足 `GOAL.md` 的全部交付物和完成标准后，才能宣布 Rust 重写完成。
