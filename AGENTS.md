# NapCatQQ-RS Agent Instructions

本文件适用于本仓库中的所有开发、版本管理、构建、测试和部署工作。

## 执行环境边界

开发机为当前 Mac，运行服务器为：

```text
root@152.42.241.53
```

必须严格遵守以下边界：

- 在本机执行：代码阅读、代码编辑、文档编写、`git add`、`git commit`、`git push`、`gh` 仓库管理。
- 在服务器执行：`git pull`、依赖安装、`cargo build`、`cargo check`、`cargo test`、`cargo bench`、`cargo fmt --check`、`cargo clippy`、程序运行、部署、真实 QQ 登录和 E2E 验证。
- 禁止在本机安装项目依赖、执行 Cargo 命令、编译、测试或运行 NapCatQQ-RS。
- 禁止直接在服务器编辑项目源代码或创建提交；服务器工作区只用于拉取并验证本机已经推送的提交。
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

首次使用服务器时检查：

```bash
rustc --version
cargo --version
git --version
gh --version
```

缺少工具时只在服务器安装。服务器仓库不存在时，在 `/root/projects` 下从 GitHub 克隆；已存在时使用：

```bash
cd /root/projects/NapCatQQ-RS
git pull --ff-only origin main
```

SSH 不可达、服务器依赖安装失败或服务器验证没有执行时，必须明确标记为阻塞或未验证，不得声称任务、阶段或目标已经完成。

## GitHub 与版本管理

- 使用本机已登录的 `gh` CLI 管理 GitHub 仓库。
- 开始 GitHub 操作前检查 `gh auth status`；未登录时停止并提示用户登录。
- 默认分支为 `main`，远程仓库为公开仓库 `NapCatQQ-RS`。
- 每个提交只能包含一个清晰、可独立回滚的目的。
- 提交信息使用 Conventional Commits，例如 `feat(core): implement runtime shutdown`。
- 每完成一个独立步骤，先更新 `CHANGELOG.md`，再在本机执行 `git add`、`git commit` 和 `git push origin main`。
- 推送后服务器执行 `git pull --ff-only origin main`，再完成该提交对应的格式、静态检查、构建和测试。
- 验证失败时，用独立的修复提交处理；不得把多个无关修复压入同一个提交。
- 不得覆盖、重置或删除用户已有的未提交改动。

## 每步工作流

1. 在本机查看当前代码状态和相关文件。
2. 在本机完成单一目的的代码或文档修改。
3. 在本机更新 `CHANGELOG.md`。
4. 在本机提交并推送到 GitHub。
5. SSH 登录服务器并进入 `/root/projects/NapCatQQ-RS`。
6. 在服务器执行 `git pull --ff-only origin main`。
7. 在服务器执行该步骤需要的 `cargo fmt --check`、`cargo clippy -- -D warnings`、构建和测试。
8. 涉及 QQ 协议、登录、消息或 OneBot API 时，在服务器完成真实 QQ/E2E 验证。
9. 若验证失败，回到本机修改并创建单一目的修复提交，然后重复上述流程。

## Rust 代码规范

- Rust edition 使用 2024。
- 异步运行时使用 Tokio，HTTP/WebSocket 使用 Axum，序列化使用 Serde，日志使用 Tracing，存储使用 SQLx，CLI 使用 Clap。
- 禁止在生产代码中使用 `unwrap()`、`expect()`、`todo!()` 和无说明的 `unsafe`。
- 可失败操作返回 `Result`，错误必须带有足够上下文。
- 所有 public 接口必须包含 `///` 文档。
- 保持模块职责单一，禁止巨型文件和跨层直接依赖。
- 业务层只能依赖协议抽象，不得直接依赖具体 QQ 协议实现。
- 并发路径必须考虑取消、关闭、背压、资源释放和错误传播。
- 所有代码必须通过服务器上的 `cargo fmt --check`、`cargo clippy -- -D warnings` 和 `cargo test`。

## 完成判定

- Mock、空实现、占位符、仅接口设计或仅能编译的骨架不算完成。
- 单元测试通过但真实 QQ 登录、消息收发或 OneBot E2E 未验证，不算对应功能完成。
- 未 push 到 GitHub 的本机修改不算交付。
- 未在服务器验证的提交只能标记为“已实现，待服务器验证”。
- 只有满足 `GOAL.md` 的全部交付物和完成标准后，才能宣布 Rust 重写完成。
