# NapCatQQ-RS 部署文档

## 1. 环境与前置

- Rust 1.97+
- Cargo 与 Git
- Linux/macOS 构建环境，推荐 Linux x86_64

```bash
rustc --version
cargo --version
git --version
```

## 2. 仓库准备

```bash
git clone https://github.com/Mist-wu/NapCatQQ-RS.git
cd NapCatQQ-RS

git submodule update --init --recursive || true
git checkout main
```

## 3. 本地构建与校验

```bash
cd napcat-rs
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets
cargo bench
```

## 4. 本机开发与版本管理

用户要求的工作模式为：

- 代码修改、提交与 `git push` 在本机完成。
- 远端服务器仅承担构建、测试与运行。

示例提交流程：

```bash
git add <files>
git commit -m "feat(api): add ..."
git push origin main
```

## 5. 运行服务

当前仓库仅提供 API/协议/插件等运行时组件，未提供完整生产级进程入口（`napcat-cli` 目前为参数占位入口）。可通过 Rust 调用 `napcat_api::run` 或在应用层包装为主服务。

示例：

```bash
cd napcat-rs
cargo run -p napcat-cli -- --help
```

如需临时运行 API：

```rust
napcat_api::run("127.0.0.1:3000").await
```

## 6. 环境变量

- `NAPCAT_HOST`
- `NAPCAT_PORT`
- `NAPCAT_LOG_LEVEL`
- `NAPCAT_DATABASE_URL`
- `NAPCAT_CONFIG_PATH`

## 7. 示例配置文件

```json
{
  "host": "127.0.0.1",
  "port": 3000,
  "log_level": "info",
  "database_url": "sqlite://./napcat.db"
}
```
