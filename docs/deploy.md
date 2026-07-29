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

## 3. 远端验证与校验（推荐在服务器执行）

```bash
cd napcat-rs
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets
cargo bench
```

## 4. 开发与版本管理

代码、构建、测试与提交建议在远端主机执行。

示例提交流程：

```bash
git add <files>
git commit -m "feat(api): add ..."
git push origin main
```

## 5. 运行服务

CLI 现已提供可直接启动 API 的入口。

```bash
cd napcat-rs
cargo run -p napcat-cli -- --help
cargo run -p napcat-cli -- --host 127.0.0.1 --port 3000
cargo run -p napcat-cli -- --host 0.0.0.0 --port 8080 --debug
```

也可直接调用 API 启动函数：

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
