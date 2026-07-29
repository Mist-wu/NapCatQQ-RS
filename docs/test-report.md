# NapCatQQ-RS 测试与基准报告

## 环境

- 执行时间：2026-07-29T14:05:00Z
- 执行位置：云服务器 `root@152.42.241.53`
- 项目路径：`/root/projects/NapCatQQ-RS/napcat-rs`

## 验证命令

### 格式化检查

```bash
cd /root/projects/NapCatQQ-RS/napcat-rs
cargo fmt --all -- --check
```

- 结果：通过（无输出）。

### 静态检查

```bash
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

- 结果：通过。

### 测试

```bash
cargo test --workspace --all-targets
```

- 测试用例数：
  - API: 5
  - Config: 3
  - Core: 3
  - Message: 2
  - Plugin: 6
  - Protocol: 2
  - Storage: 0
  - Integration: 0
  - Runtime: 3
- 结果：全部通过。

### 基准

```bash
cargo bench
```

- `register_and_shutdown_runtime`: `10.501 µs ~ 11.539 µs`（与基线相比有提升）
- `register_and_shutdown_runtime_with_8_services`: `32.608 µs ~ 36.686 µs`（与基线相比有提升）
- 说明：两项基准均正常执行；`Gnuplot` 不存在时自动降级为 `plotters` 后端，未影响执行。
