# NapCatQQ-RS 测试与基准报告

## 环境

- 执行时间：2026-07-29T13:29:54Z
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
  - API: 4
  - Config: 3
  - Core: 3
  - Message: 2
  - Plugin: 6
  - Protocol: 2
  - Storage: 0
  - Integration: 4
  - Runtime: 3
- 结果：全部通过。

### 基准

```bash
cargo bench
```

- `register_and_shutdown_runtime`: `9.5319 µs ~ 9.7830 µs`
- `register_and_shutdown_runtime_with_8_services`: `31.236 µs ~ 31.955 µs`
- 说明：两项基准均显示“性能有提升”（相对历史比对环境）并且无失败。

### 备注

- Benchmark 期间 `gnuplot` 不存在，Criterion 自动降级为 `plotters` 后端，未影响基准执行。
