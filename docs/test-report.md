# NapCatQQ-RS 测试与基准报告

## 环境

- 执行时间：2026-07-29T14:46:12Z
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
  - Storage: 4
  - Integration: 4
  - Runtime: 3
- 结果：全部通过。

### 基准

```bash
cargo bench
```

- `register_and_shutdown_runtime`: `10.348 µs ~ 11.103 µs`
- `register_and_shutdown_runtime_with_8_services`: `39.325 µs ~ 44.226 µs`
- 说明：两项基准均正常执行；`Gnuplot` 不存在时自动降级为 `plotters` 后端，未影响执行。
- 最新一次 run（同环境）：
  - `register_and_shutdown_runtime`: `10.348 µs ~ 11.103 µs`
  - `register_and_shutdown_runtime_with_8_services`: `39.325 µs ~ 44.226 µs`
  - 说明：该次运行对比历史版本有明显回退风险已缓解（基准总体改善），建议继续保留近期基线用于后续回归检查。
