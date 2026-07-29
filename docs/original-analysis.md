# NapCatQQ Original Analysis

## Repository metadata

- Source: https://github.com/NapNeko/NapCatQQ
- Commit checked: 33546b936e008c017b2b9c1c41a0bb4f9e86c5be
- Commit count: 4953

## 技术栈与构建方式

- Primary language: TypeScript / Node.js
- Package manager: pnpm (root uses `pnpm-lock.yaml`)
- Monorepo layout: `packages/*`
- Build/test style: module-scoped `pnpm` scripts in `original/package.json`

## 主要运行入口（原文档与脚本抽取）

- `README` 为主入口文档，提供发布与基础使用说明
- `original/package.json` 的脚本主要集中在：
  - `build:shell`、`build:framework`、`build:webui` 等模块化构建
  - `dev:shell` 开发启动
  - `test`、`test:ui` 测试入口

## 目录结构（一级）

- `.github/`：CI 与协作流程
- `packages/`：核心实现区域
  - `napcat-core`
  - `napcat-protocol`
  - `napcat-onebot`
  - `napcat-rpc`
  - `napcat-plugin-builtin`
  - `napcat-database`
  - `napcat-webui-backend`
  - `napcat-webui-frontend`
  - `napcat-framework`
  - `napcat-develop`
  - `napcat-common`
  - `napcat-types`

## 核心模块关系（按包名）

- `napcat-core`: 基础运行逻辑
- `napcat-protocol`: 协议能力层
- `napcat-onebot`: OneBot 兼容 API 与上层适配
- `napcat-rpc`: RPC 通道实现
- `napcat-plugin-builtin`: 插件加载与内置功能
- `napcat-database`: 存储和持久化能力
- `napcat-webui-backend`: 后端服务
- `napcat-framework`: 启动与协调层

## Rust 重写迁移建议（与目标仓库对齐）

- 目标仓库建议拆分为 `core` / `protocol` / `message` / `api` / `plugin` / `storage` / `config` / `cli`
- 以事件驱动 runtime + trait 抽象重构，确保业务层与协议实现解耦
- 可优先完成 `protocol` trait，再实现 `api`（HTTP + WebSocket）与统一消息模型
