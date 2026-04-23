# NRI 说明

## 文档目的

本文档说明 `crius` 当前 NRI 能力、配置模型、运行方式和已知边界。它面向需要启用、验证或维护 NRI 的使用者和开发者。

## NRI 在项目中的位置

`crius` 的 NRI 实现位于 CRI 服务与运行时实现之间，负责把 Pod / Container 生命周期映射为 NRI 调用，并把插件调整结果落到本地 OCI spec 与运行时状态中。

逻辑关系如下：

```text
CRI handlers (src/server)
    ->
NRI API / Manager / Domain (src/nri)
    ->
Runtime abstraction (src/runtime)
    ->
runc / shim / bundle
```

主要模块职责如下：

| 模块 | 职责 |
| --- | --- |
| `src/server/` | 将 CRI 生命周期映射为 NRI 调用顺序 |
| `src/nri/api.rs` | 定义 `NriApi`、`NriDomain` 和 `NopNri` |
| `src/nri/manager.rs` | 插件注册表、排序、超时、同步和 dispatch |
| `src/nri/transport.rs` | ttRPC 传输层 |
| `src/nri/convert.rs` | Pod / Container 与 NRI 对象之间的转换 |
| `src/nri/domain.rs` | 快照、unsolicited update、evict 的运行时落地 |
| `src/nri/merge.rs` | 多插件 adjustment / update 合并与冲突检测 |
| `src/nri/adjust.rs` | 把 adjustment 应回本地 OCI spec |

## 当前能力

当前仓库已实现以下 NRI 主路径能力：

- ttRPC server/client
- 插件注册、排序、超时控制和 sync gate
- Pod / Container 生命周期接线
- adjustment merge、冲突检测和 `ValidateContainerAdjustment`
- unsolicited update / eviction 落地
- 重启恢复后的 `Synchronize`

首版未纳入的扩展能力：

- 通用 builtin plugin 框架
- wasm plugin 支持
- OpenTelemetry ttRPC tracing 注入

## 配置模型

NRI 配置位于 `src/config/mod.rs::NriConfig`。

| 字段 | 说明 | 默认值 |
| --- | --- | --- |
| `enable` | 是否启用 NRI | `false` |
| `runtime_name` | 上报给插件的 runtime 名称 | `crius` |
| `runtime_version` | 上报给插件的 runtime 版本 | crate 版本 |
| `socket_path` | 运行时侧 NRI socket | `/run/crius/nri.sock` |
| `plugin_path` | 预装插件扫描目录 | `/opt/nri/plugins` |
| `plugin_config_path` | 插件配置目录 | `/etc/nri/conf.d` |
| `blockio_config_path` | `blockio_class` 映射文件 | 空 |
| `allowed_annotation_prefixes` | 全局 annotation allowlist | `[]` |
| `runtime_allowed_annotation_prefixes` | 按 runtime handler 扩展 allowlist | `{}` |
| `workload_allowed_annotation_prefixes` | 按 workload 激活的 allowlist | `[]` |
| `registration_timeout_ms` | 插件注册超时 | `5000` |
| `request_timeout_ms` | 插件请求超时 | `2000` |
| `enable_external_connections` | 允许外部插件主动连入 | `false` |
| `default_validator` | 默认 validator 配置 | 全部关闭 |

示例配置：

```toml
[nri]
enable = true
runtime_name = "crius"
runtime_version = "0.1.0"
socket_path = "/run/crius/nri.sock"
plugin_path = "/opt/nri/plugins"
plugin_config_path = "/etc/nri/conf.d"
blockio_config_path = "/etc/nri/blockio.json"
registration_timeout_ms = 5000
request_timeout_ms = 2000
enable_external_connections = false
allowed_annotation_prefixes = ["io.kubernetes."]

[nri.default_validator]
enable = true
reject_namespace_adjustment = true
```

`blockio_config_path` 当前使用本项目的映射格式，例如：

```json
{
  "classes": {
    "gold": {
      "weight": 500
    }
  }
}
```

## 生命周期接入点

当前已接入的生命周期包括：

- `RunPodSandbox`
- `StopPodSandbox`
- `RemovePodSandbox`
- `UpdatePodSandbox`
- `PostUpdatePodSandbox`
- `CreateContainer`
- `PostCreateContainer`
- `StartContainer`
- `PostStartContainer`
- `UpdateContainer`
- `PostUpdateContainer`
- `StopContainer`
- `RemoveContainer`
- 容器异步退出后的状态通知

实现上，`crius` 会先生成 pristine OCI spec，再应用 NRI adjustment，最后写入 bundle 并交由 runtime 执行。

## 支持的 adjustment / update 能力

当前已支持的调整项包括：

- annotations、env、args、mounts、hooks
- devices、CDI devices、rlimits、OOM score
- Linux namespaces、seccomp、sysctl、cgroups path
- CPU：`shares`、`quota`、`period`、`cpus`、`mems`、`realtimeRuntime`、`realtimePeriod`
- Memory：`limit`、`reservation`、`swap`、`kernel`、`kernelTCP`、`swappiness`、`disableOOMKiller`、`useHierarchy`
- `hugepage_limits`、`unified`、`pids`
- `blockio_class`
- `rdt_class`
- unsolicited `UpdateContainers`
- eviction

## 启用方式

启用 NRI 的典型流程如下：

1. 在配置文件中设置 `[nri].enable = true`
2. 准备插件目录和插件配置目录
3. 启动 `crius`
4. 确认运行时创建了 `socket_path`
5. 启动或放置 NRI 插件
6. 观察插件注册、`Configure` 和 `Synchronize` 是否成功

如果使用预装插件模式，运行时会按 `plugin_path` 扫描并拉起插件；如果启用了外部连接，则插件也可以通过 socket 主动接入。

## 排障建议

排查 NRI 问题时，建议按以下顺序检查：

1. 确认 `[nri].enable` 已开启，且 `socket_path` 可创建
2. 检查 `plugin_path`、`plugin_config_path` 是否存在且权限正确
3. 检查插件是否完成注册、是否通过 `Configure` 和 `Synchronize`
4. 确认插件请求未被 `ValidateContainerAdjustment` 或 annotation allowlist 拒绝
5. 检查资源更新是否触发运行时能力裁剪，例如 cgroup 或 block I/O 限制

如果问题集中在 `blockio_class`，应同时核对：

- `blockio_config_path` 是否可读
- 类名是否存在于映射文件
- 插件下发的 class 是否与本地配置一致

## 已知边界

- `blockio_class` 当前使用 `crius` 自有映射格式，不直接复用上游 Go blockio loader
- 文档只覆盖运行时侧 NRI，不包含上游 builtin / wasm 插件生态能力
- annotation allowlist 已接入全局、runtime handler 和 workload 三层，但当前配置模型仍以本项目自身规则为准
- 关闭 NRI 时会走 `NopNri` 路径，目标是保持原有 CRI 行为不变
