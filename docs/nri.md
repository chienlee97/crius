# NRI 说明

本文档说明 `crius` 当前的 NRI 能力、配置模型、生命周期接线与运维边界，面向需要启用或排查 NRI 的用户和开发者。

## NRI 在 `crius` 中的位置

`crius` 的 NRI 实现位于 CRI 服务与底层 runtime 之间。它的职责不是直接运行容器，而是在关键生命周期阶段接收插件调整，并把调整结果落到 OCI spec、运行时状态和后续资源更新中。

逻辑关系如下：

```text
CRI handlers (`src/server`)
    ->
NRI manager / domain / adjust (`src/nri`)
    ->
Runtime abstraction (`src/runtime`)
    ->
runc / shim / bundle
```

## 当前能力

当前仓库已经接入以下 NRI 主路径能力：

- ttRPC server/client
- 插件注册、排序与请求超时控制
- Pod / Container 生命周期分发
- 多插件 adjustment merge 与冲突检测
- `ValidateContainerAdjustment`
- unsolicited update
- eviction
- 守护进程重启后的 `Synchronize`

## 主要模块

| 模块 | 职责 |
| --- | --- |
| `src/nri/api.rs` | NRI 抽象接口与 `NopNri` |
| `src/nri/manager.rs` | 插件发现、注册、排序、超时与调度 |
| `src/nri/transport.rs` | ttRPC 传输层 |
| `src/nri/convert.rs` | CRI / runtime 对象与 NRI 对象之间的转换 |
| `src/nri/merge.rs` | 多插件 adjustment 合并与冲突检测 |
| `src/nri/adjust.rs` | 把 adjustment 落回本地 OCI spec |
| `src/nri/domain.rs` | unsolicited update 与 eviction 的运行时落地 |

## 最小配置

```toml
[nri]
enable = true
enable_cdi = true
runtime_name = "crius"
socket_path = "/run/crius/nri.sock"
plugin_path = "/opt/nri/plugins"
plugin_config_path = "/etc/nri/conf.d"
blockio_config_path = ""
cdi_spec_dirs = ["/etc/cdi", "/var/run/cdi"]
allowed_annotation_prefixes = []
registration_timeout_ms = 5000
request_timeout_ms = 2000
enable_external_connections = false
```

说明：

- 如果不需要 NRI，请保持 `enable = false`。
- `runtime_version` 建议使用运行时实际版本，而不是手工写入固定示例值。
- `plugin_path` 用于预装插件模式。
- `enable_external_connections` 控制是否允许外部插件主动连接到 runtime socket。
- `enable_cdi` 只影响 CDI 设备调整链路，不等价于整个 NRI 开关。

## 生命周期接入点

当前已接入的关键生命周期包括：

- `RunPodSandbox`
- `StopPodSandbox`
- `RemovePodSandbox`
- `CreateContainer`
- `PostCreateContainer`
- `StartContainer`
- `PostStartContainer`
- `UpdateContainer`
- `PostUpdateContainer`
- `StopContainer`
- `RemoveContainer`
- 容器异步退出后的状态同步

实现上，`crius` 会先准备基础 OCI spec，再应用 NRI adjustment，最后把结果写入 bundle 并交给 runtime 执行。

## 支持的调整能力

当前已接入的调整范围包括：

- annotations、env、args、mounts、hooks
- devices、CDI devices、rlimits、OOM score
- namespaces、seccomp、sysctl、cgroups path
- CPU、memory、hugepage、pids 等 Linux 资源
- `blockio_class`
- `rdt_class`
- unsolicited `UpdateContainers`
- eviction

这意味着 NRI 可以参与“创建前调整”和“运行中资源调整”两类主路径。

## 启用流程

建议按以下顺序启用：

1. 在配置文件中打开 `[nri].enable = true`
2. 准备插件目录和插件配置目录
3. 启动 `crius`
4. 确认 `socket_path` 已创建
5. 启动插件或放置预装插件
6. 检查插件是否完成注册、`Configure` 和 `Synchronize`

## 运维建议

- 把 NRI 视为运行时扩展层，而不是核心容器启动必需层
- 先在单节点验证插件行为，再推广到更多节点
- 对会修改资源、devices、annotations 的插件，务必建立回归测试
- 对 `blockio_class`、`rdt_class` 这类依赖宿主能力的功能，先验证宿主内核和文件系统前提

## 排障顺序

出现 NRI 相关问题时，建议按以下顺序排查：

1. 确认 `[nri].enable` 已开启
2. 检查 `socket_path`、`plugin_path`、`plugin_config_path` 是否存在且权限正确
3. 确认插件已成功注册，并通过 `Configure` 与 `Synchronize`
4. 检查 adjustment 是否被 annotation allowlist 或 validator 拒绝
5. 检查插件下发的资源是否超出宿主或 OCI runtime 能力边界

如果问题集中在 `blockio_class`：

- 确认 `blockio_config_path` 可读
- 确认类名存在于映射文件中
- 确认宿主环境真的支持对应 block I/O 调整

## 已知边界

- 文档只覆盖 `crius` 的运行时侧 NRI 能力，不覆盖完整上游插件生态
- `blockio_class` 依赖本项目的本地映射配置，而不是自动复用外部 Go 实现
- 关闭 NRI 时会退回 `NopNri` 路径，目标是保持原有 CRI 行为尽可能不变
- 当前仓库尚未提供覆盖不同 Kubernetes 版本、插件实现和宿主发行版组合的正式验证矩阵

## 相关文档

- [架构设计](architecture.md)
- [配置参考](config-matrix.md)
