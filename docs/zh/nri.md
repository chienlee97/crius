# NRI

本文档说明 `crius` 当前 Node Resource Interface（NRI）支持的能力、配置、生命周期、
adjustment、内建验证器和运维边界。

## 在 crius 中的位置

NRI 位于 CRI handler 和 runtime 执行之间：

```text
CRI handlers (`src/server`)
    |
    v
NRI manager / conversion / merge / validation (`src/nri`)
    |
    v
Runtime backend (`src/runtime`)
    |
    v
runc / shim / bundle
```

NRI 不直接运行容器。它接收生命周期通知，并返回由 `crius` 应用到 OCI spec、runtime
状态和资源更新中的 adjustment。

## 当前能力

当前 NRI 支持包括：

- ttRPC server / client transport
- 插件发现和注册
- 插件排序
- 注册和请求超时
- `Configure` 与 `Synchronize`
- Pod 和 Container 生命周期分发
- 多插件 adjustment merge 和冲突检测
- `ValidateContainerAdjustment`
- 内建 default adjustment validator
- unsolicited container update
- eviction request
- `blockio_class` 和 `rdt_class`
- 启用时的 CDI device adjustment
- daemon 重启后的 synchronize

## 主要模块

| 模块 | 作用 |
| --- | --- |
| `src/nri/api.rs` | runtime-facing NRI interface 和 `NopNri` |
| `src/nri/manager.rs` | 插件生命周期、排序、超时和分发 |
| `src/nri/transport.rs` | ttRPC transport |
| `src/nri/convert.rs` | CRI / runtime 对象到 NRI 对象的转换 |
| `src/nri/merge.rs` | adjustment merge 和冲突检测 |
| `src/nri/adjust.rs` | 将 NRI adjustment 应用到本地 OCI spec |
| `src/nri/domain.rs` | unsolicited update 和 eviction 的 runtime domain |
| `src/nri/default_validator.rs` | 内建 adjustment validation policy |

## 最小配置

```toml
[nri]
enable = true
enable_cdi = true
runtime_name = "crius"
runtime_version = "0.1.0"
socket_path = "/run/crius/nri.sock"
plugin_path = "/opt/nri/plugins"
plugin_config_path = "/etc/nri/conf.d"
blockio_config_path = ""
cdi_spec_dirs = ["/etc/cdi", "/var/run/cdi"]
allowed_annotation_prefixes = []
workload_allowed_annotation_prefixes = []
registration_timeout_ms = 5000
request_timeout_ms = 2000
enable_external_connections = false

[nri.runtime_allowed_annotation_prefixes]

[nri.default_validator]
enable = false
reject_oci_hook_adjustment = false
reject_runtime_default_seccomp_adjustment = false
reject_unconfined_seccomp_adjustment = false
reject_custom_seccomp_adjustment = false
reject_namespace_adjustment = false
required_plugins = []
tolerate_missing_plugins_annotation = ""
```

说明：

- 不需要 NRI 时保持 `enable = false`。
- `runtime_version` 默认来自当前 package version。
- `socket_path` 保持默认值时会跟随 `runtime.root`。
- `enable_cdi` 只控制 CDI device adjustment，不是 NRI 总开关。
- `enable_external_connections` 控制是否允许外部启动的插件连接 runtime socket。

## 生命周期接入点

NRI 已接入的主要生命周期：

- Pod Sandbox run、stop、remove
- Container create 和 post-create
- Container start 和 post-start
- Container update 和 post-update
- Container stop 和 remove
- 异步 container exit 同步
- daemon restart synchronize
- unsolicited update
- eviction

`crius` 会先准备基础 OCI spec，再应用通过验证的 NRI adjustment，然后写入 bundle 并交给
所选 runtime backend 执行。

## Adjustment 范围

支持的 adjustment 包括：

- annotations
- environment variables
- args
- mounts
- OCI hooks
- devices
- CDI devices
- rlimits
- OOM score
- namespaces
- seccomp
- sysctls
- cgroups path
- Linux CPU、memory、hugepage、pids 等资源
- `blockio_class`
- `rdt_class`

runtime update 和 eviction 通过 NRI runtime domain 落地。

## Annotation Allowlist

NRI annotation 过滤可以按全局、runtime handler 或 workload activation 配置：

```toml
[nri]
allowed_annotation_prefixes = ["example.com/global/"]

[nri.runtime_allowed_annotation_prefixes]
runc = ["example.com/runc/"]

[[nri.workload_allowed_annotation_prefixes]]
activation_annotation = "example.com/workload"
activation_value = "latency-sensitive"
allowed_annotation_prefixes = ["example.com/workload/"]
```

为依赖 workload annotation 的插件配置 allowlist 时，应保持小而明确。

## 内建验证器

内建 validator 可在 adjustment 到达 runtime 前拒绝指定类型的插件修改：

```toml
[nri.default_validator]
enable = true
reject_oci_hook_adjustment = true
reject_unconfined_seccomp_adjustment = true
reject_namespace_adjustment = true
required_plugins = ["plugin-a"]
tolerate_missing_plugins_annotation = "example.com/tolerate-missing-nri"
```

它适合用于节点镜像中的插件策略验证，但不能替代插件自身的审查和测试。

## 启用流程

建议顺序：

1. 设置 `[nri].enable = true`。
2. 准备 `plugin_path` 和 `plugin_config_path`。
3. 审查 annotation allowlist 和 validator policy。
4. 启动 `crius`。
5. 确认 runtime socket 已创建。
6. 启动插件或放置预装插件 binary。
7. 确认插件注册、`Configure` 和 `Synchronize` 完成。
8. 运行会触发插件的 Pod / Container 生命周期测试。

## 运维建议

- 将 NRI 视为 runtime extension layer。
- 每个插件先在单节点验证，再扩大范围。
- 对会修改 resources、devices、mounts、annotations、namespaces、hooks、seccomp 或
  cgroups 的插件建立回归测试。
- 使用 `blockio_class`、`rdt_class`、CDI 或大量 device adjustment 前先验证宿主能力。
- 插件请求超时应足够短，避免影响 kubelet-facing latency。

## 排障

建议按以下顺序检查：

1. `[nri].enable` 为 true。
2. `socket_path` 存在且权限符合预期。
3. `plugin_path` 和 `plugin_config_path` 存在。
4. 插件完成注册。
5. `Configure` 和 `Synchronize` 成功。
6. annotation allowlist 允许目标 workload annotations。
7. 内建 validator policy 未拒绝 adjustment。
8. 请求的 devices、resources、classes 和 hooks 被宿主机与 runtime 支持。

`blockio_class` 问题应额外确认：

- `blockio_config_path` 可读。
- class name 存在于配置映射中。
- 宿主机支持对应 block I/O 控制。

## 边界

- 本文只覆盖 `crius` runtime-side NRI 行为，不覆盖完整上游插件生态。
- 插件兼容性需要按插件和主机验证。
- `blockio_class` 使用本项目本地 mapping 配置。
- NRI 关闭时，`crius` 使用 `NopNri` 路径，并尽量保持普通 CRI 行为不变。

## 相关文档

- [README.zh-CN.md](../../README.zh-CN.md)
- [architecture.md](architecture.md)
- [config-matrix.md](config-matrix.md)
- [kubeadm.md](kubeadm.md)
