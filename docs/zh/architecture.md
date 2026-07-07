# 架构设计

本文档描述当前 `crius` 代码库已经实现的架构，面向需要理解、评估或扩展该运行时的
开发者和使用者。这里关注代码事实，而不是理想化目标架构。

## 项目定位

`crius` 是面向 Kubernetes 的 CRI runtime。它对 kubelet 暴露 CRI v1 gRPC 服务，
并将 CRI 对象映射到以下内部能力：

- OCI runtime 执行
- 镜像存储与 registry 访问
- Pod 网络
- 流式 I/O
- 本地状态持久化与恢复
- NRI、CDI、checkpoint / restore 等运行时扩展点

因此，`crius` 不是单纯的 `runc` 包装层，而是 CRI 对象模型到执行、镜像、网络、
状态和插件系统之间的编排层。

## 进程模型

| 进程 | 角色 |
| --- | --- |
| `crius` | 主 daemon。负责 CRI gRPC、ImageService、RuntimeService、streaming 装配、状态恢复、metrics、tracing、NRI 和控制面逻辑。 |
| `crius-shim` | 默认 runtime 路径使用的 per-container helper。负责进程、I/O、attach socket、task RPC、checkpoint / restore task 和退出状态协作。 |

外部依赖包括 `runc`、CNI plugins、OCI registry、SQLite、可选 CRIU、可选 NRI
plugins，以及可选 rootless network helpers。

## 高层结构

```text
Kubelet / crictl
    |
    v
CRI gRPC server (`src/main.rs`, `src/server`)
    |
    +--> Runtime orchestration (`src/runtime`)
    +--> Pod sandbox management (`src/pod`)
    +--> Image service (`src/image`)
    +--> Network service (`src/network`)
    +--> Streaming service (`src/streaming`)
    +--> Persistence (`src/storage`)
    +--> NRI (`src/nri`)
```

## 模块边界

| 模块 | 职责 |
| --- | --- |
| `src/main.rs` | CLI、配置加载、环境变量与 CLI 覆盖、日志、metrics、tracing、gRPC 和 streaming 启动 |
| `src/config/` | TOML 模型、默认值、派生路径归一化、校验、环境变量覆盖 |
| `src/server/` | CRI RuntimeService、生命周期、status、stats、events、recovery、checkpoint / restore 编排 |
| `src/image/` | CRI ImageService、registry pull、auth、镜像元数据、image volumes、artifact 与 snapshotter 配置 |
| `src/runtime/` | runtime backend trait、默认 `runc` backend、`wasm-direct` 接线、OCI bundle、shim manager |
| `src/shim/` | shim daemon、子进程、I/O、attach socket、task RPC |
| `src/network/` | CNI 加载、ADD / DEL、hostPort、PodCIDR 模板渲染、rootless helper |
| `src/streaming/` | exec、attach、port-forward 的 HTTP streaming server |
| `src/storage/` | SQLite 状态账本和持久化 runtime metadata |
| `src/nri/` | NRI transport、plugin manager、对象转换、adjustment merge、验证、domain action |
| `src/security/` | seccomp、AppArmor、SELinux、CDI、devices、resource classes、OCI spec patch |
| `src/rootless/` | rootless 配置解析与状态模型 |
| `src/metrics/` | runtime、resources、images metrics 服务 |

## 配置生命周期

daemon 按以下顺序构造 effective configuration：

```text
内置默认值 -> 配置文件 -> 环境变量 -> CLI 覆盖 -> 派生路径归一化 -> 校验
```

重要归一化行为包括：

- runtime 派生路径跟随 `runtime.root`。
- 持久化派生路径跟随 `root`。
- NRI socket 默认跟随 runtime state 目录。
- rootless 模式会将仍处于默认值的 root、runtime、image、logging 路径移动到 XDG
  目录。
- `runtime.container_stop_timeout` 会被归一化到至少 30 秒。
- `runtime.handlers` 会与 `runtime.runtimes.*` 和默认 `runtime.runtime_type`
  一起归一化。

## Runtime Handler

默认 handler 是 `runtime.runtime_type`，通常为 `runc`。额外 handler 通过
`runtime.handlers` 和 `runtime.runtimes.<name>` 声明。

单个 handler 可配置：

- backend 类型：`runc` 或 `wasm-direct`
- runtime binary 和 runtime root
- platform-specific runtime path
- monitor / shim path
- monitor cgroup 和环境变量
- allowed annotations 和默认 OCI annotations
- privileged host-device 行为
- container create timeout
- snapshotter
- handler-specific CNI 配置

kubelet 可通过 `RuntimeClass` 按名称选择 handler。

## 核心请求链路

### RunPodSandbox

典型流程：

1. 校验请求和 runtime handler。
2. 分配逻辑 Pod Sandbox 状态。
3. 准备 Pod 目录和 network namespace 状态。
4. 生成 pause container OCI spec。
5. 在适用阶段执行 NRI pod / container lifecycle hook。
6. 通过 runtime backend 和 shim 创建并启动 pause container。
7. 应用 CNI 网络和 hostPort 映射。
8. 持久化状态并发布生命周期事件。

### CreateContainer / StartContainer

容器创建和启动分为两个阶段：

1. 解析 Pod Sandbox 和 runtime handler。
2. 解析镜像元数据和 rootfs 行为。
3. 根据 CRI config、security context、mounts、resources、runtime 默认配置和
   workload preset 构造 OCI spec。
4. 应用并验证 NRI container adjustment。
5. 写入 bundle 并创建容器。
6. 启动容器、更新持久化状态并发布事件。

如果 container config 引用了 checkpoint artifact，`CreateContainer` 会加载
checkpoint metadata，`StartContainer` 会走 runtime checkpoint / restore 路径。

### Exec / Attach / PortForward

streaming 采用控制面和数据面分离的两阶段设计：

1. kubelet 调用 CRI gRPC 方法。
2. `crius` 注册短生命周期 token 和请求上下文。
3. `crius` 返回 streaming URL。
4. kubelet 连接 HTTP streaming server。
5. streaming server 解析 token，并连接 runtime 或 shim I/O 路径。

streaming server 支持 TLS 配置和 per-request token 过期。

### Checkpoint / Restore

`CheckpointContainer` 要求 `runtime.enable_criu_support = true`，并且 runtime path
支持 checkpoint / restore。实现会导出 checkpoint metadata、OCI config 和 rootfs
snapshot。restore 在 container create / start 阶段使用 checkpoint metadata 完成。

更多细节见 [checkpoint-restore.md](checkpoint-restore.md)。

## 状态模型与恢复

`crius` 同时维护三类状态：

- daemon 运行期间的内存状态
- bundle、shim socket、netns mount、exit file 等 runtime 工件
- `root` 下的 SQLite 持久化账本

daemon 重启后，recovery 会读取持久化状态、重建内存对象、检查 runtime 和 shim 工件、
在可能时恢复 monitor、按配置清理孤儿工件，并通过 status 和 events 暴露恢复信息。

设计目标不是让三层状态在任意瞬间完全一致，而是在 daemon 重启和异常场景后能够重新
收敛，并继续向 kubelet 提供一致的 CRI 语义。

## 网络

默认网络实现是 CNI。本地 `crs` Pod 工作流从 `[network.local]` 加载 CNI 配置，
默认目录为 `/etc/crius/cni/net.d`。kubelet 和通用 CRI 工作流从 `[network.cri]`
加载 CNI 配置，默认目录为 `/etc/cni/net.d` 和 `/etc/kubernetes/cni/net.d`。
旧版顶层 `[network]` CNI 字段仍被接受，并映射到 CRI 网络域。

支持行为包括：

- Pod Sandbox CNI ADD / DEL
- hostPort 映射
- 通过 `UpdateRuntimeConfig` 和 `network.conf_template` 渲染 PodCIDR 模板
- handler-specific CNI config dir
- rootless 模式下启动对应 network helper

## 可观测性

当前可观测性入口包括：

- 基于 `tracing` 的结构化日志
- 可选日志文件输出
- CRI `GetContainerEvents`
- Pod / container stats
- `RuntimeConfig` 和 `Status` 详细字段
- 可选 metrics service，支持 TCP 或 Unix socket
- 可选 tracing export

这些能力适合开发和验证。生产候选环境仍应补充日志采集、告警和运维 runbook。

## 当前边界

- 默认且最主要的执行路径是 Linux + `runc`。
- 仓库仍以源码构建和节点验证为主。
- rootless、`wasm-direct`、NRI、external snapshotter、checkpoint / restore 都已经
  接线，但需要按主机和场景单独验证。
- 本文档描述当前行为，不构成对任意 Kubernetes 版本或宿主发行版的兼容性承诺。
