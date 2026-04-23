# 架构设计

## 文档目标

本文档描述 `crius` 当前代码库的整体架构、核心模块职责、主请求链路、状态模型和关键设计取舍。它面向需要理解、维护或扩展运行时实现的开发者。

本文档基于当前仓库实现，而不是抽象的目标架构。因此这里描述的是“现在系统如何工作”，不是“未来可能如何工作”。

## 总体定位

`crius` 是一个以 Rust 实现的 Kubernetes CRI 运行时。它对外提供 CRI v1 gRPC 服务，对内负责：

- 管理 Pod Sandbox 与业务容器生命周期
- 生成并维护 OCI bundle / `config.json`
- 调用 `runc` 执行容器创建、启动、停止、删除和资源更新
- 管理镜像元数据与本地镜像存储
- 配置 Pod 网络与端口映射
- 提供 `exec`、`attach`、`port-forward` 等流式能力
- 通过 SQLite 持久化运行时状态，并在进程重启后恢复
- 作为运行时侧 NRI endpoint 与插件交互

## 进程模型

当前系统主要由两个二进制组成：

| 进程 | 角色 |
| --- | --- |
| `crius` | 主守护进程，暴露 CRI gRPC 和 ImageService，负责状态、调度和大多数控制面逻辑 |
| `crius-shim` | 可选 shim 进程，负责容器进程、IO、退出状态和 attach 相关的执行面工作 |

此外，系统还依赖若干外部组件：

| 外部组件 | 作用 |
| --- | --- |
| `runc` | 实际执行 OCI 容器生命周期 |
| CNI plugins | 配置 Pod 网络 |
| OCI registry | 拉取镜像 |
| SQLite | 状态持久化 |
| NRI plugins | 对 Pod / Container 生命周期和资源进行扩展控制 |

## 高层结构

整体调用关系可以概括为：

```text
Kubelet / crictl
    ->
CRI gRPC server (`src/main.rs` + `src/server`)
    ->
Runtime orchestration
    |-> Pod sandbox management (`src/pod`)
    |-> OCI / runc integration (`src/runtime`)
    |-> Image management (`src/image`)
    |-> Network setup (`src/network`)
    |-> Streaming endpoint (`src/streaming`)
    |-> State persistence / recovery (`src/storage`)
    |-> NRI integration (`src/nri`)
```

从职责上看，`crius` 不是单纯的 `runc` 封装层，而是一个把 CRI 对象模型翻译成 OCI、网络、镜像、持久化和插件协作的编排层。

## 模块划分

### 入口与装配

`src/main.rs` 负责：

- 解析 CLI 参数
- 加载配置文件；失败时回退到默认配置
- 从环境变量解析运行时和 CNI 相关设置
- 创建 `RuntimeServiceImpl`
- 启动本地 streaming server
- 初始化状态恢复和 NRI
- 启动 gRPC 服务并注册 `RuntimeService`、`ImageService`、reflection

这一层只做装配，不承载具体的业务逻辑。

### CRI RuntimeService

`src/server/mod.rs` 中的 `RuntimeServiceImpl` 是系统的控制中心。它聚合了：

- 运行时内存态缓存
- `RuncRuntime`
- `PodSandboxManager`
- `PersistenceManager`
- `StreamingServer`
- NRI API 句柄
- 事件广播能力

这一层负责把 CRI 请求转换为内部操作序列，并协调各子系统的顺序，例如：

1. 校验和提取 CRI 请求
2. 组织 Pod / Container 配置
3. 调用 NRI 做 pre-create / update / stop 等流程
4. 调用 pod/runtime/network 子系统执行
5. 更新内存态和持久化状态
6. 产出 CRI 响应或 streaming URL

### Pod Sandbox 管理

`src/pod/mod.rs` 负责 Pod 级别对象管理，核心关注点是：

- pause 容器创建与销毁
- network namespace 生命周期
- Pod 级 DNS 配置
- CNI 网络接入与回收
- 端口映射
- Pod 级安全和资源配置向 pause 容器的投影

Pod sandbox 在当前实现中是 CRI Pod 的承载外壳。业务容器通过 namespace 共享、注解和运行时状态与其关联。

### 容器运行时抽象

`src/runtime/mod.rs` 定义了 `ContainerRuntime` trait，并由 `RuncRuntime` 提供当前实现。该层负责：

- 生成和维护 OCI spec
- 管理 bundle 目录
- 调用 `runc create/start/kill/delete`
- 查询容器状态
- 更新 cgroup 资源
- 对接 shim 管理器
- 处理 checkpoint / restore 相关状态

设计上，这层把“CRI 语义”与“OCI 执行细节”隔开。`server` 层关心容器对象和流程，`runtime` 层关心 bundle、spec、命令行和底层执行。

### 网络

`src/network/` 提供网络抽象和默认 CNI 实现，主要职责是：

- 解析 CNI 配置目录、插件目录和缓存目录
- 创建 / 删除网络命名空间
- 调用 CNI 插件配置 Pod 网络
- 回收网络和缓存状态
- 维护端口映射逻辑

网络子系统被 `PodSandboxManager` 使用，而不是直接暴露给 CRI 层。这意味着当前的网络模型是“Pod 先有网络，再有业务容器”。

### 镜像

`src/image/mod.rs` 实现 CRI `ImageService`，负责：

- 镜像引用规范化
- OCI registry 拉取
- 本地镜像元数据维护
- 镜像查询、列举、删除
- 镜像文件系统统计

镜像服务与运行时服务是并列的 gRPC service，但它们共享同一个运行时根目录下的存储约定。

### 流式服务

`src/streaming/mod.rs` 提供独立的 HTTP streaming server，用于：

- `Exec`
- `Attach`
- `PortForward`

其设计不是直接在 gRPC 长连接里承载数据流，而是：

1. CRI 请求先到 `RuntimeService`
2. `RuntimeService` 把请求上下文注册到本地 `StreamingServer`
3. 返回带 token 的 URL
4. 客户端随后访问该 URL
5. StreamingServer 根据 token 取出上下文并执行实际流转

这样做的好处是把 CRI 控制面和流式数据面解耦，也符合 Kubernetes 常见的 streaming 交互方式。

### 持久化与恢复

`src/storage/` 使用 SQLite 持久化：

- Pod sandbox 记录
- Container 记录
- 状态事件

恢复逻辑由 `RuntimeServiceImpl` 在启动时触发。目标是：

- 恢复 daemon 内存态
- 恢复 Pod / Container 关联关系
- 对重启前的对象重新建立管理视图
- 在 NRI 启用时重新做 `Synchronize`

当前设计是“SQLite 作为运行时状态账本”，而不是只做缓存。内存态是活跃视图，SQLite 是重启恢复基础。

### NRI

`src/nri/` 是运行时侧 NRI 集成，负责：

- ttRPC transport
- 插件注册和排序
- 生命周期事件 dispatch
- adjustment / update merge
- adjustment 校验和落地
- unsolicited update / eviction 处理
- 恢复后的同步

NRI 处在 CRI 编排层与 OCI/runtime 落地层之间。它可以修改将要写入 OCI spec 的内容，也可以在运行中对资源更新产生影响。

NRI 的专题细节见 [`docs/nri.md`](/root/crius/docs/nri.md)。

### Shim

`src/shim/` 提供可选 shim 进程实现，主要处理：

- 容器进程托管
- IO 管道
- 子进程回收
- daemon 化

其设计目标是把一部分与容器进程生命周期强相关、对主守护进程不友好的职责隔离出去。

## 关键运行路径

### 1. 守护进程启动

启动时主流程如下：

```text
parse args
    ->
load config / fall back to defaults
    ->
build RuntimeConfig
    ->
create RuntimeServiceImpl
    ->
start StreamingServer
    ->
recover persisted state
    ->
initialize NRI
    ->
load local images
    ->
serve CRI gRPC
```

这里有两个值得注意的点：

- 配置文件加载失败不会阻止进程启动，而是回退到默认配置
- streaming server 在 CRI gRPC server 之前启动，因此 `Exec/Attach/PortForward` 能尽早返回可用 URL

### 2. `RunPodSandbox`

Pod sandbox 创建大致分为：

```text
CRI RunPodSandbox request
    ->
build PodSandboxConfig
    ->
prepare pod directories / resolv.conf / netns
    ->
create pause container config
    ->
create and start pause container through runtime
    ->
setup CNI network
    ->
persist pod state
    ->
return pod sandbox id
```

这一路径的核心目标是先建立 Pod 级隔离边界，再允许后续业务容器加入这个边界。

### 3. `CreateContainer`

容器创建是各子系统交汇最多的路径：

```text
CRI CreateContainer request
    ->
load pod sandbox context
    ->
translate CRI config into internal ContainerConfig
    ->
build pristine OCI intent
    ->
invoke NRI create hooks / apply adjustments
    ->
materialize OCI bundle and config.json
    ->
create container via runtime (runc + optional shim)
    ->
persist container metadata
    ->
return container id
```

这一阶段通常还没有真正开始执行业务进程；真正运行发生在 `StartContainer`。

### 4. `StartContainer`

启动路径相对简单：

```text
CRI StartContainer
    ->
optional NRI pre/post start hooks
    ->
runtime start
    ->
register exit monitor
    ->
update state and emit event
```

退出监控的目标是把异步的容器进程退出转换成运行时状态更新和事件广播。

### 5. `Exec` / `Attach` / `PortForward`

这些请求并不直接在 CRI gRPC 响应中承载数据，而是先返回 URL：

```text
CRI streaming request
    ->
validate request
    ->
cache request context in StreamingServer
    ->
return tokenized URL
    ->
client connects to HTTP endpoint
    ->
server consumes token and starts stream session
```

这意味着 streaming server 是控制面之外的第二个入口，但它只接受由主服务预登记过的请求 token。

## 状态模型

系统内部同时维护三类状态：

### 1. 内存态

`RuntimeServiceImpl` 中保存运行中对象的内存映射，用于：

- 快速响应 CRI 查询
- 维护当前进程视角下的活跃对象
- 事件广播和并发流程协调

### 2. 持久化状态

SQLite 中保存对象记录和状态事件，用于：

- 进程重启后恢复
- 保留 Pod / Container 基本事实
- 为异常退出后的重新接管提供基础

### 3. 外部真实状态

外部真实状态存在于：

- `runc` / 容器进程
- bundle 目录和 OCI `config.json`
- CNI 网络命名空间与网络资源
- shim 进程

三者并不天然一致，因此服务启动时需要恢复和重新协调。当前实现本质上是“内存态 + SQLite 账本 + 外部运行事实”三方对齐模型。

## 目录与资源布局

当前默认布局大致如下：

| 路径 | 作用 |
| --- | --- |
| `/run/crius/crius.sock` | CRI gRPC Unix socket |
| `/run/crius/nri.sock` | NRI socket |
| `/var/lib/crius` | 运行时根目录 |
| `/var/lib/crius/crius.db` | SQLite 状态库 |
| `/var/log/crius` | 运行时日志目录 |
| `/var/run/crius/shims` | shim 工作目录 |

运行时还会派生出：

- Pod / Container bundle 目录
- streaming 运行期 socket / token 上下文
- CNI cache 与 netns
- 镜像存储目录

## 关键设计取舍

### 控制面与执行面分层

`server` 负责 CRI 语义和流程编排，`runtime` 负责 OCI 与 `runc` 落地，`shim` 负责进程与 IO 托管。这种拆分让每层的变化范围更可控。

### Pod 级网络先于业务容器

网络由 `PodSandboxManager` 管理，而不是容器直接管理。这样更符合 CRI 的 Pod 级网络模型，也简化了共享 namespace 的处理。

### 流式请求走独立 HTTP 服务

Streaming 不直接挂在 gRPC 连接上，而是用 token URL 方式跳转到本地 HTTP server。这样可以把长连接流量、协议升级和终端交互从 CRI 主服务中剥离。

### SQLite 作为恢复账本

当前实现没有把全部事实只放在 `runc state` 或文件系统扫描上，而是使用 SQLite 做显式记录。这降低了恢复时对目录扫描和外部命令结果的依赖。

### NRI 插入在“生成 OCI spec”与“运行时执行”之间

这让插件既能影响创建前的 spec，也能影响运行中的资源更新，同时避免它直接与 `runc` 细节耦合。

## 扩展点

如果后续要扩展系统，当前较稳定的切入点包括：

- 新 runtime 后端：扩展 `ContainerRuntime`
- 新网络实现：扩展 `NetworkManager`
- 新 NRI 策略或 validator：扩展 `src/nri/`
- 新持久化结构：扩展 `src/storage/`
- 新 streaming 行为：扩展 `src/streaming/`

其中最需要保持谨慎的是：

- `RuntimeServiceImpl` 中的跨模块流程顺序
- Pod / Container 状态落库时机
- NRI adjustment 与 OCI spec 写盘之间的一致性
- 容器退出后的异步状态收敛

## 当前边界与已知限制

从架构角度看，当前实现有几项明显边界：

- 主服务对象较集中，`RuntimeServiceImpl` 承担了较多编排职责
- 部分 CLI 参数已暴露，但并未完全参与主流程控制
- 镜像、运行时、网络和恢复路径已经贯通，但生产级健壮性仍需更多异常场景验证
- 当前文档描述的是单节点、本地组件视角，不包含多实例协调设计

## 阅读建议

如果要快速建立代码认知，建议按下面顺序阅读：

1. `src/main.rs`
2. `src/server/mod.rs`
3. `src/pod/mod.rs`
4. `src/runtime/mod.rs`
5. `src/storage/`
6. `src/streaming/mod.rs`
7. `src/network/`
8. `src/nri/`

如果要修改容器创建路径，优先从 `CreateContainer`、NRI adjustment 和 runtime bundle 生成三段一起看；如果要修改 Pod 启动路径，优先同时看 pod、network 和 persistence。
