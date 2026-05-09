# 架构设计

本文档描述 `crius` 当前代码库的实际架构，面向需要理解、评估或扩展该运行时的开发者。这里关注的是“现在系统如何工作”，而不是理想化的目标架构。

## 项目定位

`crius` 是一个面向 Kubernetes 的 CRI 运行时实现。它对外暴露 CRI v1 gRPC 服务，对内负责：

- 管理 Pod Sandbox 与业务容器生命周期
- 生成 OCI bundle 与 `config.json`
- 调用 `runc` 执行容器创建、启动、停止、删除与资源更新
- 处理镜像拉取、查询、删除和本地元数据
- 配置 CNI 网络与 hostPort
- 提供 `exec`、`attach`、`port-forward` 流式能力
- 持久化运行时状态并在守护进程重启后恢复
- 作为运行时侧 NRI endpoint 与插件交互

## 进程模型

当前系统主要由两个二进制组成：

| 进程 | 角色 |
| --- | --- |
| `crius` | 主守护进程，提供 CRI gRPC、ImageService、streaming 编排、状态恢复与大部分控制面逻辑 |
| `crius-shim` | 可选 shim 进程，负责容器 IO、attach socket、退出状态与部分执行面协作 |

外部依赖包括：

- `runc`
- CNI plugins
- OCI registry
- SQLite
- NRI plugins

## 高层结构

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
    |-> State persistence (`src/storage`)
    |-> NRI integration (`src/nri`)
```

从职责上看，`crius` 不是单纯的 `runc` 包装层，而是把 CRI 对象模型翻译为底层 OCI、镜像、网络、持久化与插件协作的一层编排服务。

## 模块边界

### 入口与装配

`src/main.rs` 负责：

- 解析 CLI 参数
- 加载配置并应用环境变量 / CLI 覆盖
- 初始化日志、metrics 与 tracing
- 创建 `RuntimeService` 和 `ImageService`
- 启动 streaming server 与 gRPC server

这一层只负责装配，不承载核心业务逻辑。

### CRI RuntimeService

`src/server/` 是控制面中心，负责把 CRI 请求转成内部操作序列。它会协调：

- Pod / Container 元数据校验
- NRI pre-create / post-create / update / stop 生命周期
- runtime、network、storage 子系统调用顺序
- 事件广播、状态缓存与持久化更新

### Pod Sandbox 管理

`src/pod/` 负责 Pod 级资源和 pause 容器生命周期，包括：

- pause 容器创建与销毁
- network namespace 管理
- CNI 网络接入与回收
- DNS、hostname 与 hostPort 配置

### 容器运行时抽象

`src/runtime/` 通过 `ContainerRuntime` trait 封装底层执行细节，当前默认实现为 `RuncRuntime`。该层主要负责：

- OCI spec 生成与调整
- bundle 目录管理
- `runc create/start/kill/delete/update`
- 与 `crius-shim` 的协作
- checkpoint / restore 相关接线

### 镜像服务

`src/image/` 实现 CRI `ImageService`，负责：

- 镜像引用规范化
- registry 拉取
- 本地镜像元数据管理
- 镜像查询、删除与镜像文件系统统计

### 网络

`src/network/` 提供基于 CNI 的网络实现，主要包含：

- CNI 配置目录与插件目录解析
- Pod netns 生命周期
- CNI `ADD` / `DEL`
- hostPort 映射与回收

### 流式服务

`src/streaming/` 提供独立的 HTTP streaming server。CRI 请求不会直接在 gRPC 通道上承载数据流，而是采用常见的两段式流程：

1. kubelet 调用 `Exec` / `Attach` / `PortForward`
2. `RuntimeService` 注册一次性 token 和上下文
3. 返回带 token 的 URL
4. 客户端再连接 streaming server
5. streaming server 根据 token 建立真实数据面

这使控制面和数据面可以解耦，也更符合 Kubernetes 生态中的现有交互模式。

### 持久化与恢复

`src/storage/` 使用 SQLite 维护 Pod / Container 元数据。守护进程重启时会：

- 读取持久化状态
- 重建内存对象
- 清理失去引用的 runtime / shim 工件
- 继续监控运行中的容器

恢复逻辑是 `crius` 的关键能力之一，因为 kubelet 期望 CRI runtime 在重启后仍能回答对象状态和执行后续清理。

### NRI 集成

`src/nri/` 位于 CRI 服务与底层 runtime 之间，负责：

- 插件注册、排序与超时控制
- 生命周期事件分发
- 多插件 adjustment merge
- unsolicited update / eviction 落地

## 核心请求链路

### RunPodSandbox

典型流程如下：

1. `RuntimeService` 校验请求与 runtime handler
2. 创建 Pod 级工作目录和状态对象
3. 准备网络命名空间
4. 生成 pause 容器 OCI spec
5. 调用 NRI 进行 pre-create 调整
6. 调用 `runc` / shim 启动 pause 容器
7. 配置 CNI 网络并记录结果
8. 更新内存态与 SQLite 持久化

### CreateContainer / StartContainer

容器创建与启动分为两个阶段：

1. 根据 Pod Sandbox 上下文生成容器 spec
2. 合并安全、挂载、资源与 NRI 调整
3. 写入 bundle 并创建容器
4. 启动容器并发布状态事件

这种分层方式让 CRI 语义与 OCI 执行细节相对解耦。

### Exec / Attach / PortForward

流式请求的控制流和数据流分开处理：

- CRI gRPC 负责鉴权、参数校验和 URL 返回
- streaming server 负责协议升级和字节流转发
- 真实容器 IO 最终通过 runtime 或 shim 路径进入

## 状态模型

`crius` 同时维护三层状态：

- 内存态：服务运行期间的快速访问对象
- 运行时态：`runc`、shim、socket、bundle、netns 等实际工件
- 持久化态：SQLite 中的对象账本

设计目标不是让三者永远完全一致，而是在异常和重启场景下能够重新对齐，并优先保证 kubelet 可观测到的 CRI 语义稳定。

## 可观测性

当前公开能力包括：

- 标准日志输出
- `GetEvents` 事件流
- 可选 metrics 服务
- 可选 tracing 导出

这些能力主要面向调试、集成验证和节点排障，而不是完整的生产级观测平台。

## 当前边界

- 主要围绕 Linux 与 `runc` 路径设计
- 虽然支持较多高级配置，但默认部署方式仍以源码构建和手工节点集成为主
- 文档描述的是当前代码事实，不意味着所有路径都已完成大规模生产验证

如果你希望继续深入具体配置和节点接入，请阅读：

- [配置参考](config-matrix.md)
- [kubeadm / kubelet 接入](kubeadm.md)
- [NRI 说明](nri.md)
