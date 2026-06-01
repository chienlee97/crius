# crs 计划

本文档描述 `crs` 的最终计划：它是 `crius` daemon 的本地命令行客户端，用于查看、验证、诊断和维护 `crius` 已提供的运行时能力。

`crs` 覆盖范围以本项目为边界：CRI v1 RuntimeService / ImageService、基于 runc 的 Pod sandbox 与 container 生命周期、镜像管理、CNI 网络状态、streaming、事件、统计、metrics、NRI 接线、tracing 配置、恢复账本、存储 GC 诊断和 daemon 本地诊断接口。

## 目标

- 通过一个项目内置 CLI 覆盖 `crius` 的日常开发验证、节点排障和本地维护操作。
- 为常用操作提供短命令入口，例如 `run`、`ps`、`images`、`pull`、`logs`、`exec`、`stop`、`rm`、`inspect`、`doctor`；底层 `pod`、`container`、`image` 命令继续保留完整控制面。
- 常规生命周期、镜像、事件、统计、streaming 等能力通过 CRI v1 gRPC 调用完成。
- `crius` 自有诊断能力由 daemon 提供结构化接口，CLI 只做展示、参数校验和用户交互。
- 默认连接 `unix:///run/crius/crius.sock`，支持 `--address`、`CRIUS_ADDRESS`、`--timeout`、`--debug`、`--output`。
- 默认配置直接用于本机 daemon；CLI 不读取独立配置文件，连接参数只来自命令行、环境变量和内置默认值。
- 默认输出面向人类阅读的表格，查看和诊断命令支持 `--output json`；日志和 metrics scrape 文本流命令支持 `--output text`。
- 常用命令的错误信息应给出可执行的下一步，例如对象不存在、daemon 不可达、权限不足、runtime 未就绪、网络未就绪。
- CLI 不读取 SQLite、bundle、shim 工作目录等内部文件，不把内部文件格式作为用户接口。

## 设计边界

`crs` 面向 daemon 暴露的稳定能力，而不是面向内部文件和内部组件。

- CLI 通过 CRI v1 gRPC 和 `crius` 明确提供的诊断接口访问 daemon。
- SQLite、bundle、shim 工作目录等内部文件只作为 daemon 实现细节。
- runc、shim、CNI、镜像存储由 daemon 统一编排，CLI 不为这些内部组件单独设计控制命令。
- 跨节点管理、多用户配置、镜像构建、集群级编排和调度不属于 `crs` 的覆盖范围。

## 覆盖矩阵

按“项目暴露能力和应暴露诊断能力”的口径，`crs` 需要覆盖以下能力面。

### CRI RuntimeService

| CRI 能力                                  | `crs` 覆盖                                             |
| ----------------------------------------- | ------------------------------------------------------ |
| `Version`                                 | `version`                                              |
| `Status`                                  | `status`、`status --verbose`、`config show`、`debug *` |
| `RuntimeConfig`                           | `runtime config`                                       |
| `UpdateRuntimeConfig`                     | `runtime update --pod-cidr`                            |
| `RunPodSandbox`                           | `pod run`、`run`                                       |
| `StopPodSandbox`                          | `pod stop`                                             |
| `RemovePodSandbox`                        | `pod remove`                                           |
| `ListPodSandbox`                          | `pod list`                                             |
| `PodSandboxStatus`                        | `pod inspect`                                          |
| `UpdatePodSandboxResources`               | `pod update-resources`                                 |
| `CreateContainer`                         | `container create`、`run`                              |
| `StartContainer`                          | `container start`、`run`                               |
| `StopContainer`                           | `container stop`                                       |
| `RemoveContainer`                         | `container remove`                                     |
| `ListContainers`                          | `container list`                                       |
| `ContainerStatus`                         | `container inspect`                                    |
| `UpdateContainerResources`                | `container update`                                     |
| `ReopenContainerLog`                      | `container reopen-log`                                 |
| `ExecSync`                                | `container exec-sync`、`run --exec-mode sync`          |
| `Exec`                                    | `container exec` 的 streaming 模式                     |
| `Attach`                                  | `container attach`、`run` 前台模式                     |
| `PortForward`                             | `pod port-forward`                                     |
| `ContainerStats` / `ListContainerStats`   | `container stats`、`stats`                             |
| `PodSandboxStats` / `ListPodSandboxStats` | `pod stats`                                            |
| `GetContainerEvents`                      | `events`                                               |
| `CheckpointContainer`                     | `container checkpoint`                                 |
| `ListMetricDescriptors`                   | `metrics descriptors`                                  |
| `ListPodSandboxMetrics`                   | `pod metrics`                                          |

### CRI ImageService

| CRI 能力      | `crs` 覆盖                                              |
| ------------- | ------------------------------------------------------- |
| `ListImages`  | `image list`                                            |
| `ImageStatus` | `image inspect`                                         |
| `PullImage`   | `image pull`、`image pull [AUTH OPTIONS]`、`run --pull` |
| `RemoveImage` | `image remove`                                          |
| `ImageFsInfo` | `image fs-info`                                         |

### 项目诊断能力

| 项目能力                                             | `crs` 覆盖                                                   |
| ---------------------------------------------------- | ------------------------------------------------------------ |
| CNI 网络、PodCIDR、hostPort、CNI watcher             | `debug network`、`runtime update --pod-cidr`、`config reload-status` |
| shim 进程、socket、task 状态、日志                   | `debug shims`、`container logs`                              |
| SQLite 持久化与重启恢复摘要                          | `recovery status`、`recovery check`、`recovery repair`       |
| 镜像传输、签名策略、解密、snapshotter、pinned images | `image transfers`、`image config`、`gc candidates`、`gc run` |
| NRI、CDI、blockio、RDT、资源类支持                   | `debug nri`、`pod update-resources`、`container update`      |
| seccomp、AppArmor、SELinux、设备策略、rootless       | `debug security`、`debug rootless`                           |
| cgroup driver、cgroup 版本、资源控制支持             | `runtime config`、`debug cgroups`                            |
| metrics 端点、collector、pod metrics                 | `metrics descriptors`、`metrics scrape`、`debug metrics`     |
| tracing 导出配置                                     | `debug tracing`                                              |
| streaming 监听、TLS、token TTL、port-forward 超时    | `debug streaming`                                            |

### 用户入口矩阵

快捷命令是稳定的日常入口，底层命令是完整能力入口。二者调用同一套 daemon API 和输出模型。

| 快捷命令                       | 等价或组合能力                                               |
| ------------------------------ | ------------------------------------------------------------ |
| `run IMAGE [COMMAND...]`       | 拉取缺失镜像、创建临时 Pod、创建并启动容器；`--detach=false` 时进入前台 attach；`--rm` 时清理由 CLI 创建的对象 |
| `ps`                           | `container list`                                             |
| `pods`                         | `pod list`                                                   |
| `images`                       | `image list`                                                 |
| `pull IMAGE`                   | `image pull IMAGE`                                           |
| `inspect TARGET`               | 未指定 `--type` 时查询 container、pod、image 候选；指定 `--type` 时只查询对应类型 |
| `logs CONTAINER`               | `container logs CONTAINER`                                   |
| `exec CONTAINER -- COMMAND...` | `container exec CONTAINER -- COMMAND...`                     |
| `stop TARGET`                  | 停止 container 或 pod；歧义时要求 `--type`                   |
| `rm TARGET`                    | 删除 container、pod 或 image；歧义时要求 `--type`            |
| `doctor`                       | 聚合 `status`、`runtime config`、`debug network`、`debug runtime`、`debug security`、`debug shims` 的摘要 |

## 架构

当前项目已有 `crius` daemon、`crius-shim`、CRI RuntimeService / ImageService、`src/server/`、`src/services/`、`src/runtime/`、`src/storage/`、`src/streaming/` 等模块。`crs` 是在现有 daemon 架构上新增的本地 CLI，不替换 daemon 入口，不改变 `crius-shim`。

新增内容：

- `crs` 独立二进制。
- `src/crs/` CLI 代码。
- `proto/crius/diagnostics/v1/diagnostics.proto` 诊断协议。
- daemon 侧 `src/services/diagnostics.rs` gRPC 适配层。

daemon 侧 diagnostics 适配层实现为 `src/services/diagnostics.rs`。它必须复用当前已有的 `src/services/introspection.rs`、`src/services/health.rs`、`src/services/event.rs` 和 `RuntimeServiceImpl` 中已维护的状态句柄；当前服务缺少的只读摘要或维护动作，通过服务层显式 helper 补齐。diagnostics 适配层不得重新实现 runtime 编排、镜像存储扫描或恢复账本解析。

调用关系：

```text
crs binary
  src/crs/main.rs
  crius::crs::run_cli
  src/crs/{args,context,commands,client,format,error,streaming}
  CRI RuntimeService / ImageService and DiagnosticsService

crius daemon
  src/main.rs
  RuntimeService / ImageService
  DiagnosticsService adapter
  src/services/{introspection,health,event}.rs
  RuntimeServiceImpl and daemon state handles
```

分层职责：

- `src/crs/main.rs` 只负责二进制入口，调用 `crius::crs::run_cli` 并转换最终退出码。
- `src/crs/mod.rs` 汇总 CLI 子模块，提供 `run_cli` 作为二进制入口调用点。
- `src/crs/args.rs` 只定义命令表面和 clap 解析，不构造 CRI request。
- `src/crs/commands/*` 负责把 args 转换为 CRI / diagnostics request，调用 client，并组装展示模型。
- `src/crs/client.rs` 负责 endpoint 解析、Unix / TCP channel、CRI client、diagnostics client 和 per-RPC timeout。
- `src/crs/format.rs`、`ids.rs`、`error.rs` 负责输出、截断、脱敏和错误映射，不发起 RPC。
- `src/crs/streaming.rs` 负责 Exec / Attach / PortForward 的交互式连接和终端状态恢复。
- daemon 侧 diagnostics service 运行在 `crius` 进程内，复用 `src/services/*` 的聚合结果，只通过 daemon 已有状态句柄读取或执行诊断动作。

依赖方向：

- `src/crs/main.rs` 通过 crate library 入口依赖 `crius::crs::run_cli`。
- `src/crs/commands/*` 依赖 `client`、`context`、`format`、`parsers`、`streaming` 和生成的 proto 类型。
- `src/crs/client.rs` 依赖生成的 CRI / diagnostics client。
- `src/crs/*` 不依赖 daemon 内部模块、SQLite、bundle、shim 工作目录或存储实现。
- daemon diagnostics 模块不依赖 CLI 模块；依赖范围限定为 `src/services/*`、`src/server/*` 暴露的稳定内部句柄和生成的 diagnostics proto。
- 快捷命令不复制底层命令逻辑；`ps`、`images`、`pull`、`logs`、`exec` 复用对应底层 handler，`inspect`、`stop`、`rm` 只负责对象类型解析和分发。

文件布局：

```text
src/crs/
    main.rs
    mod.rs
    args.rs
    client.rs
    context.rs
    error.rs
    format.rs
    ids.rs
    parsers.rs
    streaming.rs
    commands/
        mod.rs
        version.rs
        status.rs
        config.rs
        image.rs
        runtime.rs
        pod.rs
        container.rs
        logs.rs
        run.rs
        exec.rs
        attach.rs
        port_forward.rs
        doctor.rs
        shortcuts.rs
        events.rs
        stats.rs
        metrics.rs
        recovery.rs
        gc.rs
        debug.rs
        completion.rs
proto/crius/diagnostics/v1/diagnostics.proto
src/services/
    diagnostics.rs
```

`Cargo.toml` 新增 bin：

```toml
[[bin]]
name = "crs"
path = "src/crs/main.rs"
```

依赖策略：

- 复用现有 `clap`、`tokio`、`tonic`、`serde_json`、`anyhow`。
- 表格输出先实现轻量内部 formatter。
- streaming 客户端复用项目 streaming 模块中的协议常量、行为约定和测试夹具；客户端连接逻辑在 CLI 层补齐。
- 终端 raw mode、窗口 resize 等交互能力在 CLI 层封装，不影响 daemon 接口形态。
- diagnostics proto 由 `build.rs` 生成，与现有 CRI proto 一起进入 `src/proto/mod.rs`。

## 客户端层

`src/crs/client.rs` 提供统一客户端：

```rust
pub struct CrsClient {
    runtime: RuntimeServiceClient<Channel>,
    image: ImageServiceClient<Channel>,
    diagnostics: Option<DiagnosticsServiceClient<Channel>>,
}
```

连接策略：

- endpoint 优先级为 `--address`、`CRIUS_ADDRESS`、默认 `unix:///run/crius/crius.sock`。
- 支持 `unix:///run/crius/crius.sock` 和裸路径 `/run/crius/crius.sock`。
- 默认路径不可达时，错误信息提示当前尝试的 endpoint、daemon 启动建议和 `--address` 覆盖方式。
- `--connect-timeout` 控制建连。
- `--timeout` 控制单次 RPC。
- 错误信息包含 endpoint、命令名、对象 ID 或镜像引用、gRPC status code 和 daemon message。
- TCP endpoint 仅在 daemon 显式启用时可用，并配套 TLS、鉴权和配置校验。

CLI 配置边界：

- `crs` 不读取独立 CLI 配置文件。
- `/etc/crius/` 下的配置属于 daemon；`crs` 不把 daemon 配置文件当作自身配置入口。
- registry password、token 和 daemon 内部路径不写入 CLI 侧持久化文件。

## 输出与 ID 处理

统一参数：

- `--output table|json`，默认 `table`；`text` 只用于日志和 metrics scrape 等文本输出命令。
- `--quiet`，只输出 ID、镜像引用或最小机器可读值。
- `--no-trunc`，控制 ID、镜像 ID、长字段截断。
- `--verbose`，用于状态类命令读取 CRI verbose 信息。

ID 处理原则：

- 快捷命令接受短 ID；CLI 不访问本地状态文件，候选解析通过 daemon 查询完成。
- list 输出短 ID。
- inspect、status、remove 等命令把用户输入交给 daemon 的解析逻辑。
- CLI 不扫描 SQLite 来解析 ID 或名称。
- 模糊或冲突 ID 返回明确错误，并提示使用 `--type container|pod|image` 或更长 ID。
- `inspect`、`stop`、`rm` 这类多类型快捷命令先收集候选对象；如果多个类型匹配，返回歧义错误而不是猜测。

默认表格字段：

- containers: `CONTAINER ID`, `POD`, `IMAGE`, `STATE`, `CREATED`, `NAME`, `ATTEMPT`
- pods: `POD ID`, `NAME`, `NAMESPACE`, `STATE`, `IP`, `CREATED`, `ATTEMPT`
- images: `IMAGE`, `IMAGE ID`, `SIZE`, `USER SPEC`, `PINNED`
- stats: `ID`, `NAME`, `CPU`, `MEMORY`, `PIDS`
- events: `TIME`, `TYPE`, `CONTAINER`, `POD`

## 命令设计

### 基础信息

| 命令                       | 说明                                                         | 实现来源                                                     |
| -------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `crs version`              | 显示 `crius` runtime 版本和 API 版本                         | CRI `Version`                                                |
| `crs status`               | 显示 runtime/network/health 条件                             | CRI `Status`                                                 |
| `crs status --verbose`     | 显示 CRI verbose 诊断信息                                    | CRI `Status(verbose=true)`                                   |
| `crs config show`          | 显示有效配置摘要                                             | diagnostics `EffectiveConfig`；diagnostics 不可用时读取 `Status(verbose=true).info["config"]` |
| `crs config reload-status` | 显示配置 watcher、最近 reload 错误和 CNI watcher 状态        | diagnostics `EffectiveConfig`；diagnostics 不可用时读取 `Status(verbose=true).info["config"]` |
| `crs doctor`               | 聚合 daemon、runtime、network、storage、security、shim 和 diagnostics 健康摘要 | CRI + diagnostics                                            |
| `crs completion <shell>`   | 生成 shell completion                                        | clap                                                         |

### 快捷命令

| 命令                               | 说明                                           | 实现来源                                                   |
| ---------------------------------- | ---------------------------------------------- | ---------------------------------------------------------- |
| `crs ps`                           | 列出容器，默认只显示 running；`--all` 显示全部 | CRI `ListContainers`                                       |
| `crs pods`                         | 列出 Pod sandbox                               | CRI `ListPodSandbox`                                       |
| `crs images`                       | 列出本地镜像                                   | CRI `ListImages`                                           |
| `crs pull IMAGE`                   | 拉取镜像                                       | CRI `PullImage`                                            |
| `crs inspect TARGET`               | 自动识别 container、pod 或 image 并输出详情    | CRI status / image status                                  |
| `crs logs CONTAINER`               | 读取容器日志                                   | diagnostics `ContainerLog`                                 |
| `crs exec CONTAINER -- COMMAND...` | 进入容器执行命令                               | CRI `Exec` + streaming                                     |
| `crs stop TARGET`                  | 停止容器或 Pod sandbox                         | CRI `StopContainer` / `StopPodSandbox`                     |
| `crs rm TARGET`                    | 删除容器、Pod sandbox 或镜像                   | CRI `RemoveContainer` / `RemovePodSandbox` / `RemoveImage` |

### Runtime

| 命令                                 | 说明                                                         | 实现来源                                                     |
| ------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `crs runtime config`                 | 显示 CRI `RuntimeConfig` 返回的 runtime 配置，例如 cgroup driver | CRI `RuntimeConfig`                                          |
| `crs runtime update --pod-cidr CIDR` | 更新 runtime network config，并触发 CNI 模板渲染链路         | CRI `UpdateRuntimeConfig`                                    |
| `crs runtime handlers`               | 显示 runtime handler、runtime path、runtime config path、特性探测结果 | diagnostics `RuntimeHandlers`；diagnostics 不可用时读取 `Status(verbose=true).info["runtimeBackend"]` |

### 镜像

| 命令                                  | 说明                                                         | 实现来源                                                     |
| ------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `crs image list`                      | 列出本地镜像                                                 | CRI `ListImages`                                             |
| `crs image pull IMAGE`                | 拉取镜像                                                     | CRI `PullImage`                                              |
| `crs image pull IMAGE [AUTH OPTIONS]` | 使用显式 registry auth 拉取镜像                              | CRI `PullImage.auth`                                         |
| `crs image inspect IMAGE`             | 查看镜像详情                                                 | CRI `ImageStatus(verbose=true)`                              |
| `crs image remove IMAGE`              | 删除镜像                                                     | CRI `RemoveImage`                                            |
| `crs image fs-info`                   | 查看镜像文件系统信息                                         | CRI `ImageFsInfo`                                            |
| `crs image transfers`                 | 查看镜像拉取传输状态、失败原因和中断恢复信息                 | diagnostics `ImageTransfers`；diagnostics 不可用时读取 `Status(verbose=true).info["imageTransfers"]` |
| `crs image config`                    | 查看镜像鉴权、签名策略、解密、snapshotter、pinned images 等配置摘要 | diagnostics `EffectiveConfig`；diagnostics 不可用时读取 `Status(verbose=true).info["config"].image` |

### Pod Sandbox

| 命令                                              | 说明                      | 实现来源                                      |
| ------------------------------------------------- | ------------------------- | --------------------------------------------- |
| `crs pod list`                                    | 列出 Pod sandbox          | CRI `ListPodSandbox`                          |
| `crs pod inspect POD`                             | 查看 Pod sandbox 详情     | CRI `PodSandboxStatus(verbose=true)`          |
| `crs pod run --name NAME --namespace NS`          | 创建并启动 Pod sandbox    | CRI `RunPodSandbox`                           |
| `crs pod stop POD`                                | 停止 Pod sandbox          | CRI `StopPodSandbox`                          |
| `crs pod remove POD`                              | 删除 Pod sandbox          | CRI `RemovePodSandbox`                        |
| `crs pod stats [POD]`                             | 查看 Pod 统计             | CRI `PodSandboxStats` / `ListPodSandboxStats` |
| `crs pod metrics`                                 | 查看 Pod metrics          | CRI `ListPodSandboxMetrics`                   |
| `crs pod update-resources POD ...`                | 更新 Pod sandbox 资源配置 | CRI `UpdatePodSandboxResources`               |
| `crs pod port-forward POD --forward LOCAL:REMOTE` | 转发到 Pod 网络命名空间   | CRI `PortForward` + streaming                 |

### 容器

| 命令                                          | 说明                                       | 实现来源                                    |
| --------------------------------------------- | ------------------------------------------ | ------------------------------------------- |
| `crs container list`                          | 列出容器                                   | CRI `ListContainers`                        |
| `crs container inspect ID`                    | 查看容器详情                               | CRI `ContainerStatus(verbose=true)`         |
| `crs container create POD IMAGE [COMMAND...]` | 在指定 Pod 中创建容器                      | CRI `CreateContainer`                       |
| `crs container start ID`                      | 启动容器                                   | CRI `StartContainer`                        |
| `crs container stop ID`                       | 停止容器                                   | CRI `StopContainer`                         |
| `crs container remove ID`                     | 删除容器                                   | CRI `RemoveContainer`                       |
| `crs container exec ID -- COMMAND...`         | 进入容器执行命令                           | CRI `Exec` + streaming                      |
| `crs container exec-sync ID COMMAND...`       | 同步执行命令并返回 stdout/stderr/exit code | CRI `ExecSync`                              |
| `crs container attach ID`                     | 连接容器 IO                                | CRI `Attach` + streaming                    |
| `crs container stats [ID]`                    | 查看容器统计                               | CRI `ContainerStats` / `ListContainerStats` |
| `crs container checkpoint ID --location PATH` | checkpoint 容器                            | CRI `CheckpointContainer`                   |
| `crs container update ID ...`                 | 更新容器资源                               | CRI `UpdateContainerResources`              |
| `crs container reopen-log ID`                 | 触发日志 reopen                            | CRI `ReopenContainerLog`                    |
| `crs container logs ID`                       | 读取容器日志                               | diagnostics `ContainerLog`                  |

### 组合验证命令

`crs run` 是项目内置的本地验证快捷入口。它把 `crius` 的 CRI 对象模型组合起来，方便开发者手工验证镜像拉取、Pod 创建、容器创建、启动、streaming 和清理链路。

```bash
crs run [OPTIONS] IMAGE [COMMAND] [ARG...]
```

默认行为：

1. 根据 `--pull missing|always|never` 判断是否调用 `PullImage`。
2. 创建临时 Pod sandbox，默认 namespace 为 `default`，name 为 `crius-<short-id>`。
3. 调用 `CreateContainer`。
4. 调用 `StartContainer`。
5. 前台模式下通过 attach/streaming 获取输出；同步执行场景使用 `ExecSync`。
6. 设置 `--rm` 时停止并删除 container 与 sandbox。

关键选项：

- `--name`
- `--namespace`
- `--detach`
- `--rm`
- `--tty`
- `--stdin`
- `--env`
- `--label`
- `--annotation`
- `--mount type=bind,src=...,dst=...,options=ro:rbind`
- `--workdir`
- `--user`
- `--runtime-handler`
- `--pod POD`，复用已有 sandbox
- `--host-network`
- `--hostname`
- `--dns-server`
- `--dns-search`
- `--dns-option`
- `--publish hostPort:containerPort[/protocol]`
- `--pull missing|always|never`
- `--exec-mode sync|attach`

失败处理需要输出已创建但未清理的 container / pod ID，方便用户后续清理。

### 事件、统计、metrics 和诊断

| 命令                            | 说明                                                         | 实现来源                                                     |
| ------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `crs events`                    | 订阅容器事件                                                 | CRI `GetContainerEvents`                                     |
| `crs stats`                     | 查看容器统计摘要                                             | CRI `ListContainerStats`                                     |
| `crs metrics descriptors`       | 查看 CRI pod metrics 描述符                                  | CRI `ListMetricDescriptors`                                  |
| `crs metrics scrape`            | 拉取 `crius` metrics 快照                                    | daemon 配置中的 metrics endpoint                             |
| `crs recovery status`           | 查看恢复状态和 ledger 健康                                   | diagnostics `RecoveryStatus`                                 |
| `crs recovery check`            | 查看恢复账本 dry-run 检查结果                                | diagnostics `RecoveryCheck(execute=false)`                   |
| `crs recovery repair --dry-run` | 预览恢复修复动作                                             | diagnostics `RecoveryCheck`                                  |
| `crs recovery repair --execute` | 执行恢复修复动作                                             | diagnostics `RecoveryCheck`                                  |
| `crs gc candidates`             | 查看存储 GC 候选                                             | diagnostics `ContentGc(execute=false)`                       |
| `crs gc run --dry-run`          | 预览存储 GC 动作                                             | diagnostics `ContentGc`                                      |
| `crs gc run --execute`          | 执行存储 GC 动作                                             | diagnostics `ContentGc`                                      |
| `crs debug network`             | 查看 CNI / PodCIDR / 网络 ready 诊断                         | CRI `Status(verbose=true)` + diagnostics `EffectiveConfig`   |
| `crs debug runtime`             | 查看 runc 路径、runtime version、handler 摘要                | CRI `Version` + CRI `RuntimeConfig` + diagnostics `RuntimeHandlers` |
| `crs debug shims`               | 查看 shim socket、pid、task 状态                             | diagnostics `ShimStatus`                                     |
| `crs debug nri`                 | 查看 NRI 启用状态、CDI、插件目录、blockio / RDT 配置和资源类支持 | diagnostics `NriStatus`                                      |
| `crs debug security`            | 查看 seccomp、AppArmor、SELinux、设备策略和安全特性可用性    | diagnostics `SecurityStatus`                                 |
| `crs debug cgroups`             | 查看 cgroup driver、cgroup 版本和资源控制支持                | CRI `RuntimeConfig` + `Status(verbose=true)`                 |
| `crs debug streaming`           | 查看 streaming 监听地址、TLS、token TTL、port-forward 超时配置 | diagnostics `EffectiveConfig` + diagnostics `ServerInfo`     |
| `crs debug metrics`             | 查看 metrics 端点、TLS、collector 和 pod metrics 配置        | diagnostics `EffectiveConfig` + metrics endpoint             |
| `crs debug tracing`             | 查看 tracing 导出开关、endpoint 和采样率                     | diagnostics `EffectiveConfig`                                |
| `crs debug rootless`            | 查看 rootless 路径、网络和用户命名空间状态                   | diagnostics `SecurityStatus`                                 |

## 命令语法

### 全局参数

```bash
crs [GLOBAL OPTIONS] <COMMAND>
```

| Flag / Env                   | 默认值                                            | 说明                                  |
| ---------------------------- | ------------------------------------------------- | ------------------------------------- |
| `--address ENDPOINT`         | `CRIUS_ADDRESS` 或 `unix:///run/crius/crius.sock` | daemon endpoint                       |
| `CRIUS_ADDRESS`              | 空                                                | endpoint 环境变量                     |
| `--connect-timeout DURATION` | `5s`                                              | 建连超时                              |
| `--timeout DURATION`         | `30s`                                             | 单次 RPC 超时                         |
| `--debug`                    | false                                             | 打印调试日志，不打印密码或 token 明文 |
| `--output table|json`        | `table`                                           | 输出格式                              |
| `--quiet`                    | false                                             | 最小输出                              |
| `--no-trunc`                 | false                                             | 不截断 ID 和长字段                    |

`DURATION` 支持 `500ms`、`5s`、`2m`。字节值支持纯数字和 `KiB`、`MiB`、`GiB` 后缀。

### 基础信息命令

```bash
crs version [--output table|json]
crs status [--verbose] [--output table|json]
crs config show [--output table|json]
crs config reload-status [--output table|json]
crs doctor [--output table|json]
crs completion <bash|zsh|fish|powershell>
```

### 快捷命令

```bash
crs run [RUN OPTIONS] IMAGE [COMMAND] [ARG...]
crs ps [--all] [--pod POD] [--label KEY=VALUE] [--quiet] [--no-trunc] [--output table|json]
crs pods [--all] [--label KEY=VALUE] [--quiet] [--no-trunc] [--output table|json]
crs images [--image IMAGE] [--quiet] [--no-trunc] [--output table|json]
crs pull IMAGE [AUTH OPTIONS] [--quiet] [--output table|json]
crs inspect [--type container|pod|image] TARGET [--output json]
crs logs CONTAINER [--follow] [--tail N] [--since RFC3339|DURATION] [--timestamps] [--output text|json]
crs exec CONTAINER [EXEC OPTIONS] -- COMMAND [ARG...]
crs stop [--type container|pod] TARGET [--timeout SECONDS] [--output table|json]
crs rm [--type container|pod|image] TARGET [--force] [--output table|json]
```

快捷命令与底层命令共享 request builder、formatter 和错误映射。`--force` 仅允许对停止失败后的删除流程做显式确认；不绕过 daemon 的状态校验。

### Runtime 命令

```bash
crs runtime config [--output table|json]
crs runtime update --pod-cidr CIDR [--output table|json]
crs runtime handlers [--output table|json] [--verbose]
```

`runtime update --pod-cidr` 接受单个 CIDR 或逗号分隔的 CIDR 列表，CLI 先做格式校验，再调用 CRI `UpdateRuntimeConfig`。

### 镜像命令

```bash
crs image list [--image IMAGE] [--quiet] [--no-trunc] [--output table|json]
crs image pull IMAGE [AUTH OPTIONS] [--pod POD] [--quiet] [--output table|json]
crs image inspect IMAGE [--output json]
crs image remove IMAGE [--quiet] [--output table|json]
crs image fs-info [--output table|json]
crs image transfers [--output table|json]
crs image config [--output table|json]
```

`image inspect` 默认输出 JSON，因为 verbose image status 本身是结构化诊断对象。

### Pod Sandbox 命令

```bash
crs pod list [--id ID] [--state ready|notready] [--label KEY=VALUE] [--all] [--quiet] [--no-trunc] [--output table|json]
crs pod inspect POD [--output json]
crs pod run [POD CREATE OPTIONS]
crs pod stop POD [--timeout SECONDS] [--output table|json]
crs pod remove POD [--output table|json]
crs pod stats [POD] [--label KEY=VALUE] [--output table|json]
crs pod metrics [--output table|json]
crs pod update-resources POD [--overhead RESOURCE] [--pod-resource RESOURCE] [--output table|json]
crs pod port-forward POD [--forward LOCAL:REMOTE]... [--protocol websocket|spdy]
```

`pod run` 使用“Pod Sandbox 创建参数”章节中的 flags。成功时输出 Pod sandbox ID。

### 容器命令

```bash
crs container list [--id ID] [--pod POD] [--state created|running|exited|unknown] [--label KEY=VALUE] [--all] [--quiet] [--no-trunc] [--output table|json]
crs container inspect ID [--output json]
crs container create POD IMAGE [COMMAND...] [CONTAINER CREATE OPTIONS]
crs container start ID [--output table|json]
crs container stop ID [--timeout SECONDS] [--output table|json]
crs container remove ID [--output table|json]
crs container exec ID [EXEC OPTIONS] -- COMMAND [ARG...]
crs container exec-sync ID [--timeout SECONDS] -- COMMAND [ARG...]
crs container attach ID [ATTACH OPTIONS]
crs container stats [ID] [--pod POD] [--label KEY=VALUE] [--output table|json]
crs container checkpoint ID --location PATH [--timeout SECONDS] [--output table|json]
crs container update ID [--resource RESOURCE] [--annotation KEY=VALUE] [--output table|json]
crs container reopen-log ID [--output table|json]
crs container logs ID [--follow] [--tail N] [--since RFC3339|DURATION] [--timestamps] [--output text|json]
```

`container create` 使用“Container 创建参数”章节中的 flags。成功时输出 container ID。`container update` 的 `--resource` 支持重复传入，合并成一个 `LinuxContainerResources`。

### 组合验证命令

```bash
crs run [RUN OPTIONS] IMAGE [COMMAND] [ARG...]
```

`run` 同时接受 Pod Sandbox 创建参数和 Container 创建参数。冲突时命名更具体的参数优先生效，例如 `--sandbox-seccomp` 只作用于 Pod sandbox，`--seccomp` 只作用于 workload container。

### 事件、统计、metrics 和诊断命令

```bash
crs events [--no-trunc] [--output table|json]
crs stats [--label KEY=VALUE] [--output table|json]
crs metrics descriptors [--output table|json]
crs metrics scrape [--output text|json]
crs recovery status [--output table|json]
crs recovery check [--output table|json]
crs recovery repair (--dry-run|--execute) [--output table|json]
crs gc candidates [--output table|json]
crs gc run (--dry-run|--execute) [--output table|json]
crs debug network [--output table|json]
crs debug runtime [--output table|json]
crs debug shims [--output table|json]
crs debug nri [--output table|json]
crs debug security [--output table|json]
crs debug cgroups [--output table|json]
crs debug streaming [--output table|json]
crs debug metrics [--output table|json]
crs debug tracing [--output table|json]
crs debug rootless [--output table|json]
```

`recovery repair` 和 `gc run` 必须显式指定 `--dry-run` 或 `--execute`，避免误操作。

## 命令实现明细

本节按最终 CLI 表面列出每个命令的实现契约。实现时以本节为准：命令 handler 只做参数转换、RPC 调用和输出模型组装；daemon 状态解析、路径安全校验和实际维护动作由 daemon 完成。

### 基础信息实现

| 命令                   | Request                                                      | 关键字段                                                     | 输出 kind              | CLI 校验                                              | 失败语义                                                     |
| ---------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ---------------------- | ----------------------------------------------------- | ------------------------------------------------------------ |
| `version`              | `VersionRequest`                                             | `version` 字段传空字符串，由 daemon 返回默认 runtime 版本    | `RuntimeVersion`       | 无额外校验                                            | daemon 不可达返回 125                                        |
| `status`               | `StatusRequest`                                              | `verbose=false`                                              | `RuntimeStatus`        | 无额外校验                                            | runtime / network condition 为 false 不改变退出码，RPC 失败才非 0 |
| `status --verbose`     | `StatusRequest`                                              | `verbose=true`                                               | `RuntimeStatusVerbose` | `--output table` 时只展示摘要，详细对象通过 JSON 输出 | verbose info 不是合法 JSON 时保留原字符串并给 warning        |
| `config show`          | `EffectiveConfigRequest`；diagnostics 不可用时调用 `StatusRequest(verbose=true)` | diagnostics: `include_sensitive=false`；降级字段: `info["config"]` | `EffectiveConfig`      | 不提供显示敏感字段的 flag                             | diagnostics 和降级字段都不可用时返回 1                       |
| `config reload-status` | `EffectiveConfigRequest`；diagnostics 不可用时调用 `StatusRequest(verbose=true)` | diagnostics: watcher/reload/CNI watcher 摘要；降级字段: `info["config"]` | `ConfigReloadStatus`   | 无额外校验                                            | 字段缺失显示 `unknown` 并给 warning                          |
| `doctor`               | `Version`、`Status`、`RuntimeConfig`、diagnostics            | 聚合本地健康检查                                             | `Doctor`               | 无额外校验                                            | daemon 不可达返回 125；diagnostics 不可用但 CRI 可用时返回 0 并给 warning |
| `completion <shell>`   | clap generator                                               | shell enum                                                   | 无 JSON envelope       | shell 需要是 clap 支持值                              | 参数错误返回 2                                               |

`status` 表格至少展示：

- runtime ready condition。
- network ready condition。
- runtime API version。
- cgroup driver。
- pod CIDR。
- diagnostics 是否可用。

`doctor` 聚合检查项：

| 检查项                  | 数据来源                                                   | 输出                                               |
| ----------------------- | ---------------------------------------------------------- | -------------------------------------------------- |
| daemon connectivity     | CRI `Version` / `Status`                                   | endpoint、runtime version、API version             |
| runtime readiness       | CRI `Status` / `RuntimeConfig`                             | runtime ready、cgroup driver、runtime handler 摘要 |
| network readiness       | CRI `Status(verbose=true)` + diagnostics `EffectiveConfig` | network ready、pod CIDR、CNI 配置和最近错误        |
| image/storage readiness | ImageService + diagnostics `ImageTransfers` / `ContentGc`  | image fs、snapshotter、GC 候选摘要                 |
| security readiness      | diagnostics `SecurityStatus`                               | seccomp、AppArmor、SELinux、rootless 可用性        |
| shim readiness          | diagnostics `ShimStatus`                                   | shim 数量、异常 task、最近错误                     |

`doctor` 默认返回 0，只要 daemon 可达且检查完成；发现 unhealthy 项时在 summary 中标记并在 table 中显示 `WARN`。如果 daemon 不可达返回 125；如果 diagnostics 不可用但 CRI 可用，返回 0 并输出 warning。

### 快捷命令实现

| 命令                           | 实现                                                         |
| ------------------------------ | ------------------------------------------------------------ |
| `ps`                           | 调用 `container list` handler；默认设置 state 为 running，`--all` 时取消 state filter |
| `pods`                         | 调用 `pod list` handler；默认显示 ready sandbox，`--all` 时包含全部状态 |
| `images`                       | 调用 `image list` handler                                    |
| `pull IMAGE`                   | 调用 `image pull` handler                                    |
| `inspect TARGET`               | 按 `--type` 或 container、pod、image 收集候选；多个类型匹配时返回歧义错误 |
| `logs CONTAINER`               | 调用 `container logs` handler                                |
| `exec CONTAINER -- COMMAND...` | 调用 `container exec` handler                                |
| `stop TARGET`                  | 按 `--type` 或 container、pod 收集候选并停止；image 不是可停止对象 |
| `rm TARGET`                    | 按 `--type` 或 container、pod、image 收集候选并删除；运行中对象由 daemon 返回 failed precondition |

快捷命令错误文案应包含对应底层命令建议，例如 `crs inspect --type container <id>`、`crs container list --all`、`crs pod list --all`。

### Runtime 实现

| 命令                             | Request                                                      | 关键字段                                                     | 输出 kind             | CLI 校验                                  | 失败语义                                                     |
| -------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ | --------------------- | ----------------------------------------- | ------------------------------------------------------------ |
| `runtime config`                 | `RuntimeConfigRequest`                                       | 无                                                           | `RuntimeConfig`       | 无额外校验                                | daemon 返回 `Unimplemented` 时返回 1，不从 verbose status 拼装 RuntimeConfig |
| `runtime update --pod-cidr CIDR` | `UpdateRuntimeConfigRequest`                                 | `runtime_config.network_config.pod_cidr`                     | `RuntimeConfigUpdate` | CIDR 语法；多个 CIDR 用逗号分隔后保留顺序 | daemon 拒绝 CIDR 返回 `FailedPrecondition`                   |
| `runtime handlers`               | `RuntimeHandlersRequest`；diagnostics 不可用时调用 `StatusRequest(verbose=true)` | diagnostics: handler name、runtime path、config path、feature list；降级字段: `info["runtimeBackend"]` | `RuntimeHandlers`     | `--verbose` 只影响附加字段展示            | diagnostics 和降级字段都不可用时返回 1                       |

`runtime update` 成功输出更新后的 pod CIDR 摘要；如果 daemon 返回空响应，CLI 再调用一次 `status --verbose` 读取当前值并展示。

### 镜像实现

| 命令                  | Request                                                      | 关键字段                                                     | 输出 kind         | CLI 校验                  | 失败语义                                   |
| --------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ----------------- | ------------------------- | ------------------------------------------ |
| `image list`          | `ListImagesRequest`                                          | 传入 `--image` 时设置 `filter.image.image`；未传时不设置 filter | `ImageList`       | `--image` 非空            | 空列表退出码 0                             |
| `image pull IMAGE`    | `PullImageRequest`                                           | `image.image`、`auth`、`sandbox_config`                      | `ImagePull`       | IMAGE 非空；auth 来源互斥 | registry/auth 错误透传 daemon message      |
| `image inspect IMAGE` | `ImageStatusRequest`                                         | `image.image=IMAGE`、`verbose=true`                          | 直接 inspect JSON | IMAGE 非空                | 不存在返回 4                               |
| `image remove IMAGE`  | `RemoveImageRequest`                                         | `image.image=IMAGE`                                          | `ImageRemove`     | IMAGE 非空                | 不存在返回 4；被使用返回 6                 |
| `image fs-info`       | `ImageFsInfoRequest`                                         | 无                                                           | `ImageFsInfo`     | 无额外校验                | daemon 返回 `Unimplemented` 时输出明确错误 |
| `image transfers`     | `ImageTransfersRequest`；diagnostics 不可用时调用 `StatusRequest(verbose=true)` | diagnostics: `include_completed=false` 默认；降级字段: `info["imageTransfers"]` | `ImageTransfers`  | `--all` 可包含已完成传输  | diagnostics 和降级字段都不可用时返回 1     |
| `image config`        | `EffectiveConfigRequest`；diagnostics 不可用时调用 `StatusRequest(verbose=true)` | diagnostics: image 相关配置摘要；降级字段: `info["config"].image` | `ImageConfig`     | 不显示敏感 auth 明文      | 字段缺失显示 `unknown`                     |

`image pull --pod POD` 的实现顺序：

1. 调用 `PodSandboxStatus(POD, verbose=false)`。
2. 从 response 中取 daemon 返回的 sandbox config 上下文。
3. 构造 `PullImageRequest.sandbox_config`。
4. 如果 Pod 不存在，返回 4，不再尝试拉取镜像。

### Pod Sandbox 实现

| 命令                       | Request                                                      | 关键字段                                             | 输出 kind           | CLI 校验                                                     | 失败语义                                 |
| -------------------------- | ------------------------------------------------------------ | ---------------------------------------------------- | ------------------- | ------------------------------------------------------------ | ---------------------------------------- |
| `pod list`                 | `ListPodSandboxRequest`                                      | `filter.id`、`filter.state`、`filter.label_selector` | `PodList`           | state 枚举；label 为 `KEY=VALUE`                             | 空列表退出码 0                           |
| `pod inspect POD`          | `PodSandboxStatusRequest`                                    | `pod_sandbox_id=POD`、`verbose=true`                 | 直接 inspect JSON   | POD 非空                                                     | 不存在返回 4                             |
| `pod run`                  | `RunPodSandboxRequest`                                       | `config`、`runtime_handler`                          | `PodRun`            | name、namespace、attempt、DNS、namespace/security/resource flags | CNI / runtime handler 错误透传 daemon    |
| `pod stop POD`             | `StopPodSandboxRequest`                                      | `pod_sandbox_id=POD`                                 | `PodStop`           | POD 非空；timeout 秒数非负                                   | 已停止视 daemon 语义；不存在返回 4       |
| `pod remove POD`           | `RemovePodSandboxRequest`                                    | `pod_sandbox_id=POD`                                 | `PodRemove`         | POD 非空                                                     | pod 仍有容器时返回 6                     |
| `pod stats [POD]`          | 指定 POD 时用 `PodSandboxStatsRequest`；未指定时用 `ListPodSandboxStatsRequest` | 单个 POD 或 filter                                   | `PodStats`          | label filter 合法                                            | stats 不可用显示 warning                 |
| `pod metrics`              | `ListPodSandboxMetricsRequest`                               | 无                                                   | `PodMetrics`        | 无额外校验                                                   | daemon 不支持返回清晰错误                |
| `pod update-resources POD` | `UpdatePodSandboxResourcesRequest`                           | `pod_sandbox_id`、`overhead`、`resources`            | `PodResourceUpdate` | resource spec 至少一个；数值非负                             | 不支持的 cgroup 字段由 daemon 返回 6     |
| `pod port-forward POD`     | `PortForwardRequest`                                         | `pod_sandbox_id`、`port[]`                           | `PortForward`       | 至少一个 `LOCAL:REMOTE`；端口 1-65535                        | 本地 listen 失败返回 1；Pod 不存在返回 4 |

`pod run` 输出：

- table: `POD ID`, `NAME`, `NAMESPACE`, `STATE`。
- quiet: 仅 Pod sandbox ID。
- json: `kind=PodRun`，`summary.podSandboxId` 必填。

### Container 实现

| 命令                         | Request                                                      | 关键字段                                                     | 输出 kind             | CLI 校验                                        | 失败语义                                                     |
| ---------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ | --------------------- | ----------------------------------------------- | ------------------------------------------------------------ |
| `container list`             | `ListContainersRequest`                                      | `filter.id`、`filter.pod_sandbox_id`、`filter.state`、`filter.label_selector` | `ContainerList`       | state 枚举；label 合法                          | 空列表退出码 0                                               |
| `container inspect ID`       | `ContainerStatusRequest`                                     | `container_id=ID`、`verbose=true`                            | 直接 inspect JSON     | ID 非空                                         | 不存在返回 4                                                 |
| `container create POD IMAGE` | `CreateContainerRequest`                                     | `pod_sandbox_id`、`config`、`sandbox_config`                 | `ContainerCreate`     | POD/IMAGE 非空；create flags 合法               | Pod 不存在返回 4；配置被 daemon 拒绝返回 6                   |
| `container start ID`         | `StartContainerRequest`                                      | `container_id=ID`                                            | `ContainerStart`      | ID 非空                                         | 已运行按 daemon 语义返回；不存在返回 4                       |
| `container stop ID`          | `StopContainerRequest`                                       | `container_id=ID`、`timeout`                                 | `ContainerStop`       | timeout 非负                                    | 不存在返回 4                                                 |
| `container remove ID`        | `RemoveContainerRequest`                                     | `container_id=ID`                                            | `ContainerRemove`     | ID 非空                                         | daemon 拒绝删除运行中容器时返回 6                            |
| `container exec ID`          | `ExecRequest`                                                | `container_id`、`cmd`、`tty`、`stdin/stdout/stderr`          | streaming 无 envelope | command 至少一个；TTY 自动关闭 stderr           | CRI `Exec` RPC 不可达返回 125；streaming URL 连接失败返回 1  |
| `container exec-sync ID`     | `ExecSyncRequest`                                            | `container_id`、`cmd`、`timeout`                             | `ExecSync`            | command 至少一个；timeout 非负                  | 返回容器进程 exit code                                       |
| `container attach ID`        | `AttachRequest`                                              | `container_id`、`tty`、`stdin/stdout/stderr`                 | streaming 无 envelope | stdout/stderr 至少一个为 true，或 stdin 为 true | 正常断开返回 0                                               |
| `container stats [ID]`       | 指定 ID 时用 `ContainerStatsRequest`；未指定时用 `ListContainerStatsRequest` | 单个 ID 或 filter                                            | `ContainerStats`      | label filter 合法                               | stats 缺失给 warning                                         |
| `container checkpoint ID`    | `CheckpointContainerRequest`                                 | `container_id`、`location`、`timeout`                        | `ContainerCheckpoint` | location 非空；timeout 非负                     | runtime 不支持返回 6                                         |
| `container update ID`        | `UpdateContainerResourcesRequest`                            | `container_id`、`linux.resources`、`annotations`             | `ContainerUpdate`     | 至少一个 resource 或 annotation                 | 不支持字段由 daemon 返回 6                                   |
| `container reopen-log ID`    | `ReopenContainerLogRequest`                                  | `container_id=ID`                                            | `ContainerReopenLog`  | ID 非空                                         | 日志未配置返回 6                                             |
| `container logs ID`          | `ContainerLogRequest`                                        | `container_id`、`follow`、`tail_lines`、`since_unix_nanos`、`timestamps` | `ContainerLogs`       | tail >= 0；since 可解析                         | 日志未配置或状态不满足返回 6；daemon 路径权限校验失败返回 13 |

`container create` 需要先调用 `PodSandboxStatus(POD, verbose=false)` 获取 `sandbox_config`，再调用 `CreateContainer`。CLI 必须传入获取到的 `sandbox_config`，以保持与 CRI 字段语义一致。

### 组合 `run` 实现

| 步骤 | 条件                        | RPC / 动作                                                   | 输出 / 状态记录              |
| ---- | --------------------------- | ------------------------------------------------------------ | ---------------------------- |
| 1    | `--pull always`             | `PullImage`                                                  | 记录 image ref               |
| 2    | `--pull missing`            | `ImageStatus`，缺失时 `PullImage`                            | image 缺失不直接失败         |
| 3    | 未指定 `--pod`              | `RunPodSandbox`                                              | 记录 created_pod_id          |
| 4    | 指定 `--pod`                | `PodSandboxStatus`                                           | 不把该 Pod 标记为 CLI 创建   |
| 5    | 始终                        | `CreateContainer`                                            | 记录 created_container_id    |
| 6    | 始终                        | `StartContainer`                                             | 记录 started=true            |
| 7    | `--detach`                  | 无 streaming                                                 | 输出 container ID            |
| 8    | 前台 + `--exec-mode sync`   | `ExecSync`                                                   | 返回进程 exit code           |
| 9    | 前台 + `--exec-mode attach` | `Attach` streaming                                           | 正常断开后按 daemon 状态处理 |
| 10   | `--rm`                      | `StopContainer`、`RemoveContainer`；未指定 `--pod` 时再执行 `StopPodSandbox`、`RemovePodSandbox` | 只清理由 CLI 创建的对象      |

`run` 的失败输出必须包含执行过程中已经获得的以下字段；尚未生成的字段不输出：

- image ref。
- 已创建 pod sandbox ID。
- 已创建 container ID。
- 已执行的清理动作。
- 未能清理的对象和建议命令。

`run --rm` 清理失败时，主命令退出码保留原始失败原因；清理失败作为 warning 输出。

### Events、Stats、Metrics 实现

| 命令                  | Request / 来源                 | 关键字段                                                     | 输出 kind                   | CLI 校验                                   | 失败语义                           |
| --------------------- | ------------------------------ | ------------------------------------------------------------ | --------------------------- | ------------------------------------------ | ---------------------------------- |
| `events`              | `GetContainerEventsRequest`    | 当前命令不提供过滤参数                                       | `ContainerEvents` 或 NDJSON | 无额外校验                                 | SIGINT 返回 130；daemon EOF 返回 0 |
| `stats`               | `ListContainerStatsRequest`    | label filter                                                 | `ContainerStats`            | label 合法                                 | 空列表返回 0                       |
| `metrics descriptors` | `ListMetricDescriptorsRequest` | 无                                                           | `MetricDescriptors`         | 无额外校验                                 | 不支持返回清晰错误                 |
| `metrics scrape`      | metrics endpoint               | 从 diagnostics `EffectiveConfig` 的 metrics endpoint 字段确定访问方式 | `MetricsScrape` 或 text     | endpoint 来自 daemon config，不由 CLI 猜测 | endpoint 未启用返回 6              |

`metrics scrape --output text` 输出 daemon metrics 端点返回的文本内容；`--output json` 只输出 metadata 和文本摘要，不改变 metrics 语义。

### Recovery 和 GC 实现

| 命令                        | Request                 | 关键字段        | 输出 kind              | CLI 校验               | 失败语义                                         |
| --------------------------- | ----------------------- | --------------- | ---------------------- | ---------------------- | ------------------------------------------------ |
| `recovery status`           | `RecoveryStatusRequest` | 无              | `RecoveryStatus`       | 无额外校验             | diagnostics 不可用返回 1                         |
| `recovery check`            | `RecoveryCheckRequest`  | `execute=false` | `RecoveryCheck`        | 无额外校验             | 检查发现问题仍返回 0，用 summary 标识 unhealthy  |
| `recovery repair --dry-run` | `RecoveryCheckRequest`  | `execute=false` | `RecoveryRepairPlan`   | dry-run/execute 二选一 | 无修改动作                                       |
| `recovery repair --execute` | `RecoveryCheckRequest`  | `execute=true`  | `RecoveryRepairResult` | dry-run/execute 二选一 | 单项失败不隐藏，summary 统计失败数               |
| `gc candidates`             | `ContentGcRequest`      | `execute=false` | `GcCandidates`         | 无额外校验             | 空候选返回 0                                     |
| `gc run --dry-run`          | `ContentGcRequest`      | `execute=false` | `GcPlan`               | dry-run/execute 二选一 | 无删除动作                                       |
| `gc run --execute`          | `ContentGcRequest`      | `execute=true`  | `GcResult`             | dry-run/execute 二选一 | 单项删除失败不隐藏，summary 统计 reclaimed bytes |

`recovery check` 与 `recovery repair --dry-run` 复用同一个 RPC，但输出 kind 不同，便于脚本区分“只检查”和“修复计划”。

### Debug 实现

| 命令              | 数据来源                                                  | 输出 kind        | 最少字段                                                     |
| ----------------- | --------------------------------------------------------- | ---------------- | ------------------------------------------------------------ |
| `debug network`   | `Status(verbose=true)` + diagnostics `EffectiveConfig`    | `DebugNetwork`   | ready、reason、podCIDR、CNI config dir、CNI bin dir、template、last error、hostPort 支持 |
| `debug runtime`   | `Version`、`RuntimeConfig`、diagnostics `RuntimeHandlers` | `DebugRuntime`   | runtime name/version、runc path、handler、features、cgroup driver |
| `debug shims`     | diagnostics `ShimStatus`                                  | `DebugShims`     | container ID、pid、task socket、attach socket、state、error  |
| `debug nri`       | diagnostics `NriStatus`                                   | `DebugNri`       | enabled、plugin path、config path、CDI dirs、blockio、RDT、warnings |
| `debug security`  | diagnostics `SecurityStatus`                              | `DebugSecurity`  | seccomp、AppArmor、SELinux、rootless、devices policy、warnings |
| `debug cgroups`   | CRI `RuntimeConfig` + `Status(verbose=true)`              | `DebugCgroups`   | cgroup version、driver、systemd support、controllers、delegation |
| `debug streaming` | diagnostics `EffectiveConfig` + diagnostics `ServerInfo`  | `DebugStreaming` | bind address、TLS、token TTL、idle timeout、port-forward timeout |
| `debug metrics`   | diagnostics `EffectiveConfig` + metrics endpoint          | `DebugMetrics`   | endpoint、TLS、collector list、pod metrics enabled、last scrape error |
| `debug tracing`   | diagnostics `EffectiveConfig`                             | `DebugTracing`   | enabled、endpoint、protocol、sampling、resource attrs        |
| `debug rootless`  | diagnostics `SecurityStatus`                              | `DebugRootless`  | enabled、uid/gid map、network mode、cgroup mode、warnings    |

debug 命令的 table 输出只展示摘要，不倾倒大段 JSON；详细结构通过 `--output json` 获取。

## 诊断接口

CRI 和 `Status(verbose=true)` 覆盖常规状态查询。日志读取、shim 状态、GC 执行、恢复修复等维护能力由 daemon 提供项目内诊断接口：

```text
proto/crius/diagnostics/v1/diagnostics.proto
src/services/diagnostics.rs
```

接口定义：

```protobuf
service DiagnosticsService {
  rpc ServerInfo(ServerInfoRequest) returns (ServerInfoResponse);
  rpc EffectiveConfig(EffectiveConfigRequest) returns (EffectiveConfigResponse);
  rpc RuntimeHandlers(RuntimeHandlersRequest) returns (RuntimeHandlersResponse);
  rpc ImageTransfers(ImageTransfersRequest) returns (ImageTransfersResponse);
  rpc RecoveryStatus(RecoveryStatusRequest) returns (RecoveryStatusResponse);
  rpc RecoveryCheck(RecoveryCheckRequest) returns (RecoveryCheckResponse);
  rpc NriStatus(NriStatusRequest) returns (NriStatusResponse);
  rpc SecurityStatus(SecurityStatusRequest) returns (SecurityStatusResponse);
  rpc ShimStatus(ShimStatusRequest) returns (ShimStatusResponse);
  rpc ContentGc(ContentGcRequest) returns (ContentGcResponse);
  rpc ContainerLog(ContainerLogRequest) returns (stream ContainerLogChunk);
}
```

接口边界：

- 默认通过 `crius` daemon 的本地 Unix socket 暴露，socket 权限由 daemon 启动配置控制。
- TCP 暴露需要 daemon 显式配置，并补齐 TLS、鉴权和配置校验。
- 日志读取、shim 查询、GC、恢复修复等命令由 daemon 做路径校验和状态校验。
- CLI 只传对象 ID 和操作参数；宿主路径由 daemon 基于自身状态解析。

## Flag 规格

本节把复杂 CLI 参数细化到 CRI 字段或 diagnostics 字段。重复参数按出现顺序累积；map 类参数使用 `KEY=VALUE`；布尔参数默认 false，出现即 true，除非另有说明。

### 通用过滤与输出

| Flag                     | 适用命令                                     | 映射 / 行为                                                  |
| ------------------------ | -------------------------------------------- | ------------------------------------------------------------ |
| `--output table|json`    | list / inspect / status / debug 等查看类命令 | 控制输出格式                                                 |
| `--quiet`                | list、pull、create、run                      | 只输出 ID、image ref 或最小结果                              |
| `--no-trunc`             | list、events、stats                          | 不截断 ID 和长字段                                           |
| `--label KEY=VALUE`      | list、pod/container create/run               | list 时映射 CRI filter labels；create/run 时写入 config labels |
| `--annotation KEY=VALUE` | create/run/update                            | 写入 config annotations 或 update annotations                |
| `--id ID`                | list/stats 过滤                              | 传入时映射 CRI filter id；未传时不设置 id filter             |
| `--state STATE`          | pod/container list                           | 映射 CRI state filter                                        |
| `--all`                  | pod/container list                           | 包含非 ready / 非 running 对象                               |

### 镜像参数

| Flag                     | 适用命令     | 映射 / 行为                                                  |
| ------------------------ | ------------ | ------------------------------------------------------------ |
| `--auth-json JSON`       | `image pull` | 解析为 CRI `AuthConfig`                                      |
| `--auth-file PATH`       | `image pull` | 读取 auth JSON 后解析为 `AuthConfig`                         |
| `--username USER`        | `image pull` | `AuthConfig.username`                                        |
| `--password PASS`        | `image pull` | `AuthConfig.password`                                        |
| `--server ADDRESS`       | `image pull` | `AuthConfig.server_address`                                  |
| `--identity-token TOKEN` | `image pull` | `AuthConfig.identity_token`                                  |
| `--registry-token TOKEN` | `image pull` | `AuthConfig.registry_token`                                  |
| `--pod POD`              | `image pull` | 读取 Pod sandbox config 作为 `PullImageRequest.sandbox_config` 上下文 |

`--auth-json`、`--auth-file` 和分散 auth flags 只能选择一种来源。密码类参数不在 debug 日志中明文打印。

### Pod Sandbox 创建参数

| Flag                                                         | 映射 / 行为                                                  |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| `--name NAME`                                                | `PodSandboxMetadata.name`                                    |
| `--uid UID`                                                  | `PodSandboxMetadata.uid`，默认生成本地 UID                   |
| `--namespace NS`                                             | `PodSandboxMetadata.namespace`，默认 `default`               |
| `--attempt N`                                                | `PodSandboxMetadata.attempt`                                 |
| `--hostname NAME`                                            | `PodSandboxConfig.hostname`                                  |
| `--log-dir PATH`                                             | `PodSandboxConfig.log_directory`                             |
| `--dns-server IP`                                            | 追加 `DNSConfig.servers`                                     |
| `--dns-search DOMAIN`                                        | 追加 `DNSConfig.searches`                                    |
| `--dns-option OPTION`                                        | 追加 `DNSConfig.options`                                     |
| `--publish HOST:CONTAINER[/PROTO]`                           | 追加 `PortMapping`，PROTO 为 `tcp|udp|sctp`，默认 `tcp`      |
| `--host-ip IP`                                               | 应用于后续 `--publish`，写入 `PortMapping.host_ip`           |
| `--runtime-handler NAME`                                     | `RunPodSandboxRequest.runtime_handler`                       |
| `--cgroup-parent PATH`                                       | `LinuxPodSandboxConfig.cgroup_parent`                        |
| `--sysctl KEY=VALUE`                                         | `LinuxPodSandboxConfig.sysctls`                              |
| `--host-network`                                             | `NamespaceOption.network=NODE`                               |
| `--host-pid`                                                 | `NamespaceOption.pid=NODE`                                   |
| `--host-ipc`                                                 | `NamespaceOption.ipc=NODE`                                   |
| `--userns pod|node`                                          | `UserNamespace.mode`                                         |
| `--uid-map HOST:CONTAINER:LEN`                               | 追加 `UserNamespace.uids`                                    |
| `--gid-map HOST:CONTAINER:LEN`                               | 追加 `UserNamespace.gids`                                    |
| `--sandbox-user UID[:GID]`                                   | `LinuxSandboxSecurityContext.run_as_user/run_as_group`       |
| `--sandbox-group GID`                                        | `LinuxSandboxSecurityContext.run_as_group`                   |
| `--sandbox-supplemental-group GID`                           | 追加 `supplemental_groups`                                   |
| `--sandbox-readonly-rootfs`                                  | `LinuxSandboxSecurityContext.readonly_rootfs`                |
| `--sandbox-privileged`                                       | `LinuxSandboxSecurityContext.privileged`                     |
| `--sandbox-seccomp runtime/default|unconfined|localhost:PATH` | `LinuxSandboxSecurityContext.seccomp`                        |
| `--sandbox-apparmor runtime/default|unconfined|localhost:PROFILE` | `LinuxSandboxSecurityContext.apparmor`                       |
| `--sandbox-selinux user:role:type:level`                     | `LinuxSandboxSecurityContext.selinux_options`                |
| `--overhead RESOURCE`                                        | `UpdatePodSandboxResourcesRequest.overhead` shape; also usable on create |
| `--pod-resource RESOURCE`                                    | `LinuxPodSandboxConfig.resources`                            |

`--publish` 的 IPv6 host IP 必须使用 bracket 形式：`[HOST_IP]:HOST_PORT:CONTAINER_PORT/PROTO`。未使用 bracket 的 IPv6 输入返回参数错误。

### Container 创建参数

| Flag                                                      | 映射 / 行为                                                  |
| --------------------------------------------------------- | ------------------------------------------------------------ |
| `--name NAME`                                             | `ContainerMetadata.name`                                     |
| `--attempt N`                                             | `ContainerMetadata.attempt`                                  |
| `--image IMAGE` 或位置参数 `IMAGE`                        | `ImageSpec.image`                                            |
| `--command ARG`                                           | 追加 `ContainerConfig.command`                               |
| `--arg ARG` 或 `--` 后参数                                | 追加 `ContainerConfig.args`                                  |
| `--workdir DIR`                                           | `ContainerConfig.working_dir`                                |
| `--env KEY=VALUE`                                         | 追加 `ContainerConfig.envs`                                  |
| `--env-file PATH`                                         | 逐行追加 `envs`，空行和 `#` 注释忽略                         |
| `--mount SPEC`                                            | 追加 `Mount`，见 mount 规格                                  |
| `--device HOST[:CONTAINER[:PERMS]]`                       | 追加 `Device`                                                |
| `--cdi-device NAME`                                       | 追加 `CDIDevice.name`                                        |
| `--log-path PATH`                                         | `ContainerConfig.log_path`，相对 pod log dir                 |
| `--stdin`                                                 | create 时记录 stdin intent，exec/attach 时映射 request stdin |
| `--tty`                                                   | create 时设置 TTY intent，exec/attach 时映射 request tty     |
| `--cpu-period N`                                          | `LinuxContainerResources.cpu_period`                         |
| `--cpu-quota N`                                           | `LinuxContainerResources.cpu_quota`                          |
| `--cpu-shares N`                                          | `LinuxContainerResources.cpu_shares`                         |
| `--memory BYTES`                                          | `LinuxContainerResources.memory_limit_in_bytes`              |
| `--memory-swap BYTES`                                     | `LinuxContainerResources.memory_swap_limit_in_bytes`         |
| `--oom-score-adj N`                                       | `LinuxContainerResources.oom_score_adj`                      |
| `--cpuset-cpus LIST`                                      | `LinuxContainerResources.cpuset_cpus`                        |
| `--cpuset-mems LIST`                                      | `LinuxContainerResources.cpuset_mems`                        |
| `--hugepage SIZE=BYTES`                                   | 追加 `HugepageLimit`                                         |
| `--unified KEY=VALUE`                                     | `LinuxContainerResources.unified`                            |
| `--privileged`                                            | `LinuxContainerSecurityContext.privileged`                   |
| `--cap-add CAP`                                           | 追加 `capabilities.add_capabilities`                         |
| `--cap-drop CAP`                                          | 追加 `capabilities.drop_capabilities`                        |
| `--ambient-cap-add CAP`                                   | 追加 `capabilities.add_ambient_capabilities`                 |
| `--user UID[:GID]`                                        | `run_as_user/run_as_group` 或 `run_as_username`              |
| `--group GID`                                             | `run_as_group`                                               |
| `--supplemental-group GID`                                | 追加 `supplemental_groups`                                   |
| `--readonly-rootfs`                                       | `readonly_rootfs`                                            |
| `--no-new-privs`                                          | `no_new_privs`                                               |
| `--masked-path PATH`                                      | 追加 `masked_paths`                                          |
| `--readonly-path PATH`                                    | 追加 `readonly_paths`                                        |
| `--seccomp runtime/default|unconfined|localhost:PATH`     | `SecurityProfile seccomp`                                    |
| `--apparmor runtime/default|unconfined|localhost:PROFILE` | `SecurityProfile apparmor`                                   |
| `--selinux user:role:type:level`                          | `SELinuxOption`                                              |
| `--pid pod|container|node|target:ID`                      | `NamespaceOption.pid/target_id`                              |
| `--ipc pod|node`                                          | `NamespaceOption.ipc`                                        |
| `--blockio-class CLASS`                                   | annotations 或 NRI resource field，由 daemon 统一解析        |
| `--rdt-class CLASS`                                       | annotations 或 NRI resource field，由 daemon 统一解析        |

`container create POD IMAGE [COMMAND...]` 中，`COMMAND...` 默认作为 `args`；如果指定 `--command`，位置参数追加到 `args`。

### Mount 规格

`--mount` 使用逗号分隔的 key/value：

```text
type=bind,src=/host,dst=/container,options=ro:rprivate
type=image,image=IMAGE_REF,dst=/container,subpath=PATH
```

| Key                               | 映射 / 行为                                                  |
| --------------------------------- | ------------------------------------------------------------ |
| `type=bind`                       | 使用 `host_path`                                             |
| `type=image`                      | 使用 `Mount.image`，禁止同时设置 `src`                       |
| `src` / `source`                  | `Mount.host_path`                                            |
| `dst` / `target`                  | `Mount.container_path`                                       |
| `image`                           | `Mount.image.image`                                          |
| `subpath`                         | `Mount.image_sub_path`                                       |
| `options=ro|rw`                   | `Mount.readonly`                                             |
| `options=rprivate|rslave|rshared` | `Mount.propagation`                                          |
| `options=z|Z`                     | `Mount.selinux_relabel`                                      |
| `options=recursive-ro`            | `Mount.recursive_read_only`，同时要求 `ro` 和 private propagation |
| `uidmap=HOST:CONTAINER:LEN`       | 追加 `Mount.uidMappings`                                     |
| `gidmap=HOST:CONTAINER:LEN`       | 追加 `Mount.gidMappings`                                     |

### Resource 规格

`RESOURCE` 使用逗号分隔 key/value，可用于 `container update`、`pod update-resources`、`--pod-resource`、`--overhead`：

```text
cpu-period=100000,cpu-quota=50000,memory=268435456,cpuset-cpus=0-1
```

支持字段：`cpu-period`、`cpu-quota`、`cpu-shares`、`memory`、`memory-swap`、`oom-score-adj`、`cpuset-cpus`、`cpuset-mems`、`hugepage.SIZE=BYTES`、`unified.KEY=VALUE`。

### Streaming 参数

| Flag                        | 适用命令                   | 映射 / 行为                                                  |
| --------------------------- | -------------------------- | ------------------------------------------------------------ |
| `--stdin` / `-i`            | exec、attach、run          | `stdin=true`                                                 |
| `--tty` / `-t`              | exec、attach、run          | `tty=true`；stderr stream 自动关闭                           |
| `--stdout=false`            | exec、attach               | `stdout=false`                                               |
| `--stderr=false`            | exec、attach               | `stderr=false`                                               |
| `--timeout SECONDS`         | exec-sync                  | `ExecSyncRequest.timeout`                                    |
| `--resize COLSxROWS`        | exec、attach               | 初始 pty resize                                              |
| `--protocol websocket|spdy` | exec、attach、port-forward | 选择 streaming protocol；未指定时 CLI 按 websocket、spdy 顺序尝试 |
| `--forward LOCAL:REMOTE`    | port-forward               | 支持重复传入；LOCAL 为本地监听端口，REMOTE 写入 `PortForwardRequest.port` |

### 维护命令参数

| Flag                       | 适用命令                    | 映射 / 行为                           |
| -------------------------- | --------------------------- | ------------------------------------- |
| `--location PATH`          | `container checkpoint`      | `CheckpointContainerRequest.location` |
| `--timeout SECONDS`        | `container checkpoint`      | `CheckpointContainerRequest.timeout`  |
| `--follow`                 | `container logs`            | diagnostics log stream follow         |
| `--tail N`                 | `container logs`            | daemon 侧从日志尾部读取               |
| `--since RFC3339|DURATION` | `container logs`            | daemon 侧时间过滤                     |
| `--timestamps`             | `container logs`            | 保留或显示日志时间戳                  |
| `--dry-run`                | `gc run`、`recovery repair` | 只返回候选和动作                      |
| `--execute`                | `gc run`、`recovery repair` | 执行 daemon 校验后的维护动作          |

## 输出契约

### JSON envelope

除 `inspect` 输出 CRI / diagnostics 响应结构及 CLI 解析字段外，其他 JSON 输出统一使用 envelope，方便脚本消费：

```json
{
  "kind": "ContainerList",
  "apiVersion": "crius.crius.dev/v1",
  "endpoint": "unix:///run/crius/crius.sock",
  "items": [],
  "summary": {},
  "warnings": []
}
```

| 字段         | 说明                                                  |
| ------------ | ----------------------------------------------------- |
| `kind`       | 输出类型，例如 `ImageList`、`RuntimeStatus`、`GcPlan` |
| `apiVersion` | CLI 输出结构版本，初始为 `crius.crius.dev/v1`         |
| `endpoint`   | 当前 daemon endpoint                                  |
| `items`      | 列表数据；非列表命令使用空数组                        |
| `summary`    | 摘要对象；单对象命令也可放在这里                      |
| `warnings`   | daemon 或 CLI 返回的非致命警告                        |

`--quiet` 不使用 envelope，只输出一行或多行机器可读值。错误输出始终走 stderr，不混入 JSON stdout。

### 表格列

| 命令                        | 默认列                                                       |
| --------------------------- | ------------------------------------------------------------ |
| `image list`                | `IMAGE`, `IMAGE ID`, `SIZE`, `USER SPEC`, `PINNED`           |
| `image transfers`           | `REF`, `STATUS`, `UPDATED`, `ERROR`                          |
| `pod list`                  | `POD ID`, `NAME`, `NAMESPACE`, `STATE`, `IP`, `CREATED`, `ATTEMPT` |
| `container list`            | `CONTAINER ID`, `POD`, `IMAGE`, `STATE`, `CREATED`, `NAME`, `ATTEMPT` |
| `container stats` / `stats` | `ID`, `NAME`, `CPU`, `MEMORY`, `PIDS`                        |
| `pod stats`                 | `POD ID`, `NAME`, `CPU`, `MEMORY`, `NETWORK`, `PIDS`         |
| `events`                    | `TIME`, `TYPE`, `CONTAINER`, `POD`                           |
| `runtime handlers`          | `HANDLER`, `RUNTIME`, `PATH`, `CONFIG`, `FEATURES`           |
| `doctor`                    | `CHECK`, `STATUS`, `DETAIL`, `ACTION`                        |
| `debug network`             | `READY`, `REASON`, `POD CIDR`, `CNI TEMPLATE`, `LAST ERROR`  |
| `debug nri`                 | `ENABLED`, `CDI`, `PLUGIN PATH`, `BLOCKIO`, `RDT`            |
| `debug security`            | `SECCOMP`, `APPARMOR`, `SELINUX`, `ROOTLESS`, `DEVICES`      |
| `gc candidates`             | `TYPE`, `ID`, `PATH`, `REASON`, `SIZE`                       |

`--no-trunc` 控制 ID、image ref、path、error message 的截断。时间统一按本地时区显示，JSON 中保留 Unix nanos 或 RFC3339 字段，不丢原始值。

### Inspect 输出

| 命令                | JSON 输出                                                    |
| ------------------- | ------------------------------------------------------------ |
| `pod inspect`       | CRI `PodSandboxStatusResponse`，包含 verbose `info` 解析后的 `infoJson` 字段 |
| `container inspect` | CRI `ContainerStatusResponse`，包含 verbose `info` 解析后的 `infoJson` 字段 |
| `image inspect`     | CRI `ImageStatusResponse`，包含 verbose `info` 解析后的 `infoJson` 字段 |
| `status --verbose`  | CRI `StatusResponse`，包含 `info.config` 解析后的 `configJson` 字段 |

CLI 保留原始 CRI 字段，同时增加解析后的 JSON 字段，避免调用方重复解析字符串化 JSON。

### JSON Kind 与字段

非 inspect JSON 输出统一使用以下 `kind`。`items` 用于列表或流式条目，`summary` 用于单次操作结果；没有对应内容时使用空对象或空数组。

| kind                   | 命令                           | `items`               | `summary` 必填字段                                        |
| ---------------------- | ------------------------------ | --------------------- | --------------------------------------------------------- |
| `RuntimeVersion`       | `version`                      | 空                    | `runtimeName`、`runtimeVersion`、`runtimeApiVersion`      |
| `RuntimeStatus`        | `status`                       | conditions            | `runtimeReady`、`networkReady`                            |
| `RuntimeStatusVerbose` | `status --verbose`             | conditions            | `infoJson` 或 `infoRaw`                                   |
| `EffectiveConfig`      | `config show`                  | 空                    | `configJson`、`redactedFields`                            |
| `ConfigReloadStatus`   | `config reload-status`         | watchers              | `lastReloadAt`、`lastError`                               |
| `Doctor`               | `doctor`                       | checks                | `healthy`、`warningCount`、`errorCount`                   |
| `RuntimeConfig`        | `runtime config`               | 空                    | `cgroupDriver`                                            |
| `RuntimeConfigUpdate`  | `runtime update`               | 空                    | `podCidrs`、`updated`                                     |
| `RuntimeHandlers`      | `runtime handlers`             | handlers              | `count`                                                   |
| `ImageList`            | `image list`                   | images                | `count`                                                   |
| `ImagePull`            | `image pull`                   | 空                    | `imageRef`                                                |
| `ImageRemove`          | `image remove`                 | 空                    | `image`、`removed`                                        |
| `ImageFsInfo`          | `image fs-info`                | filesystems           | `count`、`totalBytes`、`usedBytes`                        |
| `ImageTransfers`       | `image transfers`              | transfers             | `active`、`failed`                                        |
| `ImageConfig`          | `image config`                 | 空                    | `snapshotter`、`signaturePolicy`、`pinnedImages`          |
| `PodList`              | `pod list`                     | pods                  | `count`                                                   |
| `PodRun`               | `pod run`                      | 空                    | `podSandboxId`、`name`、`namespace`                       |
| `PodStop`              | `pod stop`                     | 空                    | `podSandboxId`、`stopped`                                 |
| `PodRemove`            | `pod remove`                   | 空                    | `podSandboxId`、`removed`                                 |
| `PodStats`             | `pod stats`                    | stats                 | `count`                                                   |
| `PodMetrics`           | `pod metrics`                  | metrics               | `count`                                                   |
| `PodResourceUpdate`    | `pod update-resources`         | 空                    | `podSandboxId`、`updated`                                 |
| `PortForward`          | `pod port-forward`             | listeners             | `podSandboxId`、`ports`                                   |
| `ContainerList`        | `container list`               | containers            | `count`                                                   |
| `ContainerCreate`      | `container create`             | 空                    | `containerId`、`podSandboxId`                             |
| `ContainerStart`       | `container start`              | 空                    | `containerId`、`started`                                  |
| `ContainerStop`        | `container stop`               | 空                    | `containerId`、`stopped`                                  |
| `ContainerRemove`      | `container remove`             | 空                    | `containerId`、`removed`                                  |
| `ExecSync`             | `container exec-sync`          | 空                    | `containerId`、`exitCode`、`stdout`、`stderr`             |
| `ContainerStats`       | `container stats` / `stats`    | stats                 | `count`                                                   |
| `ContainerCheckpoint`  | `container checkpoint`         | 空                    | `containerId`、`location`                                 |
| `ContainerUpdate`      | `container update`             | 空                    | `containerId`、`updated`                                  |
| `ContainerReopenLog`   | `container reopen-log`         | 空                    | `containerId`、`reopened`                                 |
| `ContainerLogs`        | `container logs --output json` | log chunks            | `containerId`、`follow`                                   |
| `RunResult`            | `run`                          | cleanup actions       | `image`、`podSandboxId`、`containerId`、`exitCode`        |
| `ContainerEvents`      | `events --output json`         | events 或 NDJSON 单条 | `stream`                                                  |
| `MetricDescriptors`    | `metrics descriptors`          | descriptors           | `count`                                                   |
| `MetricsScrape`        | `metrics scrape --output json` | 空                    | `contentType`、`bytes`、`scrapedAt`                       |
| `RecoveryStatus`       | `recovery status`              | warnings              | `status`、`unhealthyObjectCount`                          |
| `RecoveryCheck`        | `recovery check`               | actions               | `dryRun`、`actionCount`                                   |
| `RecoveryRepairPlan`   | `recovery repair --dry-run`    | actions               | `dryRun`、`actionCount`                                   |
| `RecoveryRepairResult` | `recovery repair --execute`    | actions               | `executed`、`failed`                                      |
| `GcCandidates`         | `gc candidates`                | candidates            | `count`、`totalBytes`                                     |
| `GcPlan`               | `gc run --dry-run`             | candidates            | `dryRun`、`totalBytes`                                    |
| `GcResult`             | `gc run --execute`             | candidates            | `reclaimedBytes`、`failed`                                |
| `DebugNetwork`         | `debug network`                | checks                | `ready`、`podCidrs`                                       |
| `DebugRuntime`         | `debug runtime`                | handlers              | `runtimeVersion`、`cgroupDriver`                          |
| `DebugShims`           | `debug shims`                  | shims                 | `count`                                                   |
| `DebugNri`             | `debug nri`                    | plugins               | `enabled`、`cdiEnabled`                                   |
| `DebugSecurity`        | `debug security`               | checks                | `seccompAvailable`、`apparmorAvailable`、`selinuxEnabled` |
| `DebugCgroups`         | `debug cgroups`                | controllers           | `version`、`driver`                                       |
| `DebugStreaming`       | `debug streaming`              | listeners             | `enabled`、`tokenTtl`                                     |
| `DebugMetrics`         | `debug metrics`                | collectors            | `endpoint`、`enabled`                                     |
| `DebugTracing`         | `debug tracing`                | exporters             | `enabled`、`endpoint`                                     |
| `DebugRootless`        | `debug rootless`               | checks                | `enabled`、`userns`                                       |

JSON 字段命名使用 lower camel case。ID 字段使用完整 ID，不受 `--no-trunc` 影响；截断只影响 table/text 输出。

### 展示模型定义

CLI 不直接把 tonic response 交给 formatter。每个命令先转换为展示模型，展示模型再序列化为 table 或 JSON。展示模型只包含 CLI 输出需要的字段，原始 CRI / diagnostics response 只在 inspect 类命令中保留。

```rust
#[derive(Serialize)]
pub struct RuntimeVersionView {
    pub runtime_name: String,
    pub runtime_version: String,
    pub runtime_api_version: String,
}

#[derive(Serialize)]
pub struct ConditionView {
    pub kind: String,
    pub status: bool,
    pub reason: String,
    pub message: String,
}

#[derive(Serialize)]
pub struct ImageView {
    pub image: String,
    pub image_id: String,
    pub repo_tags: Vec<String>,
    pub repo_digests: Vec<String>,
    pub size_bytes: u64,
    pub uid: Option<i64>,
    pub username: String,
    pub pinned: bool,
}

#[derive(Serialize)]
pub struct PodView {
    pub id: String,
    pub name: String,
    pub namespace: String,
    pub uid: String,
    pub attempt: u32,
    pub state: String,
    pub ip: String,
    pub created_at_unix_nanos: i64,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
}

#[derive(Serialize)]
pub struct ContainerView {
    pub id: String,
    pub pod_sandbox_id: String,
    pub name: String,
    pub attempt: u32,
    pub image: String,
    pub image_ref: String,
    pub state: String,
    pub created_at_unix_nanos: i64,
    pub started_at_unix_nanos: i64,
    pub finished_at_unix_nanos: i64,
    pub exit_code: i32,
    pub reason: String,
    pub message: String,
}

#[derive(Serialize)]
pub struct ResourceUsageView {
    pub id: String,
    pub name: String,
    pub cpu_nano_cores: u64,
    pub cpu_usage_core_nano_seconds: u64,
    pub memory_working_set_bytes: u64,
    pub memory_available_bytes: u64,
    pub memory_usage_bytes: u64,
    pub pids: u64,
}

#[derive(Serialize)]
pub struct OperationView {
    pub object_type: String,
    pub object_id: String,
    pub action: String,
    pub status: String,
    pub reason: String,
    pub error: String,
}
```

表格展示从这些 view 派生；JSON 输出直接使用这些 view 的 lower camel case 字段名。新增命令时必须先定义 view，再写 formatter。

### Table 格式细则

| 类型     | 格式                                                         |
| -------- | ------------------------------------------------------------ |
| ID       | 默认 12 字符；`--no-trunc` 完整输出                          |
| image ID | 去掉常见 digest 前缀后默认 12 字符；`--no-trunc` 完整输出    |
| 时间     | 小于 24 小时显示相对时间，例如 `3m ago`；超过 24 小时显示本地 `YYYY-MM-DD HH:MM:SS` |
| bytes    | table 使用 IEC 单位，例如 `64.0 MiB`；JSON 使用 bytes 整数   |
| CPU      | table 显示 millicores，例如 `125m`；JSON 使用 CRI 原始 nanos |
| bool     | table 使用 `true` / `false`，不使用图标                      |
| 空字符串 | table 显示 `-`；JSON 保留空字符串                            |
| 空列表   | table 不输出数据行；JSON `items: []`                         |
| warning  | table 模式写 stderr；JSON 模式写 envelope `warnings`         |

列宽计算：

- 先计算 header 宽度。
- 再计算已截断后的 cell 宽度。
- cell 内换行替换为空格。
- 多字节字符按 Unicode display width 处理。
- 最大列宽默认 80；超过后尾部使用 `...`。

## Diagnostics Proto 规格

诊断接口只承载 CRI 无法安全表达的本项目维护能力。以下 proto 是首版固定接口；新增字段必须遵循“Diagnostics 字段演进规则”，并同步更新命令实现明细、输出契约和验收矩阵。

```protobuf
syntax = "proto3";

package diagnostics.v1;

service DiagnosticsService {
  rpc ServerInfo(ServerInfoRequest) returns (ServerInfoResponse);
  rpc EffectiveConfig(EffectiveConfigRequest) returns (EffectiveConfigResponse);
  rpc RuntimeHandlers(RuntimeHandlersRequest) returns (RuntimeHandlersResponse);
  rpc ImageTransfers(ImageTransfersRequest) returns (ImageTransfersResponse);
  rpc RecoveryStatus(RecoveryStatusRequest) returns (RecoveryStatusResponse);
  rpc RecoveryCheck(RecoveryCheckRequest) returns (RecoveryCheckResponse);
  rpc NriStatus(NriStatusRequest) returns (NriStatusResponse);
  rpc SecurityStatus(SecurityStatusRequest) returns (SecurityStatusResponse);
  rpc ShimStatus(ShimStatusRequest) returns (ShimStatusResponse);
  rpc ContentGc(ContentGcRequest) returns (ContentGcResponse);
  rpc ContainerLog(ContainerLogRequest) returns (stream ContainerLogChunk);
}

message ServerInfoRequest {}
message ServerInfoResponse {
  string version = 1;
  string git_commit = 2;
  string config_path = 3;
  string state_dir = 4;
  string socket_path = 5;
}

message EffectiveConfigRequest {
  bool include_sensitive = 1;
}
message EffectiveConfigResponse {
  string config_json = 1;
  repeated string redacted_fields = 2;
  repeated string warnings = 3;
}

message RuntimeHandlersRequest {}
message RuntimeHandlerInfo {
  string name = 1;
  string runtime_type = 2;
  string runtime_path = 3;
  string runtime_config_path = 4;
  repeated string features = 5;
  repeated string warnings = 6;
}
message RuntimeHandlersResponse {
  repeated RuntimeHandlerInfo handlers = 1;
}

message ImageTransfersRequest {
  bool include_completed = 1;
}
message ImageTransferInfo {
  string image = 1;
  string status = 2;
  int64 updated_at_unix_nanos = 3;
  string error = 4;
}
message ImageTransfersResponse {
  repeated ImageTransferInfo transfers = 1;
}

message RecoveryStatusRequest {}
message RecoveryStatusResponse {
  string status = 1;
  string last_startup = 2;
  uint64 unhealthy_object_count = 3;
  string ledger_summary_json = 4;
  repeated string warnings = 5;
}

message RecoveryCheckRequest {
  bool execute = 1;
}
message RecoveryAction {
  string object_type = 1;
  string object_id = 2;
  string action = 3;
  string reason = 4;
  bool executed = 5;
  string error = 6;
}
message RecoveryCheckResponse {
  bool dry_run = 1;
  repeated RecoveryAction actions = 2;
  repeated string warnings = 3;
}

message NriStatusRequest {}
message NriStatusResponse {
  bool enabled = 1;
  bool cdi_enabled = 2;
  repeated string cdi_spec_dirs = 3;
  string plugin_path = 4;
  string plugin_config_path = 5;
  string blockio_config_path = 6;
  bool blockio_supported = 7;
  bool rdt_supported = 8;
  repeated string warnings = 9;
}

message SecurityStatusRequest {}
message SecurityStatusResponse {
  bool seccomp_available = 1;
  bool seccomp_notifier_supported = 2;
  bool apparmor_available = 3;
  bool selinux_enabled = 4;
  bool rootless_enabled = 5;
  string devices_policy_json = 6;
  repeated string warnings = 7;
}

message ShimStatusRequest {
  string container_id = 1;
}
message ShimInfo {
  string container_id = 1;
  int64 pid = 2;
  string task_socket = 3;
  string attach_socket = 4;
  string state = 5;
  string error = 6;
}
message ShimStatusResponse {
  repeated ShimInfo shims = 1;
}

message ContentGcRequest {
  bool execute = 1;
}
message ContentGcCandidate {
  string object_type = 1;
  string object_id = 2;
  string path = 3;
  uint64 size_bytes = 4;
  string reason = 5;
  bool deleted = 6;
  string error = 7;
}
message ContentGcResponse {
  bool dry_run = 1;
  repeated ContentGcCandidate candidates = 2;
  uint64 reclaimed_bytes = 3;
  repeated string warnings = 4;
}

message ContainerLogRequest {
  string container_id = 1;
  bool follow = 2;
  int64 tail_lines = 3;
  int64 since_unix_nanos = 4;
  bool timestamps = 5;
}
message ContainerLogChunk {
  bytes data = 1;
  string stream = 2;
  int64 timestamp_unix_nanos = 3;
}
```

敏感配置默认脱敏。`include_sensitive=true` 仅用于受控的本地诊断场景，具体授权由 daemon socket 权限和 daemon 配置决定；本地 CLI 仍不得把敏感字段写入 debug 日志。

### Diagnostics 字段演进规则

- proto 使用 `proto3`。
- package 初始为 `diagnostics.v1`，Rust 模块稳定映射到 `crate::proto::diagnostics::v1`。
- 字段只追加，不复用已删除字段编号。
- 删除字段时在 proto 中保留 `reserved` 编号和名称。
- enum 第一项约定为 `UNKNOWN = 0`。
- 时间字段统一使用 Unix nanos，字段名后缀为 `_unix_nanos`。
- JSON 字段使用 `string *_json`，由 daemon 生成合法 JSON 字符串；CLI 解析失败时保留 raw 字符串并写入 warning。
- path 字段只用于展示，不作为 CLI 后续操作输入。
- request 中的对象标识使用 ID、image ref 或 bool / enum 参数，避免把宿主路径作为操作入口。

### Diagnostics 错误约定

| 场景                   | tonic code                                 | message 要求                      |
| ---------------------- | ------------------------------------------ | --------------------------------- |
| 对象不存在             | `NotFound`                                 | 包含对象类型和 ID                 |
| diagnostics 功能未启用 | `FailedPrecondition`                       | 说明需要启用的配置项              |
| 权限不足               | `PermissionDenied`                         | 不泄露敏感路径内容                |
| 参数非法               | `InvalidArgument`                          | 包含字段名和期望格式              |
| 执行动作部分失败       | `OK` + response item error                 | RPC 成功，单项 error 字段记录失败 |
| 内部状态读取失败       | `Internal`                                 | 包含模块名，不包含敏感数据        |
| 路径校验失败           | `PermissionDenied` 或 `FailedPrecondition` | 明确是 daemon 校验失败            |

GC 和 recovery 的 execute 类接口，单个候选失败时返回 `OK`，并在 item `error` 字段记录；整个计划无法生成或权限不足时返回非 OK status。

### Diagnostics Server 注册

`src/main.rs` 注册顺序：

1. 构造 `RuntimeServiceImpl`。
2. 构造 `DiagnosticsState`，写入 diagnostics 所需的只读状态句柄和维护动作入口。
3. 使用 `DiagnosticsState` 构造 `DiagnosticsServiceImpl`。
4. 在 tonic server builder 中同时添加 CRI RuntimeService、CRI ImageService、DiagnosticsService。

注册代码结构：

```rust
let diagnostics_state = DiagnosticsState::from_runtime(&runtime_service);
let diagnostics_service = DiagnosticsServiceImpl::new(diagnostics_state);

Server::builder()
    .add_service(runtime_service_server)
    .add_service(image_service_server)
    .add_service(DiagnosticsServiceServer::new(diagnostics_service))
    .serve_with_incoming(incoming)
    .await?;
```

diagnostics service 持有 `DiagnosticsState` 共享句柄。`DiagnosticsState` 由 `RuntimeServiceImpl` 初始化时构造，包含 diagnostics 需要的只读状态引用和显式维护动作入口；diagnostics 不直接读取 SQLite 或工作目录。

## 解析与校验规则

### 通用校验

- `KEY=VALUE` 中 key 不能为空；重复 key 对 map 类参数采用后者覆盖前者，对 list 类参数保留全部。
- ID 参数允许完整 ID 或 daemon 支持的前缀；CLI 不自行扫描本地状态。
- 路径参数只做语法校验和相对/绝对路径约束；安全校验由 daemon 基于自身状态完成。
- `--output json` 下 stdout 只包含 JSON；警告写入 JSON `warnings` 或 stderr，不能混用。
- 互斥参数在 CLI 层报参数错误，退出码 2。

### 互斥与依赖

| 规则                                                   | 处理                                    |
| ------------------------------------------------------ | --------------------------------------- |
| `--auth-json`、`--auth-file`、分散 auth flags 同时出现 | 参数错误                                |
| `--tty` 与 `--stderr=true`                             | 自动设置 `stderr=false`，debug 模式提示 |
| `--mount type=image` 同时设置 `src`                    | 参数错误                                |
| `recursive-ro` 未设置 `ro`                             | 参数错误                                |
| `recursive-ro` 与非 private propagation 同时出现       | 参数错误                                |
| `--sandbox-group` 未设置 sandbox user                  | 参数错误                                |
| `--group` 未设置 user / username                       | 参数错误                                |
| `--execute` 与 `--dry-run` 同时出现                    | 参数错误                                |
| `gc run` / `recovery repair` 未指定二者之一            | 参数错误                                |
| `--pid target:ID` 未提供 ID                            | 参数错误                                |

### 字段解析

| 输入                       | 规则                                                         |
| -------------------------- | ------------------------------------------------------------ |
| `--publish`                | `HOST:CONTAINER[/PROTO]`；端口范围 1-65535；协议小写归一化   |
| `--user`                   | 纯数字解析为 UID；`UID:GID` 同时设置 UID/GID；非数字解析为 username |
| `--seccomp` / `--apparmor` | `runtime/default`、`unconfined`、`localhost:VALUE` 映射 `SecurityProfile` |
| `--selinux`                | `user:role:type:level`，允许空段但保留位置                   |
| `--device`                 | `HOST[:CONTAINER[:PERMS]]`，PERMS 默认 `rwm`                 |
| `--hugepage`               | `SIZE=BYTES`，SIZE 原样传给 CRI，BYTES 解析为整数            |
| `--unified`                | `KEY=VALUE`，KEY 不做内置白名单，daemon 负责 cgroup 支持校验 |
| `--since`                  | RFC3339 或相对 duration；转换为 Unix nanos                   |

### Parser 语法定义

以下语法是 parser 单元测试的依据。实现不要求手写 parser generator，但行为必须保持一致。

```text
DURATION        := UINT ("ms" | "s" | "m" | "h")
BYTE_SIZE       := UINT ("" | "B" | "KiB" | "MiB" | "GiB" | "TiB")
KEY             := non-empty string without "="
VALUE           := string
KEY_VALUE       := KEY "=" VALUE
CIDR_LIST       := CIDR ("," CIDR)*
PORT_PROTOCOL   := "tcp" | "udp" | "sctp"
PORT_MAPPING    := PORT ":" PORT [ "/" PORT_PROTOCOL ]
PORT_MAPPING_V6 := HOST_IP "," PORT ":" PORT [ "/" PORT_PROTOCOL ]
USER_SPEC       := UINT | UINT ":" UINT | USERNAME
ID_MAPPING      := UINT ":" UINT ":" UINT
SEC_PROFILE     := "runtime/default" | "unconfined" | "localhost:" NON_EMPTY
SELINUX         := SELINUX_PART ":" SELINUX_PART ":" SELINUX_PART ":" SELINUX_PART
DEVICE          := PATH [ ":" PATH [ ":" DEVICE_PERMS ] ]
DEVICE_PERMS    := combination of "r" "w" "m"
HUGEPAGE        := NON_EMPTY "=" BYTE_SIZE
UNIFIED         := KEY "=" VALUE
MOUNT           := MOUNT_KV ("," MOUNT_KV)*
RESOURCE        := RESOURCE_KV ("," RESOURCE_KV)*
SINCE           := RFC3339 | DURATION
```

`--publish` 的 IPv6 地址必须使用 bracket 形式：`--publish [IPv6]:HOST:CONTAINER/PROTO`。未使用 bracket 的 IPv6 输入返回参数错误。也可以用 `--host-ip IPv6 --publish HOST:CONTAINER/PROTO` 表达同一映射。

### Parser 错误文案模板

| parser           | 错误模板                                                     |
| ---------------- | ------------------------------------------------------------ |
| duration         | `invalid duration "{value}": expected number followed by ms, s, m, or h` |
| byte size        | `invalid byte size "{value}": expected integer with optional KiB, MiB, GiB, or TiB suffix` |
| key/value        | `invalid {flag} value "{value}": expected KEY=VALUE`         |
| CIDR             | `invalid --pod-cidr value "{value}": expected CIDR`          |
| port             | `invalid --publish value "{value}": expected HOST:CONTAINER[/PROTO] with ports in 1-65535` |
| mount            | `invalid --mount value "{value}": {reason}`                  |
| resource         | `invalid resource value "{value}": {reason}`                 |
| security profile | `invalid {flag} value "{value}": expected runtime/default, unconfined, or localhost:VALUE` |
| SELinux          | `invalid {flag} value "{value}": expected user:role:type:level` |
| user             | `invalid {flag} value "{value}": expected UID, UID:GID, or username` |
| ID mapping       | `invalid {flag} value "{value}": expected HOST:CONTAINER:LENGTH` |
| auth JSON        | `invalid auth JSON from {source}: {reason}`                  |

错误文案需要包含 flag 名、原始 value 和期望格式。不得在错误中打印 password、token 或完整 auth JSON。

### 标准化规则

- 协议名转小写。
- state enum 转小写后匹配。
- capability 名不在 CLI 层大小写归一化，原样传 daemon。
- label / annotation key 保持原样。
- image reference 保持原样。
- path 保持原样，不做 canonicalize。
- duration 转为 `Duration`，RPC 需要秒数时向下取整但最小 1 秒，除非用户显式传 0。
- byte size 转为整数 bytes，溢出返回参数错误。
- RFC3339 时间转 Unix nanos；相对 duration 使用 CLI 当前时间计算。

## 工程实现规格

本节把功能计划细化到代码边界，作为实现 `crs` 时的直接拆分依据。

### 模块职责

| 模块                                           | 职责                                                         | 不负责                                             |
| ---------------------------------------------- | ------------------------------------------------------------ | -------------------------------------------------- |
| `src/crs/main.rs`                              | `tokio::main` 入口、调用 `crius::crs::run_cli`、把最终结果转成进程退出码 | 具体 CRI 请求构造、表格字段拼接                    |
| `src/crs/mod.rs`                               | 汇总 CLI 子模块，提供 `run_cli`                              | 具体命令实现、底层连接细节                         |
| `src/crs/args.rs`                              | clap `Parser` / `Subcommand` / `Args` 类型、help 文案、flag 互斥声明 | 网络连接、daemon 调用、路径读取                    |
| `src/crs/context.rs`                           | 保存 endpoint、timeout、输出模式、debug、no-trunc、quiet、终端能力 | 解析业务对象、维护全局可变状态                     |
| `src/crs/client.rs`                            | 创建 Unix / TCP tonic channel、构造 RuntimeService / ImageService / DiagnosticsService client、给每个 RPC 套 timeout | 命令级重试、输出格式化                             |
| `src/crs/error.rs`                             | `CliError`、`ExitCode`、gRPC status 映射、敏感字段脱敏、stderr 错误 JSON | daemon 内部错误分类                                |
| `src/crs/format.rs`                            | JSON envelope、table renderer、quiet 输出、时间/字节/百分比格式化、截断策略 | 业务请求构造                                       |
| `src/crs/ids.rs`                               | 短 ID、镜像 ID、长 path 和 error message 截断                | 通过扫描本地文件解析 ID                            |
| `src/crs/parsers.rs`                           | duration、byte size、key/value、mount、resource、auth、security profile、SELinux、user、port mapping 解析 | 调用 daemon 或访问状态目录                         |
| `src/crs/streaming.rs`                         | Exec / Attach / PortForward streaming URL 连接、stdin/stdout/stderr 转发、TTY raw mode、resize、退出码提取 | CRI `Exec` / `Attach` / `PortForward` RPC 请求生成 |
| `src/crs/commands/*.rs`                        | 每个命令的 request 构造、调用 client、组装 `CommandOutput`   | 低层连接细节、通用格式化                           |
| `proto/crius/diagnostics/v1/diagnostics.proto` | 定义项目诊断 gRPC API                                        | 暴露 CRI 已能表达的生命周期操作                    |
| `src/services/diagnostics.rs`                  | daemon 侧 diagnostics gRPC 适配层，复用 introspection / health / event 服务，实现日志、shim、GC、recovery、配置摘要等接口 | CLI 参数解析、runtime 编排重写                     |

所有 `src/crs/*` 模块默认 `pub(crate)`，只把 `src/crs/main.rs` 需要调用的 `run_cli` 作为 crate library 入口公开。命令模块之间不互相调用公共实现；共享逻辑进入 `parsers`、`format`、`client`，避免命令文件形成隐式依赖链。

### 文件级落地清单

每个文件按下表落地。新增功能必须先补充本表的模块边界，再进入实现，避免临时塞进相邻模块。

| 文件                                           | 新增 / 修改                                                  | 公开项                           | 测试要求                                               |
| ---------------------------------------------- | ------------------------------------------------------------ | -------------------------------- | ------------------------------------------------------ |
| `Cargo.toml`                                   | 新增 `[[bin]] crs`；确认 CLI 所需依赖已存在                  | 无                               | `cargo build --bins` 能发现 bin                        |
| `build.rs`                                     | 增加 diagnostics proto 编译输入和 rerun-if-changed           | 无                               | 生成文件存在且 crate 可 include                        |
| `src/lib.rs`                                   | 新增 `pub mod crs;`；保持 `pub mod services;` 作为 daemon diagnostics 的模块入口 | crate 内模块入口                 | lib 编译通过                                           |
| `src/crs/main.rs`                              | CLI main、tracing、exit code、调用 `crius::crs::run_cli`     | `main()`                         | 参数错误返回 2，RPC mock 成功返回 0                    |
| `src/crs/mod.rs`                               | 汇总子模块，暴露 `run_cli`                                   | `pub async fn run_cli()`         | 编译边界                                               |
| `src/crs/args.rs`                              | 所有 clap 结构、枚举、help、互斥规则                         | `Args`、`Command`、各子命令 args | 每个命令 parse 成功；互斥参数失败                      |
| `src/crs/context.rs`                           | `CliContext`、`Endpoint`、全局 option 合并                   | `CliContext::from_args`          | env / flag 优先级                                      |
| `src/crs/client.rs`                            | tonic channel、Unix connector、client wrapper、timeout       | `CrsClient`                      | endpoint 解析、timeout 包装                            |
| `src/crs/error.rs`                             | `CliError`、退出码映射、stderr JSON                          | `CliError`、`ExitStatus`         | gRPC code 到退出码                                     |
| `src/crs/format.rs`                            | table/json/text/quiet 输出                                   | `print_output`、`TableRow`       | golden table、JSON envelope                            |
| `src/crs/ids.rs`                               | ID、digest、path、error 截断                                 | `short_id`、`truncate_field`     | 边界长度                                               |
| `src/crs/parsers.rs`                           | 所有复杂 flag parser                                         | parser 函数                      | 每个 parser 成功/失败样例                              |
| `src/crs/streaming.rs`                         | streaming client、TTY guard、resize、port-forward            | `exec`、`attach`、`port_forward` | raw mode 恢复、stream error                            |
| `src/crs/commands/version.rs`                  | `version` handler                                            | `run`                            | 输出 kind                                              |
| `src/crs/commands/status.rs`                   | `status` handler、verbose info 解析                          | `run`                            | info JSON / raw 降级字段                               |
| `src/crs/commands/config.rs`                   | config show/reload-status                                    | `show`、`reload_status`          | diagnostics 不可用时读取 verbose status 降级字段       |
| `src/crs/commands/runtime.rs`                  | runtime config/update/handlers                               | `config`、`update`、`handlers`   | CIDR 校验                                              |
| `src/crs/commands/image.rs`                    | image 命令                                                   | image handlers                   | auth 互斥、pull pod context                            |
| `src/crs/commands/pod.rs`                      | pod 非 streaming 命令                                        | pod handlers                     | sandbox config 构造                                    |
| `src/crs/commands/container.rs`                | container 非 streaming / logs 命令                           | container handlers               | container config 构造                                  |
| `src/crs/commands/logs.rs`                     | logs 命令                                                    | `run`                            | chunk JSON/text                                        |
| `src/crs/commands/run.rs`                      | 组合验证命令                                                 | `run`                            | rollback / cleanup                                     |
| `src/crs/commands/exec.rs`                     | exec / exec-sync                                             | handlers                         | exit code                                              |
| `src/crs/commands/attach.rs`                   | attach                                                       | `run`                            | TTY 参数                                               |
| `src/crs/commands/port_forward.rs`             | port-forward                                                 | `run`                            | listener cleanup                                       |
| `src/crs/commands/doctor.rs`                   | doctor 聚合检查                                              | `run`                            | CRI 可用但 diagnostics 不可用时输出 CRI 摘要和 warning |
| `src/crs/commands/shortcuts.rs`                | inspect/stop/rm 类型解析和快捷命令分发                       | shortcut handlers                | 歧义对象提示 `--type`                                  |
| `src/crs/commands/events.rs`                   | event stream                                                 | `run`                            | EOF / SIGINT                                           |
| `src/crs/commands/stats.rs`                    | stats 汇总                                                   | `run`                            | filter                                                 |
| `src/crs/commands/metrics.rs`                  | descriptors / scrape                                         | handlers                         | text/json                                              |
| `src/crs/commands/recovery.rs`                 | recovery status/check/repair                                 | handlers                         | dry-run/execute                                        |
| `src/crs/commands/gc.rs`                       | GC candidates/run                                            | handlers                         | dry-run/execute                                        |
| `src/crs/commands/debug.rs`                    | debug 聚合                                                   | handlers                         | 每个 debug kind                                        |
| `src/crs/commands/completion.rs`               | shell completion                                             | `run`                            | shell enum                                             |
| `proto/crius/diagnostics/v1/diagnostics.proto` | diagnostics API                                              | protobuf package                 | tonic codegen                                          |
| `src/services/mod.rs`                          | 新增 `pub mod diagnostics;` 并导出 `DiagnosticsServiceImpl`  | `DiagnosticsServiceImpl`         | services 模块编译通过                                  |
| `src/services/diagnostics.rs`                  | diagnostics gRPC server 实现、daemon 到 diagnostics response 的模型转换、脱敏 helper；复用 `InternalServices`、`IntrospectionService`、`HealthService`、`EventService` | `DiagnosticsServiceImpl`         | 每个 RPC                                               |
| `src/main.rs`                                  | 注册 diagnostics server                                      | 无新增 public API                | daemon 启动包含服务                                    |

### Cargo 与 Build 精确改动

`Cargo.toml` 新增：

```toml
[[bin]]
name = "crs"
path = "src/crs/main.rs"
```

streaming client 的终端 raw mode 使用当前已有 `nix` 依赖实现。新增第三方 terminal crate 不属于本计划。

`build.rs` 改动目标：

```rust
let cri_protos = &[
    "proto/k8s.io/cri-api/pkg/apis/runtime/v1/api.proto",
    "proto/crius/diagnostics/v1/diagnostics.proto",
];

tonic_build::configure()
    .build_server(true)
    .build_client(true)
    .out_dir(&out_dir)
    .compile_with_config(config, cri_protos, &["proto"])?;

println!("cargo:rerun-if-changed=proto/crius/diagnostics/v1/diagnostics.proto");
```

diagnostics proto 的 package 固定为 `diagnostics.v1`，生成文件固定为 `diagnostics.v1.rs`。`src/proto/mod.rs` 对 crate 内部暴露的模块路径固定为：

```rust
crate::proto::diagnostics::v1
```

### Clap 类型骨架

`args.rs` 采用一层顶级 command 加命令组 enum，不把业务逻辑写进 clap 类型：

```rust
#[derive(clap::Parser)]
pub struct Args {
    #[arg(long, env = "CRIUS_ADDRESS", default_value = "unix:///run/crius/crius.sock")]
    pub address: String,
    #[arg(long, default_value = "5s", value_parser = parse_duration_arg)]
    pub connect_timeout: Duration,
    #[arg(long, default_value = "30s", value_parser = parse_duration_arg)]
    pub timeout: Duration,
    #[arg(long)]
    pub debug: bool,
    #[arg(long, value_enum, default_value_t = OutputArg::Table)]
    pub output: OutputArg,
    #[arg(long)]
    pub quiet: bool,
    #[arg(long)]
    pub no_trunc: bool,
    #[command(subcommand)]
    pub command: Command,
}

pub enum Command {
    Version(VersionArgs),
    Status(StatusArgs),
    Doctor(DoctorArgs),
    Ps(ContainerListArgs),
    Pods(PodListArgs),
    Images(ImageListArgs),
    Pull(ImagePullArgs),
    Inspect(InspectArgs),
    Logs(ContainerLogsArgs),
    Exec(ContainerExecArgs),
    Stop(StopArgs),
    Rm(RemoveArgs),
    Config(ConfigCommand),
    Runtime(RuntimeCommand),
    Image(ImageCommand),
    Pod(PodCommand),
    Container(ContainerCommand),
    Run(RunArgs),
    Events(EventsArgs),
    Stats(StatsArgs),
    Metrics(MetricsCommand),
    Recovery(RecoveryCommand),
    Gc(GcCommand),
    Debug(DebugCommand),
    Completion(CompletionArgs),
}
```

命令组 enum 需要与“命令语法”章节一一对应。每个 args struct 只保存原始输入或已由 clap value parser 转换的标量；CRI proto 结构在 `commands/*` 或 request builder 中创建。

### Args Struct 清单

| Struct                   | 字段                                                         |
| ------------------------ | ------------------------------------------------------------ |
| `VersionArgs`            | `output` 继承全局                                            |
| `StatusArgs`             | `verbose: bool`                                              |
| `DoctorArgs`             | `output` 继承全局                                            |
| `InspectArgs`            | `target`、`object_type: Option<ObjectType>`                  |
| `StopArgs`               | `target`、`object_type: Option<StopObjectType>`、`timeout`   |
| `RemoveArgs`             | `target`、`object_type: Option<ObjectType>`、`force`         |
| `ConfigCommand`          | `Show(ConfigShowArgs)`、`ReloadStatus(ConfigReloadStatusArgs)` |
| `RuntimeCommand`         | `Config(RuntimeConfigArgs)`、`Update(RuntimeUpdateArgs)`、`Handlers(RuntimeHandlersArgs)` |
| `RuntimeUpdateArgs`      | `pod_cidr: String`                                           |
| `ImageCommand`           | `List`、`Pull`、`Inspect`、`Remove`、`FsInfo`、`Transfers`、`Config` |
| `ImageListArgs`          | `image: Option<String>`                                      |
| `ImagePullArgs`          | `image: String`、`auth: ImageAuthArgs`、`pod: Option<String>` |
| `ImageInspectArgs`       | `image: String`                                              |
| `ImageRemoveArgs`        | `image: String`                                              |
| `PodCommand`             | `List`、`Inspect`、`Run`、`Stop`、`Remove`、`Stats`、`Metrics`、`UpdateResources`、`PortForward` |
| `PodListArgs`            | `id`、`state`、`labels`、`all`                               |
| `PodRunArgs`             | `PodCreateArgs`                                              |
| `PodStopArgs`            | `pod`、`timeout`                                             |
| `PodUpdateResourcesArgs` | `pod`、`overhead`、`pod_resource`                            |
| `PodPortForwardArgs`     | `pod`、`address`、`protocol`                                 |
| `ContainerCommand`       | `List`、`Inspect`、`Create`、`Start`、`Stop`、`Remove`、`Exec`、`ExecSync`、`Attach`、`Stats`、`Checkpoint`、`Update`、`ReopenLog`、`Logs` |
| `ContainerCreateArgs`    | `pod`、`image`、`command`、`ContainerCreateOptions`          |
| `ContainerExecArgs`      | `id`、`stream`、`command`                                    |
| `ContainerExecSyncArgs`  | `id`、`timeout`、`command`                                   |
| `ContainerLogsArgs`      | `id`、`follow`、`tail`、`since`、`timestamps`                |
| `RunArgs`                | `image`、`command`、`pull`、`detach`、`rm`、`pod`、`PodCreateArgs`、`ContainerCreateOptions`、`StreamOptions` |
| `RecoveryCommand`        | `Status`、`Check`、`Repair`                                  |
| `GcCommand`              | `Candidates`、`Run`                                          |
| `DebugCommand`           | `Network`、`Runtime`、`Shims`、`Nri`、`Security`、`Cgroups`、`Streaming`、`Metrics`、`Tracing`、`Rootless` |

共享 option 使用嵌套 struct：

- `ImageAuthArgs`
- `PodCreateArgs`
- `SandboxSecurityArgs`
- `ContainerCreateOptions`
- `ContainerResourceArgs`
- `ContainerSecurityArgs`
- `StreamOptions`
- `FilterArgs`

共享 struct 的字段通过 `#[command(flatten)]` 复用，避免同一 flag 在多个命令里重复定义。

### 核心类型

CLI 层使用应用型错误处理，但错误值需要保留结构化信息，方便退出码和 JSON stderr 输出：

```rust
pub struct CliContext {
    pub endpoint: Endpoint,
    pub connect_timeout: Duration,
    pub rpc_timeout: Duration,
    pub output: OutputMode,
    pub quiet: bool,
    pub no_trunc: bool,
    pub debug: bool,
}

pub enum Endpoint {
    Unix(PathBuf),
    Tcp(Uri),
}

pub enum OutputMode {
    Table,
    Json,
    Text,
}

pub struct CommandOutput<T> {
    pub kind: &'static str,
    pub items: Vec<T>,
    pub summary: Option<serde_json::Value>,
    pub warnings: Vec<String>,
}

pub enum CommandResult {
    Rendered { exit_code: i32 },
    ContainerExit { code: i32 },
}

pub enum CliError {
    Usage { message: String },
    Rpc {
        command: &'static str,
        endpoint: String,
        object: Option<String>,
        code: tonic::Code,
        message: String,
    },
    Io { action: &'static str, source: std::io::Error },
    Json { action: &'static str, source: serde_json::Error },
    Streaming { message: String },
    Interrupted,
}
```

`CliError` 使用当前已有 `thiserror` 依赖实现，在顶层转换为退出码。命令 handler 的统一签名：

```rust
pub async fn run(
    ctx: &CliContext,
    client: &mut CrsClient,
    args: CommandArgs,
) -> Result<CommandResult, CliError>;
```

格式化函数只接受已转换好的展示模型，不直接依赖 tonic response；这样命令 handler 可以清楚地区分“daemon response 到展示模型”的转换和“展示模型到 stdout”的输出。

### Build 与 Proto 接入

新增 diagnostics proto 后需要同步更新以下位置：

```text
proto/crius/diagnostics/v1/diagnostics.proto
build.rs
src/proto/mod.rs
src/services/mod.rs
src/services/diagnostics.rs
src/main.rs
```

`build.rs` 需要增加 `cargo:rerun-if-changed=proto/crius/diagnostics/v1/diagnostics.proto`，并用 `tonic_build` 生成 diagnostics service。生成模块在 `src/proto/mod.rs` 中导出，例如：

```rust
pub mod diagnostics {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/diagnostics.v1.rs"));
    }
}
```

daemon 启动时把 `DiagnosticsService` 注册到与 CRI RuntimeService / ImageService 相同的 tonic router 和同一 Unix listener。CLI 建连时总是先创建 CRI runtime/image client；只有执行诊断命令时才要求 diagnostics client 可用。诊断接口不存在时，命令返回清晰错误：

```text
diagnostics service is not available from this crius daemon
```

### 命令到 Handler 拆分

顶层分发保持一层 match，每个分支只调用对应模块：

| Subcommand                                                   | Handler 文件               | 主要调用                                                     |
| ------------------------------------------------------------ | -------------------------- | ------------------------------------------------------------ |
| `version`                                                    | `commands/version.rs`      | `RuntimeService.Version`                                     |
| `status`                                                     | `commands/status.rs`       | `RuntimeService.Status`                                      |
| `doctor`                                                     | `commands/doctor.rs`       | `Version`、`Status`、`RuntimeConfig`、diagnostics            |
| `ps` / `pods` / `images` / `pull`                            | 对应底层 command handler   | 共享 list / pull 实现                                        |
| `inspect` / `stop` / `rm`                                    | `commands/shortcuts.rs`    | 类型解析后调用底层 handler                                   |
| `logs` / `exec`                                              | 对应底层 command handler   | 共享 logs / exec 实现                                        |
| `config show` / `reload-status`                              | `commands/config.rs`       | diagnostics `EffectiveConfig`；diagnostics 不可用时读取 `Status(verbose=true).info["config"]` |
| `runtime config/update/handlers`                             | `commands/runtime.rs`      | `RuntimeConfig`、`UpdateRuntimeConfig`、diagnostics `RuntimeHandlers` |
| `image list/pull/inspect/remove/fs-info`                     | `commands/image.rs`        | ImageService 对应 RPC                                        |
| `image transfers/config`                                     | `commands/image.rs`        | `image transfers` 使用 diagnostics `ImageTransfers`，diagnostics 不可用时读取 `Status(verbose=true).info["imageTransfers"]`；`image config` 使用 diagnostics `EffectiveConfig`，diagnostics 不可用时读取 `Status(verbose=true).info["config"].image` |
| `pod list/inspect/run/stop/remove/stats/metrics/update-resources` | `commands/pod.rs`          | RuntimeService 对应 RPC                                      |
| `pod port-forward`                                           | `commands/port_forward.rs` | `PortForward` + `streaming`                                  |
| `container list/inspect/create/start/stop/remove/stats/checkpoint/update/reopen-log` | `commands/container.rs`    | RuntimeService 对应 RPC                                      |
| `container exec` / `exec-sync`                               | `commands/exec.rs`         | `Exec` / `ExecSync` + `streaming`                            |
| `container attach`                                           | `commands/attach.rs`       | `Attach` + `streaming`                                       |
| `container logs`                                             | `commands/logs.rs`         | diagnostics `ContainerLog`                                   |
| `run`                                                        | `commands/run.rs`          | ImageService + RuntimeService + streaming                    |
| `events`                                                     | `commands/events.rs`       | `GetContainerEvents` stream                                  |
| `stats`                                                      | `commands/stats.rs`        | `ListContainerStats`                                         |
| `metrics descriptors/scrape`                                 | `commands/metrics.rs`      | CRI metrics RPC、metrics endpoint                            |
| `recovery *`                                                 | `commands/recovery.rs`     | diagnostics `RecoveryStatus` / `RecoveryCheck`               |
| `gc *`                                                       | `commands/gc.rs`           | diagnostics `ContentGc`                                      |
| `debug network/runtime/cgroups`                              | `commands/debug.rs`        | `Status(verbose=true)`、`RuntimeConfig`、diagnostics         |
| `debug shims/nri/security/streaming/metrics/tracing/rootless` | `commands/debug.rs`        | diagnostics                                                  |

### 命令执行流程

`image pull`：

1. 解析 image reference、auth 来源和 `--pod` sandbox 上下文。
2. 如果传入 `--pod`，调用 `PodSandboxStatus` 获取 sandbox config 相关上下文。
3. 构造 `PullImageRequest`。
4. 输出 image ref 或 JSON envelope。
5. auth 信息只在内存中传递，不写入 debug 日志。

`pod run`：

1. 从 flags 构造 `PodSandboxConfig`。
2. 调用 `RunPodSandbox`。
3. 成功时输出 sandbox ID。
4. 失败时只输出 daemon 错误，不尝试自行清理 daemon 未确认创建的对象。

`container create`：

1. 解析 POD、IMAGE、command/args、mount、resource、security、devices。
2. 调用 `PodSandboxStatus` 校验 sandbox 存在，并获得必要上下文。
3. 构造 `ContainerConfig` 和 `CreateContainerRequest`。
4. 成功时输出 container ID。

`container exec`：

1. 解析 command、TTY、stdin/stdout/stderr、protocol。
2. 调用 CRI `Exec` 获取 streaming URL。
3. 交给 `streaming::exec` 处理 IO、raw mode、resize。
4. 命令退出时恢复终端状态。
5. streaming 协议提供远端退出码时返回远端退出码；协议未提供远端退出码时，连接成功且正常关闭返回 0。

`container exec-sync`：

1. 调用 CRI `ExecSync`。
2. stdout 写 stdout，stderr 写 stderr。
3. 返回 response exit code。
4. `--output json` 时 stdout/stderr 以 base64 或 UTF-8 字段进入 JSON，实际 stderr 不混入普通命令输出。

`container attach`：

1. 调用 CRI `Attach` 获取 URL。
2. streaming 层接管 IO。
3. Ctrl-C 只断开 attach 会话；是否停止容器由 daemon / runtime 状态决定，CLI 不隐式 stop。

`pod port-forward`：

1. 解析一个或多个 `LOCAL:REMOTE`。
2. 调用 CRI `PortForward`，remote ports 写入 request。
3. 本地创建 listener。
4. 每个连接通过 streaming URL 建立 data stream。
5. 任一 listener 绑定失败时整体失败，并关闭已创建 listener。

`container logs`：

1. 解析 `--follow`、`--tail`、`--since`、`--timestamps`。
2. 调用 diagnostics `ContainerLog`。
3. text 模式按 daemon 返回的 chunk 写出；JSON 模式按 chunk 输出结构化对象。
4. 路径解析、日志文件轮转和路径逃逸校验由 daemon 完成。

`run`：

1. 根据 `--pull` 判断是否调用 `ImageStatus` / `PullImage`。
2. 未传 `--pod` 时创建临时 Pod sandbox。
3. 构造并创建 container。
4. 调用 `StartContainer`。
5. `--detach` 时输出 container ID 后返回。
6. 前台模式按 `--exec-mode` 使用 `ExecSync` 或 `Attach`。
7. `--rm` 时按 container stop/remove、pod stop/remove 顺序清理 CLI 创建的对象。
8. 失败时输出已创建的 pod/container ID 和建议清理命令。

`events`：

1. 调用 `GetContainerEvents`。
2. table 模式持续输出行。
3. JSON 模式可输出 newline-delimited JSON，保持 stream 可消费。
4. SIGINT 返回 130，正常 EOF 返回 0。

`gc run` 和 `recovery repair`：

1. clap 层要求 `--dry-run` 与 `--execute` 二选一。
2. request 中使用 `execute: bool` 表达是否执行。
3. daemon 返回每个候选或动作的 skipped / executed / error。
4. CLI 只展示结果，不自行删除或修改任何状态文件。

### Request 构造函数

复杂命令不要在 handler 中直接堆字段，拆成可测试的纯函数：

```rust
fn build_pod_sandbox_config(args: &PodCreateArgs) -> Result<PodSandboxConfig, CliError>;
fn build_container_config(args: &ContainerCreateArgs) -> Result<ContainerConfig, CliError>;
fn build_linux_container_resources(args: &ResourceArgs) -> Result<LinuxContainerResources, CliError>;
fn build_linux_sandbox_security_context(args: &SandboxSecurityArgs) -> Result<LinuxSandboxSecurityContext, CliError>;
fn build_linux_container_security_context(args: &ContainerSecurityArgs) -> Result<LinuxContainerSecurityContext, CliError>;
fn build_auth_config(args: &ImageAuthArgs) -> Result<Option<AuthConfig>, CliError>;
```

这些函数只做参数到 CRI 结构的转换，不执行 RPC。单元测试应覆盖默认值、边界值和互斥参数。

### Parser 函数清单

| 函数                     | 输入示例                                                     | 输出                               |
| ------------------------ | ------------------------------------------------------------ | ---------------------------------- |
| `parse_endpoint`         | `unix:///run/crius/crius.sock`、`/run/crius/crius.sock`、`https://127.0.0.1:9443` | `Endpoint`                         |
| `parse_duration`         | `500ms`、`5s`、`2m`                                          | `Duration`                         |
| `parse_byte_size`        | `1024`、`64MiB`、`1GiB`                                      | `i64` 或 `u64`                     |
| `parse_key_value`        | `app=demo`                                                   | `(String, String)`                 |
| `parse_env_file`         | env 文件路径                                                 | `Vec<KeyValue>`                    |
| `parse_auth_json`        | registry auth JSON / CRI auth JSON                           | `AuthConfig`                       |
| `parse_port_mapping`     | `8080:80/tcp`                                                | `PortMapping`                      |
| `parse_mount`            | `type=bind,src=/x,dst=/y,options=ro:rbind`                   | `Mount`                            |
| `parse_device`           | `/dev/fuse:/dev/fuse:rwm`                                    | `Device`                           |
| `parse_resource_spec`    | `cpu-quota=50000,memory=256MiB`                              | `LinuxContainerResources` fragment |
| `parse_hugepage`         | `2Mi=1GiB`                                                   | `HugepageLimit`                    |
| `parse_security_profile` | `runtime/default`、`localhost:foo.json`                      | `SecurityProfile`                  |
| `parse_selinux`          | `user:role:type:level`                                       | `SELinuxOption`                    |
| `parse_user`             | `1000:1000`、`nobody`                                        | user / group enum                  |
| `parse_id_mapping`       | `0:100000:65536`                                             | `IDMapping`                        |
| `parse_since`            | `2026-05-31T10:00:00Z`、`10m`                                | Unix nanos                         |

Parser 返回的错误必须包含原始输入片段和期望格式，例如：

```text
invalid --mount value "type=bind,dst=/data": missing src/source for bind mount
```

### Formatter 接口

表格输出使用简单 trait，避免每个命令手写列宽逻辑：

```rust
pub struct FormatOptions {
    pub no_trunc: bool,
    pub quiet: bool,
    pub local_timezone: bool,
}

pub trait TableRow {
    fn headers() -> &'static [&'static str];
    fn row(&self, opts: &FormatOptions) -> Vec<String>;
}

pub fn print_table<T: TableRow>(items: &[T], opts: &FormatOptions) -> Result<(), CliError>;
pub fn print_envelope<T: Serialize>(
    ctx: &CliContext,
    output: CommandOutput<T>,
) -> Result<(), CliError>;
```

formatter 只写 stdout。debug 日志和错误进入 stderr。`--quiet` 下不输出表头。

### Streaming 实现边界

`streaming.rs` 对外提供三个入口：

```rust
pub async fn exec(ctx: &CliContext, url: &str, opts: ExecStreamOptions) -> Result<i32, CliError>;
pub async fn attach(ctx: &CliContext, url: &str, opts: AttachStreamOptions) -> Result<(), CliError>;
pub async fn port_forward(ctx: &CliContext, url: &str, opts: PortForwardOptions) -> Result<(), CliError>;
```

实现要求：

- raw mode 使用 RAII guard，任何错误路径都恢复终端。
- resize 监听与主 IO 任务通过 channel 通信。
- stdout、stderr、error stream 明确分流。
- port-forward 每个本地连接独立 task，父任务负责 shutdown。
- TLS 和 token 只从 daemon 返回的 URL / endpoint 配置中读取，CLI 不拼接私有 token。

### Daemon 侧 diagnostics 实现

diagnostics server 是 gRPC 适配层，从 daemon 已有服务和状态对象读取信息，不重新实现 runtime 编排逻辑。当前项目已经有 `InternalServices`，其中包含 `EventService`、`HealthService`、`IntrospectionService`；diagnostics RPC 复用这些服务的输出，再补齐 gRPC response 模型。

| diagnostics RPC   | daemon 侧数据来源                                            |
| ----------------- | ------------------------------------------------------------ |
| `ServerInfo`      | 启动配置、版本信息、socket 配置、`RuntimeServiceImpl` 暴露的只读摘要 |
| `EffectiveConfig` | 已解析配置对象；复用 `IntrospectionService` 的配置摘要，序列化时默认脱敏 |
| `RuntimeHandlers` | runtime 配置、runc backend 探测结果；复用 `IntrospectionService::runtime_backend` |
| `ImageTransfers`  | image service 的传输管理器和最近错误缓存；复用 `IntrospectionService::image_transfers` |
| `RecoveryStatus`  | recovery ledger、启动恢复结果；复用 `HealthService` 和 `IntrospectionService` 的恢复摘要 helper |
| `RecoveryCheck`   | recovery 模块提供的 dry-run / execute API                    |
| `NriStatus`       | NRI 初始化状态、CDI 目录、blockio/RDT 配置；复用 `IntrospectionService` 已有 NRI / security 摘要 |
| `SecurityStatus`  | seccomp/AppArmor/SELinux/rootless/cgroup 探测结果；复用 `HealthService` 和 security/introspection 输出 |
| `ShimStatus`      | runtime task/shim registry、`EventService` 最近 shim 事件    |
| `ContentGc`       | content store / snapshot store 的 GC planner；复用 `IntrospectionService::content_gc` |
| `ContainerLog`    | container status 中记录的 log path，经 daemon 校验后读取     |

daemon 侧统一做权限、路径和状态校验。尤其是 `ContainerLog`、`ContentGc`、`RecoveryCheck(execute=true)` 不接受 CLI 传来的宿主路径作为最终操作对象。

### 测试文件布局

测试布局：

```text
src/crs/parsers.rs          # parser 单元测试放在同文件 tests 模块
src/crs/format.rs           # table/json/quiet 单元测试
src/crs/error.rs            # exit code 和 status mapping 单元测试
tests/crs_args.rs           # clap 解析、help、互斥参数
tests/crs_commands.rs       # 使用 mock tonic service 的命令级测试
tests/crs_shortcuts.rs      # 快捷命令与底层命令等价性、歧义处理
tests/crs_streaming.rs      # streaming 协议和 TTY 行为测试
tests/diagnostics.rs        # diagnostics server/client round trip
```

测试需要至少覆盖以下断言：

- 每个公开命令能被 clap 解析。
- 每个复杂 flag 至少有一个成功样例和一个失败样例。
- `--output json` stdout 必须被 `serde_json` 成功解析。
- `--quiet` 不输出表头。
- gRPC `NotFound`、`PermissionDenied`、`Unavailable`、`DeadlineExceeded` 映射到预期退出码。
- `run --rm` 只清理 CLI 本次创建的对象。
- diagnostics `--execute` 类命令没有显式 `--execute` 时不会修改状态。

### CRI 请求构造

`pod run` 和 `run` 需要构造 `PodSandboxConfig`：

- metadata: name、namespace、uid、attempt。
- hostname。
- dns_config。
- labels / annotations。
- log_directory。
- namespace options，例如 host network。
- runtime_handler。
- port mappings，用于 hostPort 验证。
- cgroup parent、sysctls、overhead、resources。
- Linux sandbox security context，用于 userns、seccomp、SELinux、AppArmor 等配置。

`container create` 和 `run` 需要构造 `ContainerConfig`：

- image -> `ImageSpec`。
- command / args。
- env -> `KeyValue`。
- labels / annotations。
- mounts -> CRI `Mount`。
- devices -> CRI `Device`。
- log_path。
- TTY / stdin。
- resources -> `LinuxContainerResources`。
- security context -> privileged、capabilities、seccomp、AppArmor、SELinux、devices、proc mount、read-only rootfs。
- CDI devices、blockio class、RDT class 等通过 CRI 字段或 annotations 传递，由 daemon 按项目规则校验和落地。

### PodSandboxConfig 字段映射

| CRI 字段                                                     | 来源 flag / 默认值                            | 处理规则                                                    |
| ------------------------------------------------------------ | --------------------------------------------- | ----------------------------------------------------------- |
| `metadata.name`                                              | `--name`；`run` 默认 `crius-<short-id>`       | 必填；空字符串参数错误                                      |
| `metadata.uid`                                               | `--uid`；默认生成 UUID 字符串                 | CLI 只保证非空                                              |
| `metadata.namespace`                                         | `--namespace`；默认 `default`                 | 必填                                                        |
| `metadata.attempt`                                           | `--attempt`；默认 `0`                         | `u32`，非负                                                 |
| `hostname`                                                   | `--hostname`；默认空                          | 空表示 daemon 默认                                          |
| `log_directory`                                              | `--log-dir`；默认空                           | 相对/绝对合法性由 daemon 校验                               |
| `dns_config.servers`                                         | repeated `--dns-server`                       | IP 格式 CLI 校验                                            |
| `dns_config.searches`                                        | repeated `--dns-search`                       | 非空字符串                                                  |
| `dns_config.options`                                         | repeated `--dns-option`                       | 非空字符串                                                  |
| `port_mappings`                                              | repeated `--publish` + 当前 `--host-ip`       | host/container 端口 1-65535；协议默认 tcp                   |
| `labels`                                                     | repeated `--label`                            | 后出现 key 覆盖前者                                         |
| `annotations`                                                | repeated `--annotation`                       | 后出现 key 覆盖前者                                         |
| `linux.cgroup_parent`                                        | `--cgroup-parent`                             | 原样传 daemon                                               |
| `linux.security_context.namespace_options.network`           | `--host-network`                              | true -> NODE；默认 POD                                      |
| `linux.security_context.namespace_options.pid`               | `--host-pid`                                  | true -> NODE；默认 CONTAINER/POD 语义由 CRI 类型决定        |
| `linux.security_context.namespace_options.ipc`               | `--host-ipc`                                  | true -> NODE；默认 POD                                      |
| `linux.security_context.namespace_options.userns_options.mode` | `--userns pod|node`                           | pod -> POD；node -> NODE                                    |
| `linux.security_context.namespace_options.userns_options.uids` | repeated `--uid-map`                          | `host:container:length`                                     |
| `linux.security_context.namespace_options.userns_options.gids` | repeated `--gid-map`                          | `host:container:length`                                     |
| `linux.security_context.run_as_user`                         | `--sandbox-user UID[:GID]`                    | UID 数字写入                                                |
| `linux.security_context.run_as_group`                        | `--sandbox-user UID:GID` 或 `--sandbox-group` | group 依赖 user                                             |
| `linux.security_context.supplemental_groups`                 | repeated `--sandbox-supplemental-group`       | 数字                                                        |
| `linux.security_context.readonly_rootfs`                     | `--sandbox-readonly-rootfs`                   | bool                                                        |
| `linux.security_context.privileged`                          | `--sandbox-privileged`                        | bool                                                        |
| `linux.security_context.seccomp`                             | `--sandbox-seccomp`                           | security profile parser                                     |
| `linux.security_context.apparmor`                            | `--sandbox-apparmor`                          | security profile parser                                     |
| `linux.security_context.selinux_options`                     | `--sandbox-selinux`                           | SELinux parser                                              |
| `linux.sysctls`                                              | repeated `--sysctl`                           | map，后者覆盖                                               |
| `linux.overhead`                                             | `--overhead`                                  | resource parser                                             |
| `linux.resources`                                            | `--pod-resource`                              | resource parser                                             |
| `runtime_handler`                                            | `--runtime-handler`                           | 写入 `RunPodSandboxRequest.runtime_handler`，不在 config 内 |

### ContainerConfig 字段映射

| CRI 字段                                                     | 来源 flag / 默认值                                  | 处理规则                                        |
| ------------------------------------------------------------ | --------------------------------------------------- | ----------------------------------------------- |
| `metadata.name`                                              | `--name`；默认 `crius-<short-id>` 或 image basename | 必填                                            |
| `metadata.attempt`                                           | `--attempt`；默认 `0`                               | `u32`                                           |
| `image.image`                                                | 位置参数 `IMAGE` 或 `--image`                       | 必填                                            |
| `command`                                                    | repeated `--command`                                | 若未指定，保持空                                |
| `args`                                                       | `--arg` 和 `--` 后参数                              | 保留顺序                                        |
| `working_dir`                                                | `--workdir`                                         | 原样传 daemon                                   |
| `envs`                                                       | repeated `--env` + `--env-file`                     | `--env` 覆盖 env-file 同 key；保持最终 key 唯一 |
| `mounts`                                                     | repeated `--mount`                                  | mount parser                                    |
| `devices`                                                    | repeated `--device`                                 | device parser                                   |
| `cdi_devices`                                                | repeated `--cdi-device`                             | name 非空                                       |
| `labels`                                                     | repeated `--label`                                  | 后者覆盖                                        |
| `annotations`                                                | repeated `--annotation`                             | 后者覆盖；blockio/rdt 可写入项目约定 annotation |
| `log_path`                                                   | `--log-path`                                        | 相对 pod log dir；安全校验由 daemon             |
| `stdin`                                                      | `--stdin`                                           | bool                                            |
| `stdin_once`                                                 | 默认 false                                          | 当前不暴露独立 flag                             |
| `tty`                                                        | `--tty`                                             | bool                                            |
| `linux.resources`                                            | resource flags 或 `--resource`                      | 合并多个 fragment                               |
| `linux.security_context.privileged`                          | `--privileged`                                      | bool                                            |
| `linux.security_context.capabilities.add_capabilities`       | repeated `--cap-add`                                | 保留顺序，daemon 归一化                         |
| `linux.security_context.capabilities.drop_capabilities`      | repeated `--cap-drop`                               | 保留顺序                                        |
| `linux.security_context.capabilities.add_ambient_capabilities` | repeated `--ambient-cap-add`                        | 保留顺序                                        |
| `linux.security_context.run_as_user`                         | `--user UID[:GID]`                                  | 数字 UID                                        |
| `linux.security_context.run_as_username`                     | `--user NAME`                                       | 非数字用户名                                    |
| `linux.security_context.run_as_group`                        | `--user UID:GID` 或 `--group`                       | group 依赖 user                                 |
| `linux.security_context.supplemental_groups`                 | repeated `--supplemental-group`                     | 数字                                            |
| `linux.security_context.readonly_rootfs`                     | `--readonly-rootfs`                                 | bool                                            |
| `linux.security_context.no_new_privs`                        | `--no-new-privs`                                    | bool                                            |
| `linux.security_context.masked_paths`                        | repeated `--masked-path`                            | 非空                                            |
| `linux.security_context.readonly_paths`                      | repeated `--readonly-path`                          | 非空                                            |
| `linux.security_context.seccomp`                             | `--seccomp`                                         | security profile parser                         |
| `linux.security_context.apparmor`                            | `--apparmor`                                        | security profile parser                         |
| `linux.security_context.selinux_options`                     | `--selinux`                                         | SELinux parser                                  |
| `linux.security_context.namespace_options.pid`               | `--pid`                                             | pod/container/node/target                       |
| `linux.security_context.namespace_options.ipc`               | `--ipc`                                             | pod/node                                        |

### LinuxContainerResources 字段映射

| 字段                         | flag / resource key                 | 类型     | 校验                           |
| ---------------------------- | ----------------------------------- | -------- | ------------------------------ |
| `cpu_period`                 | `--cpu-period` / `cpu-period`       | i64      | `>= 0`                         |
| `cpu_quota`                  | `--cpu-quota` / `cpu-quota`         | i64      | 可为 `0`；负数参数错误         |
| `cpu_shares`                 | `--cpu-shares` / `cpu-shares`       | i64      | `>= 0`                         |
| `memory_limit_in_bytes`      | `--memory` / `memory`               | i64      | `>= 0`，支持 byte suffix       |
| `memory_swap_limit_in_bytes` | `--memory-swap` / `memory-swap`     | i64      | `>= 0`                         |
| `oom_score_adj`              | `--oom-score-adj` / `oom-score-adj` | i64      | CRI 允许范围由 daemon 最终校验 |
| `cpuset_cpus`                | `--cpuset-cpus` / `cpuset-cpus`     | string   | 非空                           |
| `cpuset_mems`                | `--cpuset-mems` / `cpuset-mems`     | string   | 非空                           |
| `hugepage_limits`            | `--hugepage` / `hugepage.SIZE`      | repeated | size 和 limit 非空             |
| `unified`                    | `--unified` / `unified.KEY`         | map      | key 非空                       |

多个 resource fragment 合并时，标量字段后者覆盖前者；hugepage 同 size 后者覆盖前者；unified 同 key 后者覆盖前者。

### Mount 字段映射

| Mount 字段            | bind mount 来源           | image mount 来源                                      | 校验                                     |
| --------------------- | ------------------------- | ----------------------------------------------------- | ---------------------------------------- |
| `host_path`           | `src` / `source`          | 禁用                                                  | bind 必填                                |
| `container_path`      | `dst` / `target`          | `dst` / `target`                                      | 必填                                     |
| `readonly`            | `options=ro|rw`           | `options=ro|rw`                                       | 默认 false                               |
| `selinux_relabel`     | `options=z|Z`             | 不适用                                                | bool                                     |
| `propagation`         | `rprivate|rslave|rshared` | 不适用                                                | 默认 private / CRI 默认                  |
| `image`               | 禁用                      | `image=IMAGE_REF`                                     | image mount 必填                         |
| `image_sub_path`      | 禁用                      | 未传 `subpath` 时为空；传入 `subpath=PATH` 时使用该值 | image mount 专用                         |
| `recursive_read_only` | `options=recursive-ro`    | 不适用                                                | 需要同时 readonly 且 private propagation |
| `uid_mappings`        | `uidmap=...`              | `uidmap=...`                                          | ID mapping parser                        |
| `gid_mappings`        | `gidmap=...`              | `gidmap=...`                                          | ID mapping parser                        |

### AuthConfig 字段映射

| 来源               | 字段                                        | 处理规则                           |
| ------------------ | ------------------------------------------- | ---------------------------------- |
| `--auth-json`      | CRI `AuthConfig` JSON 或 registry auth JSON | 解析失败返回 2                     |
| `--auth-file`      | 同上                                        | 文件读取失败返回 1；解析失败返回 2 |
| `--username`       | `username`                                  | 与 auth-json/auth-file 互斥        |
| `--password`       | `password`                                  | stderr/debug 脱敏                  |
| `--server`         | `server_address`                            | 未传时保持 CRI 默认空字符串        |
| `--identity-token` | `identity_token`                            | 脱敏                               |
| `--registry-token` | `registry_token`                            | 脱敏                               |

auth 来源优先级不存在自动覆盖：多来源同时出现就是参数错误。

### runtime config

CLI 需要覆盖 `crius` runtime config 链路：

- `runtime config` 读取 CRI `RuntimeConfig`，展示 daemon 返回的 cgroup driver。
- `runtime update --pod-cidr CIDR` 调用 CRI `UpdateRuntimeConfig`，用于更新 runtime network config 和 CNI 模板渲染。
- `config reload-status` 从 verbose status 展示配置 watcher、reload backoff、最近错误和 CNI watch 状态。
- `debug network` 同时展示 runtime PodCIDR、CNI 模板、最近 CNI 加载状态和 network ready condition。

### streaming

CRI `Exec`、`Attach`、`PortForward` 返回 URL，CLI 作为 streaming client 连接该 URL。

需要覆盖：

- stdout / stderr 转发。
- stdin 转发。
- TTY raw mode。
- terminal resize。
- attach 会话关闭。
- port-forward 数据流与错误流。

### 错误和退出码

退出码映射：

| 情况                | 退出码         | 说明                                                         |
| ------------------- | -------------- | ------------------------------------------------------------ |
| 成功                | 0              | 命令完成                                                     |
| 一般错误            | 1              | 未归类错误                                                   |
| 参数错误            | 2              | clap 错误、flag 互斥、字段解析失败                           |
| not found           | 4              | gRPC `NotFound` 或 daemon 明确不存在                         |
| already exists      | 5              | gRPC `AlreadyExists`                                         |
| failed precondition | 6              | gRPC `FailedPrecondition`                                    |
| permission denied   | 13             | gRPC `PermissionDenied`                                      |
| deadline exceeded   | 124            | gRPC `DeadlineExceeded` 或本地 timeout                       |
| daemon unavailable  | 125            | endpoint 不可达、gRPC `Unavailable`                          |
| interrupted         | 130            | SIGINT / Ctrl-C                                              |
| 容器内命令退出      | 原始 exit code | `exec-sync` 和 `run --exec-mode sync` 返回容器进程退出码；attach 模式按 streaming 结果处理 |

错误输出应包含：

- 命令名。
- endpoint。
- 对象 ID 或镜像引用。
- gRPC code。
- daemon message。

JSON 模式下错误仍输出到 stderr，格式为：

```json
{
  "error": {
    "command": "container inspect",
    "endpoint": "unix:///run/crius/crius.sock",
    "code": "NotFound",
    "message": "container not found",
    "object": "abc"
  }
}
```

### Stdout / Stderr / Logging 规则

| 内容                                | stdout                       | stderr                     |
| ----------------------------------- | ---------------------------- | -------------------------- |
| 正常 table 输出                     | 是                           | 否                         |
| 正常 JSON 输出                      | 是                           | 否                         |
| `--quiet` 输出                      | 是                           | 否                         |
| debug 日志                          | 否                           | 是                         |
| warning，table/text 模式            | 否                           | 是                         |
| warning，JSON 模式                  | JSON envelope `warnings`     | 否，除非 envelope 无法生成 |
| 错误，人类模式                      | 否                           | 是                         |
| 错误，JSON 模式                     | 否                           | 是，单独 JSON error        |
| streaming stdout                    | 是                           | 否                         |
| streaming stderr                    | 否                           | 是                         |
| `exec-sync` stdout/stderr，人类模式 | stdout 写 stdout             | stderr 写 stderr           |
| `exec-sync --output json`           | JSON 包含 stdout/stderr 字段 | 只写 CLI 错误              |

`--output json` 下 stdout 需要满足：

- 非流式命令：单个完整 JSON object。
- `events` 这类无限 stream：newline-delimited JSON，每行一个 object。
- `container logs --output json`：newline-delimited JSON，每行一个 log chunk。
- tracing、warning、人类提示不混入 stdout。

### 脱敏规则

以下字段无论来自 flag、env、config、diagnostics response 还是 daemon error，都不得明文出现在 debug 日志或错误上下文里：

| 字段模式                          | 替换         |
| --------------------------------- | ------------ |
| `password`                        | `<redacted>` |
| `passwd`                          | `<redacted>` |
| `token`                           | `<redacted>` |
| `identity_token`                  | `<redacted>` |
| `registry_token`                  | `<redacted>` |
| `authorization`                   | `<redacted>` |
| `auth` 且值看起来像 base64 secret | `<redacted>` |
| `secret`                          | `<redacted>` |
| `private_key`                     | `<redacted>` |
| `client_key`                      | `<redacted>` |

CLI 脱敏函数放在 `src/crs/error.rs`。daemon diagnostics 侧实现独立脱敏。CLI 不能假设 daemon 已经完成脱敏。

### Signal 与取消

| 场景                       | 行为                                                         |
| -------------------------- | ------------------------------------------------------------ |
| 普通 RPC 收到 Ctrl-C       | 取消本地 future，返回 130                                    |
| `events` 收到 Ctrl-C       | 关闭 stream，返回 130                                        |
| `attach` 收到 Ctrl-C       | 断开 attach，会话返回 130；不 stop container                 |
| `exec` 收到 Ctrl-C         | 先恢复终端，再关闭 streaming；返回 130，除非远端已返回 exit code |
| `port-forward` 收到 Ctrl-C | 关闭 listener 和所有连接，返回 130                           |
| `run --rm` 前台收到 Ctrl-C | 先关闭 streaming，再执行 best-effort cleanup，最终返回 130   |

清理动作失败时不覆盖 130，只在 warning 中记录失败对象。

## 测试与验收

单元测试：

- endpoint 解析。
- env / label / annotation / mount / resource flag 解析。
- auth、security profile、SELinux、namespace、userns、resource、duration、byte size 解析。
- flag 互斥与依赖规则。
- table / json / quiet 输出。
- JSON envelope 和 inspect parsed info 输出。
- ID 截断。
- gRPC status 到 CLI 错误映射。
- diagnostics proto request/response 转换。

集成测试：

- 使用测试 server 或项目内 `RuntimeServiceImpl` 测试只读命令。
- `run`、`exec`、`attach` 使用现有 runtime_backend / shim_rpc 测试夹具。
- runtime config、recovery、GC、network、NRI、安全、metrics、tracing 诊断使用 `Status(verbose=true)` 现有测试数据。
- diagnostics 接口覆盖日志读取、shim 状态、GC dry-run / execute、recovery dry-run / execute。
- streaming 覆盖 websocket、SPDY、TTY resize、stdin EOF、stderr 分离和 port-forward data/error stream。
- destructive 命令覆盖 dry-run、execute、not found、failed precondition、permission denied。

发布门禁：

- `cargo build --features shim --bins` 生成 `crius`、`crius-shim` 和 `crs`。
- 未创建 CLI 配置文件时，`crs` 默认连接 `unix:///run/crius/crius.sock`。
- daemon 启动后，`crs version`、`crs status`、`crs image list` 能成功。
- daemon 未启动时，`crs status` 输出 endpoint、失败原因和可执行的下一步。
- `crs ps`、`crs pods`、`crs images`、`crs pull` 与对应底层命令输出一致。
- `crs inspect`、`crs stop`、`crs rm` 在对象类型歧义时提示 `--type`。
- `crs doctor` 在 diagnostics 不可用但 CRI 可用时返回 0，并输出 warning。
- 对不存在 ID 的 inspect/remove 输出明确 not found。
- 在具备 root 和 runtime 依赖的环境中，`crs run --rm "$CRIUS_TEST_IMAGE" echo ok` 能完成创建、启动、输出和清理。
- `container exec --tty --stdin` 在 Linux 终端下必须完成交互式执行并恢复终端状态。
- `pod port-forward` 必须把本地连接转发到目标 Pod。
- `runtime config` 能显示 cgroup driver，`runtime update --pod-cidr` 能反映到 verbose status 的 runtime network config。
- `debug nri`、`debug security`、`debug metrics`、`debug tracing` 能展示对应配置和健康摘要。
- `container logs` 的 daemon 侧路径校验需要覆盖路径逃逸场景。
- GC / recovery 修复命令支持 dry-run，并输出候选、跳过原因和执行统计。
- `--output json` 的 stdout 必须被 `serde_json` 成功解析，stderr 不混入普通日志。

### 命令验收清单

| 命令组               | 必测命令                                                     | 必测断言                                                     |
| -------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 基础信息             | `version`、`status`、`status --verbose`、`config show`       | daemon 可达时退出 0；JSON kind 正确；verbose info 解析失败时给 warning |
| 快捷命令             | `doctor`、`ps`、`pods`、`images`、`pull`、`inspect`、`logs`、`exec`、`stop`、`rm` | 与底层命令共享实现；无需 CLI 配置文件；歧义提示 `--type`     |
| runtime              | `runtime config`、`runtime update --pod-cidr`、`runtime handlers` | pod CIDR 语法错误返回 2；update 后 status 可见；handlers 为空也输出合法 JSON |
| image                | `image list`、`pull`、`inspect`、`remove`、`fs-info`         | pull auth 互斥；inspect 不存在返回 4；remove 被使用镜像返回 6 |
| pod                  | `pod list`、`inspect`、`run`、`stop`、`remove`、`stats`、`metrics`、`update-resources` | list filter 映射正确；run 输出 ID；remove 有运行容器时失败；resource parser 覆盖所有字段 |
| container            | `container list`、`inspect`、`create`、`start`、`stop`、`remove`、`stats`、`checkpoint`、`update`、`reopen-log` | create 先取 sandbox config；运行中 remove 返回 6；checkpoint location 必填；update 至少一个变更 |
| streaming            | `exec`、`exec-sync`、`attach`、`pod port-forward`            | TTY 时 stderr 关闭；raw mode 错误路径恢复；exec-sync 返回远端 exit code；port-forward listener 失败清理 |
| logs                 | `container logs`、`container logs --follow`、`--tail`、`--since` | tail/since 校验；daemon 做路径校验；JSON chunk 可逐条解析    |
| run                  | `run`、`run --rm`、`run --detach`、`run --pod`               | 只清理 CLI 创建对象；失败输出遗留 ID；detach 输出 container ID；pull policy 正确 |
| events/stats/metrics | `events`、`stats`、`metrics descriptors`、`metrics scrape`   | events 可中断；NDJSON 合法；metrics text 输出 daemon 返回内容；stats 空列表退出 0 |
| recovery/gc          | `recovery status`、`check`、`repair --dry-run/--execute`、`gc candidates`、`gc run --dry-run/--execute` | 未指定 dry-run/execute 返回 2；dry-run 不修改状态；execute 汇总失败数 |
| debug                | 所有 `debug *`                                               | table 为摘要；JSON 字段完整；diagnostics 不可用时仅 `debug cgroups` 继续使用 CRI `RuntimeConfig` 和 `Status(verbose=true)`，其他 diagnostics-only 命令返回 1 |

### Mock Server 场景

命令级集成测试使用 mock tonic server 覆盖以下响应：

- 成功响应，包含最小字段。
- 成功响应，包含 verbose info JSON。
- 成功响应，但 verbose info 是非法 JSON。
- `NotFound`、`AlreadyExists`、`FailedPrecondition`、`PermissionDenied`、`Unavailable`、`DeadlineExceeded`。
- streaming URL 返回后连接失败。
- diagnostics service 未注册。
- diagnostics service 返回 warning。
- server stream 正常 EOF。
- server stream 中途返回 error。

### Golden 输出

对稳定输出保留 golden 样例：

```text
tests/golden/version.table
tests/golden/status.table
tests/golden/image-list.table
tests/golden/pod-list.table
tests/golden/container-list.table
tests/golden/stats.table
tests/golden/debug-network.table
tests/golden/gc-candidates.json
tests/golden/recovery-check.json
```

golden 文件只覆盖列名、字段顺序、JSON kind 和必要 summary 字段；时间、ID、路径等不稳定字段在断言前归一化。

### 逐命令机械验收矩阵

以下命令用 mock server 执行。root/runtime 依赖场景另在手工验证中覆盖。`JSON OK` 表示 stdout 可被解析为单个 JSON object；`NDJSON OK` 表示 stdout 每行可独立解析。

退出码速查：

| 退出码 | 含义 | 常见来源 |
| ------ | ---- | -------- |
| `0` | CLI 命令本身成功完成。状态类命令即使发现 runtime/network 不 ready，也仍可返回 0，只在输出中标记不健康。 | 正常查询、空列表、dry-run 成功、diagnostics warning 但可降级 |
| `1` | 一般错误或无法归入更具体类别的失败。也用于“请求已到达 daemon，但结果中至少一个 item/action 失败”的维护类命令。 | diagnostics unavailable、unimplemented、stream connect failed、GC/recovery item failure |
| `2` | 用户输入或命令用法错误。CLI 在发起 RPC 前即可判断。 | clap 解析失败、互斥参数冲突、CIDR/资源/mount 格式错误、缺少 `--dry-run/--execute` |
| `4` | 目标对象不存在。对应 gRPC `NotFound` 或 daemon 明确返回 not found。 | inspect/remove/stop/create 依赖的 container、pod、image 不存在 |
| `5` | 目标对象已存在。对应 gRPC `AlreadyExists`。 | 创建对象时 daemon 报重名或重复资源 |
| `6` | 当前状态不允许执行该操作。对应 gRPC `FailedPrecondition`。 | 运行中 container remove、Pod 仍有容器、CNI/runtime unsupported、metrics endpoint disabled |
| `13` | 权限不足。对应 gRPC `PermissionDenied` 或本地权限问题。 | socket 权限、daemon 拒绝访问、registry auth denied |
| `124` | 超时。对应 gRPC `DeadlineExceeded` 或本地 connect/RPC timeout。 | daemon 响应超时、stream 建连超时 |
| `125` | daemon 不可达。对应 gRPC `Unavailable` 或 endpoint 无法连接。 | socket 不存在、daemon 未启动、TCP endpoint 不通 |
| `130` | 用户中断。通常来自 SIGINT / Ctrl-C。 | events、attach、exec、port-forward、run 前台模式被中断 |
| 原始 exit code | 容器内进程的退出码原样透传，不表示 CLI 自身解析或 RPC 失败。 | `container exec-sync`、`run --exec-mode sync` |
| 不适用 | 该矩阵行描述的是纯失败输入或歧义场景，没有对应成功路径。 | `inspect ambiguous`、`image inspect missing`、参数冲突 |

矩阵中的“成功退出码”指该命令场景按预期完成时的退出码；如果命令用于执行容器内进程，成功连上 daemon 之后仍可能返回远端进程自己的非零退出码，例如 `crs container exec-sync ID -- false` 返回 `1`。

| 命令                                                      | 成功退出码 | 失败样例                                                 | 失败退出码                                       | 输出断言                               |
| --------------------------------------------------------- | ---------- | -------------------------------------------------------- | ------------------------------------------------ | -------------------------------------- |
| `crs version`                                             | 0          | daemon unavailable                                       | 125                                              | table 有 runtime name                  |
| `crs version --output json`                               | 0          | daemon unavailable                                       | 125                                              | JSON OK，kind=`RuntimeVersion`         |
| `crs status`                                              | 0          | daemon unavailable                                       | 125                                              | table 有 runtime/network ready         |
| `crs status --verbose --output json`                      | 0          | verbose info invalid JSON                                | 0                                                | JSON OK，warnings 非空                 |
| `crs config show --output json`                           | 0          | diagnostics unavailable + no verbose status config field | 1                                                | configJson 存在                        |
| `crs doctor`                                              | 0          | daemon unavailable                                       | 125                                              | table 有检查项和建议动作               |
| `crs ps`                                                  | 0          | daemon unavailable                                       | 125                                              | 等价于 `container list` running filter |
| `crs pods`                                                | 0          | daemon unavailable                                       | 125                                              | 等价于 `pod list`                      |
| `crs images`                                              | 0          | daemon unavailable                                       | 125                                              | 等价于 `image list`                    |
| `crs pull IMAGE`                                          | 0          | auth denied                                              | 13                                               | 等价于 `image pull IMAGE`              |
| `crs inspect ambiguous`                                   | 不适用     | 多类型匹配                                               | 2                                                | stderr 提示 `--type`                   |
| `crs logs ID --tail 10`                                   | 0          | diagnostics unavailable                                  | 1                                                | 等价于 `container logs`                |
| `crs exec ID -- echo ok`                                  | 0          | stream connect failed                                    | 1                                                | 等价于 `container exec`                |
| `crs stop ID`                                             | 0          | target ambiguous                                         | 2                                                | 输出 stopped summary                   |
| `crs rm ID`                                               | 0          | target ambiguous                                         | 2                                                | 输出 removed summary                   |
| `crs runtime config`                                      | 0          | unimplemented                                            | 1                                                | table 有 cgroup driver                 |
| `crs runtime update --pod-cidr 10.244.0.0/16`             | 0          | invalid CIDR                                             | 2                                                | summary.updated=true                   |
| `crs runtime handlers --output json`                      | 0          | diagnostics unavailable                                  | 1                                                | items 是 array                         |
| `crs image list`                                          | 0          | daemon unavailable                                       | 125                                              | 空列表也退出 0                         |
| `crs image pull IMAGE`                                    | 0          | auth denied                                              | 13                                               | quiet 输出 image ref                   |
| `crs image pull IMAGE --auth-json X --username U`         | 不适用     | auth 来源冲突                                            | 2                                                | stderr 包含互斥说明                    |
| `crs image inspect missing`                               | 不适用     | not found                                                | 4                                                | stderr 包含 image                      |
| `crs image remove in-use`                                 | 不适用     | failed precondition                                      | 6                                                | stderr 包含 daemon message             |
| `crs pod list --output json`                              | 0          | daemon unavailable                                       | 125                                              | kind=`PodList`                         |
| `crs pod inspect missing`                                 | 不适用     | not found                                                | 4                                                | stderr 包含 pod                        |
| `crs pod run --name p --namespace ns`                     | 0          | CNI failure                                              | 6                                                | quiet 输出 pod ID                      |
| `crs pod stop POD`                                        | 0          | missing                                                  | 4                                                | summary.stopped=true                   |
| `crs pod remove POD`                                      | 0          | has containers                                           | 6                                                | summary.removed=true                   |
| `crs pod stats`                                           | 0          | stats unavailable                                        | 1                                                | empty list 仍 JSON OK                  |
| `crs pod update-resources POD --pod-resource memory=1MiB` | 0          | unsupported resource                                     | 6                                                | summary.updated=true                   |
| `crs pod port-forward POD --forward 8080:80`              | 0          | local port occupied                                      | 1                                                | listener 关闭                          |
| `crs container list --output json`                        | 0          | daemon unavailable                                       | 125                                              | kind=`ContainerList`                   |
| `crs container inspect missing`                           | 不适用     | not found                                                | 4                                                | stderr 包含 container                  |
| `crs container create POD IMAGE`                          | 0          | pod missing                                              | 4                                                | quiet 输出 container ID                |
| `crs container start ID`                                  | 0          | missing                                                  | 4                                                | summary.started=true                   |
| `crs container stop ID --timeout 10`                      | 0          | timeout invalid                                          | 2                                                | summary.stopped=true                   |
| `crs container remove running`                            | 不适用     | failed precondition                                      | 6                                                | stderr 包含 running                    |
| `crs container exec ID -- echo ok`                        | 0          | stream connect failed                                    | 1                                                | terminal restored                      |
| `crs container exec-sync ID -- false`                     | 1          | command exit 1                                           | 1                                                | 返回远端 exit code                     |
| `crs container attach ID`                                 | 0          | stream connect failed                                    | 1                                                | terminal restored                      |
| `crs container checkpoint ID --location PATH`             | 0          | runtime unsupported                                      | 6                                                | summary.location 存在                  |
| `crs container update ID --resource cpu-quota=50000`      | 0          | no updates                                               | 2                                                | summary.updated=true                   |
| `crs container reopen-log ID`                             | 0          | log not configured                                       | 6                                                | summary.reopened=true                  |
| `crs container logs ID --tail 10`                         | 0          | tail negative                                            | 2                                                | text 不加 envelope                     |
| `crs container logs ID --output json`                     | 0          | daemon stream error                                      | 1                                                | NDJSON OK                              |
| `crs run --rm IMAGE echo ok`                              | 0          | create fails after pod                                   | daemon gRPC code 对应退出码；无 gRPC code 时为 1 | 输出遗留 ID 和 cleanup warning         |
| `crs run --detach IMAGE sleep 30`                         | 0          | start fails                                              | daemon gRPC code 对应退出码；无 gRPC code 时为 1 | quiet 输出 container ID                |
| `crs events --output json`                                | 0          | Ctrl-C                                                   | 130                                              | NDJSON OK                              |
| `crs stats --output json`                                 | 0          | daemon unavailable                                       | 125                                              | kind=`ContainerStats`                  |
| `crs metrics descriptors`                                 | 0          | unimplemented                                            | 1                                                | descriptors 列表                       |
| `crs metrics scrape --output text`                        | 0          | endpoint disabled                                        | 6                                                | 输出 daemon metrics 文本               |
| `crs recovery status`                                     | 0          | diagnostics unavailable                                  | 1                                                | status 字段                            |
| `crs recovery check`                                      | 0          | unhealthy objects                                        | 0                                                | summary.unhealthyObjectCount           |
| `crs recovery repair`                                     | 不适用     | missing dry-run/execute                                  | 2                                                | clap error                             |
| `crs recovery repair --dry-run`                           | 0          | diagnostics unavailable                                  | 1                                                | no executed actions                    |
| `crs recovery repair --execute`                           | 0          | item failure                                             | 1                                                | failed count                           |
| `crs gc candidates`                                       | 0          | diagnostics unavailable                                  | 1                                                | candidates array                       |
| `crs gc run`                                              | 不适用     | missing dry-run/execute                                  | 2                                                | clap error                             |
| `crs gc run --dry-run`                                    | 0          | diagnostics unavailable                                  | 1                                                | dryRun=true                            |
| `crs gc run --execute`                                    | 0          | item failure                                             | 1                                                | reclaimedBytes                         |
| `crs debug network --output json`                         | 0          | diagnostics unavailable                                  | 1                                                | kind=`DebugNetwork`                    |
| `crs debug shims --output json`                           | 0          | diagnostics unavailable                                  | 1                                                | shims array                            |
| `crs debug security --output json`                        | 0          | diagnostics unavailable                                  | 1                                                | security booleans                      |
| `crs completion bash`                                     | 0          | unsupported shell                                        | 2                                                | stdout 非空                            |

对“item failure”类命令采用退出码 1，表示操作请求已到达 daemon，但至少一个候选或动作失败；`--output json` 调用方读取详细失败项。

手工验证：

```bash
cargo build --features shim --bins
sudo ./target/debug/crius --config /etc/crius/crius.conf
sudo ./target/debug/crs version
sudo ./target/debug/crs status
sudo ./target/debug/crs image list
sudo ./target/debug/crs image pull "$CRIUS_TEST_IMAGE"
sudo ./target/debug/crs run --rm "$CRIUS_TEST_IMAGE" echo ok
```

## 实现约束

- `run` 必须输出它创建的 CRI Pod sandbox 和 container，避免用户误解遗留资源的清理边界。
- 日志内容读取必须由 daemon 校验后提供。
- recovery / GC / shim 深度操作必须通过 diagnostics 接口完成，CLI 不直接操作内部状态文件。
- streaming 保持独立模块和独立测试。
- TCP endpoint 仅在 daemon 显式配置后启用，并补齐 TLS、鉴权和配置校验。

## 交付清单

- `crs` bin、全局参数、client/context、错误处理和输出格式化。
- 基础信息命令：version、status、config show、doctor、completion。
- 快捷命令：ps、pods、images、pull、inspect、logs、exec、stop、rm。
- runtime 命令：config、update、handlers。
- 镜像命令：list、pull、pull with auth options、inspect、remove、fs-info、transfers、config。
- Pod sandbox 命令：list、inspect、run、stop、remove、stats、metrics、update-resources、port-forward。
- 容器命令：list、inspect、create、start、stop、remove、exec、attach、stats、checkpoint、update、reopen-log、logs。
- 组合验证命令：run。
- 事件、metrics、recovery、GC、runtime、NRI、安全、rootless、tracing 和 debug 诊断命令。
- diagnostics proto、server 注册和 client。
- CLI 单元测试、集成测试、发布门禁和使用文档。

## 实现任务分解

本节是编码执行清单。每个子任务都必须带状态标识，并且描述中要能回答三个问题：改哪里、产出什么、如何确认完成。完成时至少包含代码、对应测试或明确的构建验证；需要文档同步的任务同时更新文档。

状态标识：

- `[TODO]` 未开始。
- `[DOING]` 正在实现。
- `[DONE]` 已实现并通过对应验证。
- `[BLOCKED]` 被外部依赖、设计缺口或环境问题阻塞，必须写明阻塞原因。

### P0: 项目入口与可编译骨架

- `[DONE] P0.01` 在 `Cargo.toml` 注册 `crs` 二进制入口。改动：新增 `[[bin]]`，`name = "crs"`，`path = "src/crs/main.rs"`；不调整现有 `crius`、`crius-shim` bin。完成验证：`cargo metadata` 能看到 `crs` target，`cargo build --bins` 会尝试编译 `src/crs/main.rs`。
- `[DONE] P0.02` 在 `src/lib.rs` 暴露 CLI library 入口。改动：新增 `pub mod crs;`，确认不改变 daemon 当前对 `services`、`server`、`image` 等模块的引用方式。完成验证：library crate 编译时能解析 `crius::crs`。
- `[DONE] P0.03` 新建 `src/crs/main.rs` 作为进程入口。改动：添加 `tokio::main`，调用 `crius::crs::run_cli(std::env::args_os())` 或等价入口，并把 `CommandResult` / `CliError` 转为进程退出码。完成验证：`./target/debug/crs --help` 不依赖 daemon 即可返回 0。
- `[DONE] P0.04` 新建 `src/crs/mod.rs` 并声明 CLI 模块树。改动：声明 `args`、`client`、`commands`、`context`、`error`、`format`、`ids`、`parsers`、`streaming`；除 `run_cli` 外默认保持 `pub(crate)`。完成验证：新增模块空实现时 crate 能完成名称解析。
- `[DONE] P0.05` 在 `src/crs/mod.rs` 实现最小 `run_cli` 流程。改动：完成 clap parse、`CliContext::from_args`、顶层 `commands::dispatch` 调用和错误统一出口；命令 handler 暂可返回 not implemented。完成验证：参数错误返回 2，help 返回 0。
- `[DONE] P0.06` 新建 `src/crs/commands/mod.rs` 作为唯一顶层分发点。改动：定义 `dispatch(ctx, client, command)`，每个 match 分支只转调对应命令模块，不在这里拼 request。完成验证：新增命令 enum 分支后编译器能强制分发覆盖。
- `[DONE] P0.07` 新建所有 `src/crs/commands/*.rs` 文件并提供 handler stub。改动：按文件布局创建 version、status、config、image、runtime、pod、container、logs、run、exec、attach、port_forward、doctor、shortcuts、events、stats、metrics、recovery、gc、debug、completion。完成验证：所有 stub 签名统一，后续任务能逐个替换实现。
- `[DONE] P0.08` 在 `tests/crs_args.rs` 增加 CLI help smoke test。改动：通过 clap `try_parse_from` 或 assert_cmd 验证 `crs --help`、`crs version --help` 可以解析并输出帮助。完成验证：测试不启动 daemon，且失败时能定位到 clap 表面问题。
- `[DONE] P0.09` 跑通初始构建门禁。改动：执行 `cargo build --bins`，修复新增 bin、模块声明和入口签名造成的所有编译错误。完成验证：`target/debug/crs` 文件生成，现有 bin 仍可编译。

### P1: Clap 参数表面

- `[DONE] P1.01` 在 `src/crs/args.rs` 定义顶层 `Args`。改动：声明全局参数 `--address`、`--connect-timeout`、`--timeout`、`--debug`、`--output`、`--quiet`、`--no-trunc`，并为 duration 字段挂接 value parser。完成验证：全局参数可出现在任一子命令前，clap help 中默认值和 env 说明正确。
- `[DONE] P1.02` 定义 `OutputArg` value enum。改动：支持 `table`、`json`、`text` 三种输入，默认 `table`；在 context 或命令校验层拒绝不支持 text 的命令。完成验证：`--output xml` 返回参数错误，`logs --output text` 可解析。
- `[DONE] P1.03` 定义顶层快捷命令 enum。改动：覆盖 `version`、`status`、`doctor`、`ps`、`pods`、`images`、`pull`、`inspect`、`logs`、`exec`、`stop`、`rm`，并把快捷命令 args 复用到底层 args 类型。完成验证：每个快捷命令都能被 clap 解析到唯一 enum 分支。
- `[DONE] P1.04` 定义顶层命令组 enum。改动：加入 `config`、`runtime`、`image`、`pod`、`container`、`run`、`events`、`stats`、`metrics`、`recovery`、`gc`、`debug`、`completion`，命名与命令语法章节一致。完成验证：`crs <group> --help` 对所有命令组可用。
- `[TODO] P1.05` 定义基础信息和多类型快捷 args。改动：实现 `VersionArgs`、`StatusArgs`、`DoctorArgs`、`InspectArgs`、`StopArgs`、`RemoveArgs`，包含 target、object type、timeout、force 等字段。完成验证：`inspect --type image X`、`stop --type pod P`、`rm --force X` 解析正确。
- `[TODO] P1.06` 定义 config 命令 args。改动：实现 `ConfigCommand::{Show, ReloadStatus}` 及空 args struct，为后续 diagnostics 降级逻辑保留扩展位置。完成验证：`crs config show` 和 `crs config reload-status` 能解析。
- `[TODO] P1.07` 定义 runtime 命令 args。改动：实现 `runtime config`、`runtime update --pod-cidr CIDR`、`runtime handlers --verbose`。完成验证：缺少 `--pod-cidr` 时 clap 返回参数错误。
- `[TODO] P1.08` 定义 image 命令 args。改动：覆盖 list、pull、inspect、remove、fs-info、transfers、config，并在 pull 中 flatten `ImageAuthArgs` 和可选 `--pod`。完成验证：`image pull IMAGE --username u --password p` 解析到 auth 字段。
- `[TODO] P1.09` 定义 pod 命令 args。改动：覆盖 list、inspect、run、stop、remove、stats、metrics、update-resources、port-forward，字段与命令语法一一对应。完成验证：`pod port-forward POD --forward 8080:80` 可重复解析多个 forward。
- `[TODO] P1.10` 定义 container 命令 args。改动：覆盖 list、inspect、create、start、stop、remove、exec、exec-sync、attach、stats、checkpoint、update、reopen-log、logs，保证 `-- COMMAND...` 不被 clap 吞掉。完成验证：`container exec ID -- echo ok` 的 command 为 `["echo", "ok"]`。
- `[TODO] P1.11` 定义 `RunArgs`。改动：组合 image、command、pull policy、detach、rm、pod、Pod 创建参数、Container 创建参数和 Stream 参数；解决同名 flag 的 clap 命名冲突。完成验证：`run --rm --tty IMAGE sh` 和 `run --pod POD IMAGE echo ok` 解析正确。
- `[TODO] P1.12` 定义剩余命令组 args。改动：实现 `MetricsCommand`、`RecoveryCommand`、`GcCommand`、`DebugCommand`、`CompletionArgs`，其中 repair/run 必须表达 dry-run/execute 互斥。完成验证：`debug rootless`、`metrics scrape`、`completion bash` 可解析。
- `[TODO] P1.13` 定义共享 `ImageAuthArgs`。改动：收集 `--auth-json`、`--auth-file`、`--username`、`--password`、`--server`、`--identity-token`、`--registry-token`，用 clap group 或后续校验表达来源互斥。完成验证：多来源 auth 同时出现时测试返回退出码 2。
- `[TODO] P1.14` 定义共享 `PodCreateArgs` 和 `SandboxSecurityArgs`。改动：覆盖 name、uid、namespace、attempt、hostname、DNS、publish、runtime-handler、cgroup、sysctl、host namespace、userns、sandbox security、overhead、pod-resource。完成验证：`pod run --name p --namespace ns --host-network` 解析到对应字段。
- `[TODO] P1.15` 定义共享 `ContainerCreateOptions`、`ContainerResourceArgs`、`ContainerSecurityArgs`。改动：覆盖 command/arg、workdir、env、mount、device、CDI、log、stdin/tty、资源和安全参数。完成验证：`container create POD IMAGE --mount type=bind,src=/x,dst=/y --memory 64MiB` 可解析。
- `[TODO] P1.16` 定义共享 `StreamOptions`。改动：覆盖 `--stdin/-i`、`--tty/-t`、`--stdout=false`、`--stderr=false`、`--resize COLSxROWS`、`--protocol websocket|spdy`，并给 exec、attach、run 复用。完成验证：TTY 场景下 stderr 默认处理由后续校验可以读取到字段。
- `[TODO] P1.17` 定义 value enum 类型。改动：实现 `ObjectType`、`StopObjectType`、`PodStateArg`、`ContainerStateArg`、`PullPolicyArg`、`StreamProtocolArg`，输入统一小写匹配。完成验证：非法状态值返回 clap 参数错误而不是落到 handler。
- `[TODO] P1.18` 为所有公开命令增加 parse 成功测试。改动：在 `tests/crs_args.rs` 用最小合法参数覆盖每个命令和命令组。完成验证：新增命令漏接 clap 时测试失败。
- `[TODO] P1.19` 为关键互斥规则增加 parse/validation 测试。改动：覆盖 auth 来源冲突、`gc run` 未指定 dry-run/execute、`recovery repair` 同时指定 dry-run 和 execute。完成验证：错误返回参数错误语义，stderr 中包含冲突字段名。
- `[TODO] P1.20` 跑通 args 测试门禁。改动：执行 `cargo test --test crs_args` 并修复所有 clap 表面回归。完成验证：测试通过且不需要 daemon。

### P2: Context、Endpoint 与客户端基础

- `[TODO] P2.01` 在 `src/crs/context.rs` 定义运行上下文类型。改动：新增 `CliContext`、`Endpoint`、`OutputMode`、`FormatOptions`，字段包含 endpoint、connect timeout、rpc timeout、output、quiet、no_trunc、debug。完成验证：命令 handler 只需要借用 `CliContext` 即可获得所有全局选项。
- `[TODO] P2.02` 实现 `CliContext::from_args`。改动：合并 clap 解析结果、`CRIUS_ADDRESS` 和内置默认值，明确优先级为 flag、env、默认值。完成验证：测试覆盖 env 存在但 flag 覆盖、env 不存在时使用默认 socket。
- `[TODO] P2.03` 在 `src/crs/parsers.rs` 实现 `parse_endpoint`。改动：把 `unix:///run/crius/crius.sock` 和裸路径转为 `Endpoint::Unix`，把 `http://` / `https://` 转为 `Endpoint::Tcp`，拒绝未知 scheme。完成验证：错误文案包含原始 endpoint 和支持格式。
- `[TODO] P2.04` 为 endpoint parser 添加单元测试。改动：覆盖 Unix URI、裸 Unix path、HTTP URI、HTTPS URI、空字符串、非法 scheme。完成验证：合法输入得到期望 enum，非法输入返回参数错误。
- `[TODO] P2.05` 在 `src/crs/client.rs` 定义 `CrsClient`。改动：封装 RuntimeService client、ImageService client、可选 DiagnosticsService client，并保存 endpoint 字符串用于错误上下文。完成验证：命令层不直接接触 tonic channel 构造细节。
- `[TODO] P2.06` 实现 Unix socket tonic channel connector。改动：使用 tonic endpoint + UnixStream connector 连接 socket path，处理 socket 缺失、权限不足和 connect timeout。完成验证：连接不存在 socket 时返回 daemon unavailable 语义。
- `[TODO] P2.07` 实现 TCP endpoint channel 构造。改动：支持 daemon 显式提供的 HTTP/HTTPS endpoint，保留 TLS / 鉴权后续扩展点；不把 TCP 作为默认值。完成验证：`http://127.0.0.1:PORT` mock server 可连接。
- `[TODO] P2.08` 为 RPC 添加 per-RPC timeout helper。改动：封装 `tokio::time::timeout` 或 tonic request timeout，所有 CRI / diagnostics 调用经过同一入口。完成验证：超时被映射为 `DeadlineExceeded` 或本地 timeout 退出码 124。
- `[TODO] P2.09` 实现 client getter。改动：提供 `runtime()`、`image()`、`diagnostics()`，其中 diagnostics 未生成、未注册或返回 unimplemented 时转换为清晰的 `diagnostics service is not available from this crius daemon`。完成验证：diagnostics 命令能区分 daemon 不可达和 diagnostics 不可用。
- `[TODO] P2.10` 为 client 基础逻辑添加单元测试。改动：覆盖 endpoint display、timeout 包装、diagnostics unavailable 错误构造。完成验证：测试不依赖真实 `/run/crius/crius.sock`。
- `[TODO] P2.11` 用 mock server 覆盖基础连通性。改动：在 `tests/crs_commands.rs` 启动临时 tonic service，执行 `version` handler，断言 request 到达 mock server 且输出成功。完成验证：证明 context、client、handler、formatter 的最短路径贯通。

### P3: 错误、退出码与脱敏

- `[TODO] P3.01` 在 `src/crs/error.rs` 定义结构化错误类型。改动：实现 `CliError`、`ExitStatus`、`CommandResult`，错误中保留 command、endpoint、object、tonic code、daemon message、source error。完成验证：handler 可以用 `?` 返回错误，顶层能统一渲染。
- `[TODO] P3.02` 实现 clap 参数错误映射。改动：把 clap parse / validation failure 转为退出码 2，help 和 version 不按错误处理。完成验证：缺少必填参数、非法 enum、互斥参数冲突都返回 2。
- `[TODO] P3.03` 实现 tonic status 到退出码映射。改动：覆盖 `NotFound -> 4`、`AlreadyExists -> 5`、`FailedPrecondition -> 6`、`PermissionDenied -> 13`、`DeadlineExceeded -> 124`、`Unavailable -> 125`，其他 status 默认 1。完成验证：单元测试逐个断言映射表。
- `[TODO] P3.04` 实现本地 timeout 错误。改动：把本地 connect timeout、RPC wrapper timeout 转成 `CliError::Timeout` 或等价类型，并映射到 124。完成验证：mock RPC 挂起时能稳定返回 124。
- `[TODO] P3.05` 实现 interrupted 错误。改动：为 Ctrl-C、SIGINT、用户中断路径提供 `CliError::Interrupted`，退出码为 130。完成验证：events / streaming 后续测试能复用该类型。
- `[TODO] P3.06` 实现容器进程 exit code 透传。改动：定义 `CommandResult::ContainerExit { code }`，让 `exec-sync` 和 `run --exec-mode sync` 不被普通错误包装。完成验证：远端进程退出 7 时 CLI 退出码为 7。
- `[TODO] P3.07` 实现文本错误输出。改动：stderr 文案包含命令名、endpoint、对象 ID / image ref、gRPC code、daemon message，并附带可执行下一步建议。完成验证：NotFound 错误中能看到对象类型和建议 list/inspect 命令。
- `[TODO] P3.08` 实现 JSON 错误输出。改动：当全局输出为 JSON 时，错误仍写 stderr，格式为 `{ "error": { ... } }`，stdout 保持空。完成验证：测试分别解析 stderr JSON，并断言 stdout 为空。
- `[TODO] P3.09` 实现敏感字段脱敏。改动：提供 password、token、identity token、registry token、auth JSON、敏感 config key 的 redaction helper，debug 日志和错误路径都调用它。完成验证：测试输入含密码或 token 时输出只包含 `<redacted>`。
- `[TODO] P3.10` 为错误系统添加单元测试。改动：覆盖所有退出码、文本错误、JSON 错误和脱敏边界。完成验证：`cargo test error` 或对应单元测试通过。

### P4: 输出模型与 Formatter

- `[TODO] P4.01` 在 `src/crs/format.rs` 定义统一输出模型。改动：新增 `CommandOutput<T>`、JSON envelope struct、`TableRow` trait、`FormatOptions` 适配函数。完成验证：命令 handler 可以返回展示模型而不是直接写 stdout。
- `[TODO] P4.02` 实现 `print_envelope`。改动：输出 `kind`、`apiVersion`、`endpoint`、`items`、`summary`、`warnings`，字段使用 lower camel case。完成验证：任意命令 JSON stdout 可被 `serde_json` 解析。
- `[TODO] P4.03` 实现 `print_table`。改动：根据 header 和 cell 计算列宽，替换 cell 内换行，空字符串显示 `-`，超长列按最大宽度截断。完成验证：golden 测试覆盖普通行、空值、长字段、换行字段。
- `[TODO] P4.04` 实现 `--quiet` 输出路径。改动：quiet 模式不输出表头、不使用 envelope，只输出 ID、image ref 或命令约定的最小机器可读值。完成验证：`image list --quiet` 不包含 `IMAGE` header。
- `[TODO] P4.05` 实现 warning 输出规则。改动：table/text 模式 warning 写 stderr；JSON 模式 warning 放入 envelope `warnings`，不混入 stderr，致命错误除外。完成验证：diagnostics 降级 warning 在 JSON 中可见。
- `[TODO] P4.06` 在 `src/crs/ids.rs` 实现截断 helper。改动：提供 `short_id`、`short_image_id`、`truncate_field`、`truncate_error`，尊重 `--no-trunc`。完成验证：长度小于、等于、大于阈值的边界测试通过。
- `[TODO] P4.07` 实现数值格式化 helper。改动：bytes 使用 IEC 单位，CPU 使用 millicores，bool 使用 `true` / `false`，空列表不输出数据行。完成验证：64MiB、125m、0 值和空列表格式稳定。
- `[TODO] P4.08` 实现时间格式化 helper。改动：小于 24 小时显示相对时间，超过 24 小时显示本地 `YYYY-MM-DD HH:MM:SS`；JSON 保留原始时间字段。完成验证：固定时钟测试覆盖相对和绝对两种路径。
- `[TODO] P4.09` 定义基础 view 类型。改动：实现 `RuntimeVersionView`、`ConditionView`、`ImageView`、`PodView`、`ContainerView`、`ResourceUsageView`、`OperationView` 并派生 Serialize。完成验证：view 字段能覆盖基础命令的默认表格列和 JSON 输出。
- `[TODO] P4.10` 为基础 view 实现 `TableRow`。改动：每个 view 返回固定 headers 和行字段，字段顺序匹配输出契约。完成验证：container、pod、image 表格列名与计划中的默认列一致。
- `[TODO] P4.11` 添加 table golden 测试。改动：在 `src/crs/format.rs` tests 模块中固定输入 view，断言输出表格文本。完成验证：列宽或默认列变动会触发测试失败。
- `[TODO] P4.12` 添加 JSON envelope 测试。改动：构造带 items、summary、warnings 的 `CommandOutput`，输出后用 `serde_json` 解析并断言字段。完成验证：JSON 输出不会多出 table 文本或 stderr warning。
- `[TODO] P4.13` 添加截断边界测试。改动：在 `src/crs/ids.rs` tests 中覆盖 ID、digest、path、error message、`--no-trunc`。完成验证：默认 12 字符 ID 和完整输出行为稳定。

### P5: Diagnostics Proto 与生成接入

- `[TODO] P5.01` 新建 diagnostics proto 文件。改动：创建 `proto/crius/diagnostics/v1/diagnostics.proto`，写入 `DiagnosticsService`、request/response message、streaming log message，并保持 package 为 `diagnostics.v1`。完成验证：proto 字段编号、字段名和“Diagnostics Proto 规格”章节完全一致。
- `[TODO] P5.02` 修改 `build.rs` 的 proto 输入列表。改动：把 diagnostics proto 加入现有 `tonic_build` 编译输入，继续同时生成 client 和 server。完成验证：构建时 CRI proto 和 diagnostics proto 都被编译，不破坏现有 generated module。
- `[TODO] P5.03` 增加 diagnostics proto 变更追踪。改动：在 `build.rs` 输出 `cargo:rerun-if-changed=proto/crius/diagnostics/v1/diagnostics.proto`。完成验证：修改 proto 后重新构建会触发 codegen。
- `[TODO] P5.04` 修改 `src/proto/mod.rs` 的模块导出。改动：新增 `pub mod diagnostics { pub mod v1 { include!(concat!(env!("OUT_DIR"), "/diagnostics.v1.rs")); } }`。完成验证：业务代码可通过 `crate::proto::diagnostics::v1` 引用生成类型。
- `[TODO] P5.05` 跑通 proto 构建。改动：执行 `cargo build --bins`，修复 package 名、include path、tonic feature 或 prost 类型错误。完成验证：`OUT_DIR` 下存在 diagnostics generated file，crate 编译通过。
- `[TODO] P5.06` 添加 codegen smoke test。改动：在 `tests/diagnostics.rs` 引用 `DiagnosticsServiceClient`、`DiagnosticsServiceServer` 和至少一个 request/response 类型。完成验证：生成代码路径变化会被测试捕获。

### P6: Daemon Diagnostics Server

- `[TODO] P6.01` 在 services 模块树接入 diagnostics。改动：修改 `src/services/mod.rs`，新增 `pub mod diagnostics;`，必要时导出 `DiagnosticsServiceImpl`。完成验证：daemon 侧代码可引用 `crate::services::diagnostics`。
- `[TODO] P6.02` 新建 daemon diagnostics 实现文件。改动：创建 `src/services/diagnostics.rs`，定义 `DiagnosticsState`、`DiagnosticsServiceImpl`、内部错误类型和 response 转换 helper。完成验证：文件本身不依赖 CLI 模块，只依赖 daemon 服务和生成 proto。
- `[TODO] P6.03` 实现 `DiagnosticsState::from_runtime`。改动：从 `RuntimeServiceImpl` 或 daemon 初始化阶段收集 introspection、health、event、runtime、image、storage 所需只读句柄和维护动作入口。完成验证：不会直接打开 SQLite、bundle 或 shim 工作目录。
- `[TODO] P6.04` 实现 `ServerInfo` RPC。改动：返回版本、git commit、config path、state dir、socket path 等只读信息，缺失字段用空字符串或 unknown 约定处理。完成验证：mock state 下 response 字段与输入 state 一致。
- `[TODO] P6.05` 实现 `EffectiveConfig` RPC。改动：复用 introspection 已解析配置摘要，`include_sensitive=false` 时脱敏 password、token、registry auth 等字段，并返回 redacted_fields。完成验证：测试确认敏感值不出现在 response JSON。
- `[TODO] P6.06` 实现 `RuntimeHandlers` RPC。改动：复用 runtime backend / handler 配置和探测结果，返回 handler name、runtime type、path、config path、features、warnings。完成验证：无 handler 时返回空列表而不是 internal error。
- `[TODO] P6.07` 实现 `ImageTransfers` RPC。改动：读取 image service 的传输管理器状态、进行中任务、最近失败原因，按 `include_completed` 控制已完成项。完成验证：默认不包含 completed，指定 true 后包含。
- `[TODO] P6.08` 实现 `RecoveryStatus` RPC。改动：复用 recovery ledger、启动恢复结果、health 摘要，返回 status、last_startup、unhealthy_object_count、ledger_summary_json、warnings。完成验证：ledger JSON 是合法 JSON 字符串。
- `[TODO] P6.09` 实现 `RecoveryCheck` RPC。改动：`execute=false` 只返回计划动作，`execute=true` 调用 recovery 模块维护入口；单项失败填充 action.error，整体生成失败才返回非 OK status。完成验证：dry-run 测试确认无状态修改。
- `[TODO] P6.10` 实现 `NriStatus` RPC。改动：返回 NRI enabled、CDI enabled、CDI spec dirs、plugin path、plugin config、blockio/RDT 配置和支持状态。完成验证：NRI 未启用时返回 enabled=false 且不是错误。
- `[TODO] P6.11` 实现 `SecurityStatus` RPC。改动：返回 seccomp、seccomp notifier、AppArmor、SELinux、rootless、devices policy JSON 和 warnings。完成验证：devices_policy_json 合法，敏感 host path 不被泄露。
- `[TODO] P6.12` 实现 `ShimStatus` RPC。改动：从 runtime task/shim registry 和 event service 摘要读取 shim pid、task socket、attach socket、state、error，可按 container_id 过滤。完成验证：指定 container_id 时只返回匹配项。
- `[TODO] P6.13` 实现 `ContentGc` RPC。改动：接入 content store / snapshot store GC planner，`execute=false` 返回候选，`execute=true` 执行 daemon 校验后的删除动作并统计 reclaimed_bytes。完成验证：单项删除失败写 item error，RPC 仍可 OK。
- `[TODO] P6.14` 实现 `ContainerLog` streaming RPC。改动：根据 container status 查找日志路径，daemon 侧做路径归属和逃逸校验，支持 tail、since、follow、timestamps，并以 chunk stream 返回。完成验证：CLI 无法通过请求传入任意宿主路径读取文件。
- `[TODO] P6.15` 实现 diagnostics 错误映射。改动：把 NotFound、FailedPrecondition、PermissionDenied、InvalidArgument、Internal 等内部错误映射到规定 tonic code，message 不包含敏感路径。完成验证：单元测试覆盖错误约定表。
- `[TODO] P6.16` 在 daemon 启动流程注册 diagnostics server。改动：修改 `src/main.rs`，构造 `DiagnosticsState` 和 `DiagnosticsServiceImpl`，在同一 tonic server builder 中 add_service。完成验证：daemon 启动后同一 Unix socket 可访问 RuntimeService、ImageService、DiagnosticsService。
- `[TODO] P6.17` 添加 diagnostics round trip 测试。改动：在 `tests/diagnostics.rs` 为每个 RPC 添加 mock state 或 in-process server 测试。完成验证：client 调用能拿到 expected response，streaming log 能读到 chunk。
- `[TODO] P6.18` 跑通 diagnostics 测试门禁。改动：执行 `cargo test --test diagnostics`，修复 codegen、server impl 和状态 mock 的所有错误。完成验证：测试通过且不要求 root。

### P7: Parser 函数

- `[TODO] P7.01` 实现 duration parser。改动：`parse_duration` 支持 `500ms`、`5s`、`2m`、`1h`，拒绝空值、无单位、小数和溢出。完成验证：错误文案使用 `invalid duration "{value}"` 模板。
- `[TODO] P7.02` 实现 byte size parser。改动：`parse_byte_size` 支持整数、`B`、`KiB`、`MiB`、`GiB`、`TiB`，转换为 bytes，溢出返回参数错误。完成验证：`64MiB` 解析为 `67108864`。
- `[TODO] P7.03` 实现 key/value parser。改动：`parse_key_value(flag, value)` 要求 key 非空，value 可为空，保留原始大小写。完成验证：`=x` 返回包含 flag 和原始值的错误。
- `[TODO] P7.04` 实现 env file parser。改动：逐行读取 env 文件，忽略空行和 `#` 注释，复用 key/value parser，读取失败返回 IO 错误。完成验证：同 key 覆盖规则由 builder 测试覆盖。
- `[TODO] P7.05` 实现 auth JSON parser。改动：支持 CRI `AuthConfig` JSON 和 Docker / registry auth JSON，解析失败不打印完整 JSON。完成验证：错误信息只包含 source 和 reason，不泄露密码。
- `[TODO] P7.06` 实现 CIDR list parser。改动：解析单个 CIDR 或逗号分隔 CIDR 列表，保留顺序，拒绝空段和非法网段。完成验证：`10.244.0.0/16,fd00::/64` 可解析。
- `[TODO] P7.07` 实现 port mapping parser。改动：支持 `HOST:CONTAINER[/PROTO]`、bracket IPv6、`--host-ip` 搭配，端口范围 1-65535，协议默认 tcp 并转小写。完成验证：未加 bracket 的 IPv6 返回参数错误。
- `[TODO] P7.08` 实现 mount parser。改动：解析 `type=bind`、`type=image`、src/source、dst/target、image、subpath、options、uidmap、gidmap，校验 recursive-ro 依赖。完成验证：bind 缺 src、image mount 带 src 都返回明确错误。
- `[TODO] P7.09` 实现 device parser。改动：支持 `HOST[:CONTAINER[:PERMS]]`，默认 container path 等于 host path，默认 perms 为 `rwm`，拒绝非法 perms。完成验证：`/dev/fuse:/dev/fuse:rx` 返回参数错误。
- `[TODO] P7.10` 实现 resource spec parser。改动：解析逗号分隔 key/value，支持 cpu、memory、swap、oom、cpuset、hugepage、unified，返回可合并的资源 fragment。完成验证：未知 key 返回包含 key 的错误。
- `[TODO] P7.11` 实现 hugepage parser。改动：解析 `SIZE=BYTES`，SIZE 原样保留，BYTES 复用 byte size parser。完成验证：空 size 或空 bytes 返回参数错误。
- `[TODO] P7.12` 实现 security profile parser。改动：支持 `runtime/default`、`unconfined`、`localhost:VALUE`，映射到 CRI `SecurityProfile`。完成验证：`localhost:` 返回参数错误。
- `[TODO] P7.13` 实现 SELinux parser。改动：解析 `user:role:type:level` 四段，允许空段但保留位置。完成验证：三段或五段输入返回参数错误。
- `[TODO] P7.14` 实现 user parser。改动：区分 UID、UID:GID、username，数字写入 run_as_user，非数字写入 username。完成验证：`1000:1000`、`nobody`、`1000:` 的结果和错误符合契约。
- `[TODO] P7.15` 实现 ID mapping parser。改动：解析 `HOST:CONTAINER:LENGTH`，三段均为非负整数，length 必须大于 0。完成验证：字段缺失或负数返回参数错误。
- `[TODO] P7.16` 实现 since parser。改动：支持 RFC3339 绝对时间和相对 duration，统一转换 Unix nanos；相对时间使用可测试的 clock 注入或 helper。完成验证：固定当前时间下 `10m` 结果稳定。
- `[TODO] P7.17` 添加 parser 成功样例测试。改动：每个 parser 至少一个合法输入测试，并对关键边界增加多例。完成验证：测试名称能指出 parser 和场景。
- `[TODO] P7.18` 添加 parser 失败样例测试。改动：每个 parser 至少一个非法输入测试，断言错误文案包含原始值和期望格式。完成验证：错误模板回归会被测试捕获。

### P8: CRI Request Builder

- `[TODO] P8.01` 实现 Pod sandbox config builder。改动：在 `commands/pod.rs` 或独立 builder 模块提供 `build_pod_sandbox_config(&PodCreateArgs)`，只做 args 到 CRI proto 的纯转换，不发 RPC。完成验证：默认 namespace、attempt、metadata 必填规则稳定。
- `[TODO] P8.02` 测试 Pod 基础字段映射。改动：断言 metadata name、uid、namespace、attempt、hostname、log_directory、labels、annotations 正确写入 `PodSandboxConfig`。完成验证：重复 label / annotation 后者覆盖前者。
- `[TODO] P8.03` 测试 Pod 网络字段映射。改动：断言 DNS servers/searches/options、port_mappings、runtime_handler 的转换结果。完成验证：协议默认 tcp，host IP 应用于后续 publish。
- `[TODO] P8.04` 测试 Pod namespace 和 userns 字段。改动：覆盖 host network、host pid、host ipc、userns pod/node、uid-map、gid-map。完成验证：CRI namespace enum 和 IDMapping 字段正确。
- `[TODO] P8.05` 实现 sandbox security context builder。改动：覆盖 sandbox user/group、supplemental groups、readonly rootfs、privileged、seccomp、AppArmor、SELinux。完成验证：未设置 user 但设置 group 时返回参数错误。
- `[TODO] P8.06` 测试 Pod resources、overhead、sysctls。改动：断言 `--overhead`、`--pod-resource`、`--sysctl` 映射到 LinuxPodSandboxConfig，重复 key 覆盖。完成验证：资源数值非负校验生效。
- `[TODO] P8.07` 实现 Container config builder。改动：提供 `build_container_config(&ContainerCreateArgs)`，构造 `ContainerConfig`，不校验 Pod 是否存在、不发 RPC。完成验证：image 和 metadata 必填，默认 name 生成规则明确。
- `[TODO] P8.08` 测试 container 基础字段映射。改动：断言 metadata、image、command、args、working_dir 按位置参数和 flags 保留顺序。完成验证：指定 `--command` 时位置参数进入 args。
- `[TODO] P8.09` 测试 env、label、annotation 覆盖规则。改动：env-file 先加载，`--env` 后覆盖同 key；labels/annotations 后者覆盖。完成验证：最终 CRI `KeyValue` 不含重复 env key。
- `[TODO] P8.10` 测试 mounts、devices、CDI、log path。改动：断言 parser 结果写入 CRI `Mount`、`Device`、`CDIDevice`、`log_path`。完成验证：image mount 和 recursive read-only 字段不丢失。
- `[TODO] P8.11` 实现 container resources builder。改动：合并 resource flags 和 `--resource` fragments，标量后者覆盖，hugepage/unified 同 key 后者覆盖。完成验证：多个 fragments 顺序影响符合文档。
- `[TODO] P8.12` 实现 container security context builder。改动：覆盖 privileged、cap add/drop/ambient、user/group、supplemental groups、readonly rootfs、no-new-privs、masked/readonly paths、seccomp、AppArmor、SELinux、pid/ipc namespace。完成验证：`--group` 未设置 user 时返回参数错误。
- `[TODO] P8.13` 实现 auth config builder。改动：从 `ImageAuthArgs` 构造 `AuthConfig`，支持 auth-json、auth-file、分散 flags 三类来源，禁止多来源自动覆盖。完成验证：只在内存传递敏感字段，debug 输出走脱敏。
- `[TODO] P8.14` 测试 auth 互斥和脱敏。改动：覆盖 auth-json + username 冲突、auth-file 读取失败、password/token 脱敏。完成验证：错误不包含明文 secret。
- `[TODO] P8.15` 跑通 parser 和 builder 测试。改动：执行相关单元测试，修复所有字段映射、错误文案和互斥规则回归。完成验证：复杂 flag 转 CRI request 的纯函数层稳定。

### P9: 只读 CRI 命令

- `[TODO] P9.01` 实现 `crs version` handler。改动：在 `commands/version.rs` 调用 CRI `VersionRequest { version: "" }`，转换为 `RuntimeVersionView`，支持 table、JSON、quiet。完成验证：mock response 中 runtime name/version/API version 全部进入输出。
- `[TODO] P9.02` 测试 `version` 输出和错误。改动：添加 table、JSON envelope、daemon unavailable 三类测试。完成验证：Unavailable 映射到退出码 125，JSON kind 为 `RuntimeVersion`。
- `[TODO] P9.03` 实现 `crs status` handler。改动：在 `commands/status.rs` 调用 `Status(verbose=false)`，提取 runtime/network condition，输出 `RuntimeStatus`。完成验证：condition false 不改变退出码，只在输出中体现。
- `[TODO] P9.04` 实现 `crs status --verbose`。改动：调用 `Status(verbose=true)`，解析 `info` map 中 JSON 字符串，输出 raw response 加 `infoJson` / `infoRaw`。完成验证：解析失败时 warning 出现且原始字符串不丢失。
- `[TODO] P9.05` 测试 status 路径。改动：覆盖 runtime ready、network not ready、verbose JSON、verbose 非 JSON。完成验证：table 模式只显示摘要，JSON 模式包含详细字段。
- `[TODO] P9.06` 实现 `runtime config`。改动：在 `commands/runtime.rs` 调用 CRI `RuntimeConfig`，提取 cgroup driver 等字段，输出 `RuntimeConfig`。完成验证：daemon 返回 unimplemented 时退出码为 1，不从 verbose status 拼装假数据。
- `[TODO] P9.07` 实现 `image list`。改动：调用 `ListImages`，有 `--image` 时设置 filter，转换为 `ImageView`，支持 quiet 和 no-trunc。完成验证：空列表退出码 0，quiet 每行只输出 image ID 或约定字段。
- `[TODO] P9.08` 实现 `image inspect`。改动：调用 `ImageStatus(verbose=true)`，输出原始 response，并把 verbose info 解析为 `infoJson` 字段。完成验证：不存在 image 时返回退出码 4。
- `[TODO] P9.09` 实现 `image fs-info`。改动：调用 `ImageFsInfo`，输出 filesystem items 和 total/used summary。完成验证：unimplemented 返回清晰错误，成功 JSON kind 为 `ImageFsInfo`。
- `[TODO] P9.10` 实现 `pod list`。改动：调用 `ListPodSandbox`，根据 id、state、label、all 构造 filter，转换为 `PodView`。完成验证：默认和 `--all` 的 state filter 行为符合快捷命令矩阵。
- `[TODO] P9.11` 实现 `pod inspect`。改动：调用 `PodSandboxStatus(verbose=true)`，输出原始 response 加解析后的 verbose info。完成验证：missing pod 返回 4，JSON stdout 合法。
- `[TODO] P9.12` 实现 `container list`。改动：调用 `ListContainers`，根据 id、pod、state、label、all 构造 filter，转换为 `ContainerView`。完成验证：默认 table 列为 container 默认列。
- `[TODO] P9.13` 实现 `container inspect`。改动：调用 `ContainerStatus(verbose=true)`，输出原始 response 加解析后的 verbose info。完成验证：missing container 返回 4。
- `[TODO] P9.14` 实现快捷 `ps`。改动：在 `commands/shortcuts.rs` 复用 container list builder / handler，默认设置 running filter，`--all` 取消 filter。完成验证：`ps` 与 `container list --state running` 请求等价。
- `[TODO] P9.15` 实现快捷 `pods`。改动：复用 pod list builder / handler，默认 ready，`--all` 包含全部状态。完成验证：`pods --all` 与 `pod list --all` 输出一致。
- `[TODO] P9.16` 实现快捷 `images`。改动：复用 `image list` handler，不复制 request 构造逻辑。完成验证：`images --output json` 和 `image list --output json` 的 kind / items 一致。
- `[TODO] P9.17` 实现 CRI-only `doctor`。改动：聚合 `Version`、`Status`、`RuntimeConfig`，diagnostics 不可用时输出 warning 而非失败。完成验证：CRI 可用但 diagnostics 不可用时退出码 0，summary 中有 diagnostics unavailable。
- `[TODO] P9.18` 为只读命令添加 mock 成功测试。改动：覆盖 version、status、runtime config、image list/inspect/fs-info、pod list/inspect、container list/inspect、shortcuts。完成验证：每个 handler 至少断言一次 request 和 output kind。
- `[TODO] P9.19` 添加 inspect NotFound 测试。改动：对 image/pod/container inspect 分别模拟 `NotFound`。完成验证：退出码均为 4，stderr 包含对象类型和输入 target。

### P10: 生命周期写命令

- `[TODO] P10.01` 实现 `image pull`。改动：在 `commands/image.rs` 构造 `PullImageRequest`，填充 `ImageSpec`、auth、可选 sandbox_config，成功输出 image ref。完成验证：auth 字段只进入 request，不进入 debug 明文。
- `[TODO] P10.02` 实现 `image pull --pod`。改动：先调用 `PodSandboxStatus(POD, verbose=false)` 获取 sandbox 上下文，再构造 `PullImageRequest.sandbox_config`；Pod 不存在时不拉取镜像。完成验证：mock 断言 Pod missing 时没有 PullImage 调用。
- `[TODO] P10.03` 实现快捷 `pull`。改动：顶层 `crs pull IMAGE` 直接复用 `image pull` handler 和输出模型。完成验证：快捷命令和底层命令对同一输入发出相同 PullImage request。
- `[TODO] P10.04` 实现 `image remove`。改动：调用 `RemoveImage`，成功输出 image 和 removed=true；NotFound 返回 4，被使用返回 6。完成验证：quiet 模式输出被删除 image ref 或约定最小值。
- `[TODO] P10.05` 实现 `runtime update --pod-cidr`。改动：先解析 CIDR list，再调用 `UpdateRuntimeConfig`，成功后输出 podCidrs 和 updated=true；空响应时按计划补一次 status verbose。完成验证：非法 CIDR 返回参数错误 2。
- `[TODO] P10.06` 实现 `pod run`。改动：用 builder 构造 `PodSandboxConfig`，调用 `RunPodSandbox`，成功输出 sandbox ID、name、namespace、state。完成验证：CNI / runtime handler 失败透传 daemon code。
- `[TODO] P10.07` 实现 `pod stop`。改动：调用 `StopPodSandbox`，传入 pod ID 和 timeout，成功输出 stopped summary。完成验证：missing pod 返回 4，timeout 非法返回 2。
- `[TODO] P10.08` 实现 `pod remove`。改动：调用 `RemovePodSandbox`，成功输出 removed summary；Pod 仍有容器时按 daemon `FailedPrecondition` 返回 6。完成验证：stderr 包含 daemon message。
- `[TODO] P10.09` 实现 `pod update-resources`。改动：解析 overhead 和 pod-resource，构造 `UpdatePodSandboxResourcesRequest`，至少一个资源字段才允许调用 RPC。完成验证：无更新字段返回参数错误 2。
- `[TODO] P10.10` 实现 `container create`。改动：先调用 `PodSandboxStatus` 校验 Pod 存在并获得 sandbox_config，再构造 `CreateContainerRequest`。完成验证：Pod missing 时返回 4 且不调用 CreateContainer。
- `[TODO] P10.11` 实现 `container start`。改动：调用 `StartContainer`，成功输出 containerId 和 started=true。完成验证：Already running 按 daemon 语义透传，不在 CLI 猜测状态。
- `[TODO] P10.12` 实现 `container stop`。改动：调用 `StopContainer`，传入 timeout 秒数，成功输出 stopped summary。完成验证：timeout 负数或非法输入返回 2。
- `[TODO] P10.13` 实现 `container remove`。改动：调用 `RemoveContainer`，成功输出 removed summary；运行中容器被 daemon 拒绝时返回 6。完成验证：错误文案包含 running / failed precondition 原因。
- `[TODO] P10.14` 实现 `container update`。改动：解析 `--resource` 和 annotation，构造 `UpdateContainerResourcesRequest`，无更新字段返回参数错误。完成验证：资源字段写入 CRI request。
- `[TODO] P10.15` 实现 `container checkpoint`。改动：调用 `CheckpointContainer`，传入 container_id、location、timeout，路径安全由 daemon 校验。完成验证：runtime unsupported 映射为 6。
- `[TODO] P10.16` 实现 `container reopen-log`。改动：调用 `ReopenContainerLog`，成功输出 reopened summary。完成验证：日志未配置时透传 failed precondition。
- `[TODO] P10.17` 实现快捷 `stop`。改动：当未指定 `--type` 时收集 container 和 pod 候选，唯一匹配才执行；多个匹配返回歧义错误并提示 `--type`。完成验证：image target 不可 stop，返回参数/语义错误。
- `[TODO] P10.18` 实现快捷 `rm`。改动：按 container、pod、image 收集候选，唯一匹配后调用底层 remove；歧义时不猜测。完成验证：歧义错误提示 `--type container|pod|image`。
- `[TODO] P10.19` 为写命令添加成功测试。改动：每个写命令 mock RPC 成功，断言 summary、quiet 输出和 JSON kind。完成验证：所有 destructive 操作都通过 daemon RPC，不访问本地状态文件。
- `[TODO] P10.20` 为写命令错误映射添加测试。改动：覆盖 FailedPrecondition、PermissionDenied、Unavailable，以及 NotFound。完成验证：退出码分别为 6、13、125、4。

### P11: Streaming 命令

- `[TODO] P11.01` 定义 streaming 参数类型。改动：在 `src/crs/streaming.rs` 新增 `ExecStreamOptions`、`AttachStreamOptions`、`PortForwardOptions`，字段覆盖 tty、stdin、stdout、stderr、resize、protocol、forward ports。完成验证：命令层不直接操作底层 stream flags。
- `[TODO] P11.02` 实现 TTY raw mode guard。改动：用 RAII guard 保存并恢复终端状态，任何错误路径、Ctrl-C、panic unwind 安全路径都尽量恢复。完成验证：测试模拟错误后确认 restore 被调用。
- `[TODO] P11.03` 实现 terminal resize。改动：读取初始终端尺寸，监听 resize 事件，通过 channel 发送给 streaming 协议层。完成验证：无 TTY 环境下不失败，只跳过 resize。
- `[TODO] P11.04` 实现 websocket streaming 连接。改动：根据 CRI 返回 URL 建立 websocket，处理 token、TLS、stdout/stderr/error channel。完成验证：mock websocket server 能收到 exec/attach 数据。
- `[TODO] P11.05` 实现 spdy streaming 连接或封装兼容层。改动：复用项目现有 streaming 协议常量和行为约定，按 websocket、spdy 顺序尝试或按 `--protocol` 强制选择。完成验证：不支持 spdy 时错误文案明确，不静默降级到错误协议。
- `[TODO] P11.06` 实现 stdout/stderr/error 分流。改动：TTY 模式自动关闭 stderr；非 TTY 模式把 stdout 写 stdout、stderr 写 stderr、error stream 转 CliError。完成验证：mock stream 分别发送 stdout/stderr 后落到正确 writer。
- `[TODO] P11.07` 实现 stdin 转发和 EOF shutdown。改动：stdin 打开时把本地输入转发远端；本地 EOF 后关闭远端 stdin channel，不强制断开 stdout/stderr。完成验证：stdin=false 时不读取 stdin。
- `[TODO] P11.08` 实现 `streaming::exec`。改动：连接 URL、设置 raw mode、转发 IO、处理 resize、提取远端 exit code；协议未提供 exit code 且正常关闭时返回 0。完成验证：远端 exit code 3 时返回 3。
- `[TODO] P11.09` 实现 `streaming::attach`。改动：连接 attach URL 并接管 IO，Ctrl-C 只断开本次 attach session，不调用 stop container。完成验证：中断返回 130 或约定 attach 断开语义，不发送 StopContainer。
- `[TODO] P11.10` 实现 `streaming::port_forward`。改动：为每个 `LOCAL:REMOTE` 创建本地 listener，每个连接独立 task 转发到 streaming URL，父任务负责 shutdown。完成验证：任一 bind 失败时关闭已绑定 listener。
- `[TODO] P11.11` 实现 `container exec` 和顶层 `exec`。改动：命令 handler 调用 CRI `Exec` 获取 URL，再交给 `streaming::exec`；顶层 `exec` 复用 container exec。完成验证：command 为空返回参数错误 2。
- `[TODO] P11.12` 实现 `container exec-sync`。改动：调用 CRI `ExecSync`，普通模式把 stdout 写 stdout、stderr 写 stderr，JSON 模式进入 envelope，最终退出码为 response exit_code。完成验证：`false` 远端 exit 1 时 CLI exit 1。
- `[TODO] P11.13` 实现 `container attach`。改动：调用 CRI `Attach` 获取 URL，再交给 `streaming::attach`，校验 stdin/stdout/stderr 至少一个开启。完成验证：全部关闭时参数错误。
- `[TODO] P11.14` 实现 `pod port-forward`。改动：解析 forward，调用 CRI `PortForward` 获取 URL，再交给 streaming port_forward。完成验证：Pod missing 返回 4，本地端口冲突返回 1。
- `[TODO] P11.15` 添加 raw mode 恢复测试。改动：模拟 streaming 连接失败、远端错误、本地中断三种路径。完成验证：每条路径都恢复终端状态。
- `[TODO] P11.16` 添加 exec-sync exit code 测试。改动：mock `ExecSyncResponse` exit_code 为 0、1、127，断言 CLI 退出码透传。完成验证：stderr 不污染 JSON stdout。
- `[TODO] P11.17` 添加 port-forward listener 清理测试。改动：先成功绑定一个端口，再让第二个端口失败，确认第一个 listener 被关闭。完成验证：测试后端口可重新绑定。

### P12: 组合验证 `run`

- `[TODO] P12.01` 实现 `run` pull policy。改动：在 `commands/run.rs` 根据 `missing|always|never` 决定是否调用 `ImageStatus` 和 `PullImage`；missing 只在 image 不存在时拉取。完成验证：mock 测试断言三种策略的 RPC 顺序。
- `[TODO] P12.02` 实现临时 Pod sandbox 创建。改动：未传 `--pod` 时构造 PodSandboxConfig，默认 namespace 为 `default`，name 为 `crius-<short-id>`，调用 `RunPodSandbox` 并记录由 CLI 创建。完成验证：成功 summary 包含 podSandboxId。
- `[TODO] P12.03` 实现复用已有 Pod。改动：传入 `--pod` 时调用 `PodSandboxStatus` 校验存在并取得 sandbox context，不创建新 sandbox，不把该 Pod 放入 cleanup 列表。完成验证：`--rm` 不删除用户传入的 Pod。
- `[TODO] P12.04` 实现 container create 阶段。改动：合并 RunArgs 中的 container 创建参数，构造 `CreateContainerRequest`，记录创建出的 container ID。完成验证：create 失败时如果 pod 已创建，warning 中给出 pod 清理命令。
- `[TODO] P12.05` 实现 container start 阶段。改动：调用 `StartContainer`，成功后进入 detach 或前台执行路径。完成验证：start 失败时输出已创建 container ID 和建议清理命令。
- `[TODO] P12.06` 实现 `--detach`。改动：detach 模式在 start 成功后输出 container ID，跳过 attach / exec-sync，不自动清理，除非未来明确支持 detach+rm 语义。完成验证：quiet 模式只输出 container ID。
- `[TODO] P12.07` 实现 `--exec-mode sync`。改动：前台 sync 模式在 start 后调用 `ExecSync` 执行用户命令或默认 command，stdout/stderr 和 exit code 语义与 container exec-sync 一致。完成验证：远端 exit code 透传。
- `[TODO] P12.08` 实现 attach 前台模式。改动：前台 attach 模式调用 CRI `Attach` 获取 URL 并复用 streaming attach，支持 stdin/tty/resize。完成验证：TTY 错误路径恢复终端。
- `[TODO] P12.09` 实现 `--rm` cleanup。改动：按 container stop、container remove、pod stop、pod remove 顺序清理，只操作本次 run 创建的对象；cleanup 失败写 warning。完成验证：cleanup RPC 顺序测试通过。
- `[TODO] P12.10` 实现失败遗留资源提示。改动：任何阶段失败时，把已确认创建但未清理成功的 pod/container ID、对象类型和建议命令写入 stderr 或 JSON warnings。完成验证：create after pod failure 的 warning 可被测试断言。
- `[TODO] P12.11` 添加 pull policy mock 测试。改动：分别模拟 image 存在、不存在、pull 失败，断言 RPC 顺序和错误码。完成验证：`never` 不调用 PullImage。
- `[TODO] P12.12` 添加 `run --rm` cleanup 顺序测试。改动：mock 所有生命周期 RPC，记录调用序列。完成验证：只清理 CLI 创建对象，复用 Pod 不被删除。
- `[TODO] P12.13` 添加失败遗留 ID warning 测试。改动：模拟 pod 创建成功、container create 失败，断言 warning 中含 pod ID 和 `crs pod remove` 建议。完成验证：错误退出码仍来自原始失败。
- `[TODO] P12.14` 手工验证基础 run 流程。改动：在可运行 daemon 环境执行 `crs run --rm "$CRIUS_TEST_IMAGE" echo ok`。完成验证：命令输出 `ok`，退出码 0，运行后无本次创建的残留 pod/container。

### P13: Diagnostics CLI 命令

- `[TODO] P13.01` 实现 `config show`。改动：在 `commands/config.rs` 优先调用 diagnostics `EffectiveConfig(include_sensitive=false)`，diagnostics 不可用时降级读取 `Status(verbose=true).info["config"]`。完成验证：输出 kind 为 `EffectiveConfig`，敏感字段被脱敏。
- `[TODO] P13.02` 实现 `config reload-status`。改动：从 EffectiveConfig 或 verbose status 中提取 watcher、last reload、last error、CNI watcher 状态，缺失字段显示 `unknown` 并 warning。完成验证：字段缺失不导致 panic 或 fatal error。
- `[TODO] P13.03` 实现 `runtime handlers`。改动：优先调用 diagnostics `RuntimeHandlers`，降级读取 verbose status `runtimeBackend`，`--verbose` 控制附加字段展示。完成验证：diagnostics 和降级路径都能输出 `RuntimeHandlers` kind。
- `[TODO] P13.04` 实现 `image transfers`。改动：调用 diagnostics `ImageTransfers(include_completed)`，diagnostics 不可用时尝试 verbose status `imageTransfers`，输出 ref、status、updated、error。完成验证：默认不显示 completed transfer。
- `[TODO] P13.05` 实现 `image config`。改动：调用 diagnostics `EffectiveConfig` 并提取 image 配置摘要，降级读取 verbose status `config.image`。完成验证：registry auth 明文不进入输出。
- `[TODO] P13.06` 实现 `container logs` text 输出。改动：调用 diagnostics `ContainerLog` stream，按 chunk data 写 stdout，timestamps 由 daemon chunk 或 CLI 选项控制。完成验证：text 模式不使用 JSON envelope。
- `[TODO] P13.07` 实现 `container logs --output json`。改动：把每个 log chunk 输出为 NDJSON 或约定 JSON stream，包含 data、stream、timestamp。完成验证：每行都能被 `serde_json` 单独解析。
- `[TODO] P13.08` 实现快捷 `logs`。改动：顶层 `crs logs CONTAINER` 复用 `container logs` handler，不复制 diagnostics 调用。完成验证：两者对相同参数生成相同 ContainerLogRequest。
- `[TODO] P13.09` 实现 `recovery status`。改动：调用 diagnostics `RecoveryStatus`，输出 status、last_startup、unhealthy_object_count、ledger summary、warnings。完成验证：unhealthy count 非 0 时命令仍可返回 0。
- `[TODO] P13.10` 实现 `recovery check`。改动：调用 `RecoveryCheck(execute=false)`，输出 dry-run actions，不修改 daemon 状态。完成验证：request execute 字段为 false。
- `[TODO] P13.11` 实现 `recovery repair --dry-run`。改动：调用 `RecoveryCheck(execute=false)`，输出 `RecoveryRepairPlan` kind，明确没有执行动作。完成验证：summary.dryRun 为 true。
- `[TODO] P13.12` 实现 `recovery repair --execute`。改动：调用 `RecoveryCheck(execute=true)`，统计 executed 和 failed，存在 item error 时退出码为 1。完成验证：RPC OK 但 item failure 时 CLI 仍返回 1。
- `[TODO] P13.13` 实现 `gc candidates`。改动：调用 diagnostics `ContentGc(execute=false)`，输出候选对象、路径、原因、大小和 totalBytes。完成验证：不执行任何删除动作。
- `[TODO] P13.14` 实现 `gc run --dry-run`。改动：调用 `ContentGc(execute=false)`，输出 `GcPlan` kind 和 dryRun=true。完成验证：request execute 为 false。
- `[TODO] P13.15` 实现 `gc run --execute`。改动：调用 `ContentGc(execute=true)`，输出 reclaimedBytes、failed count 和每个候选的 deleted/error。完成验证：单项失败时退出码为 1。
- `[TODO] P13.16` 实现 `debug network`。改动：聚合 CRI status verbose、EffectiveConfig 中的 PodCIDR、CNI template、CNI watcher 和最近错误。完成验证：JSON kind 为 `DebugNetwork`。
- `[TODO] P13.17` 实现 `debug runtime`。改动：聚合 CRI Version、RuntimeConfig、RuntimeHandlers，展示 runtime version、cgroup driver、handler 摘要。完成验证：diagnostics 不可用时仍显示 CRI 可得信息并 warning。
- `[TODO] P13.18` 实现 `debug shims`。改动：调用 diagnostics `ShimStatus`，展示 shim count、pid、socket、task state 和 error。完成验证：空 shim 列表退出码 0。
- `[TODO] P13.19` 实现 `debug nri`。改动：调用 diagnostics `NriStatus`，展示 enabled、CDI、plugin path、blockio、RDT。完成验证：NRI disabled 不视作命令失败。
- `[TODO] P13.20` 实现 `debug security`。改动：调用 diagnostics `SecurityStatus`，展示 seccomp、AppArmor、SELinux、rootless、devices policy 摘要。完成验证：devices_policy_json 解析失败时保留 raw 并 warning。
- `[TODO] P13.21` 实现 `debug cgroups`。改动：聚合 CRI `RuntimeConfig` 和 verbose status 中的 cgroup version/controller 信息。完成验证：输出 version、driver、controllers summary。
- `[TODO] P13.22` 实现 `debug streaming`。改动：从 EffectiveConfig 和 ServerInfo 提取 streaming listener、TLS、token TTL、port-forward timeout。完成验证：streaming disabled 时显示 enabled=false，不返回错误。
- `[TODO] P13.23` 实现 `debug metrics`。改动：从 EffectiveConfig 和 metrics endpoint 状态提取 endpoint、enabled、TLS、collector、pod metrics 配置。完成验证：endpoint disabled 显示 failed precondition 或 warning 语义一致。
- `[TODO] P13.24` 实现 `debug tracing`。改动：从 EffectiveConfig 提取 tracing enabled、exporter、endpoint、sampling rate，敏感 header 脱敏。完成验证：JSON kind 为 `DebugTracing`。
- `[TODO] P13.25` 实现 `debug rootless`。改动：基于 SecurityStatus 和相关 config 输出 rootless enabled、userns、network、path 状态。完成验证：rootless disabled 不视作失败。
- `[TODO] P13.26` 为 diagnostics 命令添加 JSON kind 测试。改动：每个 diagnostics / debug 命令至少一个 JSON 输出测试，断言 kind、summary 必填字段、warnings 行为。完成验证：kind 表变更会被测试捕获。
- `[TODO] P13.27` 添加 diagnostics unavailable 测试。改动：模拟 daemon 只有 CRI 无 diagnostics，断言需要 diagnostics 的命令返回统一错误；支持降级的命令输出 warning。完成验证：错误文案包含 `diagnostics service is not available from this crius daemon`。

### P14: Events、Stats 与 Metrics

- `[TODO] P14.01` 实现 `events` stream handler。改动：在 `commands/events.rs` 调用 CRI `GetContainerEvents`，持续读取 stream，转换为事件 view。完成验证：正常 EOF 返回 0。
- `[TODO] P14.02` 实现 events table 输出。改动：首次输出 header，后续每个事件输出 `TIME`、`TYPE`、`CONTAINER`、`POD` 行，支持 no-trunc。完成验证：连续事件不会重新输出 header。
- `[TODO] P14.03` 实现 events JSON / NDJSON 输出。改动：流式场景使用 newline-delimited JSON 或约定 envelope stream，每条事件可独立消费。完成验证：每行 JSON 都能解析且包含 event time/type。
- `[TODO] P14.04` 实现 events 中断语义。改动：正常 EOF 返回 0，SIGINT / Ctrl-C 转为 interrupted 返回 130，stream error 返回 1 或对应 RPC code。完成验证：测试模拟 EOF 和 interrupt。
- `[TODO] P14.05` 实现顶层 `stats`。改动：在 `commands/stats.rs` 调用 `ListContainerStats`，支持 label filter，输出 `ContainerStats` kind。完成验证：空列表返回 0。
- `[TODO] P14.06` 实现 `container stats [ID]`。改动：指定 ID 时调用 `ContainerStats`，未指定时调用 `ListContainerStats`，支持 pod 和 label filter。完成验证：单个和列表路径都转换为统一 view。
- `[TODO] P14.07` 实现 `pod stats [POD]`。改动：指定 POD 时调用 `PodSandboxStats`，未指定时调用 `ListPodSandboxStats`，输出 CPU、memory、network、pids。完成验证：stats 不可用时显示 warning 或明确错误。
- `[TODO] P14.08` 实现 `pod metrics`。改动：调用 `ListPodSandboxMetrics`，输出 metrics items 和 count summary。完成验证：daemon 不支持时返回清晰错误。
- `[TODO] P14.09` 实现 `metrics descriptors`。改动：调用 CRI `ListMetricDescriptors`，输出 descriptor name、help、type、labels 等字段。完成验证：JSON kind 为 `MetricDescriptors`。
- `[TODO] P14.10` 实现 `metrics scrape --output text`。改动：根据 daemon metrics endpoint 配置抓取 metrics 快照，原样输出文本内容和 content type。完成验证：stdout 只包含 metrics 文本。
- `[TODO] P14.11` 实现 `metrics scrape --output json`。改动：抓取 metrics 后输出 `MetricsScrape` summary，包含 contentType、bytes、scrapedAt，可选择不内嵌大文本。完成验证：JSON stdout 合法且 bytes 与实际内容长度一致。
- `[TODO] P14.12` 添加空 stats 测试。改动：mock 空 stats response，断言退出码 0、items 为空、summary.count 为 0。完成验证：空列表不被当成错误。
- `[TODO] P14.13` 添加 metrics endpoint disabled 测试。改动：模拟 daemon 配置禁用或 endpoint 不可用，断言退出码 6 或约定 failed precondition 语义。完成验证：错误文案提示需要启用 metrics endpoint。

### P15: Completion、文档与发布门禁

- `[TODO] P15.01` 实现 shell completion 命令。改动：在 `commands/completion.rs` 调用 clap completion generator，支持 bash、zsh、fish、powershell，输出到 stdout。完成验证：`crs completion bash` stdout 非空且退出码 0。
- `[TODO] P15.02` 添加 unsupported shell 测试。改动：在 `tests/crs_args.rs` 验证非法 shell 名称返回参数错误 2。完成验证：错误来自 clap value enum，不进入 handler。
- `[TODO] P15.03` 更新 README 构建说明。改动：增加 `crs` binary 的构建命令、默认 endpoint、常见全局参数和与 `crictl` 的关系说明。完成验证：README 示例命令与实际 clap 表面一致。
- `[TODO] P15.04` 补充常用命令输出示例。改动：在文档中添加 version、status、image list、pod list、container list、run、debug 的简短示例。完成验证：示例不包含敏感路径或环境特定 ID。
- `[TODO] P15.05` 增加 golden 输出样例测试。改动：为 version、status、image list、pod list、container list 固定 mock response 和期望 table/json 输出。完成验证：formatter 或列名变化时测试失败。
- `[TODO] P15.06` 文档化发布门禁命令。改动：记录 `cargo build --features shim --bins`、`cargo test --features shim`、completion 生成命令和可选手工验证步骤。完成验证：发布前检查列表能直接复制执行。
- `[TODO] P15.07` 跑完整构建门禁。改动：执行 `cargo build --features shim --bins`，修复 feature、bin、codegen、daemon 注册相关编译问题。完成验证：`crius`、`crius-shim`、`crs` 均构建成功。
- `[TODO] P15.08` 跑完整测试门禁。改动：执行 `cargo test --features shim`，修复单元测试、集成测试、mock server 和 feature 组合问题。完成验证：测试套件通过。
- `[TODO] P15.09` 验证 completion 生成。改动：执行 `./target/debug/crs completion bash >/tmp/crs.bash`，确认文件非空。完成验证：命令不需要 daemon，退出码 0。

### P16: 最终手工验证

- `[TODO] P16.01` 启动本地 daemon。操作：执行 `sudo ./target/debug/crius --config /etc/crius/crius.conf`，确认默认 socket `unix:///run/crius/crius.sock` 已创建。完成验证：daemon 日志无启动失败，`crictl version` 或 `crs version` 可连接。
- `[TODO] P16.02` 手工验证 version。操作：执行 `sudo ./target/debug/crs version` 和 `--output json` 版本。完成验证：table 显示 runtime name/version/API version，JSON kind 为 `RuntimeVersion`。
- `[TODO] P16.03` 手工验证 status。操作：执行 `sudo ./target/debug/crs status` 和 `status --verbose --output json`。完成验证：runtime/network condition 可读，verbose JSON 合法或 warning 明确。
- `[TODO] P16.04` 手工验证 image list。操作：执行 `sudo ./target/debug/crs image list`、`images`、`image list --quiet`。完成验证：快捷命令与底层命令输出一致，quiet 无表头。
- `[TODO] P16.05` 手工验证 image pull。操作：执行 `sudo ./target/debug/crs image pull "$CRIUS_TEST_IMAGE"`。完成验证：拉取成功输出 image ref，再次 list 能看到镜像。
- `[TODO] P16.06` 手工验证 run。操作：执行 `sudo ./target/debug/crs run --rm "$CRIUS_TEST_IMAGE" echo ok`。完成验证：stdout 包含 `ok`，退出码 0，`ps --all` 和 `pods --all` 不留下本次创建对象。
- `[TODO] P16.07` 手工验证 events。操作：执行 `sudo ./target/debug/crs events`，制造容器事件后观察输出，再用 Ctrl-C 退出。完成验证：事件行可读，Ctrl-C 返回 130 或文档约定的 interrupted 语义。
- `[TODO] P16.08` 手工验证 diagnostics 命令。操作：至少执行 `config show`、`debug network`、`recovery status`、`gc candidates`。完成验证：diagnostics 可用时输出结构化摘要，不可用时错误文案清晰且不影响 CRI 基础命令。

## 完成定义

`crs` 计划视为完成时必须同时满足：

- 覆盖矩阵中每个 CRI RuntimeService / ImageService RPC 都有 CLI 命令或明确组合命令覆盖。
- 项目诊断能力表中每一项都有 diagnostics RPC、CLI 命令、输出 kind 和验收测试。
- 每个命令都有 clap args、handler、request 构造、输出模型、错误映射测试。
- 所有 destructive 命令都有 dry-run 或明确的前置校验。
- CLI 不读取 SQLite、bundle、shim 工作目录等内部文件。
- `--output json` 的 stdout 是合法 JSON 或 NDJSON。
- 错误写 stderr，且不泄露 auth、token、敏感配置。
- streaming 命令在所有错误路径恢复终端状态。
- `cargo build --features shim --bins` 通过。
- 核心命令具备 mock server 集成测试。
- root/runtime 依赖环境下手工验证命令通过。

## 常用命令示例

```bash
crs version
crs status
crs status --verbose --output json
crs config show
crs runtime config
crs image list
crs image pull "$CRIUS_TEST_IMAGE"
crs pod list
crs container list
crs run --rm "$CRIUS_TEST_IMAGE" echo ok
crs events
crs recovery status
crs gc candidates
crs debug nri
crs debug security
```
