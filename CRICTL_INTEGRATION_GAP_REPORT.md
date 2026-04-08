# Crius 对接 `crictl` 能力差距分析报告

## 1. 报告信息

- 分析日期: 2026-04-08
- 分析对象: `crius` 项目当前工作区代码
- 对照客户端: `crictl v1.29.0`
- 核心结论: 项目已经具备 CRI Runtime/Image Service 的主框架与基础生命周期能力；本轮进一步补齐后，6.3（`runp/pods/inspectp/stopp/rmp`）在当前架构下已形成完整闭环，并新增了 `runtime_handler` 配置化校验、Pod `SELinux/seccomp` 执行链路、netns 地址探测与更完整的状态恢复语义。`TTY` 容器的 console socket / shim console 主链路也已打通；`attach` 已进一步从“固定假 URL / 纯骨架”推进到“fresh live container 上可被 `crictl` 真实使用”的阶段，`exec` 也已具备 token 化 streaming 入口。但距离“完整对接 `crictl`”仍有明显差距，当前最大的剩余阻塞点已经收敛为：`exec` 数据面未实现，以及 `attach` 的 resize/exit-status/重启恢复语义仍未补齐。

### 1.1 本轮同步与复核基线

- Git 同步方式: `git pull --ff-only`
- Git 同步结果: `Already up to date`
- 复核基线提交: `37c43bcec909fc608fd468f649311d705977b9c5`
- 基线提交摘要: `37c43bc (HEAD -> main, origin/main, origin/HEAD) Merge pull request #7 from TDnorthgarden/fix/container-lifecycle-id-matching`
- 复核结论: 本轮在 `origin/main` 基线之上，结合当前工作区增量实现继续复核。6.3/6.4 的增量落地（Pod 状态字段恢复、namespace 解析、stop 退出码语义改进）已同步进本报告。

### 1.2 本轮增量同步（2026-04-08）

- 已同步的代码改动文件: `src/main.rs`、`src/server/mod.rs`、`src/pod/mod.rs`、`src/runtime/mod.rs`
- 已同步的关键增量:
  - `runp/inspectp`：Pod 内部状态新增 `additional_ips` 与更多安全/资源字段持久化恢复
  - `runp`：新增 `runtime_handler` 配置化校验，以及 `SELinux/seccomp` 下传与持久化
  - `stopp`：停止 Pod 内业务容器后回读运行时状态，避免固定写死 `Stopped(0)`
  - `runp`：新增 netns 地址探测，改善 `inspectp.network` 的 `ip/additionalIPs` 回填
  - `create`：按 `namespace_options` 解析并注入 `pid/ipc` namespace path（Pod/Target 模式）
  - runtime 新增 `container_pid` 查询，供 namespace path 解析复用

## 2. 分析方法

本报告采用以下方法进行比对：

1. 使用本机 `crictl --help` 与子命令 `--help` 收集命令面。
2. 根据 CRI v1 协议定义，建立 `crictl` 命令到 CRI RPC/字段的映射关系。
3. 检查 `RuntimeService` 与 `ImageService` 的服务实现。
4. 继续向下检查支撑模块，包括 `runtime`、`pod`、`shim`、`metrics`、`storage`、`image/layer`，判断是否存在“底层已有能力，但尚未接入服务层”的情况。
5. 额外检查 daemon 重启恢复后的状态语义，避免只看冷启动路径。

本次分析重点关注“对 `crictl` 是否真正可用”，因此不仅检查“有没有 RPC”，也检查：

- 返回值是否满足 `crictl` 预期
- 关键字段是否真实有效
- 是否只是占位响应
- 是否在 daemon 重启后仍能正常工作

## 3. 评级标准

- `L3 接近完整`: 命令路径已基本闭环，字段较完整，语义基本正确
- `L2 基础可用`: 主流程可工作，但缺少部分字段或边界能力
- `L1 部分实现`: RPC 能调通，但返回值或语义明显不完整
- `L0 占位/未接通`: 只有空响应、固定 URL、空数组、空对象，或关键链路完全未落地

## 4. 总体结论

本轮在执行 `git pull --ff-only` 后，仓库已经是最新状态，因此下面的结论可视为“针对当前最新 `main` 分支代码”的复核结论。

### 4.1 已具备的能力

- RuntimeService 与 ImageService 已经搭起完整服务骨架，主要 CRI v1 RPC 均有实现入口，见 `src/server/mod.rs` 与 `src/image/mod.rs`。
- 容器基础生命周期 `CreateContainer / StartContainer / StopContainer / RemoveContainer` 已接到 `runc`，见 `src/server/mod.rs:869-1082` 与 `src/runtime/mod.rs:519-666`。
- Pod 沙箱基础生命周期 `RunPodSandbox / StopPodSandbox / RemovePodSandbox` 已接到 `PodSandboxManager`，见 `src/server/mod.rs:301-385`、`src/server/mod.rs:389-785`、`src/pod/mod.rs:147-295`。
- 镜像基本拉取、查询、删除已具备初版实现，见 `src/image/mod.rs:517-757`。
- 项目内已经存在事件表、metrics 采集器、shim attach 基础组件、镜像层管理器等支撑模块，但多数尚未贯通到 CRI 出口，见 `src/storage/mod.rs:109-142`、`src/metrics/mod.rs:135-220`、`src/shim/io.rs:85-150`、`src/image/layer.rs:262-339`。

### 4.2 当前最主要的缺口

- `attach` 主链路已经打通：fresh live container 的 non-TTY stdout、stdin/stdout 交互，以及 TTY 输出 attach 都已通过 `crictl` 实机验证；但仍缺 resize、exit-status/error 细化语义和 daemon 重启后的 attach 恢复；`exec` 仍停留在 token URL + 骨架阶段，`port-forward` 仍未接通。
- `logs` 的 `log_directory/log_path` 已开始落地，但真实 stdout/stderr 写日志与 `ReopenContainerLog` 仍未贯通。
- `stats` / `statsp` / `metricsp` 虽有 metrics 基础模块，但服务层仍返回空。
- `events` 不是持续事件流，而是当前状态快照；同时事件类型值存在错误使用。
- `inspect` / `inspectp` / `inspecti` 已有改善，但仍有部分关键字段或语义缺口。
- `update`、`checkpoint`、`imagefsinfo`、`runtime-config` 仍接近占位。
- daemon 重启后，Pod 级恢复语义已有改善，能够回填 `pod_manager` 中的 Pod 记录；但 shim、日志流与更深层运行态仍未完整恢复。

## 5. `crictl` 命令矩阵

| `crictl` 命令 | 主要依赖的 CRI 能力 | 当前评级 | 结论 |
| --- | --- | --- | --- |
| `version` | `Version` | `L3` | 已实现，返回固定版本字符串 |
| `info` | `Status` | `L1` | 能返回结果，但健康状态为硬编码，真实性不足 |
| `runp` | `RunPodSandbox` | `L3` | 主链路已闭环，`runtime_handler`/安全上下文/资源与持久化语义可用 |
| `pods` | `ListPodSandbox` | `L3` | 列表语义完整度明显提升，支持恢复后 `runtime_handler` 回填与内部注解过滤 |
| `inspectp` | `PodSandboxStatus` | `L3` | 已回填 `network/linux/runtime_handler/containers_statuses`，并补充 verbose 调试信息 |
| `stopp` | `StopPodSandbox` | `L3` | 主链路闭环，已回读并回填 Pod 内容器实际停止状态与退出码 |
| `rmp` | `RemovePodSandbox` | `L3` | 级联清理与恢复后删除语义可用，行为更接近主流 CRI 运行时 |
| `create` | `CreateContainer` | `L2` | 已接入 `tty/log_path/resources/设备映射/Pod netns` 等关键字段 |
| `run` | `RunPodSandbox + CreateContainer + StartContainer` | `L2` | 基础可用，Pod/Container 配置语义较上一版更完整 |
| `start` | `StartContainer` | `L2` | 基础可用 |
| `stop` | `StopContainer` | `L2` | 基础可用，退出码语义已有改进，但仍未达到事件驱动精度 |
| `rm` | `RemoveContainer` | `L2` | 基础可用 |
| `ps` | `ListContainers` | `L1` | 可列出，但状态与元数据可能陈旧或退化 |
| `inspect` | `ContainerStatus` | `L1` | 可返回对象，但关键状态字段未填 |
| `exec --sync` | `ExecSync` | `L1` | 只拿到退出码，拿不到 stdout/stderr，timeout 未落地 |
| `exec` | `Exec` | `L1` | 已返回项目内 token 化 streaming URL，并具备独立 streaming 骨架，但数据面仍未实现 |
| `attach` | `Attach` | `L2` | fresh live container 的 non-TTY stdout、stdin/stdout 交互与 TTY 输出 attach 已实机验证通过，但仍缺 resize/更完整 remotecommand 语义与重启恢复 |
| `logs` | `ContainerStatus.log_path` + host log file + `ReopenContainerLog` | `L0` | 日志路径和 reopen 均未贯通 |
| `port-forward` | `PortForward` | `L0` | 只返回固定 URL，没有端口转发链路 |
| `stats` | `ContainerStats` / `ListContainerStats` | `L0` | 服务层返回空 |
| `statsp` | `PodSandboxStats` / `ListPodSandboxStats` | `L0` | 服务层返回空 |
| `metricsp` | `ListMetricDescriptors` / `ListPodSandboxMetrics` | `L0` | 服务层返回空 |
| `pull` | `PullImage` | `L2` | 基础拉取可用，但元数据、认证、尺寸等语义不完整 |
| `images` | `ListImages` | `L1` | 可列出，但不支持 filter，且可能重复 |
| `inspecti` | `ImageStatus` | `L1` | 可查已有镜像，但未命中时行为不符合 CRI 规范 |
| `rmi` | `RemoveImage` | `L1` | 可删除目录，但未接镜像层引用计数与 GC |
| `imagefsinfo` | `ImageFsInfo` | `L0` | 直接返回空数组 |
| `update` | `UpdateContainerResources` | `L0` | 只记日志，未调用 `runc update` |
| `checkpoint` | `CheckpointContainer` | `L0` | 空实现 |
| `runtime-config` | `RuntimeConfig` | `L0/L1` | 能调通，但返回内容近乎空配置 |
| `events` | `GetContainerEvents` | `L0` | 不是持续流，且事件类型使用错误 |
| `config` | 客户端本地配置 | `N/A` | 不属于 runtime 服务对接范围 |
| `completion` / `help` | 客户端本地能力 | `N/A` | 不属于 runtime 服务对接范围 |

## 6. 逐项详细分析

### 6.1 `version`

实现位置：

- `src/server/mod.rs:288-297`

结论：

- `Version` 已实现，足以支撑 `crictl version`。
- 目前返回值为固定字符串，不反映真实构建信息，但不影响基础可用性。

### 6.2 `info`

实现位置：

- `src/server/mod.rs:656-686`

问题：

- `Status` 直接返回固定的 `RuntimeReady=true`、`NetworkReady=true`。
- 没有检查 `runc`、网络子系统、镜像子系统、shim、CNI 是否真实可用。
- `StatusRequest.verbose` 没有参与逻辑，属于“忽略 verbose 语义”。

影响：

- `crictl info` 能返回内容，但更像静态说明，不是健康检查。

### 6.3 `runp` / `pods` / `inspectp` / `stopp` / `rmp`

相关实现：

- `RunPodSandbox`: `src/server/mod.rs:1070-1387`
- `StopPodSandbox`: `src/server/mod.rs:1395-1520`
- `RemovePodSandbox`: `src/server/mod.rs:1765-1862`
- `PodSandboxStatus`: `src/server/mod.rs:1865-1978`
- `ListPodSandbox`: `src/server/mod.rs:1981-2040`
- 底层 Pod 管理: `src/pod/mod.rs:220-456`

本轮处理结果：

1. `runtime_handler` 从“仅接受默认值”升级到“支持配置化 handler 集”。
   - `RuntimeConfig` 新增 `runtime_handlers`，并在服务启动时做去重/归一化。
   - `RunPodSandbox` 会校验请求的 `runtime_handler` 是否在允许列表中，避免硬编码只允许单值。
   - `ListPodSandbox/PodSandboxStatus/recover_state` 在恢复路径上也会做 handler 合法性回填，见 `src/server/mod.rs:445-753`、`src/server/mod.rs:2005-2032`。

2. Pod 安全上下文补齐到可执行链路。
   - `runp` 现在会解析并下传 `SELinux` 与 `seccomp`（含 deprecated `seccomp_profile_path` 兼容）到 `PodSandboxConfig` 与内部持久化状态，见 `src/server/mod.rs:1087-1325`。
   - pause 容器创建会带上 `selinux_label/seccomp_profile`，并在 runtime 侧落到 OCI `process.selinux_label/linux.mount_label/linux.seccomp`，见 `src/pod/mod.rs:315-354`、`src/runtime/mod.rs:699-818`。

3. Pod 网络状态进一步补强。
   - Pod 创建后会额外探测 netns 实际地址（`ip -n <ns> -o addr show`），用于补齐 `network.ip/interfaces`，见 `src/pod/mod.rs:145-218`、`src/pod/mod.rs:258-272`。
   - `inspectp` 的 `additional_ips` 与恢复后网络状态回填已贯通，见 `src/server/mod.rs:463-494`、`src/server/mod.rs:941-1062`。

4. `inspectp` 与 `stopp/rmp` 语义继续收敛到 CRI 预期。
   - `inspectp` 已包含 `network/linux/runtime_handler/containers_statuses`，且 `verbose` 增加 `selinuxLabel/seccompProfile` 等排障信息，见 `src/server/mod.rs:1888-1959`。
   - `stopp` 会回读并回填 Pod 内业务容器的真实停止状态与退出码，不再统一写 `Stopped(0)`，见 `src/server/mod.rs:1414-1480`。
   - `rmp` 继续保留恢复后级联清理路径（容器、netns、持久化）闭环，见 `src/server/mod.rs:1765-1862`。

残余问题（6.3 范围内）：

1. 虽然 `runtime_handler` 已支持多名称配置，但当前仍共享同一 runtime 后端实现（并非真正多 runtime 二进制调度）。
2. 默认网络管理器在未配置有效 CNI 输出时，`inspectp.network` 仍可能为空（当前已做 best-effort 探测与回填）。

结论：

- 6.3 目标链路（`runp/pods/inspectp/stopp/rmp`）在当前架构下已形成完整闭环，可按 `crictl` 主路径稳定使用；剩余问题主要是“多 runtime 后端能力”与“环境依赖型网络可见性”。

实机验证：

- 已使用 `CRIUS_PAUSE_IMAGE="cr.kylinos.cn/k8s/pause:3.9" target/debug/crius` 启动 daemon，并通过 `crictl` 顺序执行 `runp -> pods -> inspectp -> stopp -> rmp` 做验证。
- 顺序验证结果为通过：
  - `crictl runp /root/crius/.tmp/verify-pod-config.json`
  - `crictl pods`
  - `crictl inspectp <pod-id>`
  - `crictl stopp <pod-id>`
  - `crictl rmp <pod-id>`
- 观察到的有效输出包括：
  - `pods` 列表已显示 `RUNTIME=runc`
  - `inspectp` 已返回 `runtimeHandler/logDirectory/netnsPath/pauseContainerId/linux.namespaces`
  - 若环境中可探测到 netns 地址，`inspectp.network` 会返回 `ip/additionalIPs`；若网络插件未提供可见地址则仍可能为 `null`

### 6.4 `create` / `run` / `start` / `stop` / `rm`

相关实现：

- `CreateContainer`: `src/server/mod.rs:1776-2146`
- `StartContainer`: `src/server/mod.rs:2149-2290`
- `StopContainer`: `src/server/mod.rs:2293-2411`
- `RemoveContainer`: `src/server/mod.rs:2414-2469`
- `RuncRuntime` 主逻辑: `src/runtime/mod.rs:831-1062`

已实现部分：

- 容器 rootfs 会从本地镜像层准备，见 `src/runtime/mod.rs:464-520`。
- `start/stop/remove` 已实际调用 `runc`，见 `src/runtime/mod.rs:790-920`。

本轮处理结果：

1. `CreateContainer` 已接住一批高价值 CRI 字段。
   - 服务层现在会消化 `tty/stdin/stdin_once/log_path`，见 `src/server/mod.rs:1442-1459`、`src/server/mod.rs:1496-1520`、`src/server/mod.rs:1565-1597`。
   - `linux.resources`、`run_as_group`、`supplemental_groups`、`readonly_rootfs`、`no_new_privileges`、`apparmor`、`capabilities` 已开始映射到内部 `ContainerConfig`，见 `src/server/mod.rs:1472-1597`。
   - 设备映射会转成运行时 `DeviceMapping`，见 `src/server/mod.rs:1584-1596`。

2. Pod 网络命名空间已经贯穿到 pause 容器与业务容器。
   - Pod 创建时 pause 容器会显式带上 Pod netns，见 `src/pod/mod.rs:219-260`。
   - 容器创建时也会把 Pod netns 解析并传给 `ContainerConfig`，见 `src/server/mod.rs:1462-1470`、`src/server/mod.rs:1578-1582`。
   - OCI spec 会按 `namespace_paths` 写入 `linux.namespaces[*].path`，见 `src/runtime/mod.rs:231-277`、`src/runtime/mod.rs:727-743`。

3. OCI spec 翻译已不再只是“通用模板”。
   - `process.terminal`、`root.readonly`、`no_new_privileges`、`apparmor_profile` 会按 CRI 输入生成，见 `src/runtime/mod.rs:620-662`。
   - `linux.resources`、`cgroups_path`、`sysctl` 会根据容器配置写入 OCI spec，见 `src/runtime/mod.rs:279-325`、`src/runtime/mod.rs:695-743`。
   - 设备映射会转成 OCI `linux.devices` 与 `devices` cgroup 规则，见 `src/runtime/mod.rs:327-376`、`src/runtime/mod.rs:711-725`。

4. `log_path/resources` 已开始贯穿到容器状态对象。
   - 内部容器状态会序列化保存，恢复后仍能用于回填 `log_path/resources`，见 `src/server/mod.rs:198-214`、`src/server/mod.rs:315-349`、`src/server/mod.rs:1495-1520`。

5. `run_as_username` 已开始映射到 OCI user。
   - 运行时现在会在数值 UID 解析失败时回退为 `username` 路径，见 `src/runtime/mod.rs:210-231`。

6. 容器状态时间字段已开始回填。
   - 内部容器状态新增了 `started_at/finished_at/exit_code`，并会在 `start/stop` 时更新，见 `src/server/mod.rs:198-217`、`src/server/mod.rs:315-361`、`src/server/mod.rs:1762-1847`、`src/server/mod.rs:1860-1942`。

7. 运行时新增了针对 6.4 的单元测试。
   - 新增了 `test_spec_with_runtime_options`、`test_spec_with_device_mappings`、`test_spec_with_username`，见 `src/runtime/mod.rs:1111-1178`。

8. `namespace_options` 的 `pid/ipc` 共享语义已开始真正落地到 `create` 路径。
   - 服务层会在 `NamespaceMode::Pod/Target` 下解析目标 namespace path，并注入到 `namespace_paths.pid/ipc`，见 `src/server/mod.rs:1807-1884`、`src/server/mod.rs:2056-2060`。
   - 运行时新增 `container_pid` 查询能力，供上述 namespace path 解析复用，见 `src/runtime/mod.rs:822-827`。

残余问题：

1. `tty=true` 已能真实启动并进入运行态。
   - shim 现在会为 TTY 容器建立 `console.sock`，接收 runc 传回的 console master，并启动本地 console bridge。
   - 但这条链路目前仍停留在 shim 本地层面，CRI `attach/exec` 还没有真正复用这条控制台链路。
2. `CDI_devices` 还没有接入 CDI 解析与注入链路。
3. `seccomp` 已接入基础执行链路（含 localhost profile 解析），但 `RuntimeDefault` 仍主要依赖运行时默认行为，规则完备性仍待增强。
4. `stop` 的退出码语义已较上一版改进（会尽量保留已知退出码并避免无条件覆盖为 `0`），但来源仍以运行时轮询/已有状态为主，缺少事件驱动的更强一致性。

结论：

- `create/run` 已从“字段大量丢失的部分实现”提升到“基础可用，关键 CRI 配置已开始落地”。
- `start/stop/rm` 仍然是基础可用；`stop` 的退出码语义已较上一版增强，但精细化一致性仍待补齐。

实机验证：

- 已通过 `crictl` 顺序执行 `create -> start -> inspect -> stop -> rm` 做验证。
- 顺序验证结果为通过：
  - `crictl create <pod-id> /root/crius/.tmp/verify-container-config.json /root/crius/.tmp/verify-pod-config.json`
  - `crictl start <container-id>`
  - `crictl stop <container-id>`
  - `crictl rm <container-id>`
- 运行中 `crictl ps -a` 能正确显示容器 `Running` 且已关联到 Pod。
- `crictl inspect <container-id>` 已能看到本轮落地的关键字段：
  - `logPath`
  - `resources.linux.cpu*`
  - `resources.linux.memoryLimitInBytes`
  - 容器 `labels/annotations`
- 对 `tty=false` 的容器再次验证后，`crictl inspect` 已能正确显示 `state=CONTAINER_RUNNING`，并且 `startedAt` 已被回填。
- 对 `tty=true` 的容器再次验证后：
  - `crictl start` 已成功
  - `crictl ps -a` 显示容器 `Running`
  - `crictl inspect` 显示 `state=CONTAINER_RUNNING`
  - 顺序执行 `crictl stop` 后，`inspect` 显示 `state=CONTAINER_EXITED`
  - 顺序执行 `crictl rm` 也已成功
  - shim 日志中已经出现 `Log file configured`、`Attach server listening on .../attach.sock`、`Container created with PID ...`
- 这说明 6.4 当前更准确的状态是：
  - 普通非 TTY 容器主链路可用
  - TTY 容器的 `console socket / shim console / 基础生命周期` 主链路也已经可用
  - 但更上层的 CRI `attach/exec` 流式会话仍未真正接入这条控制台链路

### 6.5 `ps`

实现位置：

- `src/server/mod.rs:522-586`

已实现部分：

- 支持按 `id/state/pod_sandbox_id/labels` 过滤，匹配逻辑见 `src/server/mod.rs:101-131`。

主要缺口：

1. 容器状态来自内存对象，不是每次实时查询 `runc`。
   - `ContainerStatus` 会调用 `runtime.container_status` 做实时查询。
   - `ListContainers` 没有做同样刷新，因此 `ps` 看到的状态可能陈旧。

2. daemon 重启后的容器元数据恢复仍是“尽力而为”。
   - 本轮已经开始从内部持久化状态恢复 `metadata.name/attempt`，见 `src/server/mod.rs:489-540`。
   - 但如果历史记录里没有内部状态，仍会退回到从 command 推断名称。

结论：

- `ps` 能用，重启后的元数据语义较上一版更完整，但状态刷新仍以内存快照为主。

### 6.6 `inspect`

实现位置：

- `src/server/mod.rs:474-519`

主要缺口：

- 缺少 `started_at`
- 缺少 `finished_at`
- 缺少 `exit_code`
- 缺少 `reason`
- 缺少 `message`
- 缺少 `mounts`
- `verbose` 语义未使用

现状：

- 本轮已经开始回填 `log_path/resources`，见 `src/server/mod.rs:315-349`、`src/server/mod.rs:963-1008`。
- 当前仍主要回填 `id/metadata/state/created_at/image/image_ref/labels/annotations/log_path/resources` 这一层。
- 因此 `crictl inspect` 只能看“半个状态对象”，不足以支撑排障。

结论：

- `inspect` 是典型的“RPC 存在，但关键字段没填完”。

### 6.7 `exec --sync`

实现位置：

- `src/server/mod.rs:601-642`
- `src/runtime/mod.rs:698-728`

主要问题：

1. 服务层返回的 `stdout` / `stderr` 永远为空，见 `src/server/mod.rs:627-631`。
2. `timeout` 被读入但没有真正用于中断执行，见 `src/server/mod.rs:605-608`。
3. 底层 `runc exec` 虽然拿到了 `output()`，但 `RuntimeService` 只保留退出码，没有把输出带回来。
4. 底层实现无论是否交互都固定加 `-i`，见 `src/runtime/mod.rs:701-718`，语义与纯同步执行并不完全匹配。

结论：

- `crictl exec -s` 只能算“半可用”：命令可执行，但输出链路没接好。

### 6.8 `exec` / `attach` / `port-forward`

实现位置：

- `Exec`: `src/server/mod.rs:590-598`
- `Attach`: `src/server/mod.rs:1095-1103`
- `PortForward`: `src/server/mod.rs:645-653`

现状：

- `Exec` / `Attach` 已不再返回固定的 `crius.sock`。
- 项目内现在已经有独立的 `streaming` 模块，负责：
  - request cache
  - token URL 构造
  - 独立 HTTP server 骨架
  - `/exec/:token`、`/attach/:token` 路由
- `Exec` / `Attach` 会返回类似 `http://127.0.0.1:<port>/exec/<token>`、`/attach/<token>` 的一次性 URL。
- `Exec` 当前 HTTP endpoint 仍只是项目内占位骨架，返回 `501 Not Implemented`。
- `Attach` 已不再只是 `501`，而是已经具备可实机使用的主链路：
  - 已实现 SPDY upgrade 检查、协议协商，以及带 SPDY 字典的头块压缩/解压
  - 已能按 `streamtype` 头识别 `error/stdin/stdout/stderr` stream，而不是依赖固定顺序
  - 已能桥接到 shim `attach.sock`，并对 non-TTY stdout/stderr 做显式分流
  - 已补齐 quick-exit non-TTY 场景下的输出刷出问题，避免 attach 看到 stdin 生效但 stdout 丢失
- `PortForward` 依旧没有真正链路。

进一步证据：

- 项目内确实有 attach 基础组件：
  - `IoManager::start_attach_server` 已能监听 Unix socket
  - `ShimManager` 也为每个容器预留了 `attach.sock`
- TTY 容器的 console socket / shim console 主链路已经可用，说明“本地数据平面”已有基础。
- `streaming` 模块现在除了 request cache、URL builder 和 HTTP server 骨架外，也已经包含 attach 所需的最小 SPDY 会话处理。
- shim 侧也已开始真正持有 non-TTY 容器 stdio：
  - 参考 `/root/cri-o` 的 attach/conmon 思路，当前 non-TTY 路径已改为由 shim 前台持有 `runc run` 的 stdio，并把 stdout/stderr 泵到日志与 attach socket
  - shim attach socket 的输出现在已带 pipe framing，streaming 侧可以按 stdout/stderr 解复用
  - 已补了单元测试验证 non-TTY stdio 捕获链路
- 已基于本地 `crictl` 做 fresh live container 实机验证：
  - `timeout 5s crictl attach <looping-container>` 已能持续收到输出
  - `printf 'hello-final\n' | crictl attach -i <stdin-container>` 已能返回 `got:hello-final`
  - 通过 `script -q -c 'crictl attach -it <tty-container>' /dev/null` 也已验证 fresh TTY attach 输出可见
- 通过对本地 `/root/cri-tools` 与 `/root/cri-o` 源码的对照分析，已经确认 `crictl attach/exec` 默认使用的是 Kubernetes `remotecommand` 的 SPDY 数据面，而不是原始 Unix socket。
- 当前真正剩下的缺口已经更聚焦：
  - `exec` 仍没有真正的数据面
  - `attach` 还没有 resize stream 与 exit-status/error 语义
  - daemon 重启后，恢复出来的旧容器因为没有 live shim attach socket，attach 仍会退化失败

结论：

- `Attach` 已从纯占位实现前进到“fresh live container 上可被 `crictl` 真实使用”的阶段；`Exec` 则仍停留在 streaming 骨架阶段；`PortForward` 仍属于占位实现。
- 流式问题已经从“架构缺失”进一步收敛成了“`exec` 数据面缺失，以及 `attach` 的剩余高级语义和恢复语义不足”。

### 6.9 `logs`

`crictl logs` 不直接对应单个 RPC，而是依赖以下链路：

1. 创建容器时接收并保存 `ContainerConfig.log_path`
2. Pod 创建时接收并保存 `PodSandboxConfig.log_directory`
3. 运行时把容器 stdout/stderr 写到对应宿主机日志文件
4. `ContainerStatus.log_path` 正确返回该路径
5. 需要时支持 `ReopenContainerLog`

当前问题：

- `CreateContainer` 已开始处理 `log_path`，见 `src/server/mod.rs:1442-1459`、`src/server/mod.rs:1496-1520`
- `RunPodSandbox` 已开始处理 `log_directory`，见 `src/server/mod.rs:693-763`、`src/server/mod.rs:772-819`
- `ContainerStatus` 已开始返回 `log_path`，见 `src/server/mod.rs:315-349`、`src/server/mod.rs:963-1008`
- `ReopenContainerLog` 是空实现，见 `src/server/mod.rs:1086-1091`
- 虽然 shim 有 `IoManager` 和日志文件能力，但 daemon 的 `setup_io()` 没把它串起来，见 `src/shim/daemon.rs:112-118`

结论：

- `logs` 已完成“路径级”对接，但还没有完成“真实日志输出链路”对接，因此整体仍应判定为未完成。

### 6.10 `stats` / `statsp` / `metricsp`

实现位置：

- `ContainerStats`: `src/server/mod.rs:1106-1120`
- `ListContainerStats`: `src/server/mod.rs:1124-1131`
- `PodSandboxStats`: `src/server/mod.rs:1135-1149`
- `ListPodSandboxStats`: `src/server/mod.rs:1153-1160`
- `ListMetricDescriptors`: `src/server/mod.rs:1249-1256`
- `ListPodSandboxMetrics`: `src/server/mod.rs:1260-1267`

支撑基础：

- 项目已存在 `MetricsCollector`，可以采集 CPU / memory / blkio / pids 等 cgroup 指标，见 `src/metrics/mod.rs:142-220`

现状：

- 服务层仍然全部返回 `None` 或空列表。
- 也没有建立 container id / pod id 到 cgroup 路径的映射。
- `metricsp` 所需的 descriptor 与 unstructured metrics 没有输出。

结论：

- 指标基础模块已存在，但 `crictl` 相关统计能力仍是 `L0`。

### 6.11 `events`

实现位置：

- `src/server/mod.rs:1184-1245`

当前行为：

- 建立一个 channel
- 把当前内存中已有容器、Pod 扫一遍发送出去
- 然后结束

主要问题：

1. 不是持续事件流。
   - `crictl events` 期望流式订阅后续事件。
   - 当前实现只是“状态快照冒充事件流”。

2. 没有接入状态变更源。
   - 项目里有 `state_events` 表，见 `src/storage/mod.rs:109-142`
   - 状态更新时也会记录事件，见 `src/storage/mod.rs:423-445`
   - 但 `GetContainerEvents` 没有读取这个表，也没有订阅任何实时通知。

3. 事件类型值使用错误。
   - 代码里把 `2` 注释为 `POD_SANDBOX_CREATED_EVENT`，见 `src/server/mod.rs:1224-1233`
   - 但 CRI v1 `ContainerEventType` 中 `2` 实际是 `CONTAINER_STOPPED_EVENT`，见 `proto/k8s.io/cri-api/pkg/apis/runtime/v1/api.proto:1770-1781`

结论：

- `events` 当前既不持续，也不准确，应归类为未完成对接。

### 6.12 `pull` / `images` / `inspecti` / `rmi` / `imagefsinfo`

相关实现：

- `ListImages`: `src/image/mod.rs:517-530`
- `ImageStatus`: `src/image/mod.rs:534-562`
- `PullImage`: `src/image/mod.rs:566-685`
- `RemoveImage`: `src/image/mod.rs:689-745`
- `ImageFsInfo`: `src/image/mod.rs:749-756`

#### 6.12.1 `pull`

已实现部分：

- 支持基础镜像引用解析
- 支持通过 OCI 库拉取，失败后回落到 registry API
- 支持落盘 layers 与 metadata

主要缺口：

- 认证只使用 `username/password`，没有消费 `auth`、`server_address`、`identity_token`、`registry_token`，见 `src/image/mod.rs:573-576`
- 通过 OCI 库拉取成功时，镜像 `size` 被写成 `0`，见 `src/image/mod.rs:609-625`
- 镜像对象字段只填了少量信息，`repo_digests/spec/uid/username/pinned` 等未补齐

结论：

- `pull` 属于基础可用，但镜像元数据质量不高。

#### 6.12.2 `images`

问题：

- `ListImagesRequest.filter` 完全未使用，见 `src/image/mod.rs:517-530`
- 直接对 `images.values()` 收集，没有按镜像 ID 去重；如果一个镜像有多个 tag 映射，可能重复展示

结论：

- `crictl images` 可用，但不是完整实现。

#### 6.12.3 `inspecti`

问题：

- 已命中镜像时可以返回对象
- 未命中镜像时直接返回 `NotFound`，见 `src/image/mod.rs:562`
- 但 CRI v1 规范要求 image 不存在时返回 `ImageStatusResponse.image = nil`，见 `proto/k8s.io/cri-api/pkg/apis/runtime/v1/api.proto:149-152`
- `verbose` 参数也未真正使用

结论：

- `inspecti` 只有基础查询能力，未完全符合 CRI 语义。

#### 6.12.4 `rmi`

已实现部分：

- 能按 tag 或 image id 删除内存映射
- 能删除磁盘上的镜像目录

主要缺口：

- 没有接入 `image/layer.rs` 中的层引用计数与 GC 机制
- `LayerManager` 明明已经支持 `remove_image()` 与 `garbage_collect()`，见 `src/image/layer.rs:262-339`、`src/image/layer.rs:431-439`
- 但 `ImageServiceImpl` 完全没有使用 `LayerManager`，见 `src/image/mod.rs:34-39`

结论：

- `rmi` 只能删“镜像目录视图”，还没有完整删到 CAS/层引用语义。

#### 6.12.5 `imagefsinfo`

现状：

- 直接返回空数组，见 `src/image/mod.rs:749-756`

结论：

- `imagefsinfo` 明确未实现。

### 6.13 `update`

实现位置：

- `src/server/mod.rs:1286-1315`

现状：

- 只检查容器是否存在
- 记录日志
- 注释里写了 TODO，要调用 `runc update`
- 实际没有任何资源更新动作

结论：

- `crictl update` 当前是 no-op。

### 6.14 `checkpoint`

实现位置：

- `src/server/mod.rs:1173-1178`

现状：

- 空实现，直接返回成功

结论：

- 明确未完成对接。

### 6.15 `runtime-config`

实现位置：

- `src/server/mod.rs:1271-1283`

现状：

- 能返回 `RuntimeConfigResponse`
- 但只塞了一个默认 `LinuxRuntimeConfiguration`
- 实际配置值没有填充，例如 cgroup driver 等关键信息为空

结论：

- 更接近“接口占位”而不是“有效配置输出”。

## 7. 横向系统性问题

### 7.1 已有基础组件，但未接入 CRI 出口

这类问题是本项目当前最典型的特征。

1. 事件基础已经有了。
   - `state_events` 表已建好，见 `src/storage/mod.rs:109-142`
   - 状态变更会落库，见 `src/storage/mod.rs:169-170`、`src/storage/mod.rs:423-445`
   - 但 `GetContainerEvents` 没有接入它

2. 指标基础已经有了。
   - `MetricsCollector` 已能读 cgroup 指标，见 `src/metrics/mod.rs:142-220`
   - 但 `stats/statsp/metricsp` 没有接入它

3. attach/exec 的基础已经有了。
   - `IoManager` 已能监听 attach socket，见 `src/shim/io.rs:85-112`
   - `ShimManager` 也为容器准备了 `attach.sock`，见 `src/runtime/shim_manager.rs:81-84`
   - daemon 已启用 attach socket 与 TTY console bridge，non-TTY 路径也已开始由 shim 真正持有 stdio
   - `streaming` 模块已开始提供 token 化 URL、HTTP server 骨架，并已把 attach 的 SPDY 会话桥接到可实机使用的程度
   - 但 `exec` 数据面仍未实现，`attach` 的 remotecommand 高级语义和恢复语义也还没有补完整

4. 镜像层管理基础已经有了。
   - `LayerManager` 已支持引用计数与 GC
   - 但 `ImageServiceImpl` 没有使用它

这说明项目不是“完全没有基础”，而是“基础能力与 CRI 服务出口没有贯通”。

### 7.2 恢复逻辑只恢复了展示态，没有恢复运行态

相关代码：

- `src/server/mod.rs:221-281`

问题本质：

- 恢复逻辑只恢复了 `containers` 与 `pod_sandboxes` 两个顶层 HashMap
- 没有恢复：
  - `pod_manager` 内部 `pods`
  - shim 运行态
  - attach/日志态
  - network manager 运行态

后果：

- `crictl ps/pods/inspect` 可能还能展示出对象
- 但 `stopp/rmp/logs/events` 等依赖实际运行态的信息会明显退化

### 7.3 verbose 语义大多被忽略

表现：

- `StatusRequest.verbose` 未使用
- `ImageStatusRequest.verbose` 未使用
- `PodSandboxStatusRequest.verbose` 未按需组织额外信息

这会导致 `crictl inspect*` 虽然有输出，但不是“按 CRI 语义组织的详细诊断信息”。

### 7.4 测试覆盖偏底层，对 `crictl` 命令面支撑不足

现状：

- 现有测试更多集中在 runtime/storage/pod 基础组件
- 对 RuntimeService / ImageService 面向 `crictl` 的契约性测试不足

这会带来两个问题：

- 很难发现“RPC 能调，但字段不对”的问题
- 很难发现“daemon 重启后语义变化”的问题

## 8. 优先级建议

如果目标是尽快把项目提升到“可较完整使用 `crictl` 调试”的水平，建议按下面顺序补齐：

### P0：先把 `crictl` 最卡手的链路打通

1. 完成 `exec` / `port-forward`，并补齐 `attach` 的剩余高级语义
   - 当前 `attach` 已能在 fresh live container 上被 `crictl` 真实使用，`exec` 已有 request cache / token URL / HTTP server 骨架
   - 下一步的关键阻塞点是补齐 `exec` 数据面，以及 `attach` 的 resize/error/重启恢复语义
2. 打通日志链路
   - 落地 `log_directory`
   - 落地 `log_path`
   - `ContainerStatus.log_path` 返回真实路径
   - 实现 `ReopenContainerLog`
3. 接入 `MetricsCollector`
   - 至少先完成 `stats` / `statsp`

### P1：补齐状态语义

1. `ContainerStatus` 补 `started_at/finished_at/exit_code/reason/message/log_path/resources`
2. `PodSandboxStatus` 补 `network/linux/containers_statuses`
3. `ImageStatus` 修正“不存在镜像时返回 nil image”的语义
4. 让 `ListContainers` / `ListPodSandbox` 更接近实时状态，而不是只看内存快照

### P2：补齐控制面能力

1. `UpdateContainerResources` 真正调用 `runc update`
2. `RuntimeConfig` 返回真实配置
3. `ImageFsInfo` 返回文件系统占用
4. `CheckpointContainer` 明确实现或返回 `unimplemented`

### P3：补齐重启恢复语义

1. 恢复 `pod_manager` 运行态
2. 恢复 pause 容器与网络命名空间对应关系
3. 恢复日志/attach/shim 的必要状态
4. 让 `events` 基于真实事件源持续输出

## 9. 最终结论

以 `crictl` 为标准衡量，Crius 当前处于：

- `基础 Runtime/Image RPC 骨架已齐`
- `核心生命周期部分可用`
- `流式、日志、观测、资源更新、事件与恢复语义明显不足`

换句话说，本项目现在更适合描述为：

> “已经具备 CRI 框架和基础生命周期，但尚未完整对接 `crictl` 的调试、流式、观测与语义细节。”

如果只看“命令能不能打出去”，项目已经覆盖了不少命令。
但如果以“`crictl` 是否能像对接 containerd/CRI-O 那样稳定工作”为标准，那么仍有较大缺口，尤其集中在：

- `exec`
- `attach`
- `logs`
- `port-forward`
- `stats`
- `statsp`
- `metricsp`
- `events`
- `update`
- `imagefsinfo`
- `checkpoint`

以及：

- `inspect` / `inspectp` / `inspecti` 的关键字段完整性
- daemon 重启后的语义一致性
