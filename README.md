# crius

`crius` 是一个使用 Rust 实现的 Kubernetes CRI 运行时。项目当前提供 CRI v1 gRPC 服务、`runc`/OCI 集成、镜像管理、CNI 网络、流式 `exec/attach/port-forward`、`shim` 进程管理、SQLite 状态持久化，以及运行时侧 NRI 集成。

当前代码已经覆盖核心运行时链路，适合开发验证、功能联调和协议研究。若要直接用于生产环境，仍建议补充稳定性压测、异常恢复验证和与目标 Kubernetes 版本的兼容性回归。

## 文档

- `README.md`：项目概览、构建与运行方式
- `docs/architecture.md`：整体架构、模块边界和主链路设计
- `docs/nri.md`：NRI 功能说明、配置和运维边界

## 功能概览

当前仓库已实现或接入以下能力：

- CRI v1 `RuntimeService` 与 `ImageService`
- 基于 `runc` 的 Pod Sandbox / Container 生命周期管理
- 流式 `exec`、`exec_sync`、`attach`、`port-forward`
- 基于 CNI 的网络配置与端口映射
- `crius-shim` 容器进程与 IO 管理
- 镜像拉取、查询、删除和镜像文件系统统计
- SQLite 持久化与运行时重启恢复
- NRI 插件注册、同步、生命周期 hook、unsolicited update、eviction
- gRPC reflection，便于调试与协议探查

## 主要组件

| 组件 | 说明 |
| --- | --- |
| `crius` | 主守护进程，提供 CRI gRPC 接口，默认监听 `unix:///run/crius/crius.sock` |
| `crius-shim` | 可选二进制，负责容器进程、退出码、attach socket 和日志相关管理 |
| `runc` | 底层 OCI runtime |
| CNI plugins | Pod 网络配置与回收 |
| SQLite | Pod / Container 元数据持久化 |

## 代码布局

| 路径 | 说明 |
| --- | --- |
| `src/main.rs` | 守护进程入口 |
| `src/server/` | CRI RuntimeService 实现 |
| `src/image/` | CRI ImageService 实现 |
| `src/runtime/` | `runc` 集成、bundle/spec 生成、shim 管理 |
| `src/streaming/` | 流式服务实现 |
| `src/network/` | CNI 与端口映射 |
| `src/storage/` | 持久化与恢复 |
| `src/nri/` | NRI manager、transport、merge、adjust、domain |
| `src/shim/` | `crius-shim` 实现 |
| `tests/` | 集成测试 |

## 运行前提

建议在 Linux 主机上运行，并准备以下依赖：

- Rust 稳定版工具链
- `runc`
- `protobuf-compiler`
- `tar`
- `iproute2`
- containernetworking-plugins
- 可选：`crictl`

多数真实运行路径以及一部分测试需要 `root` 权限。

## 构建

仓库使用本地 `vendor/` 依赖，并附带补丁检查/应用逻辑。首次构建建议按以下顺序执行：

```bash
make check-patch
make apply-patch
cargo build --features shim --bins
```

如果只想做一次基础编译，也可以执行：

```bash
make build
```

但完整运行时链路通常需要同时构建 `crius` 和 `crius-shim`，因此更推荐：

```bash
cargo build --features shim --bins
```

## 快速开始

### 1. 构建二进制

```bash
cargo build --features shim --bins
```

### 2. 准备环境

```bash
export CRIUS_SHIM_PATH="$(pwd)/target/debug/crius-shim"
export CRIUS_PAUSE_IMAGE="registry.k8s.io/pause:3.9"
export CRIUS_CNI_CONFIG_DIRS="/etc/cni/net.d"
export CRIUS_CNI_PLUGIN_DIRS="/usr/libexec/cni:/opt/cni/bin:/usr/lib/cni"
```

### 3. 启动服务

```bash
sudo ./target/debug/crius --listen unix:///run/crius/crius.sock
```

### 4. 用 `crictl` 验证

```bash
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock version
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock info
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock images
```

`make test` 会执行一次基础的 `crictl version` 联通性验证。

## 默认路径

| 路径 | 用途 |
| --- | --- |
| `/run/crius/crius.sock` | CRI Unix Socket |
| `/var/lib/crius` | 运行时根目录 |
| `/var/lib/crius/crius.db` | SQLite 数据库 |
| `/var/log/crius` | 日志目录 |
| `/var/run/crius/shims` | shim 工作目录 |
| `/run/crius/nri.sock` | NRI Socket |

说明：部分子目录会由运行时按需创建，实际 bundle、镜像和临时文件路径会从 `root`、runtime root 和存储实现派生。

## 配置

默认配置文件路径为 `/etc/crius/crius.conf`。主配置结构定义在 `src/config/mod.rs::Config`，包含以下一级字段：

- `root`
- `runtime`
- `image`
- `network`
- `nri`

一个最小示例：

```toml
root = "/var/lib/crius"

[runtime]
runtime_type = "runc"
runtime_path = "/usr/bin/runc"
root = "/run/crius"

[image]
driver = "overlay"
root = "/var/lib/containers/storage"

[network]
plugin = "cni"
config_dir = "/etc/cni/net.d/"

[nri]
enable = false
runtime_name = "crius"
runtime_version = "0.1.0"
socket_path = "/run/crius/nri.sock"
plugin_path = "/opt/nri/plugins"
plugin_config_path = "/etc/nri/conf.d"
registration_timeout_ms = 5000
request_timeout_ms = 2000
enable_external_connections = false
```

## 关键环境变量

| 环境变量 | 用途 | 默认行为 |
| --- | --- | --- |
| `CRIUS_RUNTIME_HANDLERS` | CRI 暴露的 runtime handler 列表，逗号分隔 | 自动追加当前 runtime type |
| `CRIUS_PAUSE_IMAGE` | Pod Sandbox pause 镜像 | `registry.k8s.io/pause:3.9` |
| `CRIUS_CNI_CONFIG_DIRS` | CNI 配置目录，冒号分隔 | 使用代码内置默认目录 |
| `CRIUS_CNI_PLUGIN_DIRS` | CNI 插件目录，冒号分隔 | 使用代码内置默认目录 |
| `CRIUS_CNI_CACHE_DIR` | CNI 缓存目录 | `/var/lib/cni/cache` |
| `CRIUS_CGROUP_DRIVER` | 强制指定 `systemd` 或 `cgroupfs` | 自动探测 |
| `CRIUS_SHIM_PATH` | `crius-shim` 二进制路径 | 优先使用显式配置，否则尝试默认查找 |
| `CRIUS_SHIM_DIR` | shim 工作目录 | `/var/run/crius/shims` |
| `CRIUS_SHIM_DEBUG` | 开启 shim debug 日志 | `false` |
| `CRIUS_ENABLE_HUGEPAGES_MOUNT` | 保留 `/dev/hugepages` 挂载 | `false` |
| `CRIUS_NRI_BLOCKIO_CONFIG` | blockio class 映射文件路径 | 未设置 |
| `CRIUS_NRI_ALLOWED_ANNOTATION_PREFIXES` | NRI annotation allowlist 扩展入口 | 未设置 |
| `CRIUS_NRI_CONTAINER_MIN_MEMORY_BYTES` | NRI 最小容器内存限制校验 | 未设置 |

`CNI_PATH` 会作为 `CRIUS_CNI_PLUGIN_DIRS` 的后备来源。

## NRI

`crius` 已实现运行时侧 NRI 集成。当前已覆盖的核心能力包括：

- 插件注册、`Configure`、`Synchronize`、`Shutdown`
- Pod / Container 生命周期 hook
- `ValidateContainerAdjustment`
- unsolicited `UpdateContainers`
- eviction
- `blockio_class`、`rdt_class`、`devices`、扩展 memory / CPU realtime / `pids` 资源更新

详细说明见 `docs/nri.md`。

## systemd 部署

仓库提供示例 unit 文件 `crius.service`。安装示例：

```bash
sudo install -Dm755 target/debug/crius /usr/bin/crius
sudo install -Dm755 target/debug/crius-shim /usr/bin/crius-shim
sudo install -Dm644 crius.service /etc/systemd/system/crius.service
sudo systemctl daemon-reload
sudo systemctl enable --now crius
```

如果 CNI 插件目录不是默认路径，需要同步调整 unit 文件中的环境变量。

## 测试

```bash
cargo test
```

需要 root、`runc` 或额外系统能力的测试可以显式运行：

```bash
cargo test -- --ignored
```

也可以执行：

```bash
make test
```

## 当前注意事项

- `--listen` 已接入实际启动流程；`--config`、`--debug`、`--log` 虽然已暴露在 CLI 中，但当前仍未完整控制主流程行为
- 项目运行通常依赖 `crius-shim`，只构建主二进制通常不足以覆盖完整容器链路
- 构建依赖本地 `vendor/` 目录及补丁状态，建议先执行 `make check-patch`
- 当前更适合开发验证和功能联调，生产接入前应补充目标环境验证

## 许可证

Apache-2.0
