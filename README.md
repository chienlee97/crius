# crius

`crius` 是一个使用 Rust 编写的 Kubernetes CRI（Container Runtime Interface）运行时实现，基于 `runc` 和 OCI 规范，提供 CRI v1 gRPC 服务、镜像管理、CNI 网络、流式 `exec/attach/port-forward`、`shim` 进程管理以及状态持久化能力。

当前仓库版本为 `v0.1.0`。从现有代码和测试覆盖来看，项目已经具备较完整的核心 CRI 路径，更适合开发、调试和集成验证场景；在直接投入生产前，仍建议补充稳定性、兼容性和长期运行验证。

## 特性

- 基于 `tonic` 提供 CRI v1 `RuntimeService` 和 `ImageService`
- 基于 `runc` 创建、启动、停止、删除 Pod Sandbox 和容器
- 支持 `exec`、`exec_sync`、`attach`、`port-forward`
- 内置流式服务器，支持 SPDY 和 WebSocket 升级
- 支持镜像拉取、镜像状态查询、镜像删除、镜像文件系统统计
- 支持 CNI 网络配置、网络命名空间和端口映射
- 支持 `crius-shim` 管理容器生命周期、IO 和退出码
- 使用 SQLite 持久化 Pod / Container 状态，支持重启恢复
- 支持容器资源更新、Pod/Container 统计、Pod 指标和事件流
- 支持 checkpoint 导出与恢复流程
- 启用了 gRPC reflection，便于调试和协议探查

## 组件概览

- `crius`
  主守护进程，提供 CRI gRPC 接口，默认监听 `unix:///run/crius/crius.sock`
- `crius-shim`
  通过 `shim` feature 构建出的 shim 二进制，负责容器进程、attach socket、日志重开和退出码跟踪
- `runc`
  实际执行 OCI bundle 的底层运行时
- CNI plugins
  负责 Pod 网络配置和清理
- SQLite
  持久化 Pod / Container 元数据和恢复信息

## 仓库结构

- `src/main.rs`
  `crius` 守护进程入口
- `src/server/`
  CRI RuntimeService 实现
- `src/image/`
  CRI ImageService 实现
- `src/runtime/`
  `runc` 集成、OCI bundle 生成、shim 管理
- `src/streaming/`
  `exec` / `attach` / `port-forward` 流式服务
- `src/network/`
  CNI、多网络和端口映射
- `src/storage/`
  状态持久化与本地存储
- `src/shim/`
  `crius-shim` 实现
- `tests/`
  集成测试

## 运行依赖

建议在 Linux 环境下运行，并准备以下依赖：

- Rust 稳定版工具链
- `runc`
- `protobuf-compiler`
- `tar`
- `iproute`（提供 `ip` 命令）
- containernetworking-plugins
- 可选：`crictl`，用于快速验证 CRI 兼容性

部分测试和绝大多数真实运行路径需要 `root` 权限。

## 构建

仓库使用了本地 `vendor/` 依赖源，并带有一个针对 `h2` 的补丁辅助目标。首次构建时，建议按下面顺序执行：

```bash
make check-patch
# 如果提示未应用，再执行这一行
make apply-patch
cargo build --features shim --bins
```

如果你只想快速验证主程序能否编译，也可以直接运行：

```bash
make build
```

但要注意，实际运行容器生命周期时通常还需要同时构建 `crius-shim`，因此更推荐使用：

```bash
cargo build --features shim --bins
```

## 快速开始

### 1. 构建二进制

```bash
cargo build --features shim --bins
```

### 2. 准备环境变量

```bash
export CRIUS_SHIM_PATH="$(pwd)/target/debug/crius-shim"
export CRIUS_PAUSE_IMAGE="registry.k8s.io/pause:3.9"
export CRIUS_CNI_CONFIG_DIRS="/etc/cni/net.d"
export CRIUS_CNI_PLUGIN_DIRS="/usr/libexec/cni:/opt/cni/bin:/usr/lib/cni"
```

### 3. 启动守护进程

```bash
sudo ./target/debug/crius --listen unix:///run/crius/crius.sock
```

### 4. 用 `crictl` 验证

```bash
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock version
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock info
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock images
```

仓库里的 `make test` 也会启动一个本地 socket 并执行一次 `crictl version` 验证。

## 默认路径

运行时默认会使用以下目录和文件：

| 路径 | 说明 |
| --- | --- |
| `/run/crius/crius.sock` | 默认 Unix Socket |
| `/var/lib/crius` | 运行时根目录 |
| `/var/lib/crius/runc-bundles` | `runc` bundle 根目录 |
| `/var/lib/crius/storage` | 镜像和运行时存储目录 |
| `/var/lib/crius/crius.db` | SQLite 持久化数据库 |
| `/var/log/crius` | 日志目录 |
| `/var/run/crius/shims` | `crius-shim` 工作目录 |

## 关键环境变量

| 环境变量 | 作用 | 默认值 |
| --- | --- | --- |
| `CRIUS_RUNTIME_HANDLERS` | 暴露给 CRI 的 runtime handler 列表，逗号分隔 | 自动包含 `runc` |
| `CRIUS_PAUSE_IMAGE` | Pod Sandbox 使用的 pause 镜像 | `registry.k8s.io/pause:3.9` |
| `CRIUS_CNI_CONFIG_DIRS` | CNI 配置目录，冒号分隔 | `/etc/cni/net.d:/etc/kubernetes/cni/net.d` |
| `CRIUS_CNI_PLUGIN_DIRS` | CNI 插件目录，冒号分隔 | `/opt/cni/bin:/usr/lib/cni:/usr/libexec/cni` |
| `CRIUS_CNI_CACHE_DIR` | CNI 缓存目录 | `/var/lib/cni/cache` |
| `CRIUS_CGROUP_DRIVER` | 强制指定 `systemd` 或 `cgroupfs` | 自动探测 |
| `CRIUS_SHIM_PATH` | `crius-shim` 二进制路径 | 优先尝试本地 `target/debug/crius-shim`，否则查找 `PATH` |
| `CRIUS_SHIM_DIR` | shim 工作目录 | `/var/run/crius/shims` |
| `CRIUS_SHIM_DEBUG` | 是否开启 shim debug 日志 | `false` |
| `CRIUS_ENABLE_HUGEPAGES_MOUNT` | 是否保留 `/dev/hugepages` 挂载 | `false` |

另外，`CNI_PATH` 也会作为 `CRIUS_CNI_PLUGIN_DIRS` 的后备来源。

## systemd 运行

仓库内提供了一个示例 unit 文件 [`crius.service`](./crius.service)。本地安装示例：

```bash
sudo install -Dm755 target/debug/crius /usr/bin/crius
sudo install -Dm755 target/debug/crius-shim /usr/bin/crius-shim
sudo install -Dm644 crius.service /etc/systemd/system/crius.service
sudo systemctl daemon-reload
sudo systemctl enable --now crius
```

如果你的 CNI 插件不在 `/usr/libexec/cni`，请同步修改 unit 文件中的环境变量。

## 测试

```bash
cargo test
```

运行需要 root、`runc`、`iproute2` 或本地 socket/TCP bind 权限的测试时，可以显式执行被标记为 `ignored` 的用例：

```bash
cargo test -- --ignored
```

也可以使用仓库自带的便捷命令：

```bash
make test
```

## 已知注意事项

- 命令行参数 `--listen` 已实际生效；`--config`、`--debug`、`--log` 当前已经暴露在 CLI 中，但主启动流程还没有完全接入这些参数
- 项目依赖 `crius-shim` 参与容器生命周期管理，因此只构建主二进制通常不足以完成完整运行时链路
- 仓库通过 `.cargo/config.toml` 使用本地 `vendor/` 依赖源，构建前建议先检查 `h2` 补丁状态
- 项目目前更偏向开发与验证环境使用，接入真实 Kubernetes 集群前建议先做额外兼容性测试

## License

Apache-2.0
