# crius

[English](README.md) | 中文

`crius` 是一个使用 Rust 实现的 Kubernetes Container Runtime Interface
（CRI）运行时。它对外提供 CRI v1 `RuntimeService` 和 `ImageService` gRPC
服务，对内编排 OCI runtime、镜像存储、CNI 网络、流式连接、本地持久化、重启恢复、
metrics、tracing、NRI 以及 rootless 相关能力。

本项目当前更适合以下场景：

- CRI、OCI、kubelet 与容器运行时机制研究
- 运行时功能验证与集成测试
- 探索 Rust 语言实现 Kubernetes CRI runtime 的工程路径

`crius` 当前不定位为成熟生产运行时的直接替代品。用于生产评估前，使用者应补充
Kubernetes 版本兼容矩阵、长时间稳定性测试、异常恢复回归、安全审计和运维手册。

## 当前状态

当前包版本为 `0.1.0`。公开仓库使用者应将其视为实验性运行时实现：

- API 行为和配置字段仍可能调整。
- 默认配置面向开发、调试和节点验证。
- 多项高级能力已经接入，但仍需要更多真实集群环境验证。

## 功能概览

- CRI v1 `RuntimeService` 与 `ImageService`
- 通过 OCI runtime backend 管理 Pod Sandbox 与业务容器生命周期
- 默认 `runc` backend，并支持按 runtime handler 细化配置
- 可选的 `wasm-direct` backend 接线，便于 handler 实验
- `crius-shim` 负责容器进程、I/O、attach socket、任务 RPC 和退出状态协作
- `exec`、`exec_sync`、`attach`、`port-forward` 流式能力
- CNI 网络、hostPort、PodCIDR 模板渲染和 CNI teardown 超时
- 镜像拉取、本地元数据、删除、文件系统统计、registry auth、签名/解密配置、
  pinned images、image volumes
- internal snapshotter 与 external snapshotter 配置引用
- SQLite 持久化、守护进程重启恢复和孤儿工件清理
- 基于 CRIU-capable runtime 的 CRI checkpoint / restore 路径
- NRI 插件注册、生命周期分发、adjustment merge、内建验证器、unsolicited update、
  eviction 和 synchronize
- rootless 配置解析、rootless 网络 helper 和 rootless 状态上报
- CRI events、Pod metrics、资源 stats、metrics endpoint 和 tracing export

## 支持范围

当前实现和文档主要面向：

- Linux 主机
- 从源码构建和验证
- 默认 `runc` OCI runtime 路径
- 以 root 权限运行的 kubelet 节点级 daemon 集成
- 开发验证、集成测试和实现研究

当前不承诺：

- 生产级 SLA 或长期兼容性保证
- 已发布的 Kubernetes 版本兼容矩阵
- 完整发行包与多发行版安装体验
- 大规模集群、异构运行时或跨发行版的充分验证

## 代码布局

| 路径 | 说明 |
| --- | --- |
| `src/main.rs` | daemon 入口、CLI、配置加载、服务装配 |
| `src/server/` | CRI `RuntimeService`、生命周期、状态、stats、events、recovery |
| `src/image/` | CRI `ImageService`、registry pull、本地镜像元数据、镜像存储 |
| `src/runtime/` | runtime backend 抽象、`runc`、`wasm-direct`、shim manager |
| `src/shim/` | `crius-shim` 实现 |
| `src/pod/` | Pod Sandbox 状态与 pause 容器处理 |
| `src/network/` | CNI、netns、hostPort、rootless 网络 helper |
| `src/streaming/` | exec、attach、port-forward 的 HTTP streaming server |
| `src/storage/` | SQLite 持久化与运行时状态恢复 |
| `src/nri/` | NRI manager、transport、convert、merge、adjust、domain |
| `src/security/` | seccomp、AppArmor、SELinux、CDI、devices、resource classes |
| `proto/` | CRI、NRI 与 protobuf 输入 |
| `tests/` | 集成测试与 release gate |

## 依赖

建议准备以下主机依赖：

- Linux
- Rust stable 工具链
- `runc`
- `protobuf-compiler`
- `zlib-devel` 或发行版等价包
- `tar`
- CNI plugins
- 可选：`crictl`、`kubelet`、`kubeadm`
- checkpoint / restore 可选：`criu` 和具备 CRIU 支持的 runtime
- rootless 验证可选：`newuidmap`、`newgidmap`、`slirp4netns` 或 `pasta`、
  `fuse-overlayfs`

真实 runtime、挂载、CNI、cgroup 与 kubelet 集成路径通常需要 root 权限。

## 构建

仓库通过 `.cargo/config.toml` 使用 vendored dependencies。部分 vendored crates
需要本地补丁后才能完整构建。

```bash
make check-patch
make apply-patch
cargo build --features shim --bins
```

标准项目构建：

```bash
make build
```

`make build` 当前构建默认 `crius` binary。需要同时构建 `crius-shim` 时，请使用
`cargo build --features shim --bins`。

## 快速开始

构建 daemon 和 shim：

```bash
cargo build --features shim --bins
```

安装到测试节点：

```bash
sudo install -Dm755 target/debug/crius /usr/bin/crius
sudo install -Dm755 target/debug/crius-shim /usr/bin/crius-shim
sudo install -Dm644 crius.service /etc/systemd/system/crius.service
```

从当前 binary 导出配置：

```bash
sudo mkdir -p /etc/crius
sudo /usr/bin/crius --write-default-config /etc/crius/crius.conf
```

检查并按节点实际情况修改 `/etc/crius/crius.conf`。接入 kubelet 前，重点确认 runtime
路径、shim 路径、CNI 路径、pause image 和 cgroup driver。

启动服务：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now crius
```

默认 CRI endpoint：

```text
unix:///run/crius/crius.sock
```

使用 `crictl` 验证：

```bash
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock version
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock info
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock images
```

## 配置

配置优先级为：

```text
CLI > 环境变量 > 配置文件 > 内置默认值
```

并非每个字段都提供 CLI 或环境变量覆盖。精确默认值和字段名称以当前 binary 为准：

```bash
crius --dump-default-config
crius --write-default-config /etc/crius/crius.conf
```

仓库中的 [crius.conf](crius.conf) 是经过整理的节点验证样例。

## Kubernetes 集成

完整流程见 [docs/zh/kubeadm.md](docs/zh/kubeadm.md)。kubelet runtime endpoint 为：

```text
unix:///run/crius/crius.sock
```

kubeadm 初始化示例：

```bash
sudo kubeadm init --cri-socket unix:///run/crius/crius.sock
```

kubeadm 加入节点示例：

```bash
sudo kubeadm join <control-plane>:6443 --token <token> \
  --discovery-token-ca-cert-hash <hash> \
  --cri-socket unix:///run/crius/crius.sock
```

## 开发与验证

常用命令：

```bash
cargo fmt --check
cargo test
make release-gate
```

具备外部依赖的环境可以继续运行 gated 测试：

```bash
make crictl-smoke
make kubelet-smoke
make fault-injection
make release-soak
```

`make test` 会在 `unix:///tmp/crius.sock` 启动本地 daemon，并执行基础
`crictl version` smoke test。

## 文档

| 文档 | 说明 |
| --- | --- |
| [docs/zh/architecture.md](docs/zh/architecture.md) | 当前架构、模块边界和核心请求链路 |
| [docs/zh/config-matrix.md](docs/zh/config-matrix.md) | 配置优先级、重载策略和关键字段 |
| [docs/zh/kubeadm.md](docs/zh/kubeadm.md) | kubelet 与 kubeadm 节点接入 |
| [docs/zh/nri.md](docs/zh/nri.md) | NRI 配置、生命周期、adjustment、验证器和运维边界 |
| [docs/zh/rootless.md](docs/zh/rootless.md) | rootless 配置、行为、限制和验证 |
| [docs/zh/checkpoint-restore.md](docs/zh/checkpoint-restore.md) | CRI checkpoint / restore 行为 |

## 已知边界

- 默认执行路径是 Linux + `runc`。
- 部署流程仍以源码构建为主。
- 示例 systemd unit 适合验证节点，生产使用前应按节点安全基线复核。
- rootless mode 有明确限制，不是 rootful kubelet 集成的直接替代方案。
- checkpoint / restore 依赖宿主机、CRIU 与 runtime 支持。

## License

`crius` 使用 [Apache-2.0](LICENSE) 许可证。
