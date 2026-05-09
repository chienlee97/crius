# crius

`crius` 是一个使用 Rust 实现的 Kubernetes Container Runtime Interface（CRI）运行时。它提供 CRI v1 gRPC 服务，并负责把 kubelet 的 Pod / Container 请求编排到 OCI runtime、镜像存储、CNI 网络、流式连接与本地状态恢复链路。

项目当前适合以下场景：

- CRI / OCI / kubelet 协议研究
- 运行时功能验证与集成测试
- 面向 Rust 的容器运行时实现探索

当前不建议直接作为生产集群默认运行时使用。公开发布前，仍建议补充长期稳定性测试、异常恢复回归、Kubernetes 版本兼容矩阵与更完整的运维文档。

## 功能概览

- CRI v1 `RuntimeService` 与 `ImageService`
- 基于 `runc` 的 Pod Sandbox / Container 生命周期管理
- 流式 `exec`、`exec_sync`、`attach`、`port-forward`
- 基于 CNI 的 Pod 网络配置与 hostPort 处理
- `crius-shim` 容器进程、IO 与退出状态管理
- 镜像拉取、查询、删除与镜像文件系统统计
- SQLite 持久化与守护进程重启恢复
- NRI 集成与插件生命周期接线
- 独立 metrics 端点与 tracing 导出入口

## 项目状态

`crius` 当前版本号为 `0.1.0`，整体状态更接近实验性实现而非稳定发行版。公开仓库使用者应默认理解为：

- API 行为和配置字段仍可能调整
- 默认配置偏向开发与调试，而不是生产硬化
- 某些能力虽然已经接线，但仍需要更多真实环境验证

## 支持范围

当前文档与实现主要面向以下范围：

- Linux 主机环境
- 基于 `runc` 的 OCI runtime 路径
- 以 `root` 权限运行的节点级 CRI 守护进程
- 面向开发验证、集成测试与实现研究的使用场景

当前不承诺：

- 生产级 SLA 或长期兼容性保证
- 完整的 Kubernetes 版本兼容矩阵
- 多发行版、异构运行时或大规模集群环境的充分验证

相关技术说明见：

- [架构设计](docs/architecture.md)
- [kubeadm / kubelet 接入](docs/kubeadm.md)
- [配置参考](docs/config-matrix.md)

## 依赖与运行前提

建议在 Linux 主机上使用，并准备以下依赖：

- Rust 稳定版工具链
- `runc`
- `protobuf-compiler`
- `tar`
- `iproute2`
- CNI plugins
- 可选：`crictl`

多数真实运行路径以及部分测试需要 `root` 权限。

## 构建

仓库当前使用 `vendor/` 依赖，并包含补丁检查与应用逻辑。首次构建推荐执行：

```bash
make check-patch
make apply-patch
cargo build --features shim --bins
```

如果只想做一次基础构建，也可以直接运行：

```bash
make build
```

## 快速开始

### 1. 构建二进制

```bash
cargo build --features shim --bins
```

### 2. 生成配置文件

推荐直接导出当前版本内置默认配置，而不是手工复制旧样例：

```bash
sudo mkdir -p /etc/crius
sudo ./target/debug/crius --write-default-config /etc/crius/crius.conf
```

也可以先打印到终端查看：

```bash
./target/debug/crius --dump-default-config
```

### 3. 启动服务

```bash
sudo ./target/debug/crius --config /etc/crius/crius.conf
```

默认 CRI socket 为：

```text
unix:///run/crius/crius.sock
```

### 4. 用 `crictl` 验证

```bash
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock version
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock info
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock images
```

## 部署到节点

[docs/kubeadm.md](docs/kubeadm.md) 说明了 `crius` 接入 kubelet 与 kubeadm 的基本流程，覆盖：

- 二进制与 systemd 安装方式
- 推荐节点配置
- kubelet `--container-runtime-endpoint`
- `kubeadm init` / `kubeadm join`
- `RuntimeClass` 与 runtime handler
- 常见排障项

## 文档

| 文档 | 说明 |
| --- | --- |
| [docs/architecture.md](docs/architecture.md) | 整体架构、主链路与设计边界 |
| [docs/kubeadm.md](docs/kubeadm.md) | kubelet / kubeadm 节点接入 |
| [docs/config-matrix.md](docs/config-matrix.md) | 配置优先级、重载策略与关键字段参考 |
| [docs/nri.md](docs/nri.md) | NRI 能力、配置与排障 |

## 代码布局

| 路径 | 说明 |
| --- | --- |
| `src/main.rs` | 守护进程入口与服务装配 |
| `src/server/` | CRI `RuntimeService` 实现 |
| `src/image/` | CRI `ImageService` 实现 |
| `src/runtime/` | OCI / `runc` 集成、bundle 与 shim 协作 |
| `src/pod/` | Pod Sandbox 管理 |
| `src/network/` | CNI 与 hostPort 处理 |
| `src/streaming/` | `exec` / `attach` / `port-forward` 流式服务 |
| `src/storage/` | SQLite 持久化与恢复 |
| `src/nri/` | NRI manager、transport、merge、adjust |
| `src/shim/` | `crius-shim` 实现 |
| `tests/` | 集成测试 |

## 开发与验证

基础命令：

```bash
cargo fmt
cargo test
make test
```

说明：

- `cargo test` 主要覆盖单元测试与集成测试。
- `make test` 会做一次基础的 `crictl version` 联通性验证。
- 涉及真实 runtime、CNI、挂载、网络命名空间的路径通常需要 root 环境。

## 已知边界

- 当前默认围绕 Linux + `runc` 路径设计
- 节点部署流程仍以源码构建为主，尚未提供稳定发行包
- 文档已按公开仓库标准重构，但不代表所有能力都已完成生产级验证

## 生产边界

在当前阶段，`crius` 更适合作为开发与验证项目，而不是直接替代成熟生产运行时。对于任何面向生产的评估，至少还应补充以下工作：

- Kubernetes 版本兼容矩阵
- 长时间稳定性与资源压力测试
- 异常退出、节点重启与状态恢复回归
- 安全基线、审计与运维流程验证

## License

项目采用 [Apache-2.0](LICENSE) 许可证。
