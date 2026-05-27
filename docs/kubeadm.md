# kubeadm / kubelet 接入

本文档给出 `crius` 作为 Kubernetes CRI runtime 的最小节点接入流程，目标是帮助你在测试节点上完成可重复部署与验证。

本文默认以下路径：

- CRI socket：`unix:///run/crius/crius.sock`
- 配置文件：`/etc/crius/crius.conf`
- systemd unit：`/etc/systemd/system/crius.service`

## 适用范围

本文适用于：

- 单节点或少量节点验证
- kubelet 直接接入自定义 CRI runtime
- kubeadm `init` / `join` 前的节点准备

本文不覆盖：

- 生产集群高可用部署
- 完整发行包制作
- 节点操作系统差异化适配

## 兼容性与验证状态

本文基于当前代码实现和常见 kubelet / kubeadm 接入方式整理，用于说明节点接入的最小可行路径。

当前仓库尚未提供正式的 Kubernetes 版本兼容矩阵，因此本文不应被理解为对特定 kubeadm、kubelet 或发行版组合的兼容性承诺。

## 前置依赖

节点上至少需要准备：

- `runc`
- CNI plugins
- `tar`
- `crictl`
- 可工作的 Kubernetes 节点基础环境

建议 kubelet 与 `crius` 使用一致的 cgroup driver。若 kubelet 采用 `systemd`，则 `crius` 也应配置为 `systemd`。

## 1. 安装二进制

如果你从源码构建：

```bash
cargo build --features shim --bins
sudo install -Dm755 target/debug/crius /usr/bin/crius
sudo install -Dm755 target/debug/crius-shim /usr/bin/crius-shim
sudo install -Dm644 crius.service /etc/systemd/system/crius.service
```

生成默认配置：

```bash
sudo mkdir -p /etc/crius
sudo /usr/bin/crius --write-default-config /etc/crius/crius.conf
```

## 2. 准备推荐配置

一个适合测试节点的最小配置如下：

```toml
root = "/var/lib/crius"

[api]
listen = "unix:///run/crius/crius.sock"
listen_aliases = []
allow_tcp_service = false

[api.streaming]
address = "127.0.0.1"
port = 0
enable_tls = false

[runtime]
runtime_type = "runc"
runtime_path = "/usr/bin/runc"
root = "/run/crius"
handlers = ["runc"]
pause_image = "registry.k8s.io/pause:3.9"
pause_command = "/pause"
cgroup_driver = "systemd"
shim_path = "/usr/bin/crius-shim"

[image]
driver = "overlay"
root = "/var/lib/containers/storage"

[network]
plugin = "cni"
config_dirs = ["/etc/cni/net.d", "/etc/kubernetes/cni/net.d"]
plugin_dirs = ["/opt/cni/bin", "/usr/lib/cni", "/usr/libexec/cni"]
cache_dir = "/var/lib/cni/cache"

[logging]
level = "info"
dir = "/var/log/crius"
```

说明：

- 建议优先使用 `crius --write-default-config` 生成全量配置，再按节点实际情况修改。
- `runtime.pause_image` 必须在本地可用；如果尚未预拉取，`RunPodSandbox` 会失败。
- `runtime.handlers` 会直接暴露给 kubelet / `RuntimeClass`。

## 3. 准备 pause 镜像与 CNI

在启动 kubelet 前，建议先确认两件事：

### pause 镜像已就绪

`runtime.pause_image` 必须能被 `crius` 在本地镜像存储中解析到。推荐做法是：

- 在节点镜像准备阶段预置该镜像
- 或者先启动 `crius`，再通过 `crictl pull` / `crictl images` 验证它已经可用

### CNI 配置已就绪

至少需要：

- `network.plugin_dirs` 指向存在可执行插件的目录
- `network.config_dirs` 下存在可被 kubelet 节点使用的 CNI 配置

如果没有 CNI 配置，Pod Sandbox 创建会卡在网络阶段。

## 4. 启动 `crius`

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now crius
sudo systemctl status crius
```

也可以先前台运行排障：

```bash
sudo /usr/bin/crius --config /etc/crius/crius.conf
```

## 5. 配置 kubelet

最直接的方式是显式指定 runtime endpoint：

```bash
--container-runtime-endpoint=unix:///run/crius/crius.sock
```

如果使用 kubeadm 管理 kubelet，建议把参数写入 kubelet 配置或 kubeadm 配置文件，而不是依赖手工临时参数。

## 6. 用 kubeadm 初始化或加入节点

如果节点上存在多个 CRI socket，执行 `kubeadm init` 或 `kubeadm join` 时建议显式指定：

```bash
sudo kubeadm init --cri-socket unix:///run/crius/crius.sock
```

或：

```bash
sudo kubeadm join <control-plane>:6443 --token <token> \
  --discovery-token-ca-cert-hash <hash> \
  --cri-socket unix:///run/crius/crius.sock
```

## 7. RuntimeClass

当 `runtime.handlers` 中暴露多个 handler 时，可以通过 `RuntimeClass` 为工作负载选择运行时：

```yaml
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: runc
handler: runc
```

如果仍需兼容 legacy 注解 `io.kubernetes.cri.untrusted-workload=true`，应确保节点上存在名为 `untrusted` 的 handler。对于新部署，建议显式使用 `RuntimeClass`。

## 8. 验证

建议按以下顺序检查：

```bash
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock version
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock info
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock pods
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock images
```

如果 kubelet 已接入，再创建一个最小 Pod 验证：

- Pod Sandbox 是否成功创建
- pause 镜像是否正常使用
- CNI 是否分配 IP
- `kubectl exec` / `kubectl logs` 是否可用

## 常见问题

### kubeadm 无法识别 runtime

优先显式传入 `--cri-socket`。不要默认依赖自动探测。

### Pod Sandbox 创建失败

优先检查：

- `runtime.pause_image` 是否已预拉取
- CNI 配置目录是否存在有效配置
- `runc`、`crius-shim` 路径是否正确
- kubelet 与 `crius` 的 cgroup driver 是否一致

### `exec` / `attach` 异常

优先检查：

- `api.streaming.address` 与返回 URL 是否可被 kubelet 访问
- 本机防火墙或网络命名空间策略是否阻断了 streaming 连接
- 容器是否仍处于 `Created` 或 `Running` 状态

### 重启后状态异常

优先检查：

- `/var/lib/crius` 是否持久保留
- `/run/crius` 是否被正常重建
- SQLite 状态、shim 工件和 runtime 实际状态是否一致

## 相关文档

- [架构设计](architecture.md)
- [配置参考](config-matrix.md)
- [NRI 说明](nri.md)

## 官方参考

- Kubernetes CRI 概念文档：
  `https://kubernetes.io/docs/concepts/architecture/cri/`
- kubeadm 安装与 runtime 自动探测说明：
  `https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/install-kubeadm/`
- kubeadm `init` 参数说明：
  `https://kubernetes.io/docs/reference/setup-tools/kubeadm/kubeadm-init/`
- RuntimeClass 官方文档：
  `https://kubernetes.io/docs/concepts/containers/runtime-class/`
