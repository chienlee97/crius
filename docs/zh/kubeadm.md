# kubeadm / kubelet 接入

本文档说明如何在 Kubernetes 验证节点上将 `crius` 作为 CRI runtime 接入 kubelet
和 kubeadm。

默认路径：

- CRI socket：`unix:///run/crius/crius.sock`
- 配置文件：`/etc/crius/crius.conf`
- systemd unit：`/etc/systemd/system/crius.service`
- runtime state：`/run/crius`
- persistent state：`/var/lib/crius`

## 适用范围

本文适用于：

- 单节点或少量节点验证
- kubelet 接入自定义 CRI runtime
- kubeadm `init` / `join` 流程

本文不提供生产高可用部署、发行包制作或 Kubernetes 版本兼容性承诺。

## 前置依赖

节点上至少需要：

- `runc`
- CNI plugins
- `tar`
- `crictl`
- kubelet 和 kubeadm
- 与 kubelet 配置兼容的 cgroup 设置
- 可选：用于 checkpoint / restore 验证的 `criu`

kubelet/kubeadm 节点接入建议使用 root 权限运行的 `crius` daemon。rootless 是
独立的开发和受限环境验证路径，见 [rootless.md](rootless.md)。

## 构建与安装

构建 daemon 和 shim：

```bash
cargo build --features shim --bins
```

安装 binary 和 unit：

```bash
sudo install -Dm755 target/debug/crius /usr/bin/crius
sudo install -Dm755 target/debug/crius-shim /usr/bin/crius-shim
sudo install -Dm644 crius.service /etc/systemd/system/crius.service
```

生成配置：

```bash
sudo mkdir -p /etc/crius
sudo /usr/bin/crius --write-default-config /etc/crius/crius.conf
```

仓库根目录的 `crius.conf` 也可以作为整理过的起点，但当前 binary 导出的配置始终是
默认值事实来源。

## 节点配置

启动 kubelet 前，重点检查以下字段：

```toml
root = "/var/lib/crius"

[api]
listen = "unix:///run/crius/crius.sock"
allow_tcp_service = false

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

- 如果省略 `runtime.cgroup_driver`，`crius` 会根据主机探测。kubelet 节点建议显式
  配置为与 kubelet 一致的 driver。
- 导出的默认配置中 `runtime.handlers` 可为空；`crius` 会把默认 `runtime.runtime_type`
  归一化到 handler 列表。节点配置中保留 `handlers = ["runc"]` 能让意图更清楚。
- `runtime.pause_image` 必须可拉取或已存在于本地镜像存储，否则 Pod Sandbox 创建会失败。
- `runtime.drop_infra_ctr = true` 不受支持。

## CNI 准备

启动 workload 前确认：

- `network.plugin_dirs` 包含可执行 CNI plugins。
- `network.config_dirs` 包含有效 CNI 配置。
- 如果使用 PodCIDR 模板渲染，`network.conf_template` 指向可由
  `UpdateRuntimeConfig` 渲染的模板。

没有有效 CNI 配置时，Pod Sandbox 会在网络阶段失败。

## Pause 镜像准备

pause 镜像可通过两种方式准备：

- 在节点镜像构建或初始化阶段预置。
- 启动 `crius` 后通过 `crictl pull` 拉取。

示例：

```bash
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock \
  pull registry.k8s.io/pause:3.9
```

## 启动 crius

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now crius
sudo systemctl status crius
```

前台调试：

```bash
sudo /usr/bin/crius --config /etc/crius/crius.conf --debug
```

验证 CRI 连通性：

```bash
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock version
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock info
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock images
```

## 配置 kubelet

kubelet runtime endpoint：

```text
unix:///run/crius/crius.sock
```

直接参数形式：

```bash
--container-runtime-endpoint=unix:///run/crius/crius.sock
```

如果由 kubeadm 管理 kubelet，建议写入 kubelet 或 kubeadm 配置文件，而不是依赖临时
命令行修改。

## kubeadm init

节点上存在多个 CRI socket 时，请显式指定 `--cri-socket`：

```bash
sudo kubeadm init --cri-socket unix:///run/crius/crius.sock
```

## kubeadm join

```bash
sudo kubeadm join <control-plane>:6443 --token <token> \
  --discovery-token-ca-cert-hash <hash> \
  --cri-socket unix:///run/crius/crius.sock
```

## Socket Alias

`api.listen_aliases` 可创建额外 Unix socket alias，用于兼容依赖标准 runtime socket
路径的环境：

```toml
[api]
listen = "unix:///run/crius/crius.sock"
listen_aliases = ["unix:///var/run/crio/crio.sock"]
```

谨慎使用该能力。在空白验证节点上最多选择一个标准 alias，并避免与已安装 runtime
冲突。

## RuntimeClass

默认 handler 是 `runtime.runtime_type`。额外 handler 通过 `runtime.handlers` 和
`runtime.runtimes.<handler>` 暴露。

示例：

```yaml
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: runc
handler: runc
```

新部署建议使用 `RuntimeClass`，而不是 legacy workload annotation。

## 验证清单

kubelet 接入后，建议检查：

- `crictl version` 和 `crictl info` 成功。
- `crictl pods` 没有 endpoint 错误。
- pause 镜像存在或可拉取。
- CNI 能为 Pod 分配 IP。
- `kubectl logs`、`kubectl exec` 和 port-forward 可用。
- daemon 重启后 Pod / Container 状态仍可查询。
- `journalctl -u crius` 没有反复出现 recovery、CNI、shim 或 NRI 错误。

## 排障

### kubeadm 没有选择 crius

显式传入 `--cri-socket unix:///run/crius/crius.sock`。不要在存在多个 socket 的主机上
依赖自动探测。

### Pod Sandbox 创建失败

检查：

- pause 镜像是否可用。
- `runtime.runtime_path` 是否正确。
- `runtime.shim_path` 是否正确。
- kubelet 与 `crius` cgroup driver 是否一致。
- CNI 配置目录和插件目录是否有效。
- `root`、`runtime.root`、image root、CNI cache 是否可写。

### exec、attach 或 port-forward 失败

检查：

- kubelet 是否能访问 `api.streaming.address`。
- TLS 配置是否与 kubelet / client 预期一致。
- token TTL 是否过短。
- 容器是否为 `Running`。
- rootless port-forward 使用的 netns 路径是否位于 rootless netns 目录下。

### daemon 重启后状态异常

检查：

- `/var/lib/crius` 是否保留。
- `/run/crius` 是否以正确权限重建。
- live container 对应的 shim 工件和 runtime state 是否存在。
- `runtime.internal_wipe` 与 `runtime.internal_repair` 是否符合预期恢复策略。

## 相关文档

- [README.zh-CN.md](../../README.zh-CN.md)
- [architecture.md](architecture.md)
- [config-matrix.md](config-matrix.md)
- [nri.md](nri.md)
- [rootless.md](rootless.md)
- [checkpoint-restore.md](checkpoint-restore.md)
