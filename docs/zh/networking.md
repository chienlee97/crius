# 网络模型

本文档说明 `crius` 的网络配置模型，以及 `crs` 本地工作流与 CRI/kubelet 工作流
之间的边界。

## 网络域

`crius` 使用 CNI 作为默认网络实现，并将本地 `crs` 网络与 CRI 网络分成两个配置域：

| 配置域 | 默认配置目录 | 使用方 |
| --- | --- | --- |
| `[network.local]` | `/etc/crius/cni/net.d` | `crs pod run`、`crs run --pod`、`crs container create <pod>` |
| `[network.cri]` | `/etc/cni/net.d`、`/etc/kubernetes/cni/net.d` | kubelet、`crictl runp`、CRI `RunPodSandbox` |

旧版顶层 `[network]` 字段仍可被解析，并映射到 CRI 网络域。新的本地 `crs` Pod
工作流不应默认继承 Kubernetes CNI 目录。

## 本地 CRS Pod 默认网络

本地 `crs pod` 的推荐默认配置为：

```toml
[network.local]
config_dirs = ["/etc/crius/cni/net.d"]
plugin_dirs = ["/opt/cni/bin", "/usr/lib/cni", "/usr/libexec/cni"]
cache_dir = "/var/lib/cni/crius-local-cache"
```

安装示例 CNI 配置：

```bash
sudo install -Dm644 examples/cni/crius-bridge.conflist \
  /etc/crius/cni/net.d/10-crius-bridge.conflist
```

该配置定义名为 `crius-bridge` 的本地 bridge 网络，使用以下标准 CNI 插件：

| 插件 | 作用 |
| --- | --- |
| `loopback` | 启用 Pod network namespace 中的 loopback 接口 |
| `bridge` | 创建并连接宿主机 bridge，默认 bridge 名称为 `crius0` |
| `host-local` | 为本机 Pod 分配本地 IP 地址 |
| `portmap` | 实现 hostPort 映射 |

默认地址段为 `10.88.0.0/16`，默认网关为 `10.88.0.1`。该示例启用
`isGateway`、`ipMasq` 和 `hairpinMode`，适用于常见的本机 Pod 出网和端口发布场景。

## 与 CRI 网络的区别

CRI 网络域用于 Kubernetes 模型：

```toml
[network.cri]
config_dirs = ["/etc/cni/net.d", "/etc/kubernetes/cni/net.d"]
plugin_dirs = ["/opt/cni/bin", "/usr/lib/cni", "/usr/libexec/cni"]
cache_dir = "/var/lib/cni/cache"
```

该配置域服务于 kubelet 和 `crictl`。例如：

```bash
crictl --runtime-endpoint unix:///run/crius/crius.sock runp pod.json
```

`crs run` 的本地普通容器路径不要求 `[network.cri]` ready。`crs pod` 使用
`[network.local]`。因此，`crs debug network` 中可以同时出现：

```text
local network: ready
cri network: not-ready
```

这表示本地 `crs pod` 可用，但 CRI/kubelet 的 CNI 配置尚未准备好。

## Calico、Flannel 与其他 CNI

`crs pod` 的默认目标是 CRS 本地显式 Pod 网络。默认配置只加载 `[network.local]`
指定的 CNI 配置目录，不会自动继承 Kubernetes 或 CRI CNI 目录，也不会假设
Calico、Flannel、kube-apiserver、etcd 或其他 Kubernetes 节点组件存在。

Calico 和 Flannel 可以通过自定义 CNI 配置进行实验性接入，但使用者需要自行满足
插件依赖：

- Calico 通常依赖 calico-node、IPAM、Kubernetes API 或其他 datastore。
- Flannel 通常依赖 flanneld 生成的 subnet 信息，并配合集群节点网络使用。
- 这些插件的生命周期、路由和地址管理超出 `crs` 默认本地 bridge 网络的范围。

默认推荐仍是标准 `containernetworking-plugins` 组合：

```text
loopback + bridge + host-local + portmap
```

## 常用场景

本地 Pod 网络：

```bash
pod=$(crs --quiet pod run --name web --publish 8080:80)
crs run --detach --pod "$pod" nginx:latest
crs pod inspect "$pod"
```

host network Pod：

```bash
crs pod run --name hostnet --host-network
```

诊断网络配置：

```bash
crs debug network
crs --output json debug network
crs doctor
```

## 运行要求

本地 CNI 工作流通常要求：

- 以 root 权限运行 `crius` daemon。
- `plugin_dirs` 中存在可执行 CNI 插件二进制。
- `config_dirs` 中存在有效 `.conf`、`.conflist` 或 `.json` 配置。
- `cache_dir` 可由 daemon 写入。
- 宿主机允许创建 network namespace、veth、bridge、iptables 或 nftables 规则。

显式 `crs pod` 网络需要有效的本地 CNI 配置。

## 故障排查

优先收集以下信息：

```bash
crs --output json debug network
crs --output json doctor
systemctl status crius
journalctl -u crius --no-pager
```

常见问题：

| 现象 | 可能原因 |
| --- | --- |
| `local network is not configured` | `/etc/crius/cni/net.d` 缺少有效本地 CNI 配置 |
| `CNIConfigMissing` | 对应网络域的配置目录中没有有效配置 |
| `missing plugin binaries` | CNI 配置引用了未安装或不可执行的插件 |
| Pod 无法出网 | bridge、IP masquerade 或宿主机转发/防火墙规则异常 |
| hostPort 不生效 | `portmap` 插件缺失，或 CNI 配置未声明 `portMappings` capability |

## 稳定性说明

本文档描述当前网络行为。`crius` 仍处于实验性运行时阶段，公开网络配置字段和诊断
输出可能随项目演进调整。
