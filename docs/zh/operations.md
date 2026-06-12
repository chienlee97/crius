# 节点运行与验证指南

本文档说明在单节点或验证节点上运行 `crius` 的标准流程。内容覆盖安装、配置审查、
服务管理、功能验证、日志诊断、恢复维护和问题报告材料。

`crius` 当前仍是实验性运行时实现。本文档面向开发验证、集成测试和生产候选评估，
不构成生产 SLA 或 Kubernetes 版本兼容承诺。

## 适用范围

本文档适用于以下场景：

- 在 Linux 主机上以 root 权限运行 `crius` daemon。
- 使用 `crs` 验证本地容器、显式 Pod、镜像、日志、exec、stats 和诊断能力。
- 使用 `crictl` 或 kubelet 验证 CRI 路径。
- 在节点重启、daemon 重启或异常退出后检查恢复状态。

rootless、checkpoint/restore、NRI 和 kubeadm 集成有独立文档，本文仅给出运行入口和
通用检查项。

## 主机依赖

建议准备：

| 依赖 | 用途 |
| --- | --- |
| Linux | 当前主要支持的宿主系统 |
| `runc` | 默认 OCI runtime |
| CNI plugins | 本地 Pod 和 CRI Pod 网络 |
| `tar` | 镜像和归档处理 |
| `protobuf-compiler` | 源码构建 |
| `crictl` | CRI 行为验证 |
| `criu` | checkpoint/restore 验证，可选 |
| `slirp4netns` 或 `pasta` | rootless 网络验证，可选 |

真实容器运行、挂载、CNI、cgroup 和 service 集成通常需要 root 权限。

## 安装

构建：

```bash
cargo build --features shim --bins
```

安装二进制和 systemd unit：

```bash
sudo install -Dm755 target/debug/crius /usr/bin/crius
sudo install -Dm755 target/debug/crs /usr/bin/crs
sudo install -Dm755 target/debug/crius-shim /usr/bin/crius-shim
sudo install -Dm644 crius.service /etc/systemd/system/crius.service
```

导出默认配置：

```bash
sudo mkdir -p /etc/crius
sudo /usr/bin/crius --write-default-config /etc/crius/crius.conf
```

本地 `crs pod` 网络建议安装独立 CNI 配置：

```bash
sudo install -Dm644 examples/cni/crius-bridge.conflist \
  /etc/crius/cni/net.d/10-crius-bridge.conflist
```

启动：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now crius
sudo systemctl status crius
```

## 配置审查

启动前至少检查以下字段：

| 字段 | 检查项 |
| --- | --- |
| `root` | 持久化状态目录是否可写，默认 `/var/lib/crius` |
| `api.listen` | CRI socket 是否符合预期，默认 `unix:///run/crius/crius.sock` |
| `runtime.root` | runtime 状态目录是否可写，默认 `/run/crius` |
| `runtime.runtime_path` | OCI runtime 是否存在且可执行 |
| `runtime.shim_path` | `crius-shim` 路径是否正确 |
| `runtime.pause_image` | CRI PodSandbox pause 镜像是否可拉取或已存在 |
| `image.root` | 镜像存储目录是否可写 |
| `network.local.*` | 本地 `crs pod` CNI 配置、插件和 cache 是否可用 |
| `network.cri.*` | kubelet / `crictl` CNI 配置、插件和 cache 是否可用 |
| `logging.dir` | 日志目录是否可写 |
| `metrics.*` | 如启用 metrics，监听地址或 socket 是否符合安全要求 |

精确默认值以当前二进制输出为准：

```bash
crius --dump-default-config
crius --write-default-config /etc/crius/crius.conf
```

## 基础验证

验证 daemon 和本地客户端：

```bash
crs version
crs status
crs doctor
crs debug runtime
```

验证镜像路径：

```bash
crs image pull registry.k8s.io/pause:3.9
crs image list
crs image inspect registry.k8s.io/pause:3.9
```

验证本地普通容器：

```bash
cid=$(crs --quiet run --detach --pull missing alpine:latest -- /bin/sh -c 'echo ok; sleep 3')
crs exec "$cid" -- /bin/echo exec-ok
crs logs "$cid"
crs rm --force "$cid"
```

验证本地显式 Pod：

```bash
pod=$(crs --quiet pod run --name verify-pod --namespace default)
cid=$(crs --quiet run --detach --pod "$pod" alpine:latest -- /bin/sh -c 'echo pod-ok; sleep 3')
crs pod list --all
crs logs "$cid"
crs rm --force "$cid"
crs pod stop "$pod"
crs pod remove "$pod"
```

验证 CRI endpoint：

```bash
crictl --runtime-endpoint unix:///run/crius/crius.sock version
crictl --runtime-endpoint unix:///run/crius/crius.sock info
crictl --runtime-endpoint unix:///run/crius/crius.sock images
```

## 网络验证

查看两个网络域：

```bash
crs debug network
crs --output json debug network
```

判断标准：

- `local` 网络域服务于 `crs pod`，默认配置目录为 `/etc/crius/cni/net.d`。
- `cri` 网络域服务于 kubelet 和 `crictl`，默认配置目录为 `/etc/cni/net.d` 和
  `/etc/kubernetes/cni/net.d`。
- `crs run` 本地普通容器不要求 `cri` 网络域 ready。
- `crictl runp` 和 kubelet PodSandbox 要求 `cri` 网络域 ready。

详细说明见 [networking.md](networking.md)。

## 日志与状态

systemd 状态：

```bash
systemctl status crius
journalctl -u crius --no-pager
journalctl -u crius -f
```

`crs` 诊断：

```bash
crs --output json doctor
crs --output json debug runtime
crs --output json debug shims
crs --output json recovery status
```

常用状态目录：

| 路径 | 内容 |
| --- | --- |
| `/var/lib/crius` | daemon 持久化状态和 SQLite 账本 |
| `/run/crius` | runtime 状态、socket、shim 状态和临时工件 |
| `/var/lib/containers/storage` | 默认镜像存储 |
| `/var/log/crius` | daemon 和容器日志目录 |
| `/etc/crius` | daemon 配置 |
| `/etc/crius/cni/net.d` | CRS 本地 CNI 配置 |

## 恢复与维护

daemon 重启后检查恢复状态：

```bash
crs recovery status
crs recovery check
```

执行修复前先 dry-run：

```bash
crs recovery repair --dry-run
crs recovery repair --execute
```

内容垃圾回收：

```bash
crs gc candidates
crs gc run --dry-run
crs gc run --execute
```

`--execute` 会修改本地状态或删除可回收工件。验证节点上也应先保存日志和诊断输出，
再执行破坏性维护操作。

## 升级与回滚

建议流程：

1. 停止或迁移正在运行的验证工作负载。
2. 保存当前配置、service 文件和诊断输出。
3. 安装新的 `crius`、`crs` 和 `crius-shim`。
4. 运行 `crius --dump-default-config` 对比新增或变更配置字段。
5. 重启 service。
6. 执行基础验证、本地容器验证、本地 Pod 验证和 CRI endpoint 验证。

保存材料示例：

```bash
sudo cp /etc/crius/crius.conf /etc/crius/crius.conf.$(date +%Y%m%d%H%M%S)
crs --output json doctor > /tmp/crius-doctor.json
crs --output json debug network > /tmp/crius-network.json
crs --output json recovery status > /tmp/crius-recovery.json
```

## 问题报告材料

公开 issue 或内部问题单建议包含：

```bash
crs --output json version
crs --output json status
crs --output json doctor
crs --output json debug network
crs --output json debug runtime
crs --output json debug shims
crs --output json recovery status
crs --output json config show
systemctl status crius --no-pager
journalctl -u crius --no-pager
```

如涉及 CRI/kubelet：

```bash
crictl --runtime-endpoint unix:///run/crius/crius.sock version
crictl --runtime-endpoint unix:///run/crius/crius.sock info
```

如涉及网络，还应提供：

```bash
ls -la /etc/crius/cni/net.d /etc/cni/net.d /etc/kubernetes/cni/net.d
ls -la /opt/cni/bin /usr/lib/cni /usr/libexec/cni
```

提交前请移除 registry 凭据、token、私有镜像地址和业务敏感环境变量。

## 相关文档

- [crs.md](crs.md)
- [networking.md](networking.md)
- [config-matrix.md](config-matrix.md)
- [kubeadm.md](kubeadm.md)
- [rootless.md](rootless.md)
- [checkpoint-restore.md](checkpoint-restore.md)
- [nri.md](nri.md)
