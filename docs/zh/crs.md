# CRS 本地客户端使用手册

本文档说明 `crs` 的公开使用方式。`crs` 是 `crius` 项目提供的本地客户端，
用于直接操作 `crius` daemon，覆盖本地容器、显式 Pod、镜像、执行、日志、资源
统计、诊断、恢复和垃圾回收等日常工作流。

`crs` 面向本机运行时管理与节点验证场景。Kubernetes CRI 行为验证仍应使用
`crictl` 或 kubelet。二者可以连接同一个 `crius` socket，但默认语义不同：
`crs run` 创建本地普通容器；`crictl` 使用 CRI PodSandbox 模型。

## 连接与输出

默认连接地址为：

```text
unix:///run/crius/crius.sock
```

可以通过全局参数或环境变量指定其他地址：

```bash
crs --address unix:///run/crius/crius.sock version
CRIUS_ADDRESS=unix:///run/crius/crius.sock crs status
```

常用全局参数：

| 参数 | 说明 |
| --- | --- |
| `--address` | daemon endpoint，支持 Unix socket 路径、`unix://`、`http://` 和 `https://` |
| `--connect-timeout` | 建连超时，例如 `5s`、`500ms` |
| `--timeout` | 命令总超时；`0s` 表示关闭，默认关闭 |
| `--output table\|json\|text` | 输出格式 |
| `--quiet` | 减少非必要输出 |
| `--no-trunc` | 不截断 ID 或长字段 |
| `--debug` | 输出调试信息 |

面向脚本和自动化时，建议使用 JSON 输出：

```bash
crs --output json status
crs --output json inspect --type container <container-id>
```

## 快速验证

安装 daemon、shim 和客户端：

```bash
sudo install -Dm755 target/debug/crius /usr/bin/crius
sudo install -Dm755 target/debug/crius-shim /usr/bin/crius-shim
sudo install -Dm755 target/debug/crs /usr/bin/crs
sudo install -Dm644 crius.service /etc/systemd/system/crius.service
sudo mkdir -p /etc/crius
sudo /usr/bin/crius --write-default-config /etc/crius/crius.conf
sudo systemctl daemon-reload
sudo systemctl enable --now crius
```

验证 daemon：

```bash
crs version
crs status
crs doctor
crs debug runtime
```

启动一个本地普通容器：

```bash
crs run --rm --pull missing registry.k8s.io/pause:3.9
```

`crs run` 默认创建本地普通容器，使用本地容器工作流。只有需要共享网络、hostPort
映射或其他 Pod 级设置时，才需要显式创建 Pod。

## 命令总览

| 命令 | 说明 |
| --- | --- |
| `crs version` | 显示 runtime 和 CRI API 版本 |
| `crs status` | 显示 runtime/network ready 状态 |
| `crs doctor` | 执行面向运维的健康检查摘要 |
| `crs run` | 创建并启动本地普通容器；可加入显式 Pod |
| `crs ps` / `crs container list` | 列出容器 |
| `crs pods` / `crs pod list` | 列出 Pod |
| `crs images` / `crs image list` | 列出镜像 |
| `crs pull` / `crs image pull` | 拉取镜像 |
| `crs inspect` | 检查容器、Pod 或镜像 |
| `crs logs` | 查看容器日志 |
| `crs exec` | 在运行中容器内执行命令 |
| `crs stop` | 停止容器或 Pod |
| `crs rm` | 删除容器 |
| `crs rmi` | 删除镜像 |
| `crs rmp` | 删除 Pod |
| `crs container ...` | 容器完整生命周期和高级操作 |
| `crs pod ...` | 显式 Pod 生命周期、stats、metrics、port-forward |
| `crs image ...` | 镜像管理、文件系统信息、拉取状态 |
| `crs runtime ...` | runtime 配置、PodCIDR 更新、handler 查看 |
| `crs config ...` | daemon 配置与 reload 状态查看 |
| `crs events` | 查看 CRI 容器事件 |
| `crs stats` | 查看容器统计 |
| `crs metrics ...` | 查看 metrics 描述与采样 |
| `crs recovery ...` | 查看、检查和修复恢复账本状态 |
| `crs gc ...` | 查看和执行内容垃圾回收 |
| `crs debug ...` | 查看网络、runtime、shim、NRI、安全、cgroup、streaming 等调试状态 |
| `crs completion` | 生成 shell completion |

## 本地普通容器

普通容器是 `crs` 的默认工作流，适合本机命令、单容器服务和 daemon 功能验证。

```bash
crs run --name hello --rm alpine:latest -- /bin/sh -c 'echo hello'
```

后台运行：

```bash
cid=$(crs --quiet run --detach --name web --pull missing nginx:latest)
crs ps --all
crs logs "$cid"
crs stop "$cid"
crs rm "$cid"
```

常用创建参数：

| 参数 | 说明 |
| --- | --- |
| `--name` | 容器名称 |
| `--namespace` | 逻辑 namespace |
| `--detach` | 后台运行 |
| `--rm` | 容器退出后自动删除 |
| `--pull missing\|always\|never` | 镜像拉取策略 |
| `--stdin` / `--tty` | 启用交互式输入和 TTY |
| `--exec-mode attach\|sync` | 前台命令使用 streaming attach 或 ExecSync 模式 |
| `--env KEY=VALUE` | 设置环境变量 |
| `--env-file PATH` | 从文件读取环境变量；空行和 `#` 注释会被忽略 |
| `--workdir PATH` | 设置工作目录 |
| `--label KEY=VALUE` | 设置容器 label |
| `--annotation KEY=VALUE` | 设置容器 annotation |
| `--mount SPEC` | 添加挂载 |
| `--device SPEC` | 添加设备 |
| `--cdi-device NAME` | 添加 CDI 设备 |
| `--memory SIZE` | 设置内存限制 |
| `--cpu-quota VALUE` | 设置 CPU quota |
| `--resource SPEC` | 使用统一资源规格设置多个资源字段 |
| `--privileged` | 启用 privileged 容器 |
| `--user USER` | 设置容器用户，支持 UID、UID:GID 或用户名 |
| `--cap-add` / `--cap-drop` | 增加或删除 Linux capability |
| `--seccomp` / `--apparmor` / `--selinux` | 设置安全 profile |

命令参数可以直接跟在镜像后，也可以使用 `--command` 和 `--arg`：

```bash
crs run alpine:latest -- /bin/sh -c 'date'
crs run --command /bin/sh --arg -c --arg 'date' alpine:latest
```

## 显式 Pod

Pod 是面向共享网络、共享 namespace、hostPort 或 Pod 级资源的显式资源。单容器
工作负载可以直接使用 `crs run` 创建本地普通容器。

```bash
pod=$(crs --quiet pod run --name web --namespace default --publish 8080:80)
cid=$(crs --quiet run --detach --pod "$pod" --name nginx nginx:latest)
crs pod list --all
crs logs "$cid"
crs rm --force "$cid"
crs pod stop "$pod"
crs pod remove "$pod"
```

也可以使用分步容器生命周期：

```bash
pod=$(crs --quiet pod run --name batch)
cid=$(crs --quiet container create "$pod" alpine:latest -- /bin/sh -c 'echo done')
crs container start "$cid"
crs container logs "$cid"
crs container remove "$cid"
crs pod remove "$pod"
```

Pod 常用参数：

| 参数 | 说明 |
| --- | --- |
| `--name` / `--namespace` / `--uid` / `--attempt` | Pod metadata |
| `--hostname` | Pod hostname |
| `--publish HOST:CONTAINER[/PROTO]` | hostPort 映射，协议支持 `tcp`、`udp`、`sctp` |
| `--dns-server` / `--dns-search` / `--dns-option` | DNS 配置 |
| `--label KEY=VALUE` | Pod label |
| `--annotation KEY=VALUE` | Pod annotation |
| `--runtime-handler` | 选择 runtime handler |
| `--cgroup-parent` | Pod cgroup parent |
| `--sysctl KEY=VALUE` | Pod sysctl |
| `--host-network` / `--host-pid` / `--host-ipc` | 使用宿主机 namespace |
| `--userns` / `--uid-map` / `--gid-map` | user namespace 和 ID 映射 |
| `--overhead SPEC` | Pod overhead resources |
| `--pod-resource SPEC` | Pod resources |

`crs pod` 还支持：

```bash
crs pod inspect <pod>
crs pod stats <pod>
crs pod metrics
crs pod update-resources <pod> --pod-resource memory=256MiB
crs pod port-forward <pod> --forward 8080:80
crs pod stop <pod> --timeout 10
crs pod remove <pod>
```

## 镜像

镜像命令覆盖拉取、查询、删除、文件系统信息和拉取状态：

```bash
crs image pull registry.k8s.io/pause:3.9
crs image list
crs image inspect registry.k8s.io/pause:3.9
crs image fs-info
crs image transfers
crs image remove registry.k8s.io/pause:3.9
```

顶层别名：

```bash
crs pull registry.k8s.io/pause:3.9
crs images
crs rmi registry.k8s.io/pause:3.9
crs rmp <pod>
```

私有仓库鉴权可以使用：

```bash
crs image pull --username USER --password PASS --server registry.example.com registry.example.com/app:tag
crs image pull --auth-file /path/to/config.json registry.example.com/app:tag
crs image pull --auth-json '{"username":"USER","password":"PASS","serverAddress":"registry.example.com"}' registry.example.com/app:tag
```

## 容器管理

容器命令提供完整生命周期：

```bash
crs container list --all
crs container inspect <container>
crs container stop <container> --timeout 10
crs container remove <container>
```

执行命令：

```bash
crs exec <container> -- /bin/echo ok
crs container exec-sync <container> -- /bin/date
crs container exec -it <container> -- /bin/sh
crs container attach <container> --stdin --tty
```

日志和统计：

```bash
crs logs <container>
crs logs --follow --timestamps <container>
crs container stats <container>
crs container stats --pod <pod>
```

更新资源：

```bash
crs container update <container> --resource memory=512MiB,cpu=50000
crs container update <container> --annotation maintenance=scheduled
```

checkpoint：

```bash
crs container checkpoint <container> --location /var/lib/crius/checkpoints/demo --timeout 30
```

该功能依赖 daemon 配置、宿主机 CRIU 和 runtime 支持。

## 参数格式

`KEY=VALUE` 格式用于 label、annotation、env、sysctl 和部分资源字段。

端口映射：

```text
HOST:CONTAINER[/PROTO]
HOST_IP:HOST:CONTAINER[/PROTO]
[IPv6_HOST]:HOST:CONTAINER[/PROTO]
```

示例：

```bash
--publish 8080:80
--publish 127.0.0.1:8443:443/tcp
--publish 5353:53/udp
```

挂载：

```text
type=bind,src=/host/path,dst=/container/path[,ro][,z][,propagation=private]
type=image,image=IMAGE,dst=/container/path[,subpath=PATH]
```

示例：

```bash
--mount type=bind,src=/data,dst=/data,ro
--mount type=bind,src=/cache,dst=/cache,readonly=true,propagation=host-to-container
--mount type=image,image=registry.example.com/assets:latest,dst=/assets,subpath=public
```

设备：

```text
HOST[:CONTAINER[:PERMS]]
```

示例：

```bash
--device /dev/fuse
--device /dev/kvm:/dev/kvm:rwm
```

资源：

```text
cpu=50000,memory=256MiB,swap=512MiB,oom=-10,cpuset=0-1,hugepage=2Mi=64MiB,unified=memory.max=268435456
```

支持的字节单位为 `B`、`KiB`、`MiB`、`GiB` 和 `TiB`。

安全 profile：

```text
runtime/default
unconfined
localhost:PROFILE
```

SELinux 参数格式：

```text
user:role:type:level
```

## 网络模型

`crs` 使用两个明确区分的网络域：

| 网络域 | 使用方 | 默认配置目录 |
| --- | --- | --- |
| `[network.local]` | `crs pod run`、`crs run --pod`、`crs container create <pod>` | `/etc/crius/cni/net.d` |
| `[network.cri]` | kubelet、`crictl runp`、通用 CRI PodSandbox 工作流 | `/etc/cni/net.d`、`/etc/kubernetes/cni/net.d` |

本地 `crs pod` 默认建议使用仓库提供的 bridge 网络示例：

```bash
sudo install -Dm644 examples/cni/crius-bridge.conflist \
  /etc/crius/cni/net.d/10-crius-bridge.conflist
```

该示例使用 `loopback`、`bridge`、`host-local` 和 `portmap`，来源于标准
`containernetworking-plugins`。它不依赖 Kubernetes API、Calico、Flannel 或集群
控制面。

Calico、Flannel 等集群网络可以通过自定义 CNI 配置尝试接入，但不属于 `crs pod`
的默认网络目标。默认本地工作流应避免从 `/etc/cni/net.d` 继承 Kubernetes CNI
配置。

详细说明见 [networking.md](networking.md)。

## Runtime、配置和诊断

查看 daemon 配置：

```bash
crs config show
crs config reload-status
crs runtime config
crs runtime handlers --verbose
```

更新 PodCIDR：

```bash
crs runtime update --pod-cidr 10.244.0.0/24
```

该命令用于 CRI 网络模板渲染场景，通常由 kubelet 节点配置流程驱动。

诊断命令：

```bash
crs doctor
crs debug network
crs debug runtime
crs debug shims
crs debug nri
crs debug security
crs debug cgroups
crs debug streaming
crs debug metrics
crs debug tracing
crs debug rootless
```

建议在问题报告中附带：

```bash
crs --output json doctor
crs --output json debug network
crs --output json debug runtime
crs --output json recovery status
```

## 事件、统计和 Metrics

```bash
crs events
crs stats
crs metrics descriptors
crs metrics scrape
```

`crs stats` 和 `crs metrics` 依赖 daemon 对应能力以及宿主机 cgroup 可见性。
生产候选环境应配合系统日志、metrics 采集和告警系统使用。

## Recovery 和 GC

查看恢复状态：

```bash
crs recovery status
crs recovery check
```

修复前应先执行 dry-run：

```bash
crs recovery repair --dry-run
crs recovery repair --execute
```

内容垃圾回收同样要求显式选择 dry-run 或 execute：

```bash
crs gc candidates
crs gc run --dry-run
crs gc run --execute
```

`--execute` 会修改本地状态或删除可回收工件，建议仅在确认 daemon 状态和业务影响后
执行。

## Shell Completion

```bash
crs completion bash > /etc/bash_completion.d/crs
crs completion zsh > "${fpath[1]}/_crs"
crs completion fish > ~/.config/fish/completions/crs.fish
```

## 与 crictl 的区别

`crs` 和 `crictl` 都可以连接 `crius.sock`，但定位不同：

| 工具 | 主要用途 | 默认模型 |
| --- | --- | --- |
| `crs` | 本地运行时操作、开发验证、节点诊断和维护 | 本地普通容器；显式 Pod |
| `crictl` | Kubernetes CRI 行为验证、kubelet 接入排查 | CRI PodSandbox |

因此，`crs run` 成功并不要求 `[network.cri]` ready；`crictl runp` 需要有效的 CRI
CNI 配置。`crs pod` 使用 `[network.local]`，不应默认读取 Kubernetes CNI 目录。

## 稳定性说明

当前项目版本为 `0.1.0`。`crs` 是项目正式提供的本地客户端，但仓库整体仍处于
实验性运行时阶段。公开文档描述当前行为，不构成生产 SLA、Kubernetes 版本兼容矩阵
或长期 CLI 兼容性承诺。
