# CRS Local Client Guide

This document describes the public usage model for `crs`, the project-native
local client for `crius`. It covers local containers, explicit Pods, images,
exec, logs, inspection, resource statistics, diagnostics, recovery, and garbage
collection.

`crs` is intended for local runtime management and node validation. Kubernetes
CRI behavior validation should still use `crictl` or kubelet. Both tools can
connect to the same `crius` socket, but their default models are different:
`crs run` creates a local ordinary container, while `crictl` uses the CRI
PodSandbox model.

## Connection And Output

The default endpoint is:

```text
unix:///run/crius/crius.sock
```

Use a global option or environment variable to select a different endpoint:

```bash
crs --address unix:///run/crius/crius.sock version
CRIUS_ADDRESS=unix:///run/crius/crius.sock crs status
```

Common global options:

| Option | Description |
| --- | --- |
| `--address` | daemon endpoint; accepts Unix socket paths, `unix://`, `http://`, and `https://` |
| `--connect-timeout` | connection timeout, such as `5s` or `500ms` |
| `--timeout` | total command timeout; `0s` disables it and is the default |
| `--output table\|json\|text` | output format |
| `--quiet` | reduce non-essential output |
| `--no-trunc` | do not truncate IDs or long fields |
| `--debug` | print debug details |

For scripts and automation, prefer JSON output:

```bash
crs --output json status
crs --output json inspect --type container <container-id>
```

## Quick Validation

Install the daemon, shim, and client:

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

Validate the daemon:

```bash
crs version
crs status
crs doctor
crs debug runtime
```

Run a local ordinary container:

```bash
crs run --rm --pull missing registry.k8s.io/pause:3.9
```

By default, `crs run` creates a local ordinary container. It uses the local
container workflow; create an explicit Pod only when shared networking,
hostPort mappings, or other Pod-level settings are required.

## Command Overview

| Command | Description |
| --- | --- |
| `crs version` | Show runtime and CRI API versions |
| `crs status` | Show runtime/network readiness |
| `crs doctor` | Run an operational health summary |
| `crs run` | Create and start a local ordinary container; can join an explicit Pod |
| `crs ps` / `crs container list` | List containers |
| `crs pods` / `crs pod list` | List Pods |
| `crs images` / `crs image list` | List images |
| `crs pull` / `crs image pull` | Pull images |
| `crs inspect` | Inspect a container, Pod, or image |
| `crs logs` | Read container logs |
| `crs exec` | Execute a command in a running container |
| `crs stop` | Stop a container or Pod |
| `crs rm` | Remove a container |
| `crs rmi` | Remove an image |
| `crs rmp` | Remove a Pod |
| `crs container ...` | Full container lifecycle and advanced operations |
| `crs pod ...` | Explicit Pod lifecycle, stats, metrics, and port-forward |
| `crs image ...` | Image management, filesystem information, and transfer state |
| `crs runtime ...` | Runtime configuration, PodCIDR update, and handler inspection |
| `crs config ...` | daemon configuration and reload status |
| `crs events` | Show CRI container events |
| `crs stats` | Show container statistics |
| `crs metrics ...` | Show metrics descriptors and samples |
| `crs recovery ...` | Show, check, and repair recovery ledger state |
| `crs gc ...` | Show and run content garbage collection |
| `crs debug ...` | Show debug state for networking, runtime, shims, NRI, security, cgroups, streaming, and related areas |
| `crs completion` | Generate shell completion |

## Local Ordinary Containers

Ordinary containers are the default `crs` workflow and are suitable for local
commands, single-container services, and daemon validation.

```bash
crs run --name hello --rm alpine:latest -- /bin/sh -c 'echo hello'
```

Run in the background:

```bash
cid=$(crs --quiet run --detach --name web --pull missing nginx:latest)
crs ps --all
crs logs "$cid"
crs stop "$cid"
crs rm "$cid"
```

Common create options:

| Option | Description |
| --- | --- |
| `--name` | Container name |
| `--namespace` | Logical namespace |
| `--detach` | Run in the background |
| `--rm` | Remove the container after exit |
| `--pull missing\|always\|never` | Image pull policy |
| `--stdin` / `--tty` | Enable interactive input and TTY |
| `--exec-mode attach\|sync` | Use streaming attach or ExecSync for foreground commands |
| `--env KEY=VALUE` | Set an environment variable |
| `--env-file PATH` | Read environment variables from a file; blank lines and `#` comments are ignored |
| `--workdir PATH` | Set the working directory |
| `--label KEY=VALUE` | Set a container label |
| `--annotation KEY=VALUE` | Set a container annotation |
| `--mount SPEC` | Add a mount |
| `--device SPEC` | Add a device |
| `--cdi-device NAME` | Add a CDI device |
| `--memory SIZE` | Set a memory limit |
| `--cpu-quota VALUE` | Set CPU quota |
| `--resource SPEC` | Set multiple resource fields with one resource specification |
| `--privileged` | Run a privileged container |
| `--user USER` | Set container user; accepts UID, UID:GID, or username |
| `--cap-add` / `--cap-drop` | Add or drop Linux capabilities |
| `--seccomp` / `--apparmor` / `--selinux` | Set security profiles |

Command arguments may follow the image directly, or be supplied through
`--command` and `--arg`:

```bash
crs run alpine:latest -- /bin/sh -c 'date'
crs run --command /bin/sh --arg -c --arg 'date' alpine:latest
```

## Explicit Pods

Pods are explicit resources for containers that need shared networking, shared
namespaces, hostPort mappings, or Pod-level resources. For single-container
workloads, use `crs run` to create a local ordinary container directly.

```bash
pod=$(crs --quiet pod run --name web --namespace default --publish 8080:80)
cid=$(crs --quiet run --detach --pod "$pod" --name nginx nginx:latest)
crs pod list --all
crs logs "$cid"
crs rm --force "$cid"
crs pod stop "$pod"
crs pod remove "$pod"
```

The same workflow can be expressed with the split container lifecycle:

```bash
pod=$(crs --quiet pod run --name batch)
cid=$(crs --quiet container create "$pod" alpine:latest -- /bin/sh -c 'echo done')
crs container start "$cid"
crs container logs "$cid"
crs container remove "$cid"
crs pod remove "$pod"
```

Common Pod options:

| Option | Description |
| --- | --- |
| `--name` / `--namespace` / `--uid` / `--attempt` | Pod metadata |
| `--hostname` | Pod hostname |
| `--publish HOST:CONTAINER[/PROTO]` | hostPort mapping; protocols are `tcp`, `udp`, and `sctp` |
| `--dns-server` / `--dns-search` / `--dns-option` | DNS configuration |
| `--label KEY=VALUE` | Pod label |
| `--annotation KEY=VALUE` | Pod annotation |
| `--runtime-handler` | Select runtime handler |
| `--cgroup-parent` | Pod cgroup parent |
| `--sysctl KEY=VALUE` | Pod sysctl |
| `--host-network` / `--host-pid` / `--host-ipc` | Use host namespaces |
| `--userns` / `--uid-map` / `--gid-map` | user namespace and ID mappings |
| `--overhead SPEC` | Pod overhead resources |
| `--pod-resource SPEC` | Pod resources |

Additional Pod operations:

```bash
crs pod inspect <pod>
crs pod stats <pod>
crs pod metrics
crs pod update-resources <pod> --pod-resource memory=256MiB
crs pod port-forward <pod> --forward 8080:80
crs pod stop <pod> --timeout 10
crs pod remove <pod>
```

## Images

Image commands cover pull, query, removal, filesystem information, and transfer
state:

```bash
crs image pull registry.k8s.io/pause:3.9
crs image list
crs image inspect registry.k8s.io/pause:3.9
crs image fs-info
crs image transfers
crs image remove registry.k8s.io/pause:3.9
```

Top-level aliases:

```bash
crs pull registry.k8s.io/pause:3.9
crs images
crs rmi registry.k8s.io/pause:3.9
crs rmp <pod>
```

Private registry authentication:

```bash
crs image pull --username USER --password PASS --server registry.example.com registry.example.com/app:tag
crs image pull --auth-file /path/to/config.json registry.example.com/app:tag
crs image pull --auth-json '{"username":"USER","password":"PASS","serverAddress":"registry.example.com"}' registry.example.com/app:tag
```

## Container Management

Container commands provide the full lifecycle:

```bash
crs container list --all
crs container inspect <container>
crs container stop <container> --timeout 10
crs container remove <container>
```

Exec and attach:

```bash
crs exec <container> -- /bin/echo ok
crs container exec-sync <container> -- /bin/date
crs container exec -it <container> -- /bin/sh
crs container attach <container> --stdin --tty
```

Logs and statistics:

```bash
crs logs <container>
crs logs --follow --timestamps <container>
crs container stats <container>
crs container stats --pod <pod>
```

Update resources:

```bash
crs container update <container> --resource memory=512MiB,cpu=50000
crs container update <container> --annotation maintenance=scheduled
```

Checkpoint:

```bash
crs container checkpoint <container> --location /var/lib/crius/checkpoints/demo --timeout 30
```

Checkpoint depends on daemon configuration, host CRIU support, and runtime
support.

## Argument Formats

`KEY=VALUE` is used for labels, annotations, environment variables, sysctls, and
some resource fields.

Port mappings:

```text
HOST:CONTAINER[/PROTO]
HOST_IP:HOST:CONTAINER[/PROTO]
[IPv6_HOST]:HOST:CONTAINER[/PROTO]
```

Examples:

```bash
--publish 8080:80
--publish 127.0.0.1:8443:443/tcp
--publish 5353:53/udp
```

Mounts:

```text
type=bind,src=/host/path,dst=/container/path[,ro][,z][,propagation=private]
type=image,image=IMAGE,dst=/container/path[,subpath=PATH]
```

Examples:

```bash
--mount type=bind,src=/data,dst=/data,ro
--mount type=bind,src=/cache,dst=/cache,readonly=true,propagation=host-to-container
--mount type=image,image=registry.example.com/assets:latest,dst=/assets,subpath=public
```

Devices:

```text
HOST[:CONTAINER[:PERMS]]
```

Examples:

```bash
--device /dev/fuse
--device /dev/kvm:/dev/kvm:rwm
```

Resources:

```text
cpu=50000,memory=256MiB,swap=512MiB,oom=-10,cpuset=0-1,hugepage=2Mi=64MiB,unified=memory.max=268435456
```

Supported byte units are `B`, `KiB`, `MiB`, `GiB`, and `TiB`.

Security profiles:

```text
runtime/default
unconfined
localhost:PROFILE
```

SELinux option format:

```text
user:role:type:level
```

## Networking Model

`crs` uses two distinct network domains:

| Domain | Used by | Default config directories |
| --- | --- | --- |
| `[network.local]` | `crs pod run`, `crs run --pod`, `crs container create <pod>` | `/etc/crius/cni/net.d` |
| `[network.cri]` | kubelet, `crictl runp`, generic CRI PodSandbox workflows | `/etc/cni/net.d`, `/etc/kubernetes/cni/net.d` |

For local `crs pod` usage, the recommended default is the repository-provided
bridge network example:

```bash
sudo install -Dm644 examples/cni/crius-bridge.conflist \
  /etc/crius/cni/net.d/10-crius-bridge.conflist
```

The example uses `loopback`, `bridge`, `host-local`, and `portmap` from the
standard `containernetworking-plugins` set. It does not depend on the
Kubernetes API, Calico, Flannel, or a cluster control plane.

Cluster network plugins such as Calico or Flannel may be attempted through
custom CNI configuration, but they are not the default target for `crs pod`.
Local workflows should not inherit Kubernetes CNI configuration from
`/etc/cni/net.d` by default.

See [networking.md](networking.md) for details.

## Runtime, Configuration, And Diagnostics

Inspect daemon configuration:

```bash
crs config show
crs config reload-status
crs runtime config
crs runtime handlers --verbose
```

Update PodCIDR:

```bash
crs runtime update --pod-cidr 10.244.0.0/24
```

This command is intended for CRI network template rendering workflows and is
usually driven by kubelet-oriented node configuration.

Diagnostics:

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

Recommended details for issue reports:

```bash
crs --output json doctor
crs --output json debug network
crs --output json debug runtime
crs --output json recovery status
```

## Events, Stats, And Metrics

```bash
crs events
crs stats
crs metrics descriptors
crs metrics scrape
```

`crs stats` and `crs metrics` depend on the corresponding daemon capabilities
and host cgroup visibility. Production candidates should combine these surfaces
with system logs, metrics collection, and alerting.

## Recovery And GC

Inspect recovery status:

```bash
crs recovery status
crs recovery check
```

Run dry-run before repair:

```bash
crs recovery repair --dry-run
crs recovery repair --execute
```

Content garbage collection also requires an explicit dry-run or execute mode:

```bash
crs gc candidates
crs gc run --dry-run
crs gc run --execute
```

`--execute` modifies local state or deletes reclaimable artifacts. Use it only
after confirming daemon state and workload impact.

## Shell Completion

```bash
crs completion bash > /etc/bash_completion.d/crs
crs completion zsh > "${fpath[1]}/_crs"
crs completion fish > ~/.config/fish/completions/crs.fish
```

## Difference From crictl

Both `crs` and `crictl` can connect to `crius.sock`, but their purposes differ:

| Tool | Primary purpose | Default model |
| --- | --- | --- |
| `crs` | Local runtime operation, development validation, diagnostics, and maintenance | Local ordinary containers; explicit Pods |
| `crictl` | Kubernetes CRI behavior validation and kubelet integration troubleshooting | CRI PodSandbox |

As a result, successful `crs run` execution does not require `[network.cri]` to
be ready. `crictl runp` requires valid CRI CNI configuration. `crs pod` uses
`[network.local]` and should not read Kubernetes CNI directories by default.

## Stability

The current project version is `0.1.0`. `crs` is the formal local client
provided by this repository, but the repository as a whole remains an
experimental runtime implementation. This document describes current behavior
and is not a production SLA, Kubernetes compatibility matrix, or long-term CLI
compatibility guarantee.
