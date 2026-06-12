# Node Operations And Validation Guide

This document describes the standard process for running `crius` on a single
node or validation node. It covers installation, configuration review, service
management, functional validation, log diagnostics, recovery maintenance, and
issue report materials.

`crius` is currently an experimental runtime implementation. This document is
intended for development validation, integration testing, and production
candidate evaluation. It is not a production SLA or Kubernetes compatibility
promise.

## Scope

This document applies to:

- running the `crius` daemon as root on Linux hosts
- validating local containers, explicit Pods, images, logs, exec, stats, and
  diagnostics with `crs`
- validating CRI paths with `crictl` or kubelet
- checking recovery state after node restart, daemon restart, or abnormal exit

Rootless, checkpoint/restore, NRI, and kubeadm integration have dedicated
documents. This guide provides the operational entry point and common checks.

## Host Dependencies

Recommended dependencies:

| Dependency | Purpose |
| --- | --- |
| Linux | Primary supported host system |
| `runc` | Default OCI runtime |
| CNI plugins | Local Pod and CRI Pod networking |
| `tar` | Image and archive handling |
| `protobuf-compiler` | Source builds |
| `crictl` | CRI compatibility validation |
| `criu` | checkpoint/restore validation, optional |
| `slirp4netns` or `pasta` | rootless network validation, optional |

Real container execution, mounts, CNI, cgroups, and service integration usually
require root privileges.

## Installation

Build:

```bash
cargo build --features shim --bins
```

Install binaries and the systemd unit:

```bash
sudo install -Dm755 target/debug/crius /usr/bin/crius
sudo install -Dm755 target/debug/crs /usr/bin/crs
sudo install -Dm755 target/debug/crius-shim /usr/bin/crius-shim
sudo install -Dm644 crius.service /etc/systemd/system/crius.service
```

Export the default configuration:

```bash
sudo mkdir -p /etc/crius
sudo /usr/bin/crius --write-default-config /etc/crius/crius.conf
```

For local `crs pod` networking, install the independent CNI configuration:

```bash
sudo install -Dm644 examples/cni/crius-bridge.conflist \
  /etc/crius/cni/net.d/10-crius-bridge.conflist
```

Start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now crius
sudo systemctl status crius
```

## Configuration Review

Before startup, review at least these fields:

| Field | Check |
| --- | --- |
| `root` | persistent state directory is writable; default `/var/lib/crius` |
| `api.listen` | CRI socket is expected; default `unix:///run/crius/crius.sock` |
| `runtime.root` | runtime state directory is writable; default `/run/crius` |
| `runtime.runtime_path` | OCI runtime exists and is executable |
| `runtime.shim_path` | `crius-shim` path is correct |
| `runtime.pause_image` | CRI PodSandbox pause image is pullable or present locally |
| `image.root` | image storage directory is writable |
| `network.local.*` | local `crs pod` CNI config, plugins, and cache are available |
| `network.cri.*` | kubelet / `crictl` CNI config, plugins, and cache are available |
| `logging.dir` | log directory is writable |
| `metrics.*` | if enabled, listener address or socket meets security requirements |

Exact defaults are defined by the current binary:

```bash
crius --dump-default-config
crius --write-default-config /etc/crius/crius.conf
```

## Basic Validation

Validate daemon and local client:

```bash
crs version
crs status
crs doctor
crs debug runtime
```

Validate image paths:

```bash
crs image pull registry.k8s.io/pause:3.9
crs image list
crs image inspect registry.k8s.io/pause:3.9
```

Validate local ordinary containers:

```bash
cid=$(crs --quiet run --detach --pull missing alpine:latest -- /bin/sh -c 'echo ok; sleep 3')
crs exec "$cid" -- /bin/echo exec-ok
crs logs "$cid"
crs rm --force "$cid"
```

Validate local explicit Pods:

```bash
pod=$(crs --quiet pod run --name verify-pod --namespace default)
cid=$(crs --quiet run --detach --pod "$pod" alpine:latest -- /bin/sh -c 'echo pod-ok; sleep 3')
crs pod list --all
crs logs "$cid"
crs rm --force "$cid"
crs pod stop "$pod"
crs pod remove "$pod"
```

Validate the CRI endpoint:

```bash
crictl --runtime-endpoint unix:///run/crius/crius.sock version
crictl --runtime-endpoint unix:///run/crius/crius.sock info
crictl --runtime-endpoint unix:///run/crius/crius.sock images
```

## Network Validation

Inspect both network domains:

```bash
crs debug network
crs --output json debug network
```

Interpretation:

- the `local` network domain serves `crs pod`; the default config directory is
  `/etc/crius/cni/net.d`
- the `cri` network domain serves kubelet and `crictl`; the default config
  directories are `/etc/cni/net.d` and `/etc/kubernetes/cni/net.d`
- local ordinary containers created by `crs run` do not require the `cri`
  network domain to be ready
- `crictl runp` and kubelet PodSandbox creation require the `cri` network domain
  to be ready

See [networking.md](networking.md) for details.

## Logs And State

systemd status:

```bash
systemctl status crius
journalctl -u crius --no-pager
journalctl -u crius -f
```

`crs` diagnostics:

```bash
crs --output json doctor
crs --output json debug runtime
crs --output json debug shims
crs --output json recovery status
```

Common state paths:

| Path | Contents |
| --- | --- |
| `/var/lib/crius` | daemon persistent state and SQLite ledger |
| `/run/crius` | runtime state, sockets, shim state, and temporary artifacts |
| `/var/lib/containers/storage` | default image storage |
| `/var/log/crius` | daemon and container log directory |
| `/etc/crius` | daemon configuration |
| `/etc/crius/cni/net.d` | CRS local CNI configuration |

## Recovery And Maintenance

After daemon restart, inspect recovery state:

```bash
crs recovery status
crs recovery check
```

Run dry-run before repair:

```bash
crs recovery repair --dry-run
crs recovery repair --execute
```

Content garbage collection:

```bash
crs gc candidates
crs gc run --dry-run
crs gc run --execute
```

`--execute` modifies local state or deletes reclaimable artifacts. Even on
validation nodes, save logs and diagnostic output before destructive
maintenance.

## Upgrade And Rollback

Recommended process:

1. Stop or move active validation workloads.
2. Save the current configuration, service file, and diagnostic output.
3. Install the new `crius`, `crs`, and `crius-shim` binaries.
4. Run `crius --dump-default-config` and compare new or changed fields.
5. Restart the service.
6. Run basic validation, local container validation, local Pod validation, and
   CRI endpoint validation.

Example material collection:

```bash
sudo cp /etc/crius/crius.conf /etc/crius/crius.conf.$(date +%Y%m%d%H%M%S)
crs --output json doctor > /tmp/crius-doctor.json
crs --output json debug network > /tmp/crius-network.json
crs --output json recovery status > /tmp/crius-recovery.json
```

## Issue Report Materials

Public issues or internal reports should include:

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

For CRI/kubelet issues:

```bash
crictl --runtime-endpoint unix:///run/crius/crius.sock version
crictl --runtime-endpoint unix:///run/crius/crius.sock info
```

For networking issues:

```bash
ls -la /etc/crius/cni/net.d /etc/cni/net.d /etc/kubernetes/cni/net.d
ls -la /opt/cni/bin /usr/lib/cni /usr/libexec/cni
```

Remove registry credentials, tokens, private image references, and sensitive
workload environment variables before sharing reports.

## Related Documents

- [crs.md](crs.md)
- [networking.md](networking.md)
- [config-matrix.md](config-matrix.md)
- [kubeadm.md](kubeadm.md)
- [rootless.md](rootless.md)
- [checkpoint-restore.md](checkpoint-restore.md)
- [nri.md](nri.md)
