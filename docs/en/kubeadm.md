# kubeadm / kubelet Integration

This document describes how to use `crius` as the CRI runtime on a Kubernetes
validation node.

Default paths:

- CRI socket: `unix:///run/crius/crius.sock`
- configuration file: `/etc/crius/crius.conf`
- systemd unit: `/etc/systemd/system/crius.service`
- runtime state: `/run/crius`
- persistent state: `/var/lib/crius`

## Scope

This guide is intended for single-node or small-node validation, kubelet
integration with a custom CRI runtime, and kubeadm `init` / `join` flows. It
does not provide production HA guidance, release packaging, or Kubernetes
compatibility guarantees.

## Prerequisites

Install or provide:

- `runc`
- CNI plugins
- `tar`
- `crictl`
- kubelet and kubeadm
- cgroup settings compatible with kubelet
- optional: `criu` for checkpoint/restore validation

For kubelet/kubeadm node integration, run the `crius` daemon with root
privileges. Rootless mode is a separate development and restricted-environment
validation path documented in [rootless.md](rootless.md).

## Build And Install

```bash
cargo build --features shim --bins
sudo install -Dm755 target/debug/crius /usr/bin/crius
sudo install -Dm755 target/debug/crius-shim /usr/bin/crius-shim
sudo install -Dm644 crius.service /etc/systemd/system/crius.service
```

Generate configuration:

```bash
sudo mkdir -p /etc/crius
sudo /usr/bin/crius --write-default-config /etc/crius/crius.conf
```

The root [crius.conf](../../crius.conf) is also a curated starting point, but
the current binary remains the source of truth for defaults.

## Node Configuration

Review these fields before starting kubelet:

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
```

Notes:

- If `runtime.cgroup_driver` is omitted, `crius` detects the host driver. For
  kubelet nodes, explicitly set the same driver as kubelet.
- `runtime.handlers` can be empty in exported defaults; `crius` normalizes the
  default `runtime.runtime_type` into the handler list.
- `runtime.pause_image` must be pullable or already present.
- `runtime.drop_infra_ctr = true` is not supported.

## CNI And Pause Image

Verify that `network.plugin_dirs` contains executable CNI plugins and
`network.config_dirs` contains valid CNI config. Without valid CNI config, pod
sandbox creation fails during networking.

Prepare the pause image by preloading it or pulling it through CRI:

```bash
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock \
  pull registry.k8s.io/pause:3.9
```

## Start crius

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now crius
sudo systemctl status crius
```

Foreground debugging:

```bash
sudo /usr/bin/crius --config /etc/crius/crius.conf --debug
```

Validate CRI connectivity:

```bash
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock version
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock info
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock images
```

## Configure kubelet

Set kubelet's runtime endpoint to:

```text
unix:///run/crius/crius.sock
```

Direct flag:

```bash
--container-runtime-endpoint=unix:///run/crius/crius.sock
```

For kubeadm-managed nodes, prefer kubelet or kubeadm configuration files over
manual temporary flags.

## kubeadm

When multiple CRI sockets exist, pass `--cri-socket` explicitly:

```bash
sudo kubeadm init --cri-socket unix:///run/crius/crius.sock
```

```bash
sudo kubeadm join <control-plane>:6443 --token <token> \
  --discovery-token-ca-cert-hash <hash> \
  --cri-socket unix:///run/crius/crius.sock
```

## RuntimeClass

The default handler is `runtime.runtime_type`; extra handlers are exposed
through `runtime.handlers` and `runtime.runtimes.<handler>`.

```yaml
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: runc
handler: runc
```

## Troubleshooting

- If kubeadm does not choose `crius`, pass `--cri-socket` explicitly.
- If pod sandbox creation fails, check the pause image, runtime path, shim path,
  cgroup driver alignment, CNI config, and writable state directories.
- If exec, attach, or port-forward fails, check streaming address reachability,
  TLS settings, token TTL, and container state.
- If state looks wrong after daemon restart, check `/var/lib/crius`, `/run/crius`,
  shim artifacts, runtime state, `runtime.internal_wipe`, and
  `runtime.internal_repair`.
