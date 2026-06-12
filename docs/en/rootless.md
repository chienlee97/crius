# Rootless Mode

This document describes current rootless support in `crius`.

Rootless mode is primarily for development and restricted-environment
validation. kubelet/kubeadm node integration should use a `crius` daemon
running with root privileges.

## Configuration

```toml
[rootless]
enabled = true
uid_mappings = []
gid_mappings = []
sub_uid_start = 100000
sub_uid_count = 65536
sub_gid_start = 100000
sub_gid_count = 65536
auto_configure_subids = true
use_fuse_overlayfs = true
network_mode = "Slirp4netns"
xdg_runtime_dir = ""
xdg_data_home = ""
storage_root = ""
runtime_root = ""
netns_dir = ""
slirp4netns_path = "slirp4netns"
pasta_path = "pasta"
```

Common environment overrides:

- `CRIUS_ROOTLESS`
- `CRIUS_ROOTLESS_XDG_RUNTIME_DIR`
- `CRIUS_ROOTLESS_XDG_DATA_HOME`
- `CRIUS_ROOTLESS_STORAGE_ROOT`
- `CRIUS_ROOTLESS_RUNTIME_ROOT`
- `CRIUS_ROOTLESS_NETNS_DIR`
- `CRIUS_ROOTLESS_USE_FUSE_OVERLAYFS`
- `CRIUS_ROOTLESS_NETWORK_MODE`
- `CRIUS_ROOTLESS_SLIRP4NETNS_PATH`
- `CRIUS_ROOTLESS_PASTA_PATH`

## Path Resolution

When `rootless.enabled = true`, `crius` resolves effective rootless settings
and rewrites paths that are still set to defaults:

- `root` moves under XDG data home.
- `runtime.root` moves under XDG runtime dir.
- `image.root` moves under the rootless storage root.
- `logging.dir` moves under the rootless data directory.
- rootless network namespaces are placed under the rootless netns directory.

Explicitly configured paths are preserved.

## Network Modes

| Mode | Behavior |
| --- | --- |
| `Slirp4netns` | Starts `slirp4netns` for pod networking |
| `Pasta` | Starts `pasta` for pod networking |
| `None` | Does not start a rootless network helper |

`Rootlesskit` is recognized by the configuration model but rejected during
validation because it is not implemented.

## Runtime Limitations

Current rootless limitations include:

- privileged containers are rejected
- explicit device requests are rejected
- `runtime.additional_devices` cannot be applied
- cgroup behavior may be disabled or degraded
- hugetlb controller assumptions are relaxed
- port-forward requires the sandbox netns path to be under the configured
  rootless netns directory

## Host Requirements

Depending on the validation path, the host may need subordinate UID/GID ranges,
`newuidmap`, `newgidmap`, `fuse-overlayfs`, `slirp4netns` or `pasta`, and XDG
runtime/data directories with correct ownership.

## Validation

```bash
CRIUS_ROOTLESS=true \
CRIUS_ROOTLESS_XDG_RUNTIME_DIR=/run/user/$(id -u) \
CRIUS_ROOTLESS_XDG_DATA_HOME=$HOME/.local/share \
crius --dump-default-config
```

Then validate effective paths, helper availability, sandbox creation behavior,
expected rejection of privileged/device-heavy workloads, and daemon restart
recovery.
