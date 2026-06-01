# Configuration Reference

This document summarizes the current `crius` configuration model. The canonical
sources for exact fields and defaults are:

- `crius --dump-default-config`
- `src/config/mod.rs`
- `src/config/validation.rs`

The root [crius.conf](../../crius.conf) is a curated node-validation sample, not
a compatibility promise.

## Precedence

```text
CLI > environment variables > configuration file > built-in defaults
```

Not every field has a CLI or environment override. Large structured sections
such as `runtime.runtimes.*`, `runtime.workloads.*`,
`image.external_snapshotters.*`, and most NRI validator maps should be managed
in TOML.

## Export Defaults

```bash
crius --dump-default-config
sudo crius --write-default-config /etc/crius/crius.conf
```

## Reload Policy

`crius` does not currently implement general live reload or `SIGHUP` reload.
Most configuration changes require daemon restart.

| Area | Policy |
| --- | --- |
| `root` | restart required |
| `api.*` | restart required |
| `runtime.*` | restart required |
| `image.*` | restart required |
| `network.*` | restart required |
| `logging.*` | restart required |
| `security.*` | restart required |
| `metrics.*` | restart required |
| `tracing.*` | restart required |
| `nri.*` | restart required |
| `rootless.*` | restart required |

The main dynamic exception is `UpdateRuntimeConfig.network_config.pod_cidr`,
which can affect CNI template rendering when `network.conf_template` is set.

## Important Areas

### Base Paths

| Field | Purpose |
| --- | --- |
| `root` | Persistent daemon state, SQLite, recovery data |
| `runtime.root` | Runtime state, sockets, shim state, netns, version markers |
| `image.root` | Image content and metadata root |
| `logging.dir` | Default daemon and container log directory |

### CRI API And Streaming

Key fields include `api.listen`, `api.listen_aliases`, `api.allow_tcp_service`,
gRPC message size limits, pod event and metrics fields, `api.streaming.address`,
`api.streaming.port`, TLS settings, token TTL, and port-forward timeouts.

### Runtime

Key fields include `runtime.runtime_type`, `runtime.runtime_path`,
`runtime.root`, `runtime.handlers`, `runtime.runtimes.*`,
`runtime.workloads.*`, `runtime.pause_image`, `runtime.cgroup_driver`,
`runtime.shim_path`, `runtime.container_stop_timeout`,
`runtime.enable_criu_support`, ID mappings, default env/capabilities/sysctls,
device policy, hooks, timezone, and unprivileged network defaults.

`runtime.drop_infra_ctr = true` is rejected because `crius` requires
infra/pause containers for pod lifecycle, status, and recovery.

### Handler Configuration

Handler tables are declared as `runtime.runtimes.<handler>`. They can configure
backend type, backend options, runtime path/root, inheritance, monitor path,
streaming behavior, annotation policy, privileged device behavior, create
timeout, snapshotter, and handler-specific CNI settings.

### Images

Important fields include image driver/root, registry auth, transport,
short-name mode, pull progress timeout, download concurrency, retry count,
registry config, decryption settings, artifact stores, signature policy,
storage options, image volume behavior, pinned images, temporary directory,
OCI artifact mount support, and `image.external_snapshotters.*`.

### Network

`network.plugin` currently supports `cni`. Important fields include
`config_dirs`, `plugin_dirs`, `cache_dir`, `conf_template`, `max_conf_num`,
`ip_pref`, `teardown_timeout`, `default_network_name`,
`disable_hostport_mapping`, and `netns_mounts_under_state_dir`.

### Security

Important fields include seccomp selectors/profile path, default AppArmor
profile, AppArmor disable switch, SELinux enable switch, SELinux category range,
and hostNetwork SELinux behavior.

### Metrics, Tracing, NRI, Rootless

Metrics can listen on TCP or Unix socket and collect `runtime`, `resources`, and
`images`. Tracing uses an endpoint and sampling rate. NRI controls plugin
registration, sockets, CDI directories, annotation allowlists, timeouts, and the
built-in validator. Rootless controls XDG path resolution, subordinate IDs,
storage behavior, network helper mode, and rootless paths.

## CLI Overrides

Useful CLI overrides include `--config`, `--debug`, `--log`, `--listen`,
`--runtime-path`, `--runtime-config-path`, `--runtime-root`, `--pause-image`,
`--cni-config-dirs`, `--cni-plugin-dirs`, streaming TLS options,
`--seccomp-profile`, AppArmor/SELinux switches, `--dump-default-config`, and
`--write-default-config`.

## Environment Variables

Common overrides include `CRIUS_ROOT`, `CRIUS_LISTEN`,
`CRIUS_LISTEN_ALIASES`, `CRIUS_ALLOW_TCP_SERVICE`, streaming variables,
rootless variables, runtime variables, image root/registry variables, CNI
variables, logging variables, metrics variables, tracing variables, and
`CRIUS_ENABLE_NRI` / `CRIUS_ENABLE_CDI`.

Boolean values accept `1`, `true`, `yes`, `on`, `0`, `false`, `no`, and `off`.
Duration fields accept plain seconds or `ms`, `s`, `m`, and `h` suffixes.
