# Architecture

This document describes the architecture implemented by the current `crius`
codebase. It focuses on existing behavior rather than an ideal target design.

## Role

`crius` is a Kubernetes CRI runtime. It exposes CRI v1 gRPC services to kubelet
and maps CRI objects to:

- OCI runtime execution
- image storage and registry access
- pod networking
- streaming I/O
- local state persistence and recovery
- runtime extension points such as NRI, CDI, checkpoint/restore, and rootless

`crius` is therefore an orchestration layer between CRI semantics and runtime,
image, network, state, and plugin systems.

## Process Model

| Process | Role |
| --- | --- |
| `crius` | Main daemon. Owns CRI gRPC, ImageService, RuntimeService, streaming setup, recovery, metrics, tracing, NRI, and control-plane logic. |
| `crius-shim` | Per-container helper used by the default runtime path for process, I/O, attach sockets, task RPC, checkpoint/restore tasks, and exit coordination. |

External dependencies include `runc`, CNI plugins, OCI registries, SQLite,
optional CRIU, optional NRI plugins, and optional rootless network helpers.

## High-Level Structure

```text
Kubelet / crictl
    |
    v
CRI gRPC server (`src/main.rs`, `src/server`)
    |
    +--> Runtime orchestration (`src/runtime`)
    +--> Pod sandbox management (`src/pod`)
    +--> Image service (`src/image`)
    +--> Network service (`src/network`)
    +--> Streaming service (`src/streaming`)
    +--> Persistence (`src/storage`)
    +--> NRI (`src/nri`)
```

## Module Boundaries

| Module | Responsibility |
| --- | --- |
| `src/main.rs` | CLI, configuration loading, environment and CLI overrides, logging, metrics, tracing, gRPC and streaming startup |
| `src/config/` | TOML model, defaults, derived path normalization, validation, environment overrides |
| `src/server/` | CRI RuntimeService, lifecycle, status, stats, events, recovery, checkpoint/restore orchestration |
| `src/image/` | CRI ImageService, registry pull, auth, image metadata, image volumes, artifact and snapshotter configuration |
| `src/runtime/` | Runtime backend trait, default `runc` backend, `wasm-direct` wiring, OCI bundle, shim manager |
| `src/shim/` | Shim daemon, child process handling, I/O, attach sockets, task RPC |
| `src/network/` | CNI loading, ADD/DEL, hostPort, PodCIDR template rendering, rootless helpers |
| `src/streaming/` | HTTP streaming server for exec, attach, and port-forward |
| `src/storage/` | SQLite ledger and persisted runtime metadata |
| `src/nri/` | NRI transport, plugin manager, conversion, adjustment merge, validation, domain actions |
| `src/security/` | seccomp, AppArmor, SELinux, CDI, devices, resource classes, OCI spec patching |
| `src/rootless/` | Rootless configuration resolution and status models |
| `src/metrics/` | Runtime, resources, and images metrics service |

## Configuration Lifecycle

The daemon builds effective configuration in this order:

```text
built-in defaults -> configuration file -> environment variables -> CLI overrides -> normalization -> validation
```

Important normalization behavior includes:

- runtime-derived paths follow `runtime.root`
- persistent derived paths follow `root`
- the default NRI socket follows the runtime state directory
- rootless mode moves default root, runtime, image, and log paths into XDG paths
- `runtime.container_stop_timeout` is normalized to at least 30 seconds
- `runtime.handlers` is normalized together with `runtime.runtimes.*` and the
  default `runtime.runtime_type`

## Runtime Handlers

The default handler is `runtime.runtime_type`, usually `runc`. Extra handlers
are declared with `runtime.handlers` and `runtime.runtimes.<name>`.

Each handler can configure runtime binary and root, backend type, platform
runtime paths, monitor/shim path, monitor cgroup and environment, allowed and
default annotations, privileged device behavior, create timeout, snapshotter,
and handler-specific CNI settings.

Kubelet can select a handler through `RuntimeClass`.

## Core Request Flows

### RunPodSandbox

1. Validate the request and runtime handler.
2. Allocate logical pod sandbox state.
3. Prepare pod directories and network namespace state.
4. Generate the pause container OCI spec.
5. Run applicable NRI pod/container lifecycle hooks.
6. Create and start the pause container through the runtime backend and shim.
7. Apply CNI networking and hostPort mappings.
8. Persist state and publish lifecycle events.

### CreateContainer / StartContainer

1. Resolve pod sandbox and runtime handler.
2. Resolve image metadata and rootfs behavior.
3. Build an OCI spec from CRI config, security context, mounts, resources,
   runtime defaults, and workload presets.
4. Apply and validate NRI container adjustments.
5. Write the bundle and create the container.
6. Start the container, update persisted state, and publish events.

If the container config references a checkpoint artifact, `CreateContainer`
loads checkpoint metadata and `StartContainer` uses the runtime
checkpoint/restore path.

### Exec / Attach / PortForward

Streaming uses a two-stage control-plane/data-plane flow:

1. kubelet calls the CRI gRPC method.
2. `crius` registers a short-lived token and request context.
3. `crius` returns a streaming URL.
4. kubelet connects to the HTTP streaming server.
5. the streaming server resolves the token and connects to the runtime or shim
   I/O path.

The streaming server supports TLS and per-request token expiry.

## State And Recovery

`crius` maintains in-memory state, runtime artifacts, and a SQLite ledger under
`root`. After daemon restart, recovery reads persisted state, rebuilds in-memory
objects, checks runtime and shim artifacts, restores monitors where possible,
cleans orphans when configured, and exposes recovery details through status and
events.

## Networking

The default network implementation is CNI. Local `crs` Pod workflows load CNI
configuration from `[network.local]`, which defaults to `/etc/crius/cni/net.d`.
Kubelet and generic CRI workflows load CNI configuration from `[network.cri]`,
which defaults to `/etc/cni/net.d` and `/etc/kubernetes/cni/net.d`. The legacy
top-level `[network]` CNI fields remain accepted and map to the CRI network
domain.

Supported behavior includes CNI ADD/DEL, hostPort mappings, PodCIDR template
rendering through `UpdateRuntimeConfig`, handler-specific CNI directories, and
rootless network helpers.

## Observability

Current observability surfaces include tracing logs, optional log files, CRI
container events, pod/container stats, `RuntimeConfig` and `Status` details,
optional metrics over TCP or Unix sockets, and optional tracing export.

## Boundaries

- The default execution path is Linux + `runc`.
- Deployment is source-build oriented.
- Rootless, `wasm-direct`, NRI, external snapshotter, and checkpoint/restore
  paths require scenario-specific validation.
- This document describes current behavior and is not a compatibility promise
  for every Kubernetes version or host distribution.
