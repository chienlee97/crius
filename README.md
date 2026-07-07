# crius

English | [中文](README.zh-CN.md)

`crius` is a Rust implementation of the Kubernetes Container Runtime Interface
(CRI). It exposes CRI v1 `RuntimeService` and `ImageService` gRPC APIs, and
coordinates OCI runtime execution, image storage, CNI networking, streaming I/O,
local persistence, restart recovery, metrics, tracing, NRI, and rootless-related
paths.

The project is currently best suited for:

- CRI, OCI, kubelet, and container runtime research
- Runtime feature validation and integration testing
- Exploring a Rust implementation of a Kubernetes CRI runtime

`crius` is not positioned as a production replacement for established container
runtimes. Production evaluation should add a Kubernetes compatibility matrix,
long-running stability tests, failure recovery regressions, security review, and
operational runbooks.

## Status

The current package version is `0.1.0`. Public users should treat this
repository as an experimental runtime implementation:

- API behavior and configuration fields may still change.
- Defaults are oriented toward development, debugging, and node validation.
- Several advanced paths are wired but still need broader real-cluster
  validation.

## Features

- CRI v1 `RuntimeService` and `ImageService`
- Pod sandbox and container lifecycle management through OCI runtime backends
- Default `runc` backend with runtime-handler-specific configuration
- Optional `wasm-direct` backend wiring for handler experiments
- `crius-shim` for process, I/O, attach socket, task RPC, and exit coordination
- `exec`, `exec_sync`, `attach`, and `port-forward` streaming
- CNI networking, hostPort handling, PodCIDR template rendering, and CNI
  teardown timeouts
- Image pull, local metadata, removal, filesystem stats, registry auth,
  signature and decryption configuration, pinned images, and image volumes
- Internal snapshotters and external snapshotter configuration references
- SQLite persistence, daemon restart recovery, and orphan artifact cleanup
- CRI checkpoint and restore paths backed by CRIU-capable runtimes
- NRI plugin registration, lifecycle dispatch, adjustment merge, built-in
  validation, unsolicited update, eviction, and synchronize support
- Rootless configuration resolution, rootless network helpers, and rootless
  status reporting
- CRI events, pod metrics, resource stats, metrics endpoint, and tracing export

## Supported Scope

The current implementation and documentation focus on:

- Linux hosts
- Source-based builds and validation
- `runc` as the default OCI runtime path
- Rootful node-level daemon integration with kubelet
- Development, integration testing, and runtime implementation research

The project does not currently claim:

- Production SLA or long-term compatibility guarantees
- A published Kubernetes version compatibility matrix
- Complete distribution packaging
- Broad validation across large clusters, heterogeneous runtimes, or host
  distributions

## Repository Layout

| Path | Description |
| --- | --- |
| `src/main.rs` | Daemon entrypoint, CLI, configuration loading, service assembly |
| `src/server/` | CRI `RuntimeService`, lifecycle, status, stats, events, recovery |
| `src/image/` | CRI `ImageService`, registry pull, local image metadata, image storage |
| `src/runtime/` | Runtime backend abstraction, `runc`, `wasm-direct`, shim manager |
| `src/shim/` | `crius-shim` implementation |
| `src/pod/` | Pod sandbox state and pause container handling |
| `src/network/` | CNI, netns, hostPort, rootless network helpers |
| `src/streaming/` | HTTP streaming server for exec, attach, and port-forward |
| `src/storage/` | SQLite persistence and runtime state recovery |
| `src/nri/` | NRI manager, transport, conversion, merge, adjustment, domain logic |
| `src/security/` | seccomp, AppArmor, SELinux, CDI, devices, resource classes |
| `proto/` | CRI, NRI, and protobuf inputs |
| `tests/` | Integration tests and release gates |

## Prerequisites

Recommended host dependencies:

- Linux
- Rust stable toolchain
- `runc`
- `protobuf-compiler`
- `zlib-devel` or the distribution equivalent
- `tar`
- CNI plugins
- Optional: `crictl`, `kubelet`, `kubeadm`
- Optional for checkpoint/restore: `criu` and a runtime with CRIU support
- Optional for rootless validation: `newuidmap`, `newgidmap`, `slirp4netns` or
  `pasta`, and `fuse-overlayfs`

Most real runtime, mount, CNI, cgroup, and kubelet integration paths require
root privileges.

## Build

The repository uses vendored dependencies through `.cargo/config.toml`. Some
vendored crates need local patches before a complete build.

```bash
make check-patch
make apply-patch
cargo build --features shim --bins
```

Standard project build:

```bash
make build
```

`make build` currently builds the default `crius` binary. Use
`cargo build --features shim --bins` when you also need `crius-shim` and the
local `crs` client.

Build only the local `crs` client:

```bash
cargo build --bin crs
```

See [docs/en/operations.md](docs/en/operations.md) for node installation and
validation, and [docs/en/crs.md](docs/en/crs.md) for the local `crs` client.

## Quick Start

Build the project:

```bash
cargo build --features shim --bins
```

Install on a validation node, export configuration, and start the service as
described in [docs/en/operations.md](docs/en/operations.md). The default CRI
endpoint is:

```text
unix:///run/crius/crius.sock
```

Validate with `crictl`:

```bash
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock version
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock info
sudo crictl --runtime-endpoint unix:///run/crius/crius.sock images
```

Use [docs/en/crs.md](docs/en/crs.md) for local `crs` workflows and
[docs/en/networking.md](docs/en/networking.md) for local `crs` CNI setup.

## Configuration

Configuration precedence:

```text
CLI > environment variables > configuration file > built-in defaults
```

Not every field has a CLI or environment override. Exact defaults and field
names are defined by the current binary:

```bash
crius --dump-default-config
crius --write-default-config /etc/crius/crius.conf
```

The repository [crius.conf](crius.conf) is a curated node-validation sample.

## Kubernetes Integration

See [docs/en/kubeadm.md](docs/en/kubeadm.md) for the full kubelet and kubeadm
flow. The kubelet runtime endpoint is:

```text
unix:///run/crius/crius.sock
```

kubeadm init example:

```bash
sudo kubeadm init --cri-socket unix:///run/crius/crius.sock
```

kubeadm join example:

```bash
sudo kubeadm join <control-plane>:6443 --token <token> \
  --discovery-token-ca-cert-hash <hash> \
  --cri-socket unix:///run/crius/crius.sock
```

## Development And Validation

Common commands:

```bash
cargo fmt --check
cargo test
make release-gate
```

Release gate commands:

```bash
cargo fmt --check
cargo build --features shim --bins
cargo test --features shim
cargo run --bin crs -- completion bash >/tmp/crs.bash
test -s /tmp/crs.bash
```

Optional manual validation on a prepared node:

```bash
sudo systemctl restart crius
crs status
crs version
crs image list
```

Additional gated tests are available when the environment has the required
external dependencies:

```bash
make crictl-smoke
make kubelet-smoke
make fault-injection
make release-soak
```

`make test` starts a local daemon on `unix:///tmp/crius.sock` and runs a basic
`crictl version` smoke test.

## Documentation

| Document | Description |
| --- | --- |
| [docs/en/architecture.md](docs/en/architecture.md) | Architecture, module boundaries, and core request flows |
| [docs/en/crs.md](docs/en/crs.md) | Local `crs` client usage, containers, Pods, images, diagnostics, recovery, and GC |
| [docs/en/networking.md](docs/en/networking.md) | Local `crs` CNI, CRI CNI, and network domain boundaries |
| [docs/en/operations.md](docs/en/operations.md) | Node installation, validation, diagnostics, recovery, and issue report materials |
| [docs/en/config-matrix.md](docs/en/config-matrix.md) | Configuration precedence, reload policy, and important fields |
| [docs/en/kubeadm.md](docs/en/kubeadm.md) | kubelet and kubeadm node integration |
| [docs/en/nri.md](docs/en/nri.md) | NRI configuration, lifecycle, adjustments, validators, and operations |
| [docs/en/rootless.md](docs/en/rootless.md) | Rootless configuration, behavior, limitations, and validation |
| [docs/en/checkpoint-restore.md](docs/en/checkpoint-restore.md) | CRI checkpoint and restore behavior |

Chinese documentation is available under [docs/zh](docs/zh/architecture.md).

## Known Boundaries

- The default execution path is Linux + `runc`.
- Deployment is still source-build oriented.
- The sample systemd unit is suitable for validation nodes and should be
  reviewed against production security baselines before production use.
- Rootless mode is primarily for development and restricted-environment
  validation; kubelet/kubeadm node integration should use a `crius` daemon
  running with root privileges.
- Checkpoint/restore depends on host, CRIU, and runtime support.

## License

`crius` is licensed under [Apache-2.0](LICENSE).
