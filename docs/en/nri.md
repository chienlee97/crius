# NRI

This document describes current Node Resource Interface (NRI) support in
`crius`.

## Position

NRI sits between CRI handlers and runtime execution:

```text
CRI handlers (`src/server`)
    |
    v
NRI manager / conversion / merge / validation (`src/nri`)
    |
    v
Runtime backend (`src/runtime`)
    |
    v
runc / shim / bundle
```

NRI does not run containers directly. It receives lifecycle notifications and
returns adjustments that `crius` applies to OCI specs, runtime state, and
resource updates.

## Capabilities

Current support includes ttRPC transport, plugin discovery and registration,
plugin ordering, request timeouts, `Configure`, `Synchronize`, pod/container
lifecycle dispatch, multi-plugin adjustment merge, conflict detection,
`ValidateContainerAdjustment`, built-in default validation, unsolicited update,
eviction, `blockio_class`, `rdt_class`, CDI device adjustment when enabled, and
daemon restart synchronization.

## Configuration

```toml
[nri]
enable = true
enable_cdi = true
runtime_name = "crius"
runtime_version = "0.1.0"
socket_path = "/run/crius/nri.sock"
plugin_path = "/opt/nri/plugins"
plugin_config_path = "/etc/nri/conf.d"
blockio_config_path = ""
cdi_spec_dirs = ["/etc/cdi", "/var/run/cdi"]
allowed_annotation_prefixes = []
workload_allowed_annotation_prefixes = []
registration_timeout_ms = 5000
request_timeout_ms = 2000
enable_external_connections = false

[nri.runtime_allowed_annotation_prefixes]

[nri.default_validator]
enable = false
reject_oci_hook_adjustment = false
reject_runtime_default_seccomp_adjustment = false
reject_unconfined_seccomp_adjustment = false
reject_custom_seccomp_adjustment = false
reject_namespace_adjustment = false
required_plugins = []
tolerate_missing_plugins_annotation = ""
```

Keep `enable = false` when NRI is not required. `enable_cdi` only controls CDI
device adjustments.

## Lifecycle

NRI is connected to pod sandbox run/stop/remove, container create/post-create,
start/post-start, update/post-update, stop/remove, asynchronous container exit
synchronization, daemon restart synchronize, unsolicited updates, and eviction.

## Adjustments

Supported adjustment areas include annotations, environment variables, args,
mounts, OCI hooks, devices, CDI devices, rlimits, OOM score, namespaces,
seccomp, sysctls, cgroups path, Linux CPU/memory/hugepage/pids resources,
`blockio_class`, and `rdt_class`.

## Annotation Allowlists

NRI annotation filtering can be configured globally, by runtime handler, or by
workload activation:

```toml
[nri]
allowed_annotation_prefixes = ["example.com/global/"]

[nri.runtime_allowed_annotation_prefixes]
runc = ["example.com/runc/"]

[[nri.workload_allowed_annotation_prefixes]]
activation_annotation = "example.com/workload"
activation_value = "latency-sensitive"
allowed_annotation_prefixes = ["example.com/workload/"]
```

## Built-In Validator

The built-in validator can reject selected adjustment classes before they reach
the runtime:

```toml
[nri.default_validator]
enable = true
reject_oci_hook_adjustment = true
reject_unconfined_seccomp_adjustment = true
reject_namespace_adjustment = true
required_plugins = ["plugin-a"]
tolerate_missing_plugins_annotation = "example.com/tolerate-missing-nri"
```

## Operations

Enable NRI, prepare plugin and config directories, review allowlists and
validator policy, start `crius`, confirm the socket, start plugins, verify
registration/configure/synchronize, and run a lifecycle test that exercises the
plugin.

Troubleshoot by checking enablement, socket permissions, plugin directories,
registration, configure/synchronize results, annotation allowlists, validator
rejections, and host/runtime support for requested resources.
