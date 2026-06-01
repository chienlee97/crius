# Checkpoint And Restore

`crius` implements CRI container checkpoint and restore paths when the selected
runtime backend and host support CRIU checkpointing.

## Configuration

```toml
[runtime]
enable_criu_support = true
criu_path = ""
criu_image_path = ""
criu_work_path = ""
```

| Field | Purpose |
| --- | --- |
| `runtime.enable_criu_support` | Enables CRI checkpoint and restore operations |
| `runtime.criu_path` | Optional CRIU binary path passed to the runtime |
| `runtime.criu_image_path` | Optional checkpoint image staging root |
| `runtime.criu_work_path` | Optional checkpoint work staging root |
| `runtime.no_pivot` | Propagated to restore when applicable |

If `enable_criu_support` is false, checkpoint and restore start paths return an
error.

## Checkpoint Flow

`CheckpointContainer`:

1. validates checkpoint support
2. resolves the target container and ensures it is running
3. reads bundle OCI config
4. pauses the container
5. asks the runtime backend or shim to checkpoint the task
6. resumes the container
7. verifies that the container is still running
8. snapshots the rootfs into `rootfs.tar`
9. writes checkpoint metadata and OCI config
10. exports a directory, JSON artifact, or archive depending on the requested
    location

## Restore Flow

Restore is driven by container create/start with a checkpoint artifact. Create
loads metadata, validates the rootfs snapshot manifest, stores restore metadata,
and derives an OCI spec. Start validates support, restores the rootfs snapshot
when present, asks the runtime backend or shim to restore the task, clears the
restore marker, and updates persisted state.

## Artifact Forms

Checkpoint locations may be JSON artifacts, directories containing checkpoint
metadata, or archives extractable with `tar`. Archive and rootfs snapshot paths
depend on the host `tar` binary.

## Requirements

- `runtime.enable_criu_support = true`
- runtime backend support for checkpoint and restore
- CRIU support in the selected OCI runtime
- required kernel features and permissions
- writable checkpoint staging paths
- `tar` for archive and rootfs snapshot operations

## Operational Notes

Validate checkpoint/restore on a single node first. Keep artifact storage large
enough for rootfs snapshots, protect artifacts as sensitive data, and validate
mounts, security context, user namespace settings, and runtime handler
compatibility during restore.
