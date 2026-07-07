# Rootless 模式

本文档说明 `crius` 当前 rootless 支持的配置、路径解析、网络模式、限制和验证建议。

rootless 模式当前主要用于开发和受限环境验证。kubelet/kubeadm 节点接入仍建议
使用 root 权限运行的 `crius` daemon。

## 配置

rootless 由 `[rootless]` 配置域控制：

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

常用环境变量覆盖：

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

## 路径解析

当 `rootless.enabled = true` 时，`crius` 会解析 effective rootless 配置，并重写仍处于
默认值的路径：

- `root` 移动到 XDG data home 下。
- `runtime.root` 移动到 XDG runtime dir 下。
- `image.root` 移动到 rootless storage root。
- `logging.dir` 移动到 rootless data 目录下。
- rootless network namespace 放到 rootless netns 目录下。

已经显式配置过的路径会被保留。

## 网络模式

当前支持的 rootless 网络模式：

| 模式 | 行为 |
| --- | --- |
| `Slirp4netns` | 为 Pod 网络启动 `slirp4netns` |
| `Pasta` | 为 Pod 网络启动 `pasta` |
| `None` | 不启动 rootless 网络 helper |

`Rootlesskit` 可被配置模型识别，但启动期会被校验拒绝，因为该模式尚未实现。

## 运行时限制

rootless 模式会降级或拒绝需要高权限的行为。当前限制包括：

- 拒绝 privileged container。
- 拒绝显式 device 请求。
- 不允许应用 `runtime.additional_devices`。
- cgroup 行为可能被禁用或降级。
- hugetlb controller 前提会放宽。
- port-forward 要求 sandbox netns 路径位于配置的 rootless netns 目录下。

`Status` 和 `RuntimeConfig` 响应会包含 rootless 相关信息，便于调用方检查 effective
状态。

## 主机前提

根据验证路径不同，主机可能需要：

- subordinate UID / GID range
- `newuidmap` 和 `newgidmap`
- rootless overlay 场景下的 `fuse-overlayfs`
- `slirp4netns` 或 `pasta`
- 属主和权限正确的 XDG runtime / data 目录

## 验证建议

可以先用最小环境变量检查 effective 默认配置：

```bash
CRIUS_ROOTLESS=true \
CRIUS_ROOTLESS_XDG_RUNTIME_DIR=/run/user/$(id -u) \
CRIUS_ROOTLESS_XDG_DATA_HOME=$HOME/.local/share \
crius --dump-default-config
```

随后在受控测试环境启动 daemon，并检查：

- effective root 路径是否位于预期 XDG 目录。
- 所选 rootless 网络 helper 是否存在且可执行。
- Pod Sandbox 创建是否成功，或是否以清晰的 rootless 限制失败。
- privileged 和 device-heavy workload 是否按预期被拒绝。
- daemon 重启恢复后状态是否仍然一致。

## 边界

- 本仓库不把 rootless 模式作为生产 kubeadm 配方。
- `Rootlesskit` 模式尚未实现。
- 用户命名空间、网络 helper、存储和内核行为会随发行版变化，必须按主机验证。
- rootless 支持不替代 workload 安全审查。

## 相关文档

- [README.zh-CN.md](../../README.zh-CN.md)
- [architecture.md](architecture.md)
- [config-matrix.md](config-matrix.md)
- [kubeadm.md](kubeadm.md)
