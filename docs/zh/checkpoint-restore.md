# Checkpoint 与 Restore

`crius` 在所选 runtime backend 和宿主机支持 CRIU checkpoint 的前提下，实现了 CRI
container checkpoint / restore 路径。

## 配置

相关 runtime 配置：

```toml
[runtime]
enable_criu_support = true
criu_path = ""
criu_image_path = ""
criu_work_path = ""
```

字段说明：

| 字段 | 作用 |
| --- | --- |
| `runtime.enable_criu_support` | 启用 CRI checkpoint 和 restore 操作 |
| `runtime.criu_path` | 可选 CRIU binary 路径，会传给 runtime |
| `runtime.criu_image_path` | 可选 checkpoint image staging 根目录 |
| `runtime.criu_work_path` | 可选 checkpoint work staging 根目录 |
| `runtime.no_pivot` | 在适用时传播到 restore 流程 |

如果 `enable_criu_support` 为 false，checkpoint 和 restore start 路径会返回错误。

## Checkpoint 流程

`CheckpointContainer` 的高层流程：

1. 校验 checkpoint 支持已启用。
2. 解析目标容器，并确认容器处于 running 状态。
3. 读取 bundle OCI config。
4. checkpoint 前暂停容器。
5. 调用 runtime backend 或 shim 执行 task checkpoint。
6. 恢复容器运行。
7. 校验 checkpoint 后容器仍为 running。
8. 将容器 rootfs snapshot 写入 `rootfs.tar`。
9. 写入 checkpoint metadata 和 OCI config。
10. 根据请求 location 导出目录、JSON artifact 或 archive。

生成的 metadata 包括 image reference、checkpoint 时间、bundle path、runtime image path
和 rootfs snapshot 信息。

## Restore 流程

Restore 由引用 checkpoint artifact 的 container create / start 流程驱动。

Create 阶段：

- 加载 checkpoint metadata。
- 校验 rootfs snapshot manifest。
- 将 restore metadata 写入 container internal state。
- 从 checkpoint artifact 派生 OCI spec。

Start 阶段：

- 再次确认 checkpoint support 仍然启用。
- 如存在 rootfs snapshot，则恢复 rootfs。
- 调用 runtime backend 或 shim restore task。
- 成功后清理 internal restore marker。
- 更新持久化容器状态。

## Artifact 形式

checkpoint location 当前支持：

- JSON artifact
- 包含 checkpoint metadata 的目录
- 可通过 `tar` 解开的 archive

archive 和 rootfs snapshot 路径依赖宿主机 `tar` binary。

## Runtime 前提

checkpoint / restore 需要：

- `runtime.enable_criu_support = true`
- 所选 runtime backend 实现 checkpoint 和 restore
- 所选 OCI runtime path 支持 CRIU
- 宿主机具备所需内核特性和权限
- checkpoint staging 路径可写
- `tar` 可用于 archive 和 rootfs snapshot 操作

默认 `runc` backend 会将 checkpoint / restore 操作传递给 `runc` 或配置的 shim 路径。

## 运维建议

- 先在单节点上验证 checkpoint / restore，再扩大测试范围。
- checkpoint artifact 所在存储需要留足 rootfs snapshot 空间。
- restored container 对 runtime 环境敏感，应验证 mounts、security context、user
  namespace 和 runtime handler 一致性。
- 配置 `runtime.criu_path`、`criu_image_path`、`criu_work_path` 时使用绝对路径。
- daemon 重启后也应运行 restore 测试，验证持久化行为。

## 排障

常见失败点：

- checkpoint support 未启用。
- 目标容器不是 running 状态。
- runtime 不支持 CRIU。
- CRIU binary 不存在或与宿主机不兼容。
- checkpoint artifact 缺少 `manifest` 或 `ociConfig`。
- rootfs snapshot 缺失，或不是预期 `tar` 格式。
- restore 时 runtime handler 与 checkpoint 环境不一致。
- staging 目录权限不足。

请优先查看 daemon 日志和 CRI status 中的具体错误。

## 边界

- checkpoint / restore 依赖宿主机和 runtime 能力。
- 当前建议用于受控验证，直到补充更广泛的兼容性测试。
- artifact 可能包含敏感的容器文件系统和配置数据，应按敏感数据保护。

## 相关文档

- [README.zh-CN.md](../../README.zh-CN.md)
- [architecture.md](architecture.md)
- [config-matrix.md](config-matrix.md)
- [kubeadm.md](kubeadm.md)
