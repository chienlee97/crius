# 配置参考

本文档总结 `crius` 当前配置模型。精确字段和默认值的事实来源始终是：

- `crius --dump-default-config`
- `src/config/mod.rs`
- `src/config/validation.rs`

仓库根目录的 [crius.conf](../../crius.conf) 是整理过的节点验证样例，不是兼容性承诺。

## 优先级

effective configuration 的构造顺序为：

```text
CLI > 环境变量 > 配置文件 > 内置默认值
```

不是所有字段都暴露 CLI 或环境变量覆盖。大块结构化配置，例如 `runtime.runtimes.*`、
`runtime.workloads.*`、`image.external_snapshotters.*` 和多数 NRI validator map，
建议使用 TOML 管理。

## 导出默认配置

使用当前 binary 导出该版本的精确默认配置：

```bash
crius --dump-default-config
sudo crius --write-default-config /etc/crius/crius.conf
```

这样可以避免复制过期样例。

## 重载策略

`crius` 当前没有通用 live reload 或 `SIGHUP` reload 机制。大多数配置变更都需要
重启 daemon。

| 配置域 | 变更策略 |
| --- | --- |
| `root` | 需要重启 |
| `api.*` | 需要重启 |
| `runtime.*` | 需要重启 |
| `image.*` | 需要重启 |
| `network.*` | 需要重启 |
| `logging.*` | 需要重启 |
| `security.*` | 需要重启 |
| `metrics.*` | 需要重启 |
| `tracing.*` | 需要重启 |
| `nri.*` | 需要重启 |
| `rootless.*` | 需要重启 |

主要动态例外是 `UpdateRuntimeConfig.network_config.pod_cidr`。当
`network.conf_template` 已配置时，PodCIDR 更新可影响 CNI 模板渲染。

## 关键字段

### 基础路径

| 字段 | 作用 |
| --- | --- |
| `root` | daemon 持久化状态，包括 SQLite 和 recovery 数据 |
| `runtime.root` | runtime state，包括 socket、shim 状态、netns、version marker |
| `image.root` | 镜像内容和元数据根目录 |
| `logging.dir` | daemon 和 container 默认日志目录 |

派生默认值会跟随这些 root。例如 `api.listen` 默认位于 `runtime.root` 下，
`runtime.clean_shutdown_file` 默认位于 `root` 下。

### CRI API 与 Streaming

| 字段 | 作用 |
| --- | --- |
| `api.listen` | CRI gRPC listener，默认 `unix:///run/crius/crius.sock` |
| `api.listen_aliases` | 额外 Unix socket alias，用于 kubeadm 自动探测兼容 |
| `api.allow_tcp_service` | 暴露 CRI gRPC TCP listener 前必须显式开启 |
| `api.grpc_max_send_msg_size` | gRPC 最大发送消息大小 |
| `api.grpc_max_recv_msg_size` | gRPC 最大接收消息大小 |
| `api.enable_pod_events` | 是否发送额外 Pod lifecycle events |
| `api.included_pod_metrics` | `all` 或 `cpu`、`memory`、`network`、`process`、`disk` 子集 |
| `api.exec_sync_io_drain_timeout` | `ExecSync` 退出后等待 stdout / stderr drain 的时间 |
| `api.streaming.address` | 返回给 kubelet 的 streaming URL 地址 |
| `api.streaming.port` | streaming server 端口，`0` 表示动态分配 |
| `api.streaming.enable_tls` | 是否启用 streaming TLS |
| `api.streaming.request_token_ttl` | 一次性 streaming token 生命周期 |
| `api.streaming.port_forward_*` | port-forward stream 创建和 idle 超时 |

### Runtime

| 字段 | 作用 |
| --- | --- |
| `runtime.runtime_type` | 默认 runtime handler 名称 |
| `runtime.runtime_path` | 默认 OCI runtime binary |
| `runtime.runtime_config_path` | 可选 runtime-specific 配置文件 |
| `runtime.base_runtime_spec` | 可选基础 OCI spec 模板 |
| `runtime.platform_runtime_paths` | `os/arch` runtime binary 覆盖 |
| `runtime.handlers` | 额外暴露给 kubelet / `RuntimeClass` 的 handler |
| `runtime.runtimes.*` | handler-specific runtime 配置 |
| `runtime.workloads.*` | annotation 激活的 workload resource preset |
| `runtime.pause_image` | Pod Sandbox pause 镜像 |
| `runtime.pause_command` | pause 镜像内命令 |
| `runtime.cgroup_driver` | 可选 cgroup driver 覆盖；省略时根据主机探测 |
| `runtime.shim_path` | `crius-shim` binary 路径 |
| `runtime.monitor_cgroup` | 可选 shim cgroup 放置策略 |
| `runtime.container_stop_timeout` | 最小优雅停止超时，会归一化到至少 30 秒 |
| `runtime.enable_criu_support` | 启用 CRI checkpoint / restore 路径 |
| `runtime.internal_wipe` | 允许启动期清理孤儿 runtime 工件 |
| `runtime.internal_repair` | 允许启动期检查并修复持久化状态 |
| `runtime.uid_mappings` / `gid_mappings` | daemon 默认 user namespace ID mapping |
| `runtime.default_env` | 注入所有容器的环境变量 |
| `runtime.default_capabilities` | daemon 默认 OCI capabilities |
| `runtime.default_sysctls` | daemon 默认 sysctl assignments |
| `runtime.default_ulimits` | daemon 默认 ulimit assignments |
| `runtime.allowed_devices` | 允许 CRI 请求映射的宿主设备 |
| `runtime.additional_devices` | 注入所有容器的额外设备 |
| `runtime.hooks_dir` | OCI hooks 目录 |
| `runtime.timezone` | 空表示不注入，`Local` 表示跟随宿主时区 |
| `runtime.enable_unprivileged_ports` | 为非 hostNetwork Pod 开启低端口绑定 |
| `runtime.enable_unprivileged_icmp` | 为符合条件的 Pod 开启 ping group range |

`runtime.drop_infra_ctr = true` 会被明确拒绝。`crius` 需要 infra / pause container
支撑 Pod 生命周期、状态和恢复。

### Runtime Handler

handler-specific 表为 `runtime.runtimes.<handler>`。

| 字段 | 作用 |
| --- | --- |
| `backend` | runtime backend，当前支持 `runc` 或 `wasm-direct` |
| `backend_options` | backend-specific key/value options |
| `runtime_path` | handler runtime binary |
| `runtime_root` | handler runtime state root |
| `inherit_default_runtime` | 继承默认 runtime path / root |
| `monitor_path` | handler shim / monitor binary |
| `stream_websockets` | handler 是否允许 websocket streaming |
| `allowed_annotations` | handler 允许的 annotation 前缀 |
| `default_annotations` | handler 默认注入的 OCI annotations |
| `container_create_timeout` | handler create timeout，最小 30 秒 |
| `snapshotter` | 空、`internal-overlay-untar`、`internal-cached-rootfs` 或 external snapshotter key |
| `cni_conf_dir` | handler-specific CNI 配置目录 |
| `cni_max_conf_num` | handler-specific CNI 配置文件数量限制 |

### 镜像

| 字段 | 作用 |
| --- | --- |
| `image.driver` | 镜像 backend，当前为 `overlay` |
| `image.root` | 镜像存储根目录 |
| `image.global_auth_file` | Docker-compatible fallback registry auth 文件 |
| `image.namespaced_auth_dir` | namespace-specific registry auth 目录 |
| `image.default_transport` | 镜像引用 transport，当前为空或 `docker://` |
| `image.short_name_mode` | `disabled` 或 `enforcing` |
| `image.pull_progress_timeout` | pull 无进展超时；`0s` 表示关闭 |
| `image.max_concurrent_downloads` | 单镜像并发 layer 下载数 |
| `image.pull_retry_count` | pull 失败额外重试次数 |
| `image.registry_config_dir` | registry `hosts.toml` 或 certs 目录 |
| `image.decryption_*` | OCI image decryption 配置 |
| `image.additional_artifact_stores` | 额外只读 OCI artifact store root |
| `image.signature_policy` | 全局镜像签名策略 |
| `image.signature_policy_dir` | namespace-specific 签名策略目录 |
| `image.storage_options` | storage driver options |
| `image.image_volumes` | `mkdir`、`bind` 或 `ignore` |
| `image.pinned_images` | 不参与 kubelet GC 的镜像 |
| `image.big_files_temporary_dir` | 大 layer staging 目录 |
| `image.oci_artifact_mount_support` | 是否允许 OCI artifact image-volume mount |
| `image.external_snapshotters.*` | runtime handler 可引用的 external snapshotter 声明 |

### 网络

| 字段 | 作用 |
| --- | --- |
| `network.plugin` | 网络实现，当前为 `cni` |
| `network.config_dirs` | 旧版 CRI CNI 配置目录；映射到 CRI 网络域 |
| `network.plugin_dirs` | 旧版 CRI CNI plugin binary 目录；映射到 CRI 网络域 |
| `network.cache_dir` | 旧版 CRI CNI cache 目录；映射到 CRI 网络域 |
| `network.conf_template` | 根据 PodCIDR 更新渲染的模板 |
| `network.max_conf_num` | 最大加载 CNI 配置文件数量；`0` 表示不限制 |
| `network.ip_pref` | `cni`、`ipv4` 或 `ipv6` |
| `network.teardown_timeout` | CNI DEL 超时 |
| `network.default_network_name` | 可选 CNI network name |
| `network.disable_hostport_mapping` | 禁用内建 hostPort 处理 |
| `network.netns_mounts_under_state_dir` | 将 netns mount 放到 runtime state dir 下 |
| `network.local.config_dirs` | 本地 `crs pod` CNI 配置目录，默认 `/etc/crius/cni/net.d` |
| `network.local.plugin_dirs` | 本地 `crs pod` CNI plugin binary 目录 |
| `network.local.cache_dir` | 本地 `crs pod` CNI cache 目录 |
| `network.cri.config_dirs` | kubelet / `crictl` / CRI PodSandbox CNI 配置目录 |
| `network.cri.plugin_dirs` | kubelet / `crictl` / CRI PodSandbox CNI plugin binary 目录 |
| `network.cri.cache_dir` | kubelet / `crictl` / CRI PodSandbox CNI cache 目录 |

本地 `crs` Pod 工作流使用 `[network.local]`。kubelet、`crictl` 和通用 CRI
PodSandbox 工作流使用 `[network.cri]`。详细说明见
[networking.md](networking.md)。

### 安全

| 字段 | 作用 |
| --- | --- |
| `security.seccomp_profile` | runtime default seccomp profile 路径；空表示内建行为 |
| `security.privileged_seccomp_profile` | privileged 容器 seccomp selector |
| `security.unset_seccomp_profile` | CRI 未设置 seccomp 时使用的 selector |
| `security.apparmor_default_profile` | 默认 AppArmor profile 名称 |
| `security.disable_apparmor` | 禁用 AppArmor 处理 |
| `security.enable_selinux` | 启用 SELinux label 注入 |
| `security.selinux_category_range` | MCS category range 上界 |
| `security.hostnetwork_disable_selinux` | hostNetwork Pod 跳过 SELinux label |

### Metrics 与 Tracing

| 字段 | 作用 |
| --- | --- |
| `metrics.enable` | 启用 metrics 服务 |
| `metrics.host` / `metrics.port` | metrics TCP listener |
| `metrics.socket_path` | metrics Unix socket，优先于 TCP |
| `metrics.enable_tls` | TCP metrics listener 是否启用 TLS |
| `metrics.collectors` | `runtime`、`resources`、`images` |
| `tracing.enable` | 启用 tracing export |
| `tracing.endpoint` | tracing export endpoint |
| `tracing.sampling_rate_per_million` | `0` 到 `1000000` 的采样率 |

### NRI

| 字段 | 作用 |
| --- | --- |
| `nri.enable` | NRI 总开关 |
| `nri.enable_cdi` | 启用 CDI device adjustment |
| `nri.runtime_name` / `runtime_version` | 上报给插件的 runtime identity |
| `nri.socket_path` | runtime-side NRI socket |
| `nri.plugin_path` | 预装插件目录 |
| `nri.plugin_config_path` | 插件配置目录 |
| `nri.blockio_config_path` | 本地 `blockio_class` mapping 文件 |
| `nri.cdi_spec_dirs` | CDI spec 搜索目录 |
| `nri.allowed_annotation_prefixes` | 全局 NRI annotation allowlist |
| `nri.runtime_allowed_annotation_prefixes` | handler-specific NRI annotation allowlist |
| `nri.workload_allowed_annotation_prefixes` | workload-activated NRI annotation allowlist |
| `nri.registration_timeout_ms` | 插件注册超时 |
| `nri.request_timeout_ms` | 插件请求超时 |
| `nri.enable_external_connections` | 允许外部启动插件连接 |
| `nri.default_validator.*` | 内建 adjustment validation policy |

### Rootless

| 字段 | 作用 |
| --- | --- |
| `rootless.enabled` | 启用 rootless 路径归一化和 runtime 限制 |
| `rootless.uid_mappings` / `gid_mappings` | 显式 rootless ID mappings |
| `rootless.sub_uid_*` / `sub_gid_*` | 默认 subordinate ID range |
| `rootless.auto_configure_subids` | 在支持时尝试 subordinate ID setup |
| `rootless.use_fuse_overlayfs` | 使用 rootless-friendly storage 行为 |
| `rootless.network_mode` | `Slirp4netns`、`Pasta` 或 `None`；`Rootlesskit` 会被拒绝 |
| `rootless.xdg_runtime_dir` / `xdg_data_home` | 显式 XDG 路径覆盖 |
| `rootless.storage_root` / `runtime_root` / `netns_dir` | 显式 rootless 路径覆盖 |
| `rootless.slirp4netns_path` / `pasta_path` | rootless network helper binary |

## CLI 覆盖

常用 CLI 覆盖项：

| 参数 | 作用 |
| --- | --- |
| `--config` | 配置文件路径 |
| `--debug` | 将日志级别设为 debug |
| `--log` | 设置 `logging.file` |
| `--listen` | 覆盖 `api.listen` |
| `--runtime-path` | 覆盖 `runtime.runtime_path` |
| `--runtime-config-path` | 覆盖 `runtime.runtime_config_path` |
| `--runtime-root` | 覆盖 `runtime.root` |
| `--pause-image` | 覆盖 `runtime.pause_image` |
| `--cni-config-dirs` | 覆盖 `network.config_dirs` |
| `--cni-plugin-dirs` | 覆盖 `network.plugin_dirs` |
| `--stream-address` / `--stream-port` | 覆盖 streaming 地址和端口 |
| `--stream-enable-tls` | 覆盖 streaming TLS 开关 |
| `--stream-tls-*` | 覆盖 streaming TLS 文件、版本和 cipher suites |
| `--seccomp-profile` | 覆盖 `security.seccomp_profile` |
| `--apparmor-default-profile` | 覆盖 `security.apparmor_default_profile` |
| `--disable-apparmor` | 覆盖 `security.disable_apparmor` |
| `--enable-selinux` | 覆盖 `security.enable_selinux` |
| `--dump-default-config` | 打印内置默认配置并退出 |
| `--write-default-config PATH` | 写入内置默认配置并退出 |

CLI 覆盖适合调试和本地验证。长期节点配置建议写入 TOML。

## 环境变量

常用环境变量覆盖：

| 变量 | 字段 |
| --- | --- |
| `CRIUS_ROOT` | `root` |
| `CRIUS_LISTEN` | `api.listen` |
| `CRIUS_LISTEN_ALIASES` | `api.listen_aliases` |
| `CRIUS_ALLOW_TCP_SERVICE` | `api.allow_tcp_service` |
| `CRIUS_STREAM_ADDRESS` / `CRIUS_STREAM_PORT` | streaming 地址和端口 |
| `CRIUS_ROOTLESS` | `rootless.enabled` |
| `CRIUS_ROOTLESS_NETWORK_MODE` | `rootless.network_mode` |
| `CRIUS_RUNTIME_TYPE` | `runtime.runtime_type` |
| `CRIUS_RUNTIME_PATH` | `runtime.runtime_path` |
| `CRIUS_RUNTIME_ROOT` | `runtime.root` |
| `CRIUS_RUNTIME_HANDLERS` | `runtime.handlers` |
| `CRIUS_PAUSE_IMAGE` | `runtime.pause_image` |
| `CRIUS_CGROUP_DRIVER` | `runtime.cgroup_driver` |
| `CRIUS_ENABLE_CRIU_SUPPORT` | `runtime.enable_criu_support` |
| `CRIUS_IMAGE_ROOT` | `image.root` |
| `CRIUS_IMAGE_REGISTRY_CONFIG_DIR` | `image.registry_config_dir` |
| `CRIUS_CNI_CONFIG_DIRS` | `network.config_dirs` |
| `CRIUS_CNI_PLUGIN_DIRS` | `network.plugin_dirs` |
| `CNI_PATH` | `CRIUS_CNI_PLUGIN_DIRS` 未设置时作为 `network.plugin_dirs` fallback |
| `CRIUS_CNI_CACHE_DIR` | `network.cache_dir` |
| `CRIUS_LOG_LEVEL` / `CRIUS_LOG_FILE` / `CRIUS_LOG_DIR` | logging 字段 |
| `CRIUS_ENABLE_METRICS` | `metrics.enable` |
| `CRIUS_METRICS_HOST` / `CRIUS_METRICS_PORT` | metrics TCP listener |
| `CRIUS_METRICS_SOCKET` | metrics Unix socket |
| `CRIUS_ENABLE_TRACING` | `tracing.enable` |
| `CRIUS_TRACING_ENDPOINT` | `tracing.endpoint` |
| `CRIUS_ENABLE_NRI` | `nri.enable` |
| `CRIUS_ENABLE_CDI` | `nri.enable_cdi` |
| `CRIUS_CDI_SPEC_DIRS` | `nri.cdi_spec_dirs` |

布尔环境变量接受 `1`、`true`、`yes`、`on`、`0`、`false`、`no`、`off`。duration
字段接受纯秒数，或 `ms`、`s`、`m`、`h` 后缀。

## 配置管理建议

- 将稳定节点配置收敛到 `/etc/crius/crius.conf`。
- 升级后使用 `--write-default-config` 生成新基线，再做显式差异合并。
- CLI 覆盖只用于本地调试。
- 少量环境差异可通过 systemd drop-in 管理。
- runtime handlers、image storage、rootless paths、NRI、checkpoint / restore 相关变更
  都应按需要重启并重新验证节点。

## 相关文档

- [README.zh-CN.md](../../README.zh-CN.md)
- [architecture.md](architecture.md)
- [kubeadm.md](kubeadm.md)
- [nri.md](nri.md)
- [rootless.md](rootless.md)
- [checkpoint-restore.md](checkpoint-restore.md)
