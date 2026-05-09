# 配置参考

本文档面向公开仓库使用者，目标不是逐字段复述所有实现细节，而是说明：

1. 配置从哪里生效
2. 哪些字段最值得优先关注
3. 哪些修改需要重启

完整字段定义与默认值的事实来源始终是：

- `crius --dump-default-config`
- `src/config/mod.rs`

## 配置优先级

`crius` 当前遵循以下优先级模型：

```text
CLI > env > config file > default
```

但并不是每个字段都同时暴露给 CLI 和环境变量。可按下面三类理解：

| 类型 | 说明 |
| --- | --- |
| `CLI > env > file > default` | 适用于少数关键入口字段，例如监听地址、streaming、默认 runtime 路径 |
| `env > file > default` | 适用于一部分运维常用字段，例如 metrics、tracing、日志与部分 runtime 参数 |
| `file > default` | 适用于大量结构化字段，例如 `runtime.runtimes.*`、`runtime.workloads.*`、多数 `nri.*` |

## 生成当前版本默认配置

推荐直接从二进制导出当前版本默认配置：

```bash
crius --dump-default-config
crius --write-default-config /etc/crius/crius.conf
```

这样可以避免手工复制过期样例。

## 重载策略

`crius` 当前没有通用的 live reload 或 `SIGHUP` reload 机制。绝大多数配置变更都需要重启守护进程。

| 配置域 | 变更策略 |
| --- | --- |
| `api.*` | `restart-required` |
| `runtime.*` | `restart-required` |
| `image.*` | `restart-required` |
| `network.*` | `restart-required` |
| `logging.*` | `restart-required` |
| `metrics.*` | `restart-required` |
| `tracing.*` | `restart-required` |
| `nri.*` | `restart-required` |

例外：

- `UpdateRuntimeConfig.network_config.pod_cidr` 可通过 CRI RPC 动态影响基于模板的 CNI 配置渲染。

## 推荐优先关注的配置

### 基础路径

| 字段 | 作用 |
| --- | --- |
| `root` | 持久化根目录，影响 SQLite 与恢复相关路径 |
| `runtime.root` | 运行时状态根目录，影响 shim、socket、netns 等临时工件 |
| `logging.dir` | daemon / container 默认日志目录 |

### CRI 与 streaming

| 字段 | 作用 |
| --- | --- |
| `api.listen` | CRI gRPC 监听地址 |
| `api.listen_aliases` | 额外创建的 Unix socket 别名；启动时会把这些路径链接到 `api.listen` 对应的主 socket |
| `api.allow_tcp_service` | 是否允许把 CRI 服务暴露到 TCP |
| `api.streaming.address` / `port` | kubelet 后续访问流式 URL 时使用的地址 |
| `api.streaming.enable_tls` | streaming 是否启用 TLS |
| `api.exec_sync_io_drain_timeout` | `ExecSync` 在主进程退出后的 IO drain 等待时间 |

### 运行时

| 字段 | 作用 |
| --- | --- |
| `runtime.runtime_type` | 默认 runtime handler 名称 |
| `runtime.runtime_path` | 默认 OCI runtime 二进制路径 |
| `runtime.runtime_config_path` | runtime 特定配置文件路径 |
| `runtime.handlers` | 对外暴露给 kubelet / `RuntimeClass` 的 handler 集合 |
| `runtime.pause_image` | Pod Sandbox pause 镜像 |
| `runtime.pause_command` | pause 镜像内入口命令 |
| `runtime.cgroup_driver` | cgroup driver，必须与 kubelet 一致 |
| `runtime.shim_path` | `crius-shim` 二进制路径 |
| `runtime.container_stop_timeout` | 最小优雅停止时间 |
| `runtime.enable_criu_support` | checkpoint / restore 总开关 |

### 镜像

| 字段 | 作用 |
| --- | --- |
| `image.driver` | 镜像后端类型，当前默认围绕 `overlay` |
| `image.root` | 镜像内容与元数据根目录 |
| `image.global_auth_file` | 节点级 registry 凭据回退文件 |
| `image.namespaced_auth_dir` | namespace 级凭据目录 |
| `image.default_transport` | 默认镜像引用 transport |
| `image.short_name_mode` | 短名镜像解析策略 |

### 网络

| 字段 | 作用 |
| --- | --- |
| `network.plugin` | 网络实现类型，当前仅支持 `cni` |
| `network.config_dirs` | CNI 配置目录 |
| `network.plugin_dirs` | CNI 插件目录 |
| `network.cache_dir` | CNI cache 目录 |
| `network.conf_template` | 供 `UpdateRuntimeConfig` 渲染 PodCIDR 的模板 |
| `network.disable_hostport_mapping` | 是否关闭内建 hostPort 处理 |

### 安全

| 字段 | 作用 |
| --- | --- |
| `security.seccomp_profile` | 默认 seccomp profile |
| `security.privileged_seccomp_profile` | privileged 容器默认 seccomp 策略 |
| `security.unset_seccomp_profile` | CRI 未显式声明 seccomp 时使用的策略 |
| `security.apparmor_default_profile` | 默认 AppArmor profile |
| `security.disable_apparmor` | 是否全局禁用 AppArmor 处理 |
| `security.enable_selinux` | 是否启用 SELinux 标签 |

### 可观测性

| 字段 | 作用 |
| --- | --- |
| `logging.level` | daemon 日志级别 |
| `logging.file` | daemon 日志输出文件；为空时输出到 stderr |
| `metrics.enable` | 是否启用独立 metrics 服务 |
| `metrics.socket_path` | metrics Unix socket；非空时优先于 host/port |
| `tracing.enable` | 是否启用 tracing 导出 |
| `tracing.endpoint` | tracing 导出目标地址 |

### NRI

| 字段 | 作用 |
| --- | --- |
| `nri.enable` | NRI 总开关 |
| `nri.socket_path` | 运行时侧 NRI socket |
| `nri.plugin_path` | 预装插件目录 |
| `nri.plugin_config_path` | 插件配置目录 |
| `nri.blockio_config_path` | `blockio_class` 映射文件 |
| `nri.enable_cdi` | 是否启用 CDI 设备调整 |

## 关键 CLI 覆盖项

以下 CLI 参数适合调试和临时覆盖，但不建议替代正式配置管理：

| 参数 | 说明 |
| --- | --- |
| `--config` | 配置文件路径 |
| `--listen` | 覆盖 `api.listen` |
| `--runtime-path` | 覆盖 `runtime.runtime_path` |
| `--runtime-config-path` | 覆盖 `runtime.runtime_config_path` |
| `--runtime-root` | 覆盖 `runtime.root` |
| `--pause-image` | 覆盖 `runtime.pause_image` |
| `--cni-config-dirs` | 覆盖 `network.config_dirs` |
| `--cni-plugin-dirs` | 覆盖 `network.plugin_dirs` |
| `--stream-address` / `--stream-port` | 覆盖 streaming 地址与端口 |
| `--dump-default-config` | 打印内置默认配置 |
| `--write-default-config` | 把内置默认配置写入文件 |

## 常见环境变量覆盖项

项目支持较多环境变量覆盖。对公开仓库使用者，优先关注以下几类：

| 变量 | 覆盖项 |
| --- | --- |
| `CRIUS_LISTEN` | `api.listen` |
| `CRIUS_LISTEN_ALIASES` | `api.listen_aliases` |
| `CRIUS_RUNTIME_PATH` | `runtime.runtime_path` |
| `CRIUS_RUNTIME_ROOT` | `runtime.root` |
| `CRIUS_PAUSE_IMAGE` | `runtime.pause_image` |
| `CRIUS_CNI_CONFIG_DIRS` | `network.config_dirs` |
| `CRIUS_CNI_PLUGIN_DIRS` | `network.plugin_dirs` |
| `CRIUS_ENABLE_METRICS` | `metrics.enable` |
| `CRIUS_METRICS_HOST` / `CRIUS_METRICS_PORT` | metrics TCP 监听地址 |
| `CRIUS_ENABLE_TRACING` | `tracing.enable` |
| `CRIUS_TRACING_ENDPOINT` | `tracing.endpoint` |

环境变量适合：

- 临时测试
- systemd drop-in 覆盖
- 节点镜像里做少量差异化配置

大量结构化配置仍建议使用 TOML 文件管理。

## 配置管理建议

- 把长期配置收敛在 `/etc/crius/crius.conf`
- 用 `--write-default-config` 生成基线，再按版本差异做显式修改
- 对测试节点，优先只改 `api`、`runtime`、`image`、`network` 四个配置域
- 对生产候选环境，先建立自己的版本化配置模板，不要依赖手工编辑

## 进一步阅读

- [架构设计](architecture.md)
- [kubeadm / kubelet 接入](kubeadm.md)
- [NRI 说明](nri.md)
