use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use protobuf::MessageField;
use tempfile::tempdir;

use super::{
    apply_container_adjustment, apply_container_adjustment_with_blockio_config,
    apply_container_adjustment_with_options, disallowed_annotation_adjustment_keys,
    filter_annotation_adjustments_by_allowlist, rdt_adjustment_name, resolve_rdt_class,
    resource_class_names, sanitize_linux_resources_for_capabilities,
    validate_adjustment_resources_with_min_memory, validate_container_adjustment,
    validate_container_update, validate_update_linux_resources, AdjustmentOptions, REMOVAL_PREFIX,
};
use crate::nri_proto::api as nri_api;
use crate::oci::spec::{
    Device, Linux, LinuxCpu, LinuxHugepageLimit, LinuxMemory, LinuxPids, LinuxResources, Mount,
    Namespace, Process, Rlimit, Spec,
};

fn removal(name: &str) -> String {
    format!("{REMOVAL_PREFIX}{name}")
}

fn write_blockio_config(dir: &Path) -> PathBuf {
    let path = dir.join("blockio.json");
    fs::write(
        &path,
        serde_json::json!({
            "classes": {
                "gold": {
                    "weight": 500
                }
            }
        })
        .to_string(),
    )
    .unwrap();
    path
}

fn opt_int(value: i64) -> MessageField<nri_api::OptionalInt> {
    let mut item = nri_api::OptionalInt::new();
    item.value = value;
    MessageField::some(item)
}

fn opt_int64(value: i64) -> MessageField<nri_api::OptionalInt64> {
    let mut item = nri_api::OptionalInt64::new();
    item.value = value;
    MessageField::some(item)
}

fn opt_uint32(value: u32) -> MessageField<nri_api::OptionalUInt32> {
    let mut item = nri_api::OptionalUInt32::new();
    item.value = value;
    MessageField::some(item)
}

fn opt_uint64(value: u64) -> MessageField<nri_api::OptionalUInt64> {
    let mut item = nri_api::OptionalUInt64::new();
    item.value = value;
    MessageField::some(item)
}

fn spec_with_process() -> Spec {
    Spec {
        oci_version: "1.1.0".to_string(),
        process: Some(Process {
            terminal: None,
            user: None,
            args: vec!["sleep".to_string(), "10".to_string()],
            env: Some(vec!["A=1".to_string(), "B=2".to_string()]),
            cwd: "/".to_string(),
            capabilities: None,
            rlimits: Some(vec![Rlimit {
                rtype: "RLIMIT_NOFILE".to_string(),
                hard: 1024,
                soft: 512,
            }]),
            oom_score_adj: None,
            scheduler: None,
            no_new_privileges: None,
            apparmor_profile: None,
            selinux_label: None,
            io_priority: None,
        }),
        root: None,
        hostname: None,
        mounts: Some(vec![Mount {
            destination: "/data".to_string(),
            source: Some("/old".to_string()),
            mount_type: Some("bind".to_string()),
            options: Some(vec!["ro".to_string()]),
        }]),
        hooks: None,
        linux: Some(Linux {
            namespaces: Some(vec![
                Namespace {
                    ns_type: "pid".to_string(),
                    path: None,
                },
                Namespace {
                    ns_type: "network".to_string(),
                    path: None,
                },
            ]),
            uid_mappings: None,
            gid_mappings: None,
            devices: Some(vec![Device {
                device_type: "c".to_string(),
                path: "/dev/null".to_string(),
                major: Some(1),
                minor: Some(3),
                file_mode: Some(0o666),
                uid: None,
                gid: None,
            }]),
            net_devices: None,
            cgroups_path: Some("/old/path".to_string()),
            resources: Some(LinuxResources {
                network: None,
                pids: Some(LinuxPids { limit: 64 }),
                memory: Some(LinuxMemory {
                    limit: Some(1024),
                    swap: None,
                    kernel: None,
                    kernel_tcp: None,
                    reservation: None,
                    swappiness: None,
                    disable_oom_killer: None,
                    use_hierarchy: None,
                }),
                cpu: Some(LinuxCpu {
                    shares: Some(128),
                    quota: None,
                    period: None,
                    realtime_runtime: None,
                    realtime_period: None,
                    cpus: None,
                    mems: None,
                }),
                block_io: None,
                hugepage_limits: Some(vec![LinuxHugepageLimit {
                    page_size: "2MB".to_string(),
                    limit: 1,
                }]),
                devices: None,
                intel_rdt: None,
                unified: Some(HashMap::from([(
                    "memory.high".to_string(),
                    "128".to_string(),
                )])),
            }),
            rootfs_propagation: None,
            seccomp: None,
            sysctl: Some(HashMap::from([(
                "net.ipv4.ip_forward".to_string(),
                "0".to_string(),
            )])),
            mount_label: None,
            masked_paths: None,
            readonly_paths: None,
            intel_rdt: None,
        }),
        annotations: Some(HashMap::from([
            ("keep".to_string(), "old".to_string()),
            ("drop".to_string(), "gone".to_string()),
        ])),
    }
}

#[test]
fn applies_annotation_env_mount_hook_and_args_adjustments() {
    let mut spec = spec_with_process();
    let mut adjustment = nri_api::ContainerAdjustment::new();
    adjustment
        .annotations
        .insert("keep".to_string(), "new".to_string());
    adjustment
        .annotations
        .insert(removal("drop"), String::new());
    adjustment.env.push(nri_api::KeyValue {
        key: "B".to_string(),
        value: "3".to_string(),
        ..Default::default()
    });
    adjustment.env.push(nri_api::KeyValue {
        key: removal("A"),
        value: String::new(),
        ..Default::default()
    });
    adjustment.mounts.push(nri_api::Mount {
        destination: "/data".to_string(),
        source: "/new".to_string(),
        type_: "bind".to_string(),
        options: vec!["rw".to_string()],
        ..Default::default()
    });
    adjustment.mounts.push(nri_api::Mount {
        destination: removal("/tmp"),
        ..Default::default()
    });
    let mut hooks = nri_api::Hooks::new();
    hooks.prestart.push(nri_api::Hook {
        path: "/bin/hook".to_string(),
        args: vec!["hook".to_string()],
        ..Default::default()
    });
    adjustment.hooks = MessageField::some(hooks);
    adjustment.args = vec![String::new(), "/bin/echo".to_string(), "hi".to_string()];

    apply_container_adjustment(&mut spec, &adjustment).unwrap();

    assert_eq!(
        spec.annotations.as_ref().unwrap().get("keep"),
        Some(&"new".to_string())
    );
    assert!(!spec.annotations.as_ref().unwrap().contains_key("drop"));
    assert_eq!(
        spec.process.as_ref().unwrap().env.as_ref().unwrap(),
        &vec!["B=3".to_string()]
    );
    let mount = spec
        .mounts
        .as_ref()
        .unwrap()
        .iter()
        .find(|mount| mount.destination == "/data")
        .unwrap();
    assert_eq!(mount.source.as_deref(), Some("/new"));
    assert_eq!(mount.options.as_ref().unwrap(), &vec!["rw".to_string()]);
    let hook = &spec.hooks.as_ref().unwrap().prestart.as_ref().unwrap()[0];
    assert_eq!(hook.path, "/bin/hook");
    assert_eq!(
        spec.process.as_ref().unwrap().args,
        vec!["/bin/echo".to_string(), "hi".to_string()]
    );
}

#[test]
fn applies_linux_devices_resources_and_security_adjustments() {
    let mut spec = spec_with_process();
    let mut adjustment = nri_api::ContainerAdjustment::new();
    let mut linux = nri_api::LinuxContainerAdjustment::new();
    linux.devices.push(nri_api::LinuxDevice {
        path: "/dev/fuse".to_string(),
        type_: "c".to_string(),
        major: 10,
        minor: 229,
        file_mode: Some({
            let mut mode = nri_api::OptionalFileMode::new();
            mode.value = 0o660;
            mode
        })
        .into(),
        uid: opt_uint32(1000),
        gid: opt_uint32(1000),
        ..Default::default()
    });
    linux.devices.push(nri_api::LinuxDevice {
        path: removal("/dev/null"),
        ..Default::default()
    });

    let mut resources = nri_api::LinuxResources::new();
    let mut memory = nri_api::LinuxMemory::new();
    memory.limit = opt_int64(4096);
    memory.swap = opt_int64(8192);
    resources.memory = MessageField::some(memory);
    let mut cpu = nri_api::LinuxCPU::new();
    cpu.shares = opt_uint64(512);
    cpu.cpus = "0-1".to_string();
    resources.cpu = MessageField::some(cpu);
    resources.hugepage_limits.push(nri_api::HugepageLimit {
        page_size: "2MB".to_string(),
        limit: 4,
        ..Default::default()
    });
    resources
        .unified
        .insert("cpu.max".to_string(), "100000 100000".to_string());
    resources.pids = Some(nri_api::LinuxPids {
        limit: 128,
        ..Default::default()
    })
    .into();
    linux.resources = MessageField::some(resources);
    linux.cgroups_path = "/new/path".to_string();
    linux.oom_score_adj = opt_int(222);
    linux
        .sysctl
        .insert(removal("net.ipv4.ip_forward"), String::new());
    linux
        .sysctl
        .insert("vm.max_map_count".to_string(), "262144".to_string());
    linux.namespaces.push(nri_api::LinuxNamespace {
        type_: "pid".to_string(),
        path: "/proc/1/ns/pid".to_string(),
        ..Default::default()
    });
    linux.namespaces.push(nri_api::LinuxNamespace {
        type_: removal("network"),
        ..Default::default()
    });
    let mut seccomp = nri_api::LinuxSeccomp::new();
    seccomp.default_action = "SCMP_ACT_ERRNO".to_string();
    seccomp.architectures = vec!["SCMP_ARCH_X86_64".to_string()];
    seccomp.syscalls.push(nri_api::LinuxSyscall {
        names: vec!["clone3".to_string()],
        action: "SCMP_ACT_ALLOW".to_string(),
        errno_ret: opt_uint32(1),
        args: vec![nri_api::LinuxSeccompArg {
            index: 0,
            value: 1,
            value_two: 2,
            op: "SCMP_CMP_EQ".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    });
    linux.seccomp_policy = MessageField::some(seccomp);
    adjustment.linux = MessageField::some(linux);

    apply_container_adjustment(&mut spec, &adjustment).unwrap();

    let linux = spec.linux.as_ref().unwrap();
    assert!(linux
        .devices
        .as_ref()
        .unwrap()
        .iter()
        .all(|device| device.path != "/dev/null"));
    let fuse = linux
        .devices
        .as_ref()
        .unwrap()
        .iter()
        .find(|device| device.path == "/dev/fuse")
        .unwrap();
    assert_eq!(fuse.major, Some(10));
    assert_eq!(linux.cgroups_path.as_deref(), Some("/new/path"));
    assert_eq!(spec.process.as_ref().unwrap().oom_score_adj, Some(222));
    assert_eq!(
        linux.sysctl.as_ref().unwrap().get("vm.max_map_count"),
        Some(&"262144".to_string())
    );
    assert!(!linux
        .sysctl
        .as_ref()
        .unwrap()
        .contains_key("net.ipv4.ip_forward"));
    assert!(linux
        .namespaces
        .as_ref()
        .unwrap()
        .iter()
        .all(|ns| ns.ns_type != "network"));
    assert_eq!(
        linux
            .namespaces
            .as_ref()
            .unwrap()
            .iter()
            .find(|ns| ns.ns_type == "pid")
            .unwrap()
            .path
            .as_deref(),
        Some("/proc/1/ns/pid")
    );
    let resources = linux.resources.as_ref().unwrap();
    assert_eq!(resources.memory.as_ref().unwrap().limit, Some(4096));
    assert_eq!(resources.memory.as_ref().unwrap().swap, Some(8192));
    assert_eq!(resources.cpu.as_ref().unwrap().shares, Some(512));
    assert_eq!(resources.cpu.as_ref().unwrap().cpus.as_deref(), Some("0-1"));
    assert_eq!(resources.hugepage_limits.as_ref().unwrap()[0].limit, 4);
    assert_eq!(resources.pids.as_ref().unwrap().limit, 128);
    assert_eq!(
        resources.unified.as_ref().unwrap().get("cpu.max"),
        Some(&"100000 100000".to_string())
    );
    let seccomp = linux.seccomp.as_ref().unwrap();
    assert_eq!(seccomp.default_action, "SCMP_ACT_ERRNO");
    assert_eq!(
        seccomp.syscalls.as_ref().unwrap()[0].names,
        vec!["clone3".to_string()]
    );
}

#[test]
fn applies_rlimit_updates_by_type() {
    let mut spec = spec_with_process();
    let mut adjustment = nri_api::ContainerAdjustment::new();
    adjustment.rlimits.push(nri_api::POSIXRlimit {
        type_: "RLIMIT_NOFILE".to_string(),
        hard: 4096,
        soft: 2048,
        ..Default::default()
    });
    adjustment.rlimits.push(nri_api::POSIXRlimit {
        type_: "RLIMIT_NPROC".to_string(),
        hard: 1024,
        soft: 1024,
        ..Default::default()
    });

    apply_container_adjustment(&mut spec, &adjustment).unwrap();

    let rlimits = spec.process.as_ref().unwrap().rlimits.as_ref().unwrap();
    assert_eq!(rlimits.len(), 2);
    assert_eq!(
        rlimits
            .iter()
            .find(|limit| limit.rtype == "RLIMIT_NOFILE")
            .unwrap()
            .hard,
        4096
    );
    assert_eq!(
        rlimits
            .iter()
            .find(|limit| limit.rtype == "RLIMIT_NPROC")
            .unwrap()
            .soft,
        1024
    );
}

#[test]
fn applies_rdt_class_and_linux_rdt_adjustments() {
    let mut spec = spec_with_process();
    let mut adjustment = nri_api::ContainerAdjustment::new();
    let mut linux = nri_api::LinuxContainerAdjustment::new();
    let mut resources = nri_api::LinuxResources::new();
    let mut rdt_class = nri_api::OptionalString::new();
    rdt_class.value = "gold".to_string();
    resources.rdt_class = MessageField::some(rdt_class);
    linux.resources = MessageField::some(resources);

    let mut rdt = nri_api::LinuxRdt::new();
    let mut clos_id = nri_api::OptionalString::new();
    clos_id.value = "silver".to_string();
    rdt.clos_id = MessageField::some(clos_id);
    let mut schemata = nri_api::OptionalRepeatedString::new();
    schemata.value = vec!["L3:0=ff".to_string(), "MB:0=70".to_string()];
    rdt.schemata = MessageField::some(schemata);
    let mut monitoring = nri_api::OptionalBool::new();
    monitoring.value = true;
    rdt.enable_monitoring = MessageField::some(monitoring);
    linux.rdt = MessageField::some(rdt);
    adjustment.linux = MessageField::some(linux);

    apply_container_adjustment(&mut spec, &adjustment).unwrap();

    let intel_rdt = spec
        .linux
        .as_ref()
        .and_then(|linux| linux.intel_rdt.as_ref())
        .expect("intel_rdt should be configured");
    assert_eq!(intel_rdt.clos_id.as_deref(), Some("silver"));
    assert_eq!(intel_rdt.l3_cache_schema.as_deref(), Some("0=ff"));
    assert_eq!(intel_rdt.mem_bw_schema.as_deref(), Some("0=70"));
    assert_eq!(intel_rdt.enable_cmt, Some(true));
    assert_eq!(intel_rdt.enable_mbm, Some(true));
}

#[test]
fn resolves_pod_qos_rdt_class_to_none() {
    assert!(resolve_rdt_class("").is_none());
    assert!(resolve_rdt_class("/PodQos").is_none());
    assert_eq!(
        resolve_rdt_class("gold")
            .and_then(|rdt| rdt.clos_id)
            .as_deref(),
        Some("gold")
    );
}

#[test]
fn applies_blockio_class_adjustments() {
    let dir = tempdir().unwrap();
    let config_path = write_blockio_config(dir.path());
    let mut spec = spec_with_process();
    let mut adjustment = nri_api::ContainerAdjustment::new();
    let mut linux = nri_api::LinuxContainerAdjustment::new();
    let mut resources = nri_api::LinuxResources::new();
    let mut blockio_class = nri_api::OptionalString::new();
    blockio_class.value = "gold".to_string();
    resources.blockio_class = MessageField::some(blockio_class);
    linux.resources = MessageField::some(resources);
    adjustment.linux = MessageField::some(linux);

    apply_container_adjustment_with_blockio_config(
        &mut spec,
        &adjustment,
        Some(config_path.to_str().unwrap()),
    )
    .unwrap();

    let block_io = spec
        .linux
        .as_ref()
        .and_then(|linux| linux.resources.as_ref())
        .and_then(|resources| resources.block_io.as_ref())
        .expect("block io should be configured");
    assert_eq!(block_io.weight, Some(500));
}

#[test]
fn rejects_protected_annotations_in_adjustment() {
    let mut adjustment = nri_api::ContainerAdjustment::new();
    adjustment.annotations.insert(
        "io.crius.internal/container-state".to_string(),
        "forbidden".to_string(),
    );

    let err = validate_container_adjustment(&adjustment).unwrap_err();
    assert!(format!("{err}").contains("protected annotation"));
}

#[test]
fn filters_annotation_adjustments_by_allowlist() {
    let adjustments = HashMap::from([
        (
            "io.kubernetes.cri.sandbox-name".to_string(),
            "pod-a".to_string(),
        ),
        (
            "cpu-load-balancing.crio.io".to_string(),
            "disable".to_string(),
        ),
        ("-cpu-quota.crio.io".to_string(), String::new()),
    ]);
    let allowed = vec!["cpu-".to_string()];

    let filtered = filter_annotation_adjustments_by_allowlist(&adjustments, &allowed);
    assert_eq!(filtered.len(), 2);
    assert!(filtered.contains_key("cpu-load-balancing.crio.io"));
    assert!(filtered.contains_key("-cpu-quota.crio.io"));
    assert!(!filtered.contains_key("io.kubernetes.cri.sandbox-name"));

    let disallowed = disallowed_annotation_adjustment_keys(&adjustments, &allowed);
    assert_eq!(
        disallowed,
        vec!["io.kubernetes.cri.sandbox-name".to_string()]
    );
}

#[test]
fn rejects_invalid_adjustment_resource_values() {
    let mut resources = nri_api::LinuxResources::new();
    let mut memory = nri_api::LinuxMemory::new();
    let mut swappiness = nri_api::OptionalUInt64::new();
    swappiness.value = 101;
    memory.swappiness = protobuf::MessageField::some(swappiness);
    resources.memory = protobuf::MessageField::some(memory);
    resources.devices.push(nri_api::LinuxDeviceCgroup {
        allow: true,
        type_: "x".to_string(),
        access: "rwm".to_string(),
        ..Default::default()
    });

    let err = validate_adjustment_resources_with_min_memory(&resources, Some(1024)).unwrap_err();
    let rendered = format!("{err}");
    assert!(rendered.contains("swappiness") || rendered.contains("device rule type"));
}

#[test]
fn rejects_adjustment_memory_limit_below_minimum() {
    let mut resources = nri_api::LinuxResources::new();
    let mut memory = nri_api::LinuxMemory::new();
    let mut limit = nri_api::OptionalInt64::new();
    limit.value = 512;
    memory.limit = protobuf::MessageField::some(limit);
    resources.memory = protobuf::MessageField::some(memory);

    let err = validate_adjustment_resources_with_min_memory(&resources, Some(1024)).unwrap_err();
    assert!(format!("{err}").contains("below required minimum"));
}

#[test]
fn sanitizes_resources_for_missing_capabilities() {
    let mut resources = nri_api::LinuxResources::new();
    let mut memory = nri_api::LinuxMemory::new();
    let mut swap = nri_api::OptionalInt64::new();
    swap.value = 8192;
    memory.swap = protobuf::MessageField::some(swap);
    resources.memory = protobuf::MessageField::some(memory);
    resources.hugepage_limits.push(nri_api::HugepageLimit {
        page_size: "2MB".to_string(),
        limit: 4,
        ..Default::default()
    });

    sanitize_linux_resources_for_capabilities(&mut resources, false, false);

    assert!(resources
        .memory
        .as_ref()
        .and_then(|memory| memory.swap.as_ref())
        .is_none());
    assert!(resources.hugepage_limits.is_empty());
}

#[test]
fn rejects_update_with_invalid_device_access() {
    let mut resources = nri_api::LinuxResources::new();
    resources.devices.push(nri_api::LinuxDeviceCgroup {
        allow: true,
        type_: "c".to_string(),
        access: "rx".to_string(),
        ..Default::default()
    });

    let err = validate_update_linux_resources(&resources).unwrap_err();
    assert!(format!("{err}").contains("device rule access"));
}

#[test]
fn applies_cdi_device_edits_from_json_spec() {
    let dir = tempdir().unwrap();
    let spec_path = dir.path().join("vendor.json");
    fs::write(
        &spec_path,
        serde_json::json!({
            "cdiVersion": "0.7.0",
            "kind": "vendor.com/device",
            "containerEdits": {
                "env": ["FROM_SPEC=1"],
                "additionalGids": [2000]
            },
            "devices": [{
                "name": "gpu0",
                "containerEdits": {
                    "env": ["FROM_DEVICE=1"],
                    "deviceNodes": [{
                        "path": "/dev/vendor0",
                        "type": "c",
                        "major": 10,
                        "minor": 200,
                        "permissions": "rw"
                    }],
                    "mounts": [{
                        "hostPath": "/opt/vendor/lib.so",
                        "containerPath": "/usr/lib/libvendor.so",
                        "options": ["ro", "bind"],
                        "type": "bind"
                    }],
                    "hooks": [{
                        "hookName": "createContainer",
                        "path": "/bin/cdi-hook",
                        "args": ["hook"],
                        "env": ["A=B"]
                    }],
                    "intelRdt": {
                        "closID": "gold",
                        "l3CacheSchema": "0=ff",
                        "memBwSchema": "0=70",
                        "enableMonitoring": true
                    }
                }
            }]
        })
        .to_string(),
    )
    .unwrap();

    std::env::set_var("CDI_SPEC_DIRS", dir.path());
    let mut spec = spec_with_process();
    let mut adjustment = nri_api::ContainerAdjustment::new();
    adjustment.CDI_devices.push(nri_api::CDIDevice {
        name: "vendor.com/device=gpu0".to_string(),
        ..Default::default()
    });

    apply_container_adjustment(&mut spec, &adjustment).unwrap();
    std::env::remove_var("CDI_SPEC_DIRS");

    let env = spec.process.as_ref().unwrap().env.as_ref().unwrap();
    assert!(env.iter().any(|entry| entry == "FROM_SPEC=1"));
    assert!(env.iter().any(|entry| entry == "FROM_DEVICE=1"));
    assert_eq!(
        spec.process
            .as_ref()
            .unwrap()
            .user
            .as_ref()
            .and_then(|user| user.additional_gids.as_ref())
            .unwrap(),
        &vec![2000]
    );
    assert!(spec
        .mounts
        .as_ref()
        .unwrap()
        .iter()
        .any(|mount| mount.destination == "/usr/lib/libvendor.so"));
    assert!(spec
        .linux
        .as_ref()
        .unwrap()
        .devices
        .as_ref()
        .unwrap()
        .iter()
        .any(|device| device.path == "/dev/vendor0"));
    assert!(spec
        .hooks
        .as_ref()
        .unwrap()
        .create_container
        .as_ref()
        .unwrap()
        .iter()
        .any(|hook| hook.path == "/bin/cdi-hook"));
    let intel_rdt = spec.linux.as_ref().unwrap().intel_rdt.as_ref().unwrap();
    assert_eq!(intel_rdt.clos_id.as_deref(), Some("gold"));
    assert_eq!(intel_rdt.l3_cache_schema.as_deref(), Some("0=ff"));
    assert_eq!(intel_rdt.mem_bw_schema.as_deref(), Some("0=70"));
    assert_eq!(intel_rdt.enable_cmt, Some(true));
    assert_eq!(intel_rdt.enable_mbm, Some(true));
}

#[test]
fn applies_cdi_device_edits_from_configured_spec_dirs_without_env() {
    let dir = tempdir().unwrap();
    let spec_path = dir.path().join("vendor.json");
    fs::write(
        &spec_path,
        serde_json::json!({
            "cdiVersion": "0.7.0",
            "kind": "vendor.com/device",
            "devices": [{
                "name": "gpu0",
                "containerEdits": {
                    "env": ["FROM_CONFIGURED_DIR=1"]
                }
            }]
        })
        .to_string(),
    )
    .unwrap();

    let mut spec = spec_with_process();
    let mut adjustment = nri_api::ContainerAdjustment::new();
    adjustment.CDI_devices.push(nri_api::CDIDevice {
        name: "vendor.com/device=gpu0".to_string(),
        ..Default::default()
    });
    let configured_dirs = vec![dir.path().display().to_string()];

    apply_container_adjustment_with_options(
        &mut spec,
        &adjustment,
        AdjustmentOptions {
            blockio_config_path: None,
            cdi_enabled: true,
            cdi_spec_dirs: Some(&configured_dirs),
        },
    )
    .unwrap();

    let env = spec.process.as_ref().unwrap().env.as_ref().unwrap();
    assert!(env.iter().any(|entry| entry == "FROM_CONFIGURED_DIR=1"));
}

#[test]
fn rejects_invalid_cdi_reference() {
    let mut adjustment = nri_api::ContainerAdjustment::new();
    adjustment.CDI_devices.push(nri_api::CDIDevice {
        name: "vendor.com/device".to_string(),
        ..Default::default()
    });

    let mut spec = spec_with_process();
    let err = apply_container_adjustment(&mut spec, &adjustment).unwrap_err();
    assert!(format!("{err}").contains("expected <vendor>/<class>=<device>"));
}

#[test]
fn rejects_cdi_adjustments_when_disabled() {
    let mut adjustment = nri_api::ContainerAdjustment::new();
    adjustment.CDI_devices.push(nri_api::CDIDevice {
        name: "vendor.com/device=gpu0".to_string(),
        ..Default::default()
    });

    let mut spec = spec_with_process();
    let err = apply_container_adjustment_with_options(
        &mut spec,
        &adjustment,
        AdjustmentOptions {
            blockio_config_path: None,
            cdi_enabled: false,
            cdi_spec_dirs: Some(&["/etc/cdi".to_string()]),
        },
    )
    .unwrap_err();
    assert!(format!("{err}").contains("CDI device adjustments are disabled"));
}

#[test]
fn rejects_invalid_linux_net_device_adjustment() {
    let mut adjustment = nri_api::ContainerAdjustment::new();
    adjustment.linux = Some({
        let mut linux = nri_api::LinuxContainerAdjustment::new();
        linux.net_devices.insert(
            "eth0".to_string(),
            nri_api::LinuxNetDevice {
                name: String::new(),
                ..Default::default()
            },
        );
        linux
    })
    .into();

    let mut spec = spec_with_process();
    let err = apply_container_adjustment(&mut spec, &adjustment).unwrap_err();
    assert!(format!("{err}").contains("linux net device must set both host and container names"));
}

#[test]
fn accepts_extended_update_resources() {
    let mut resources = nri_api::LinuxResources::new();
    let mut memory = nri_api::LinuxMemory::new();
    let mut reservation = nri_api::OptionalInt64::new();
    reservation.value = 4096;
    memory.reservation = protobuf::MessageField::some(reservation);
    resources.memory = protobuf::MessageField::some(memory);
    resources.pids = Some(nri_api::LinuxPids {
        limit: 12,
        ..Default::default()
    })
    .into();
    resources.devices.push(nri_api::LinuxDeviceCgroup {
        allow: true,
        type_: "c".to_string(),
        access: "rwm".to_string(),
        ..Default::default()
    });

    validate_update_linux_resources(&resources).expect("extended update resources should validate");
}

#[test]
fn extracts_security_relevant_resource_class_and_rdt_adjustment_names() {
    let mut resources = nri_api::LinuxResources::new();
    let mut blockio_class = nri_api::OptionalString::new();
    blockio_class.value = " gold ".to_string();
    resources.blockio_class = protobuf::MessageField::some(blockio_class);
    let mut rdt_class = nri_api::OptionalString::new();
    rdt_class.value = "silver".to_string();
    resources.rdt_class = protobuf::MessageField::some(rdt_class);

    let (blockio, rdt) = resource_class_names(Some(&resources));
    assert_eq!(blockio.as_deref(), Some("gold"));
    assert_eq!(rdt.as_deref(), Some("silver"));

    let mut rdt_adjustment = nri_api::LinuxRdt::new();
    let mut clos_id = nri_api::OptionalString::new();
    clos_id.value = " latency ".to_string();
    rdt_adjustment.clos_id = protobuf::MessageField::some(clos_id);
    assert_eq!(
        rdt_adjustment_name(Some(&rdt_adjustment)).as_deref(),
        Some("latency")
    );

    rdt_adjustment.remove = true;
    assert!(rdt_adjustment_name(Some(&rdt_adjustment)).is_none());
}

#[test]
fn rejects_update_without_linux_resources() {
    let mut update = nri_api::ContainerUpdate::new();
    update.container_id = "ctr-1".to_string();

    let err = validate_container_update(&update).unwrap_err();
    assert!(format!("{err}").contains("missing linux payload"));
}
