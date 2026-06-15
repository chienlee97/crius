use clap::{error::ErrorKind, Parser};
use crius::crs::args::{
    Args, Command, ConfigCommand, ContainerCommand, ContainerStateArg, ExecModeArg, GcCommand,
    ImageCommand, ObjectType, PodCommand, PodStateArg, PullPolicyArg, RecoveryCommand,
    RuntimeCommand, StopObjectType, StreamProtocolArg,
};
use std::time::Duration;

type CommandCase = (&'static [&'static str], fn(&Command) -> bool);

#[test]
fn parses_top_level_help() {
    let error = Args::try_parse_from(["crs", "--help"]).expect_err("help should short-circuit");

    assert_eq!(error.kind(), ErrorKind::DisplayHelp);
}

#[test]
fn parses_version_help() {
    let error =
        Args::try_parse_from(["crs", "version", "--help"]).expect_err("help should short-circuit");

    assert_eq!(error.kind(), ErrorKind::DisplayHelp);
}

#[test]
fn run_help_describes_default_ordinary_container() {
    let error =
        Args::try_parse_from(["crs", "run", "--help"]).expect_err("help should short-circuit");
    let help = error.render().to_string();

    assert!(help.contains("ordinary container"));
    assert!(help.contains("--pod"));
    assert!(!help.contains("temporary Pod"));
}

#[test]
fn parses_minimal_version_command() {
    let args = Args::try_parse_from(["crs", "version"]).expect("version should parse");

    assert!(matches!(
        args.command,
        crius::crs::args::Command::Version(_)
    ));
}

#[test]
fn parses_global_options_before_command() {
    let args = Args::try_parse_from([
        "crs",
        "--address",
        "unix:///tmp/crius.sock",
        "--connect-timeout",
        "500ms",
        "--timeout",
        "2m",
        "--output",
        "json",
        "--quiet",
        "--no-trunc",
        "version",
    ])
    .expect("global options should parse before the command");

    assert_eq!(args.address, "unix:///tmp/crius.sock");
    assert_eq!(args.connect_timeout, Duration::from_millis(500));
    assert_eq!(args.timeout, Duration::from_secs(120));
    assert_eq!(args.output, crius::crs::args::OutputArg::Json);
    assert!(args.quiet);
    assert!(args.no_trunc);
}

#[test]
fn parses_global_options_after_command() {
    let args = Args::try_parse_from(["crs", "version", "--debug"])
        .expect("global options should parse after the command");

    assert!(args.debug);
}

#[test]
fn rejects_invalid_output_value() {
    let error = Args::try_parse_from(["crs", "--output", "xml", "version"])
        .expect_err("invalid output value should fail parsing");

    assert_eq!(error.kind(), ErrorKind::InvalidValue);
}

#[test]
fn parses_top_level_shortcut_commands() {
    let cases: &[CommandCase] = &[
        (&["crs", "version"], |command| {
            matches!(command, Command::Version(_))
        }),
        (&["crs", "status"], |command| {
            matches!(command, Command::Status(_))
        }),
        (&["crs", "doctor"], |command| {
            matches!(command, Command::Doctor(_))
        }),
        (&["crs", "ps"], |command| matches!(command, Command::Ps(_))),
        (&["crs", "pods"], |command| {
            matches!(command, Command::Pods(_))
        }),
        (&["crs", "images"], |command| {
            matches!(command, Command::Images(_))
        }),
        (&["crs", "pull", "busybox"], |command| {
            matches!(command, Command::Pull(_))
        }),
        (&["crs", "inspect", "abc123"], |command| {
            matches!(command, Command::Inspect(_))
        }),
        (&["crs", "logs", "abc123"], |command| {
            matches!(command, Command::Logs(_))
        }),
        (&["crs", "exec", "abc123", "--", "echo"], |command| {
            matches!(command, Command::Exec(_))
        }),
        (&["crs", "stop", "abc123"], |command| {
            matches!(command, Command::Stop(_))
        }),
        (&["crs", "rm", "abc123"], |command| {
            matches!(command, Command::Rm(_))
        }),
        (&["crs", "rmi", "busybox"], |command| {
            matches!(command, Command::Rmi { .. })
        }),
        (&["crs", "rmp", "pod123"], |command| {
            matches!(command, Command::Rmp { .. })
        }),
    ];

    for (argv, assert_command) in cases {
        let args = Args::try_parse_from(*argv).unwrap_or_else(|error| {
            panic!("failed to parse {argv:?}: {error}");
        });

        assert!(
            assert_command(&args.command),
            "unexpected command for {argv:?}"
        );
    }
}

#[test]
fn parses_logs_options() {
    let args = Args::try_parse_from([
        "crs",
        "logs",
        "--follow",
        "--tail",
        "10",
        "--since",
        "2026-06-03T12:00:00Z",
        "--timestamps",
        "ctr1",
    ])
    .expect("logs options should parse");
    let Command::Logs(logs) = args.command else {
        panic!("expected logs command");
    };
    assert_eq!(logs.container, "ctr1");
    assert!(logs.follow);
    assert_eq!(logs.tail, Some(10));
    assert_eq!(logs.since.as_deref(), Some("2026-06-03T12:00:00Z"));
    assert!(logs.timestamps);
}

#[test]
fn parses_top_level_command_groups() {
    let cases: &[CommandCase] = &[
        (&["crs", "config", "show"], |command| {
            matches!(command, Command::Config(_))
        }),
        (&["crs", "runtime", "config"], |command| {
            matches!(command, Command::Runtime(_))
        }),
        (&["crs", "image", "list"], |command| {
            matches!(command, Command::Image(_))
        }),
        (&["crs", "pod", "list"], |command| {
            matches!(command, Command::Pod(_))
        }),
        (&["crs", "container", "list"], |command| {
            matches!(command, Command::Container(_))
        }),
        (&["crs", "run", "busybox"], |command| {
            matches!(command, Command::Run(_))
        }),
        (&["crs", "events"], |command| {
            matches!(command, Command::Events(_))
        }),
        (&["crs", "stats"], |command| {
            matches!(command, Command::Stats(_))
        }),
        (&["crs", "metrics", "descriptors"], |command| {
            matches!(command, Command::Metrics(_))
        }),
        (&["crs", "recovery", "status"], |command| {
            matches!(command, Command::Recovery(_))
        }),
        (&["crs", "gc", "candidates"], |command| {
            matches!(command, Command::Gc(_))
        }),
        (&["crs", "debug", "network"], |command| {
            matches!(command, Command::Debug(_))
        }),
        (&["crs", "completion", "bash"], |command| {
            matches!(command, Command::Completion(_))
        }),
    ];

    for (argv, assert_command) in cases {
        let args = Args::try_parse_from(*argv).unwrap_or_else(|error| {
            panic!("failed to parse {argv:?}: {error}");
        });

        assert!(
            assert_command(&args.command),
            "unexpected command for {argv:?}"
        );
    }
}

#[test]
fn parses_pod_command_arguments() {
    let args = Args::try_parse_from([
        "crs", "pod", "list", "--id", "pod1", "--state", "ready", "--label", "app=test",
    ])
    .expect("pod list filters should parse");
    let Command::Pod(pod) = args.command else {
        panic!("expected pod command");
    };
    let PodCommand::List(list) = pod.command else {
        panic!("expected pod list command");
    };
    assert_eq!(list.id.as_deref(), Some("pod1"));
    assert_eq!(list.state, Some(PodStateArg::Ready));
    assert_eq!(list.labels, vec!["app=test"]);

    let args = Args::try_parse_from([
        "crs",
        "pod",
        "port-forward",
        "pod1",
        "--forward",
        "8080:80",
        "--forward",
        "8443:443",
    ])
    .expect("pod port-forward should parse repeated forwards");
    let Command::Pod(pod) = args.command else {
        panic!("expected pod command");
    };
    let PodCommand::PortForward { pod, forward } = pod.command else {
        panic!("expected pod port-forward command");
    };
    assert_eq!(pod, "pod1");
    assert_eq!(forward, vec!["8080:80", "8443:443"]);
}

#[test]
fn parses_pod_create_arguments() {
    let args = Args::try_parse_from([
        "crs",
        "pod",
        "run",
        "--name",
        "pod1",
        "--namespace",
        "ns1",
        "--host-network",
        "--dns-server",
        "1.1.1.1",
        "--publish",
        "8080:80",
        "--sandbox-seccomp",
        "runtime/default",
    ])
    .expect("pod create args should parse");
    let Command::Pod(pod) = args.command else {
        panic!("expected pod command");
    };
    let PodCommand::Run(run) = pod.command else {
        panic!("expected pod run command");
    };
    assert_eq!(run.name.as_deref(), Some("pod1"));
    assert_eq!(run.namespace.as_deref(), Some("ns1"));
    assert!(run.host_network);
    assert_eq!(run.dns_servers, vec!["1.1.1.1"]);
    assert_eq!(run.publish, vec!["8080:80"]);
    assert_eq!(
        run.security.sandbox_seccomp.as_deref(),
        Some("runtime/default")
    );
}

#[test]
fn parses_container_command_arguments() {
    let args = Args::try_parse_from([
        "crs",
        "container",
        "list",
        "--id",
        "ctr1",
        "--pod",
        "pod1",
        "--state",
        "running",
        "--label",
        "app=test",
    ])
    .expect("container list filters should parse");
    let Command::Container(container) = args.command else {
        panic!("expected container command");
    };
    let ContainerCommand::List(list) = container.command else {
        panic!("expected container list command");
    };
    assert_eq!(list.id.as_deref(), Some("ctr1"));
    assert_eq!(list.pod.as_deref(), Some("pod1"));
    assert_eq!(list.state, Some(ContainerStateArg::Running));
    assert_eq!(list.labels, vec!["app=test"]);

    let args = Args::try_parse_from([
        "crs",
        "container",
        "exec",
        "--tty",
        "--stdin",
        "--protocol",
        "spdy",
        "ctr1",
        "--",
        "echo",
        "ok",
    ])
    .expect("container exec command should parse");
    let Command::Container(container) = args.command else {
        panic!("expected container command");
    };
    let ContainerCommand::Exec(exec) = container.command else {
        panic!("expected container exec command");
    };
    assert_eq!(exec.container, "ctr1");
    assert!(exec.stream.tty);
    assert!(exec.stream.stdin);
    assert_eq!(exec.stream.protocol, StreamProtocolArg::Spdy);
    assert_eq!(exec.command, vec!["echo", "ok"]);
}

#[test]
fn parses_container_create_arguments() {
    let args = Args::try_parse_from([
        "crs",
        "container",
        "create",
        "--name",
        "ctr1",
        "--mount",
        "type=bind,src=/x,dst=/y",
        "--memory",
        "64MiB",
        "--cap-add",
        "NET_ADMIN",
        "pod1",
        "busybox",
        "--",
        "sleep",
        "1",
    ])
    .expect("container create args should parse");
    let Command::Container(container) = args.command else {
        panic!("expected container command");
    };
    let ContainerCommand::Create(create) = container.command else {
        panic!("expected container create command");
    };
    assert_eq!(create.pod, "pod1");
    assert_eq!(create.image, "busybox");
    assert_eq!(create.options.name.as_deref(), Some("ctr1"));
    assert_eq!(create.options.mounts, vec!["type=bind,src=/x,dst=/y"]);
    assert_eq!(create.options.resources.memory.as_deref(), Some("64MiB"));
    assert_eq!(create.options.security.cap_add, vec!["NET_ADMIN"]);
    assert_eq!(create.command, vec!["sleep", "1"]);
}

#[test]
fn parses_base_and_multitype_shortcut_args() {
    let args = Args::try_parse_from(["crs", "inspect", "--type", "image", "busybox:latest"])
        .expect("inspect object type should parse");
    let Command::Inspect(inspect) = args.command else {
        panic!("expected inspect command");
    };
    assert_eq!(inspect.object_type, Some(ObjectType::Image));
    assert_eq!(inspect.target, "busybox:latest");

    let args = Args::try_parse_from(["crs", "stop", "--type", "pod", "--timeout", "10", "pod1"])
        .expect("stop object type and timeout should parse");
    let Command::Stop(stop) = args.command else {
        panic!("expected stop command");
    };
    assert_eq!(stop.object_type, Some(StopObjectType::Pod));
    assert_eq!(stop.timeout, Some(10));
    assert_eq!(stop.target, "pod1");

    let args = Args::try_parse_from(["crs", "rm", "--force", "container1"])
        .expect("remove force should parse");
    let Command::Rm(remove) = args.command else {
        panic!("expected rm command");
    };
    assert!(remove.force);
    assert_eq!(remove.target, "container1");
}

#[test]
fn parses_config_commands() {
    let args = Args::try_parse_from(["crs", "config", "show"]).expect("config show should parse");
    let Command::Config(config) = args.command else {
        panic!("expected config command");
    };
    assert!(matches!(config.command, ConfigCommand::Show));

    let args = Args::try_parse_from(["crs", "config", "reload-status"])
        .expect("config reload-status should parse");
    let Command::Config(config) = args.command else {
        panic!("expected config command");
    };
    assert!(matches!(config.command, ConfigCommand::ReloadStatus));
}

#[test]
fn parses_runtime_commands() {
    let args =
        Args::try_parse_from(["crs", "runtime", "config"]).expect("runtime config should parse");
    let Command::Runtime(runtime) = args.command else {
        panic!("expected runtime command");
    };
    assert!(matches!(runtime.command, RuntimeCommand::Config));

    let args = Args::try_parse_from(["crs", "runtime", "update", "--pod-cidr", "10.244.0.0/16"])
        .expect("runtime update should parse");
    let Command::Runtime(runtime) = args.command else {
        panic!("expected runtime command");
    };
    let RuntimeCommand::Update { pod_cidr } = runtime.command else {
        panic!("expected runtime update command");
    };
    assert_eq!(pod_cidr, "10.244.0.0/16");

    let args = Args::try_parse_from(["crs", "runtime", "handlers", "--verbose"])
        .expect("runtime handlers should parse");
    let Command::Runtime(runtime) = args.command else {
        panic!("expected runtime command");
    };
    let RuntimeCommand::Handlers { verbose } = runtime.command else {
        panic!("expected runtime handlers command");
    };
    assert!(verbose);
}

#[test]
fn rejects_runtime_update_without_pod_cidr() {
    let error = Args::try_parse_from(["crs", "runtime", "update"])
        .expect_err("runtime update requires --pod-cidr");

    assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
}

#[test]
fn parses_image_commands_and_auth_args() {
    let args = Args::try_parse_from([
        "crs",
        "image",
        "pull",
        "registry.example.test/app:latest",
        "--username",
        "alice",
        "--password",
        "secret",
        "--server",
        "registry.example.test",
        "--pod",
        "pod1",
    ])
    .expect("image pull auth should parse");
    let Command::Image(image) = args.command else {
        panic!("expected image command");
    };
    let ImageCommand::Pull(pull) = image.command else {
        panic!("expected image pull command");
    };
    assert_eq!(pull.image, "registry.example.test/app:latest");
    assert_eq!(pull.auth.username.as_deref(), Some("alice"));
    assert_eq!(pull.auth.password.as_deref(), Some("secret"));
    assert_eq!(pull.auth.server.as_deref(), Some("registry.example.test"));
    assert_eq!(pull.pod.as_deref(), Some("pod1"));

    let args = Args::try_parse_from(["crs", "image", "list", "--image", "busybox"])
        .expect("image list filter should parse");
    let Command::Image(image) = args.command else {
        panic!("expected image command");
    };
    let ImageCommand::List(list) = image.command else {
        panic!("expected image list command");
    };
    assert_eq!(list.image.as_deref(), Some("busybox"));

    let args =
        Args::try_parse_from(["crs", "image", "fs-info"]).expect("image fs-info should parse");
    let Command::Image(image) = args.command else {
        panic!("expected image command");
    };
    assert!(matches!(image.command, ImageCommand::FsInfo));
}

#[test]
fn parses_run_command_arguments() {
    let args = Args::try_parse_from([
        "crs",
        "run",
        "--rm",
        "--tty",
        "--stdin",
        "--pod",
        "pod1",
        "--pull",
        "always",
        "--exec-mode",
        "sync",
        "--pod-name",
        "sandbox1",
        "--uid",
        "uid1",
        "--pod-attempt",
        "2",
        "--hostname",
        "host1",
        "--log-dir",
        "/var/log/pods/sandbox1",
        "--dns-server",
        "1.1.1.1",
        "--dns-search",
        "svc.cluster.local",
        "--dns-option",
        "ndots:5",
        "--publish",
        "8080:80",
        "--runtime-handler",
        "runc",
        "--cgroup-parent",
        "/kubepods",
        "--sysctl",
        "net.ipv4.ip_forward=1",
        "--host-network",
        "--host-pid",
        "--host-ipc",
        "--userns",
        "pod",
        "--uid-map",
        "0:100000:65536",
        "--gid-map",
        "0:100000:65536",
        "--sandbox-seccomp",
        "runtime/default",
        "--sandbox-apparmor",
        "localhost/crius",
        "--sandbox-selinux",
        "system_u:system_r:container_t:s0",
        "--overhead",
        "memory=16MiB",
        "--pod-resource",
        "cpu=100m",
        "--container-attempt",
        "3",
        "--command",
        "/bin/sh",
        "--arg",
        "-c",
        "--workdir",
        "/work",
        "--env",
        "A=B",
        "--env-file",
        "/tmp/env",
        "--mount",
        "type=bind,src=/x,dst=/y",
        "--device",
        "/dev/fuse",
        "--cdi-device",
        "vendor.com/device=name",
        "--log-path",
        "container.log",
        "--memory",
        "64MiB",
        "--cpu-quota",
        "100000",
        "--cap-add",
        "NET_ADMIN",
        "--user",
        "1000:1000",
        "--seccomp",
        "localhost/container.json",
        "busybox",
        "echo",
        "ok",
    ])
    .expect("run command options should parse");
    let Command::Run(run) = args.command else {
        panic!("expected run command");
    };
    assert!(run.rm);
    assert!(run.tty);
    assert!(run.stdin);
    assert_eq!(run.pod.as_deref(), Some("pod1"));
    assert_eq!(run.pull, PullPolicyArg::Always);
    assert_eq!(run.exec_mode, ExecModeArg::Sync);
    assert_eq!(run.pod_options.pod_name.as_deref(), Some("sandbox1"));
    assert_eq!(run.pod_options.uid.as_deref(), Some("uid1"));
    assert_eq!(run.pod_options.pod_attempt, Some(2));
    assert_eq!(run.pod_options.hostname.as_deref(), Some("host1"));
    assert_eq!(
        run.pod_options.log_dir.as_deref(),
        Some("/var/log/pods/sandbox1")
    );
    assert_eq!(run.pod_options.dns_servers, vec!["1.1.1.1"]);
    assert_eq!(run.pod_options.dns_searches, vec!["svc.cluster.local"]);
    assert_eq!(run.pod_options.dns_options, vec!["ndots:5"]);
    assert_eq!(run.pod_options.publish, vec!["8080:80"]);
    assert_eq!(run.pod_options.runtime_handler.as_deref(), Some("runc"));
    assert_eq!(run.pod_options.cgroup_parent.as_deref(), Some("/kubepods"));
    assert_eq!(run.pod_options.sysctls, vec!["net.ipv4.ip_forward=1"]);
    assert!(run.pod_options.host_network);
    assert!(run.pod_options.host_pid);
    assert!(run.pod_options.host_ipc);
    assert_eq!(run.pod_options.userns.as_deref(), Some("pod"));
    assert_eq!(run.pod_options.uid_maps, vec!["0:100000:65536"]);
    assert_eq!(run.pod_options.gid_maps, vec!["0:100000:65536"]);
    assert_eq!(
        run.pod_options.security.sandbox_seccomp.as_deref(),
        Some("runtime/default")
    );
    assert_eq!(
        run.pod_options.security.sandbox_apparmor.as_deref(),
        Some("localhost/crius")
    );
    assert_eq!(
        run.pod_options.security.sandbox_selinux.as_deref(),
        Some("system_u:system_r:container_t:s0")
    );
    assert_eq!(run.pod_options.overhead, vec!["memory=16MiB"]);
    assert_eq!(run.pod_options.pod_resources, vec!["cpu=100m"]);
    assert_eq!(run.container_options.container_attempt, Some(3));
    assert_eq!(run.container_options.commands, vec!["/bin/sh"]);
    assert_eq!(run.container_options.args, vec!["-c"]);
    assert_eq!(run.container_options.workdir.as_deref(), Some("/work"));
    assert_eq!(run.container_options.env, vec!["A=B"]);
    assert_eq!(run.container_options.env_files, vec!["/tmp/env"]);
    assert_eq!(
        run.container_options.mounts,
        vec!["type=bind,src=/x,dst=/y"]
    );
    assert_eq!(run.container_options.devices, vec!["/dev/fuse"]);
    assert_eq!(
        run.container_options.cdi_devices,
        vec!["vendor.com/device=name"]
    );
    assert_eq!(
        run.container_options.log_path.as_deref(),
        Some("container.log")
    );
    assert_eq!(
        run.container_options.resources.memory.as_deref(),
        Some("64MiB")
    );
    assert_eq!(run.container_options.resources.cpu_quota, Some(100000));
    assert_eq!(run.container_options.security.cap_add, vec!["NET_ADMIN"]);
    assert_eq!(
        run.container_options.security.user.as_deref(),
        Some("1000:1000")
    );
    assert_eq!(
        run.container_options.security.seccomp.as_deref(),
        Some("localhost/container.json")
    );
    assert_eq!(run.image, "busybox");
    assert_eq!(run.command, vec!["echo", "ok"]);
}

#[test]
fn parses_remaining_command_groups() {
    let args =
        Args::try_parse_from(["crs", "metrics", "scrape"]).expect("metrics scrape should parse");
    assert!(matches!(args.command, Command::Metrics(_)));

    let args = Args::try_parse_from(["crs", "recovery", "repair", "--dry-run"])
        .expect("recovery repair --dry-run should parse");
    let Command::Recovery(recovery) = args.command else {
        panic!("expected recovery command");
    };
    let RecoveryCommand::Repair(mode) = recovery.command else {
        panic!("expected recovery repair command");
    };
    assert!(mode.dry_run);
    assert!(!mode.execute);

    let args = Args::try_parse_from(["crs", "gc", "run", "--execute"])
        .expect("gc run --execute should parse");
    let Command::Gc(gc) = args.command else {
        panic!("expected gc command");
    };
    let GcCommand::Run(mode) = gc.command else {
        panic!("expected gc run command");
    };
    assert!(!mode.dry_run);
    assert!(mode.execute);

    let args =
        Args::try_parse_from(["crs", "debug", "rootless"]).expect("debug rootless should parse");
    assert!(matches!(args.command, Command::Debug(_)));

    let args =
        Args::try_parse_from(["crs", "completion", "bash"]).expect("completion should parse");
    assert!(matches!(args.command, Command::Completion(_)));
}

#[test]
fn rejects_execute_mode_without_explicit_choice() {
    let recovery_error = Args::try_parse_from(["crs", "recovery", "repair"])
        .expect_err("recovery repair requires dry-run or execute");
    assert_eq!(recovery_error.kind(), ErrorKind::MissingRequiredArgument);

    let gc_error =
        Args::try_parse_from(["crs", "gc", "run"]).expect_err("gc run requires dry-run or execute");
    assert_eq!(gc_error.kind(), ErrorKind::MissingRequiredArgument);
}

#[test]
fn rejects_unsupported_completion_shell() {
    let error = Args::try_parse_from(["crs", "completion", "elvish"])
        .expect_err("unsupported completion shell should fail parsing");

    assert_eq!(error.kind(), ErrorKind::InvalidValue);
}

#[test]
fn rejects_conflicting_auth_sources() {
    let error = Args::try_parse_from([
        "crs",
        "image",
        "pull",
        "busybox",
        "--auth-json",
        "{}",
        "--username",
        "alice",
    ])
    .expect_err("auth-json and username should conflict");

    assert_eq!(error.kind(), ErrorKind::ArgumentConflict);
}

#[test]
fn parses_every_public_command_minimal_arguments() {
    let commands: &[&[&str]] = &[
        &["crs", "version"],
        &["crs", "status"],
        &["crs", "doctor"],
        &["crs", "ps"],
        &["crs", "pods"],
        &["crs", "images"],
        &["crs", "pull", "busybox"],
        &["crs", "inspect", "target"],
        &["crs", "logs", "ctr"],
        &["crs", "exec", "ctr", "--", "sh"],
        &["crs", "stop", "target"],
        &["crs", "rm", "target"],
        &["crs", "rmi", "busybox"],
        &["crs", "rmp", "pod"],
        &["crs", "config", "show"],
        &["crs", "config", "reload-status"],
        &["crs", "runtime", "config"],
        &["crs", "runtime", "update", "--pod-cidr", "10.244.0.0/16"],
        &["crs", "runtime", "handlers"],
        &["crs", "image", "list"],
        &["crs", "image", "pull", "busybox"],
        &["crs", "image", "inspect", "busybox"],
        &["crs", "image", "remove", "busybox"],
        &["crs", "image", "fs-info"],
        &["crs", "image", "transfers"],
        &["crs", "image", "config"],
        &["crs", "pod", "list"],
        &["crs", "pod", "inspect", "pod"],
        &["crs", "pod", "run"],
        &["crs", "pod", "stop", "pod"],
        &["crs", "pod", "remove", "pod"],
        &["crs", "pod", "stats"],
        &["crs", "pod", "metrics"],
        &["crs", "pod", "update-resources", "pod"],
        &["crs", "pod", "port-forward", "pod", "--forward", "8080:80"],
        &["crs", "container", "list"],
        &["crs", "container", "inspect", "ctr"],
        &["crs", "container", "create", "pod", "busybox"],
        &["crs", "container", "start", "ctr"],
        &["crs", "container", "stop", "ctr"],
        &["crs", "container", "remove", "ctr"],
        &["crs", "container", "exec", "ctr", "--", "sh"],
        &["crs", "container", "exec-sync", "ctr", "--", "sh"],
        &["crs", "container", "attach", "ctr"],
        &["crs", "container", "stats"],
        &[
            "crs",
            "container",
            "checkpoint",
            "ctr",
            "--location",
            "/tmp/checkpoint",
        ],
        &["crs", "container", "update", "ctr"],
        &["crs", "container", "reopen-log", "ctr"],
        &["crs", "container", "logs", "ctr"],
        &["crs", "run", "busybox"],
        &["crs", "events"],
        &["crs", "stats"],
        &["crs", "metrics", "descriptors"],
        &["crs", "metrics", "scrape"],
        &["crs", "recovery", "status"],
        &["crs", "recovery", "check"],
        &["crs", "recovery", "repair", "--dry-run"],
        &["crs", "gc", "candidates"],
        &["crs", "gc", "run", "--dry-run"],
        &["crs", "debug", "network"],
        &["crs", "debug", "runtime"],
        &["crs", "debug", "shims"],
        &["crs", "debug", "nri"],
        &["crs", "debug", "security"],
        &["crs", "debug", "cgroups"],
        &["crs", "debug", "streaming"],
        &["crs", "debug", "metrics"],
        &["crs", "debug", "tracing"],
        &["crs", "debug", "rootless"],
        &["crs", "completion", "bash"],
    ];

    for argv in commands {
        Args::try_parse_from(*argv).unwrap_or_else(|error| {
            panic!("failed to parse {argv:?}: {error}");
        });
    }
}

#[test]
fn rejects_mutually_exclusive_execute_modes() {
    let recovery_error =
        Args::try_parse_from(["crs", "recovery", "repair", "--dry-run", "--execute"])
            .expect_err("recovery repair cannot be dry-run and execute");
    assert_eq!(recovery_error.kind(), ErrorKind::ArgumentConflict);

    let gc_error = Args::try_parse_from(["crs", "gc", "run", "--dry-run", "--execute"])
        .expect_err("gc run cannot be dry-run and execute");
    assert_eq!(gc_error.kind(), ErrorKind::ArgumentConflict);
}
