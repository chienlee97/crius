use clap::{error::ErrorKind, Parser};
use crius::crs::args::{
    Args, Command, ConfigCommand, ContainerCommand, ContainerStateArg, ExecModeArg, GcCommand,
    ImageCommand, ObjectType, PodCommand, PodStateArg, PullPolicyArg, RecoveryCommand,
    RuntimeCommand, StopObjectType, StreamProtocolArg,
};
use std::time::Duration;

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
fn parses_minimal_version_command() {
    let args = Args::try_parse_from(["crs", "version"]).expect("version should parse");

    assert!(matches!(args.command, crius::crs::args::Command::Version(_)));
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
    let cases: &[(&[&str], fn(&Command) -> bool)] = &[
        (&["crs", "version"], |command| matches!(command, Command::Version(_))),
        (&["crs", "status"], |command| matches!(command, Command::Status(_))),
        (&["crs", "doctor"], |command| matches!(command, Command::Doctor(_))),
        (&["crs", "ps"], |command| matches!(command, Command::Ps(_))),
        (&["crs", "pods"], |command| matches!(command, Command::Pods(_))),
        (&["crs", "images"], |command| matches!(command, Command::Images(_))),
        (&["crs", "pull", "busybox"], |command| matches!(command, Command::Pull(_))),
        (&["crs", "inspect", "abc123"], |command| {
            matches!(command, Command::Inspect(_))
        }),
        (&["crs", "logs", "abc123"], |command| matches!(command, Command::Logs(_))),
        (&["crs", "exec", "abc123", "--", "echo"], |command| {
            matches!(command, Command::Exec(_))
        }),
        (&["crs", "stop", "abc123"], |command| matches!(command, Command::Stop(_))),
        (&["crs", "rm", "abc123"], |command| matches!(command, Command::Rm(_))),
    ];

    for (argv, assert_command) in cases {
        let args = Args::try_parse_from(*argv).unwrap_or_else(|error| {
            panic!("failed to parse {argv:?}: {error}");
        });

        assert!(assert_command(&args.command), "unexpected command for {argv:?}");
    }
}

#[test]
fn parses_top_level_command_groups() {
    let cases: &[(&[&str], fn(&Command) -> bool)] = &[
        (&["crs", "config", "show"], |command| matches!(command, Command::Config(_))),
        (&["crs", "runtime", "config"], |command| {
            matches!(command, Command::Runtime(_))
        }),
        (&["crs", "image", "list"], |command| matches!(command, Command::Image(_))),
        (&["crs", "pod", "list"], |command| matches!(command, Command::Pod(_))),
        (&["crs", "container", "list"], |command| {
            matches!(command, Command::Container(_))
        }),
        (&["crs", "run", "busybox"], |command| matches!(command, Command::Run(_))),
        (&["crs", "events"], |command| matches!(command, Command::Events(_))),
        (&["crs", "stats"], |command| matches!(command, Command::Stats(_))),
        (&["crs", "metrics", "descriptors"], |command| {
            matches!(command, Command::Metrics(_))
        }),
        (&["crs", "recovery", "status"], |command| {
            matches!(command, Command::Recovery(_))
        }),
        (&["crs", "gc", "candidates"], |command| matches!(command, Command::Gc(_))),
        (&["crs", "debug", "network"], |command| matches!(command, Command::Debug(_))),
        (&["crs", "completion", "bash"], |command| {
            matches!(command, Command::Completion(_))
        }),
    ];

    for (argv, assert_command) in cases {
        let args = Args::try_parse_from(*argv).unwrap_or_else(|error| {
            panic!("failed to parse {argv:?}: {error}");
        });

        assert!(assert_command(&args.command), "unexpected command for {argv:?}");
    }
}

#[test]
fn parses_pod_command_arguments() {
    let args = Args::try_parse_from([
        "crs",
        "pod",
        "list",
        "--id",
        "pod1",
        "--state",
        "ready",
        "--label",
        "app=test",
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
    assert_eq!(run.security.sandbox_seccomp.as_deref(), Some("runtime/default"));
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
    let args =
        Args::try_parse_from(["crs", "inspect", "--type", "image", "busybox:latest"])
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

    let args =
        Args::try_parse_from(["crs", "rm", "--force", "--type", "container", "container1"])
            .expect("remove force should parse");
    let Command::Rm(remove) = args.command else {
        panic!("expected rm command");
    };
    assert!(remove.force);
    assert_eq!(remove.object_type, Some(ObjectType::Container));
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

    let args = Args::try_parse_from(["crs", "image", "fs-info"])
        .expect("image fs-info should parse");
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

    let args = Args::try_parse_from(["crs", "debug", "rootless"])
        .expect("debug rootless should parse");
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

    let gc_error = Args::try_parse_from(["crs", "gc", "run"])
        .expect_err("gc run requires dry-run or execute");
    assert_eq!(gc_error.kind(), ErrorKind::MissingRequiredArgument);
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
        &["crs", "container", "checkpoint", "ctr", "--location", "/tmp/checkpoint"],
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
    let recovery_error = Args::try_parse_from([
        "crs",
        "recovery",
        "repair",
        "--dry-run",
        "--execute",
    ])
    .expect_err("recovery repair cannot be dry-run and execute");
    assert_eq!(recovery_error.kind(), ErrorKind::ArgumentConflict);

    let gc_error = Args::try_parse_from(["crs", "gc", "run", "--dry-run", "--execute"])
        .expect_err("gc run cannot be dry-run and execute");
    assert_eq!(gc_error.kind(), ErrorKind::ArgumentConflict);
}
