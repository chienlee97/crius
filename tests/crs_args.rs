use clap::{error::ErrorKind, Parser};
use crius::crs::args::{
    Args, Command, ConfigCommand, ContainerCommand, ContainerStateArg, ImageCommand, ObjectType,
    PodCommand, PodStateArg, RuntimeCommand, StopObjectType,
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

    let args = Args::try_parse_from(["crs", "container", "exec", "ctr1", "--", "echo", "ok"])
        .expect("container exec command should parse");
    let Command::Container(container) = args.command else {
        panic!("expected container command");
    };
    let ContainerCommand::Exec(exec) = container.command else {
        panic!("expected container exec command");
    };
    assert_eq!(exec.container, "ctr1");
    assert_eq!(exec.command, vec!["echo", "ok"]);
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
