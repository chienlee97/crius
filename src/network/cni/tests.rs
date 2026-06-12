use super::*;
use crate::services::{InternalEventSeverity, LedgerInternalEventSink};
use crate::storage::StorageManager;
use std::os::unix::fs::PermissionsExt;
use tempfile::tempdir;

const FAKE_CNI_PLUGIN: &str = r#"#!/bin/sh
set -eu

record_path="${CRIUS_FAKE_CNI_PLUGIN_RECORD_PATH:-}"
if [ -n "$record_path" ]; then
  cat > "$record_path"
else
  cat >/dev/null
fi

should_fail=false
fail_on="${CRIUS_FAKE_CNI_PLUGIN_FAIL_ON:-}"
if [ -n "$fail_on" ]; then
  old_ifs=$IFS
  IFS=','
  for phase in $fail_on; do
    if [ "$(printf '%s' "$phase" | tr '[:lower:]' '[:upper:]')" = "${CNI_COMMAND:-}" ]; then
      should_fail=true
      break
    fi
  done
  IFS=$old_ifs
fi

if [ "$should_fail" = "true" ]; then
  printf '%s\n' "${CRIUS_FAKE_CNI_PLUGIN_ERROR_MESSAGE:-fake CNI failure injection}" >&2
  exit "${CRIUS_FAKE_CNI_PLUGIN_ERROR_CODE:-42}"
fi

if [ "${CNI_COMMAND:-}" = "DEL" ]; then
  exit 0
fi

if [ -n "${CRIUS_FAKE_CNI_PLUGIN_RESULT_JSON:-}" ]; then
  printf '%s\n' "${CRIUS_FAKE_CNI_PLUGIN_RESULT_JSON}"
else
  printf '%s\n' '{"cniVersion":"1.0.0","ips":[{"address":"10.88.0.2/16"}]}'
fi
"#;

#[tokio::test]
async fn load_network_configs_includes_conflist_files() {
    let dir = tempdir().unwrap();
    let config_dir = dir.path().join("net.d");
    tokio::fs::create_dir_all(&config_dir).await.unwrap();
    tokio::fs::write(
        config_dir.join("10-test.conflist"),
        r#"{
                "cniVersion":"0.4.0",
                "name":"test-net",
                "plugins":[
                    {"type":"bridge"},
                    {"type":"portmap"}
                ]
            }"#,
    )
    .await
    .unwrap();

    let mut manager = CniManager::new(
        vec![dir.path().join("bin").display().to_string()],
        vec![config_dir.display().to_string()],
        dir.path().join("cache").display().to_string(),
    )
    .unwrap();
    let status = manager.load_network_configs().await.unwrap();

    let config = manager
        .network_configs
        .get("test-net")
        .expect("expected conflist to be loaded");
    assert_eq!(config.plugin_type, "bridge");
    assert!(config.config.get("plugins").is_some());
    assert_eq!(manager.default_network_name.as_deref(), Some("test-net"));
    assert_eq!(status.reason, "CNIPluginMissing");
    assert_eq!(status.loaded_networks, vec!["test-net"]);
}

#[tokio::test]
async fn load_network_configs_treats_ipam_as_required_plugin() {
    let dir = tempdir().unwrap();
    let config_dir = dir.path().join("net.d");
    let plugin_dir = dir.path().join("bin");
    tokio::fs::create_dir_all(&config_dir).await.unwrap();
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
    for plugin in ["bridge", "loopback", "portmap"] {
        let path = plugin_dir.join(plugin);
        tokio::fs::write(&path, "#!/bin/sh\ncat >/dev/null\n")
            .await
            .unwrap();
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(path, perms).unwrap();
    }
    tokio::fs::write(
        config_dir.join("10-crius-bridge.conflist"),
        r#"{
                "cniVersion":"1.0.0",
                "name":"crius-bridge",
                "plugins":[
                    {"type":"loopback"},
                    {"type":"bridge","ipam":{"type":"host-local"}},
                    {"type":"portmap"}
                ]
            }"#,
    )
    .await
    .unwrap();

    let mut manager = CniManager::new(
        vec![plugin_dir.display().to_string()],
        vec![config_dir.display().to_string()],
        dir.path().join("cache").display().to_string(),
    )
    .unwrap();
    let status = manager.load_network_configs().await.unwrap();

    assert_eq!(status.reason, "CNIPluginMissing");
    assert_eq!(
        status.declared_plugins,
        vec!["bridge", "host-local", "loopback", "portmap"]
    );
    assert_eq!(status.missing_plugin_binaries, vec!["host-local"]);
}

#[test]
fn primary_plugin_type_prefers_type_then_first_plugin() {
    let direct = serde_json::json!({
        "type": "macvlan",
        "plugins": [{"type": "bridge"}]
    });
    assert_eq!(CniManager::primary_plugin_type(&direct), "macvlan");

    let conflist = serde_json::json!({
        "plugins": [
            {"type": "bridge"},
            {"type": "portmap"}
        ]
    });
    assert_eq!(CniManager::primary_plugin_type(&conflist), "bridge");
}

#[test]
fn find_plugin_ignores_non_executable_binaries() {
    let dir = tempdir().unwrap();
    let plugin_dir = dir.path().join("bin");
    std::fs::create_dir_all(&plugin_dir).unwrap();
    let plugin_path = plugin_dir.join("bridge");
    std::fs::write(&plugin_path, "#!/bin/sh\nexit 0\n").unwrap();
    let mut perms = std::fs::metadata(&plugin_path).unwrap().permissions();
    perms.set_mode(0o644);
    std::fs::set_permissions(&plugin_path, perms).unwrap();

    let manager = CniManager::new(
        vec![plugin_dir.display().to_string()],
        vec![dir.path().join("conf").display().to_string()],
        dir.path().join("cache").display().to_string(),
    )
    .unwrap();

    assert!(manager.find_plugin("bridge").is_none());
}

#[tokio::test]
async fn load_network_configs_selects_default_network_deterministically() {
    let dir = tempdir().unwrap();
    let config_dir = dir.path().join("net.d");
    tokio::fs::create_dir_all(&config_dir).await.unwrap();
    tokio::fs::write(
        config_dir.join("20-second.conf"),
        r#"{"cniVersion":"0.4.0","name":"second-net","type":"bridge"}"#,
    )
    .await
    .unwrap();
    tokio::fs::write(
        config_dir.join("10-first.conf"),
        r#"{"cniVersion":"0.4.0","name":"first-net","type":"bridge"}"#,
    )
    .await
    .unwrap();

    let mut manager = CniManager::new(
        vec![dir.path().join("bin").display().to_string()],
        vec![config_dir.display().to_string()],
        dir.path().join("cache").display().to_string(),
    )
    .unwrap();
    let status = manager.load_network_configs().await.unwrap();

    assert_eq!(manager.default_network_name.as_deref(), Some("first-net"));
    assert_eq!(
        manager
            .default_network_config()
            .map(|config| config.name.as_str()),
        Some("first-net")
    );
    assert_eq!(status.default_network_name.as_deref(), Some("first-net"));
}

#[tokio::test]
async fn load_network_configs_reports_loaded_networks_in_stable_order() {
    let dir = tempdir().unwrap();
    let config_dir = dir.path().join("net.d");
    let plugin_dir = dir.path().join("bin");
    tokio::fs::create_dir_all(&config_dir).await.unwrap();
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
    tokio::fs::write(plugin_dir.join("bridge"), "#!/bin/sh\ncat >/dev/null\n")
        .await
        .unwrap();
    let mut perms = std::fs::metadata(plugin_dir.join("bridge"))
        .unwrap()
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(plugin_dir.join("bridge"), perms).unwrap();

    tokio::fs::write(
        config_dir.join("20-zeta.conf"),
        r#"{"cniVersion":"0.4.0","name":"zeta-net","type":"bridge"}"#,
    )
    .await
    .unwrap();
    tokio::fs::write(
        config_dir.join("10-alpha.conf"),
        r#"{"cniVersion":"0.4.0","name":"alpha-net","type":"bridge"}"#,
    )
    .await
    .unwrap();

    let mut manager = CniManager::new(
        vec![plugin_dir.display().to_string()],
        vec![config_dir.display().to_string()],
        dir.path().join("cache").display().to_string(),
    )
    .unwrap();
    let status = manager.load_network_configs().await.unwrap();

    assert_eq!(status.loaded_networks, vec!["alpha-net", "zeta-net"]);
    assert_eq!(status.default_network_name.as_deref(), Some("alpha-net"));
}

#[tokio::test]
async fn load_network_configs_honors_configured_default_network_name() {
    let dir = tempdir().unwrap();
    let config_dir = dir.path().join("net.d");
    tokio::fs::create_dir_all(&config_dir).await.unwrap();
    tokio::fs::write(
        config_dir.join("10-first.conf"),
        r#"{"cniVersion":"0.4.0","name":"first-net","type":"bridge"}"#,
    )
    .await
    .unwrap();
    tokio::fs::write(
        config_dir.join("20-second.conf"),
        r#"{"cniVersion":"0.4.0","name":"second-net","type":"bridge"}"#,
    )
    .await
    .unwrap();

    let mut manager = CniManager::new(
        vec![dir.path().join("bin").display().to_string()],
        vec![config_dir.display().to_string()],
        dir.path().join("cache").display().to_string(),
    )
    .unwrap();
    manager.set_default_network_name(Some("second-net".to_string()));
    let status = manager.load_network_configs().await.unwrap();

    assert_eq!(manager.default_network_name.as_deref(), Some("second-net"));
    assert_eq!(
        manager
            .default_network_config()
            .map(|config| config.name.as_str()),
        Some("second-net")
    );
    assert_eq!(status.default_network_name.as_deref(), Some("second-net"));
}

#[tokio::test]
async fn load_network_configs_limits_number_of_loaded_files() {
    let dir = tempdir().unwrap();
    let config_dir = dir.path().join("net.d");
    tokio::fs::create_dir_all(&config_dir).await.unwrap();
    tokio::fs::write(
        config_dir.join("10-first.conf"),
        r#"{"cniVersion":"0.4.0","name":"first-net","type":"bridge"}"#,
    )
    .await
    .unwrap();
    tokio::fs::write(
        config_dir.join("20-second.conf"),
        r#"{"cniVersion":"0.4.0","name":"second-net","type":"bridge"}"#,
    )
    .await
    .unwrap();

    let mut manager = CniManager::new(
        vec![dir.path().join("bin").display().to_string()],
        vec![config_dir.display().to_string()],
        dir.path().join("cache").display().to_string(),
    )
    .unwrap();
    manager.set_max_conf_num(1);
    let status = manager.load_network_configs().await.unwrap();

    assert_eq!(status.discovered_files.len(), 1);
    assert_eq!(status.loaded_networks, vec!["first-net"]);
    assert!(manager.network_configs.contains_key("first-net"));
    assert!(!manager.network_configs.contains_key("second-net"));
    assert_eq!(manager.default_network_name.as_deref(), Some("first-net"));
}

#[tokio::test]
async fn load_network_configs_reports_missing_configured_default_network() {
    let dir = tempdir().unwrap();
    let config_dir = dir.path().join("net.d");
    tokio::fs::create_dir_all(&config_dir).await.unwrap();
    tokio::fs::write(
        config_dir.join("10-first.conf"),
        r#"{"cniVersion":"0.4.0","name":"first-net","type":"bridge"}"#,
    )
    .await
    .unwrap();

    let mut manager = CniManager::new(
        vec![dir.path().join("bin").display().to_string()],
        vec![config_dir.display().to_string()],
        dir.path().join("cache").display().to_string(),
    )
    .unwrap();
    manager.set_default_network_name(Some("missing-net".to_string()));
    let status = manager.load_network_configs().await.unwrap();

    assert!(!status.ready);
    assert_eq!(status.reason, "CNIDefaultNetworkMissing");
    assert!(status.message.contains("missing-net"));
    assert_eq!(status.loaded_networks, vec!["first-net"]);
    assert!(manager.default_network_config().is_none());
    assert!(manager
        .setup_pod_network(
            "pod-1",
            "/var/run/netns/test-pod",
            "test-pod",
            "default",
            "uid-1",
            None
        )
        .await
        .is_err());
}

#[tokio::test]
async fn load_network_configs_applies_limit_before_default_network_name_matching() {
    let dir = tempdir().unwrap();
    let config_dir = dir.path().join("net.d");
    tokio::fs::create_dir_all(&config_dir).await.unwrap();
    tokio::fs::write(
        config_dir.join("10-first.conf"),
        r#"{"cniVersion":"0.4.0","name":"first-net","type":"bridge"}"#,
    )
    .await
    .unwrap();
    tokio::fs::write(
        config_dir.join("20-second.conf"),
        r#"{"cniVersion":"0.4.0","name":"second-net","type":"bridge"}"#,
    )
    .await
    .unwrap();

    let mut manager = CniManager::new(
        vec![dir.path().join("bin").display().to_string()],
        vec![config_dir.display().to_string()],
        dir.path().join("cache").display().to_string(),
    )
    .unwrap();
    manager.set_max_conf_num(1);
    manager.set_default_network_name(Some("second-net".to_string()));
    let status = manager.load_network_configs().await.unwrap();

    assert!(!status.ready);
    assert_eq!(status.reason, "CNIDefaultNetworkMissing");
    assert_eq!(status.discovered_files.len(), 1);
    assert_eq!(status.loaded_networks, vec!["first-net"]);
    assert!(status.message.contains("second-net"));
}

#[tokio::test]
async fn setup_pod_network_executes_conflist_as_plugin_chain() {
    let dir = tempdir().unwrap();
    let plugin_dir = dir.path().join("bin");
    let config_dir = dir.path().join("net.d");
    let record_dir = dir.path().join("records");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
    tokio::fs::create_dir_all(&config_dir).await.unwrap();
    tokio::fs::create_dir_all(&record_dir).await.unwrap();

    tokio::fs::write(
        config_dir.join("10-test.conflist"),
        r#"{
                "cniVersion":"1.0.0",
                "name":"test-net",
                "plugins":[
                    {"type":"bridge","bridge":"cni0","ipam":{"type":"host-local"}},
                    {"type":"portmap","capabilities":{"portMappings":true}}
                ]
            }"#,
    )
    .await
    .unwrap();

    let bridge_script = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$CNI_ARGS\" > \"{0}/bridge.env\"\ncat > \"{0}/bridge.input\"\nprintf '%s\\n' '{{\"cniVersion\":\"1.0.0\",\"ips\":[{{\"address\":\"10.88.0.2/16\"}}]}}'\n",
            record_dir.display()
        );
    let bridge_path = plugin_dir.join("bridge");
    tokio::fs::write(&bridge_path, bridge_script).await.unwrap();
    let mut bridge_perms = std::fs::metadata(&bridge_path).unwrap().permissions();
    bridge_perms.set_mode(0o755);
    std::fs::set_permissions(&bridge_path, bridge_perms).unwrap();

    let host_local_path = plugin_dir.join("host-local");
    tokio::fs::write(
        &host_local_path,
        "#!/bin/sh\ncat >/dev/null\nprintf '%s\\n' '{}'\n",
    )
    .await
    .unwrap();
    let mut host_local_perms = std::fs::metadata(&host_local_path).unwrap().permissions();
    host_local_perms.set_mode(0o755);
    std::fs::set_permissions(&host_local_path, host_local_perms).unwrap();

    let portmap_script = format!(
            "#!/bin/sh\ncat > \"{}/portmap.input\"\nprintf '%s\\n' '{{\"cniVersion\":\"1.0.0\",\"ips\":[{{\"address\":\"10.88.0.2/16\"}}]}}'\n",
            record_dir.display()
        );
    let portmap_path = plugin_dir.join("portmap");
    tokio::fs::write(&portmap_path, portmap_script)
        .await
        .unwrap();
    let mut portmap_perms = std::fs::metadata(&portmap_path).unwrap().permissions();
    portmap_perms.set_mode(0o755);
    std::fs::set_permissions(&portmap_path, portmap_perms).unwrap();

    let mut manager = CniManager::new(
        vec![plugin_dir.display().to_string()],
        vec![config_dir.display().to_string()],
        dir.path().join("cache").display().to_string(),
    )
    .unwrap();
    let load_status = manager.load_network_configs().await.unwrap();
    assert!(load_status.ready);
    assert_eq!(load_status.reason, "CNINetworkConfigReady");

    let status = manager
        .setup_pod_network(
            "pod-1",
            "/var/run/netns/test-pod",
            "test-pod",
            "default",
            "uid-1",
            None,
        )
        .await
        .unwrap();

    assert_eq!(status.ip, Some("10.88.0.2".parse().unwrap()));

    let bridge_input = tokio::fs::read_to_string(record_dir.join("bridge.input"))
        .await
        .unwrap();
    assert!(bridge_input.contains("\"type\":\"bridge\""));
    let bridge_env = tokio::fs::read_to_string(record_dir.join("bridge.env"))
        .await
        .unwrap();
    assert!(bridge_env.contains("K8S_POD_NAMESPACE=default"));
    assert!(bridge_env.contains("K8S_POD_NAME=test-pod"));
    assert!(bridge_env.contains("K8S_POD_INFRA_CONTAINER_ID=pod-1"));
    assert!(bridge_env.contains("K8S_POD_UID=uid-1"));
    assert!(!bridge_input.contains("\"plugins\""));

    let portmap_input = tokio::fs::read_to_string(record_dir.join("portmap.input"))
        .await
        .unwrap();
    assert!(portmap_input.contains("\"type\":\"portmap\""));
    assert!(portmap_input.contains("\"prevResult\""));
}

#[tokio::test]
async fn fake_cni_plugin_fixture_can_inject_add_failure() {
    let dir = tempdir().unwrap();
    let plugin_dir = dir.path().join("bin");
    let config_dir = dir.path().join("net.d");
    let record_path = dir.path().join("bridge.input");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
    tokio::fs::create_dir_all(&config_dir).await.unwrap();
    tokio::fs::write(
        config_dir.join("10-test.conf"),
        r#"{"cniVersion":"1.0.0","name":"test-net","type":"bridge"}"#,
    )
    .await
    .unwrap();

    let plugin_path = plugin_dir.join("bridge");
    tokio::fs::write(
        &plugin_path,
        format!(
            "#!/bin/sh\ncat > '{}'\nprintf '%s\\n' 'injected add failure' >&2\nexit 42\n",
            record_path.display()
        ),
    )
    .await
    .unwrap();
    let mut perms = std::fs::metadata(&plugin_path).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&plugin_path, perms).unwrap();

    let mut manager = CniManager::new(
        vec![plugin_dir.display().to_string()],
        vec![config_dir.display().to_string()],
        dir.path().join("cache").display().to_string(),
    )
    .unwrap();
    let load_status = manager.load_network_configs().await.unwrap();
    assert!(load_status.ready);

    let err = manager
        .setup_pod_network(
            "pod-1",
            "/var/run/netns/pod-1",
            "pod",
            "default",
            "uid-1",
            None,
        )
        .await
        .expect_err("fake plugin should inject ADD failure");
    assert!(err.to_string().contains("injected add failure"));
    let recorded = tokio::fs::read_to_string(&record_path).await.unwrap();
    assert!(recorded.contains("\"name\":\"test-net\""));
}

#[tokio::test]
async fn cni_config_and_plugin_chain_events_are_persisted() {
    let dir = tempdir().unwrap();
    let plugin_dir = dir.path().join("bin");
    let config_dir = dir.path().join("net.d");
    let db_path = dir.path().join("state").join("crius.db");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
    tokio::fs::create_dir_all(&config_dir).await.unwrap();
    tokio::fs::write(
        config_dir.join("10-test.conf"),
        r#"{"cniVersion":"1.0.0","name":"test-net","type":"bridge"}"#,
    )
    .await
    .unwrap();

    let plugin_path = plugin_dir.join("bridge");
    tokio::fs::write(&plugin_path, FAKE_CNI_PLUGIN)
        .await
        .unwrap();
    let mut perms = std::fs::metadata(&plugin_path).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&plugin_path, perms).unwrap();

    let mut manager = CniManager::new(
        vec![plugin_dir.display().to_string()],
        vec![config_dir.display().to_string()],
        dir.path().join("cache").display().to_string(),
    )
    .unwrap();
    manager.set_event_sink(Some(LedgerInternalEventSink::new(db_path.clone())));
    let load_status = manager.load_network_configs().await.unwrap();
    manager.publish_config_load_event("pod-events", "runc", &load_status);
    manager
        .setup_pod_network(
            "pod-events",
            "/var/run/netns/pod-events",
            "pod",
            "default",
            "uid-1",
            None,
        )
        .await
        .unwrap();

    let events = StorageManager::new(&db_path)
        .unwrap()
        .get_recent_events_for_subject("network", "pod-events", 10)
        .unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event_type, "network.plugin_chain");
    assert_eq!(events[0].new_state, InternalEventSeverity::Info.as_str());
    assert_eq!(events[1].event_type, "network.config_load");
    let details: serde_json::Value =
        serde_json::from_str(events[1].details.as_ref().unwrap()).unwrap();
    assert_eq!(details["runtimeHandler"], "runc");
    assert_eq!(details["reason"], "CNINetworkConfigReady");
    assert_eq!(details["declaredPlugins"], serde_json::json!(["bridge"]));
}

#[tokio::test]
async fn teardown_uses_pod_cached_cni_config_after_reload() {
    let dir = tempdir().unwrap();
    let plugin_dir = dir.path().join("bin");
    let config_dir = dir.path().join("net.d");
    let cache_dir = dir.path().join("cache");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
    tokio::fs::create_dir_all(&config_dir).await.unwrap();

    for plugin in ["bridge", "macvlan"] {
        let plugin_path = plugin_dir.join(plugin);
        tokio::fs::write(
            &plugin_path,
            format!(
                "#!/bin/sh\nset -eu\nrecord_dir=\"{}\"\nmkdir -p \"$record_dir\"\ncat >/dev/null\nprintf '%s\\n' \"$CNI_COMMAND\" >> \"$record_dir/{plugin}.commands\"\nif [ \"${{CNI_COMMAND:-}}\" != \"DEL\" ]; then printf '%s\\n' '{{\"cniVersion\":\"1.0.0\",\"ips\":[{{\"address\":\"10.88.0.2/16\"}}]}}'; fi\n",
                dir.path().display()
            ),
        )
        .await
        .unwrap();
        let mut perms = std::fs::metadata(&plugin_path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&plugin_path, perms).unwrap();
    }

    tokio::fs::write(
        config_dir.join("10-test.conf"),
        r#"{"cniVersion":"1.0.0","name":"test-net","type":"bridge"}"#,
    )
    .await
    .unwrap();

    let mut first = CniManager::new(
        vec![plugin_dir.display().to_string()],
        vec![config_dir.display().to_string()],
        cache_dir.display().to_string(),
    )
    .unwrap();
    assert!(first.load_network_configs().await.unwrap().ready);
    first
        .setup_pod_network(
            "pod-old",
            "/var/run/netns/pod-old",
            "old",
            "default",
            "uid-old",
            None,
        )
        .await
        .unwrap();

    tokio::fs::write(
        config_dir.join("10-test.conf"),
        r#"{"cniVersion":"1.0.0","name":"test-net","type":"macvlan"}"#,
    )
    .await
    .unwrap();
    let mut reloaded = CniManager::new(
        vec![plugin_dir.display().to_string()],
        vec![config_dir.display().to_string()],
        cache_dir.display().to_string(),
    )
    .unwrap();
    assert!(reloaded.load_network_configs().await.unwrap().ready);
    reloaded
        .setup_pod_network(
            "pod-new",
            "/var/run/netns/pod-new",
            "new",
            "default",
            "uid-new",
            None,
        )
        .await
        .unwrap();
    reloaded
        .teardown_pod_network(
            "pod-old",
            "/var/run/netns/pod-old",
            "default",
            "old",
            "uid-old",
        )
        .await
        .unwrap();

    let bridge_commands = tokio::fs::read_to_string(dir.path().join("bridge.commands"))
        .await
        .unwrap();
    let macvlan_commands = tokio::fs::read_to_string(dir.path().join("macvlan.commands"))
        .await
        .unwrap();
    assert_eq!(bridge_commands.lines().collect::<Vec<_>>(), ["ADD", "DEL"]);
    assert_eq!(macvlan_commands.lines().collect::<Vec<_>>(), ["ADD"]);
    assert!(tokio::fs::metadata(cache_dir.join("pod-old.config.json"))
        .await
        .is_err());
}

#[tokio::test]
async fn teardown_pod_network_times_out_stuck_del_plugin() {
    let dir = tempdir().unwrap();
    let plugin_dir = dir.path().join("bin");
    let config_dir = dir.path().join("net.d");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
    tokio::fs::create_dir_all(&config_dir).await.unwrap();
    tokio::fs::write(
        config_dir.join("10-test.conf"),
        r#"{"cniVersion":"1.0.0","name":"test-net","type":"bridge"}"#,
    )
    .await
    .unwrap();

    let plugin_path = plugin_dir.join("bridge");
    tokio::fs::write(
            &plugin_path,
            "#!/bin/sh\ncat >/dev/null\nif [ \"${CNI_COMMAND:-}\" = \"DEL\" ]; then sleep 10; fi\nprintf '%s\\n' '{\"cniVersion\":\"1.0.0\",\"ips\":[{\"address\":\"10.88.0.2/16\"}]}'\n",
        )
        .await
        .unwrap();
    let mut perms = std::fs::metadata(&plugin_path).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&plugin_path, perms).unwrap();

    let mut manager = CniManager::new(
        vec![plugin_dir.display().to_string()],
        vec![config_dir.display().to_string()],
        dir.path().join("cache").display().to_string(),
    )
    .unwrap();
    manager.set_teardown_timeout(Duration::from_millis(100));
    let load_status = manager.load_network_configs().await.unwrap();
    assert!(load_status.ready);

    let start = std::time::Instant::now();
    let err = manager
        .teardown_pod_network("pod-1", "/var/run/netns/pod-1", "default", "pod", "uid-1")
        .await
        .expect_err("stuck DEL plugin should hit teardown timeout");
    assert!(start.elapsed() < Duration::from_secs(2));
    assert!(err.to_string().contains("timed out"));
}

#[test]
fn parse_cni_result_returns_none_when_output_missing() {
    let manager = CniManager::new(vec![], vec![], "/tmp/cache".to_string()).unwrap();
    let status = manager.parse_cni_result(None).unwrap();
    assert!(status.ip.is_none());
}

#[test]
fn network_status_from_cni_result_preserves_raw_result_and_ip_order() {
    let result = serde_json::json!({
        "cniVersion": "1.0.0",
        "ips": [
            {"address": "fd00::10/64"},
            {"address": "10.88.0.11/16"},
            {"address": "fd00::10/64"}
        ]
    });

    let status = CniManager::network_status_from_cni_result(Some(&result)).unwrap();

    assert_eq!(status.ip.unwrap().to_string(), "fd00::10");
    assert_eq!(status.interfaces.len(), 1);
    assert_eq!(status.interfaces[0].ip.unwrap().to_string(), "10.88.0.11");
    assert_eq!(status.raw_result, Some(result));
}

#[test]
fn network_status_from_cni_result_prefers_ipv4_when_configured() {
    let result = serde_json::json!({
        "cniVersion": "1.0.0",
        "ips": [
            {"address": "fd00::10/64"},
            {"address": "10.88.0.11/16"},
            {"address": "fd00::12/64"}
        ]
    });

    let status = CniManager::network_status_from_cni_result_with_preference(
        Some(&result),
        MainIpPreference::Ipv4,
    )
    .unwrap();

    assert_eq!(status.ip.unwrap().to_string(), "10.88.0.11");
    assert_eq!(status.interfaces.len(), 2);
    assert_eq!(status.interfaces[0].ip.unwrap().to_string(), "fd00::10");
    assert_eq!(status.interfaces[1].ip.unwrap().to_string(), "fd00::12");
}

#[test]
fn network_status_from_cni_result_prefers_ipv6_when_configured() {
    let result = serde_json::json!({
        "cniVersion": "1.0.0",
        "ips": [
            {"address": "10.88.0.11/16"},
            {"address": "fd00::10/64"},
            {"address": "10.88.0.12/16"}
        ]
    });

    let status = CniManager::network_status_from_cni_result_with_preference(
        Some(&result),
        MainIpPreference::Ipv6,
    )
    .unwrap();

    assert_eq!(status.ip.unwrap().to_string(), "fd00::10");
    assert_eq!(status.interfaces.len(), 2);
    assert_eq!(status.interfaces[0].ip.unwrap().to_string(), "10.88.0.11");
    assert_eq!(status.interfaces[1].ip.unwrap().to_string(), "10.88.0.12");
}

#[test]
fn network_status_from_cni_result_falls_back_to_cni_order_when_preferred_family_missing() {
    let result = serde_json::json!({
        "cniVersion": "1.0.0",
        "ips": [
            {"address": "fd00::10/64"},
            {"address": "fd00::12/64"}
        ]
    });

    let status = CniManager::network_status_from_cni_result_with_preference(
        Some(&result),
        MainIpPreference::Ipv4,
    )
    .unwrap();

    assert_eq!(status.ip.unwrap().to_string(), "fd00::10");
    assert_eq!(status.interfaces.len(), 1);
    assert_eq!(status.interfaces[0].ip.unwrap().to_string(), "fd00::12");
}

#[tokio::test]
async fn load_network_configs_reports_missing_plugin_binaries() {
    let dir = tempdir().unwrap();
    let config_dir = dir.path().join("net.d");
    tokio::fs::create_dir_all(&config_dir).await.unwrap();
    tokio::fs::write(
        config_dir.join("10-test.conflist"),
        r#"{
                "cniVersion":"1.0.0",
                "name":"test-net",
                "plugins":[
                    {"type":"bridge"},
                    {"type":"portmap"}
                ]
            }"#,
    )
    .await
    .unwrap();

    let mut manager = CniManager::new(
        vec![dir.path().join("bin").display().to_string()],
        vec![config_dir.display().to_string()],
        dir.path().join("cache").display().to_string(),
    )
    .unwrap();
    let status = manager.load_network_configs().await.unwrap();

    assert!(!status.ready);
    assert_eq!(status.reason, "CNIPluginMissing");
    assert_eq!(status.loaded_networks, vec!["test-net"]);
    assert_eq!(status.declared_plugins, vec!["bridge", "portmap"]);
    assert_eq!(status.missing_plugin_binaries, vec!["bridge", "portmap"]);
    assert_eq!(status.default_network_name.as_deref(), Some("test-net"));
}

#[tokio::test]
async fn load_network_configs_reports_invalid_configs() {
    let dir = tempdir().unwrap();
    let config_dir = dir.path().join("net.d");
    tokio::fs::create_dir_all(&config_dir).await.unwrap();
    tokio::fs::write(config_dir.join("10-bad.conf"), "{not-json")
        .await
        .unwrap();

    let mut manager = CniManager::new(
        vec![dir.path().join("bin").display().to_string()],
        vec![config_dir.display().to_string()],
        dir.path().join("cache").display().to_string(),
    )
    .unwrap();
    let status = manager.load_network_configs().await.unwrap();

    assert!(!status.ready);
    assert_eq!(status.reason, "CNIConfigInvalid");
    assert_eq!(
        status.invalid_files,
        vec![config_dir.join("10-bad.conf").display().to_string()]
    );
    assert!(status.loaded_networks.is_empty());
}
