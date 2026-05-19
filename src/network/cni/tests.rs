use super::*;
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

struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        unsafe {
            if let Some(previous) = self.previous.as_ref() {
                std::env::set_var(self.key, previous);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }
}

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
    tokio::fs::write(&plugin_path, FAKE_CNI_PLUGIN)
        .await
        .unwrap();
    let mut perms = std::fs::metadata(&plugin_path).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&plugin_path, perms).unwrap();

    let _fail_on = EnvGuard::set("CRIUS_FAKE_CNI_PLUGIN_FAIL_ON", "ADD");
    let _message = EnvGuard::set(
        "CRIUS_FAKE_CNI_PLUGIN_ERROR_MESSAGE",
        "injected add failure",
    );
    let _record = EnvGuard::set(
        "CRIUS_FAKE_CNI_PLUGIN_RECORD_PATH",
        record_path.to_str().unwrap(),
    );

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
