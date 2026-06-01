use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use futures::stream::TryStreamExt;
use nix::sched::{setns, CloneFlags};
use rtnetlink::packet_route::{
    address::AddressAttribute,
    link::{LinkAttribute, LinkMessage},
};

use super::{NetworkError, NetworkInterface};

pub(crate) async fn set_loopback_up(netns_path: PathBuf) -> Result<(), NetworkError> {
    tokio::task::spawn_blocking(move || {
        run_in_netns_thread(netns_path, || {
            run_netlink(async {
                let (connection, handle, _) = rtnetlink::new_connection().map_err(|err| {
                    NetworkError::Other(format!("netlink connection failed: {err}"))
                })?;
                tokio::spawn(connection);

                handle
                    .link()
                    .set(rtnetlink::LinkUnspec::new_with_name("lo").up().build())
                    .execute()
                    .await
                    .map_err(|err| NetworkError::Other(format!("failed to set lo up: {err}")))
            })
        })
    })
    .await
    .map_err(|err| NetworkError::Other(format!("loopback setup task failed: {err}")))?
}

pub(crate) async fn discover_interfaces(
    netns_path: &Path,
) -> Result<Vec<NetworkInterface>, NetworkError> {
    let netns_path = netns_path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        run_in_netns_thread(netns_path, || {
            run_netlink(async {
                let (connection, handle, _) = rtnetlink::new_connection().map_err(|err| {
                    NetworkError::Other(format!("netlink connection failed: {err}"))
                })?;
                tokio::spawn(connection);

                let mut links = HashMap::new();
                let mut link_stream = handle.link().get().execute();
                while let Some(link) = link_stream
                    .try_next()
                    .await
                    .map_err(|err| NetworkError::Other(format!("failed to list links: {err}")))?
                {
                    if let Some(name) = link_name(&link) {
                        links.insert(link.header.index, name);
                    }
                }

                let mut seen = HashSet::new();
                let mut interfaces = Vec::new();
                let mut addr_stream = handle.address().get().execute();
                while let Some(addr) = addr_stream.try_next().await.map_err(|err| {
                    NetworkError::Other(format!("failed to list addresses: {err}"))
                })? {
                    let Some(ip) = address_ip(&addr.attributes) else {
                        continue;
                    };
                    if ip.is_loopback() || !seen.insert(ip) {
                        continue;
                    }

                    let name = links
                        .get(&addr.header.index)
                        .cloned()
                        .unwrap_or_else(|| addr.header.index.to_string());
                    interfaces.push(NetworkInterface {
                        name,
                        ip: Some(ip),
                        mac: None,
                        netmask: None,
                        gateway: None,
                    });
                }

                Ok(interfaces)
            })
        })
    })
    .await
    .map_err(|err| NetworkError::Other(format!("netns interface probe task failed: {err}")))?
}

fn run_in_netns_thread<T, F>(netns_path: PathBuf, f: F) -> Result<T, NetworkError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, NetworkError> + Send + 'static,
{
    let thread = std::thread::Builder::new()
        .name("crius-netlink-netns".to_string())
        .spawn(move || {
            let netns_file = std::fs::File::open(&netns_path)?;
            setns(netns_file.as_raw_fd(), CloneFlags::CLONE_NEWNET).map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("failed to enter network namespace: {err}"),
                )
            })?;
            f()
        })?;

    thread
        .join()
        .map_err(|_| NetworkError::Other("netlink netns thread panicked".to_string()))?
}

fn run_netlink<T, F>(future: F) -> Result<T, NetworkError>
where
    F: std::future::Future<Output = Result<T, NetworkError>>,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .map_err(|err| NetworkError::Other(format!("failed to build netlink runtime: {err}")))?
        .block_on(future)
}

fn link_name(link: &LinkMessage) -> Option<String> {
    link.attributes.iter().find_map(|attr| match attr {
        LinkAttribute::IfName(name) => Some(name.clone()),
        _ => None,
    })
}

fn address_ip(attributes: &[AddressAttribute]) -> Option<IpAddr> {
    let mut address = None;
    for attr in attributes {
        match attr {
            AddressAttribute::Local(ip) => return Some(*ip),
            AddressAttribute::Address(ip) => address = Some(*ip),
            _ => {}
        }
    }
    address
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use futures::stream::TryStreamExt;
    use rtnetlink::LinkDummy;

    use super::*;
    use crate::network::NamespaceManager;

    #[tokio::test]
    async fn netlink_sets_loopback_up_and_discovers_netns_interfaces() {
        if !can_create_network_namespace() {
            eprintln!("skipping netlink netns test because CLONE_NEWNET is unavailable");
            return;
        }

        let temp = tempfile::tempdir().expect("tempdir");
        let manager = NamespaceManager::new(temp.path().to_path_buf());
        let netns_path = manager.create("netlink-test").await.expect("create netns");

        set_loopback_up(netns_path.clone())
            .await
            .expect("set loopback up through netlink");
        add_dummy_interface_with_address(netns_path.clone())
            .await
            .expect("add dummy interface through netlink");

        let interfaces = discover_interfaces(&netns_path)
            .await
            .expect("discover interfaces through netlink");

        assert!(interfaces.iter().any(|iface| {
            iface.name == "dummy0" && iface.ip == Some(IpAddr::V4(Ipv4Addr::new(10, 123, 45, 6)))
        }));
        assert!(interfaces.iter().all(|iface| iface.name != "lo"));

        manager
            .remove(netns_path.to_str().expect("netns path is utf-8"))
            .await
            .expect("remove netns");
    }

    async fn add_dummy_interface_with_address(netns_path: PathBuf) -> Result<(), NetworkError> {
        tokio::task::spawn_blocking(move || {
            run_in_netns_thread(netns_path, || {
                run_netlink(async {
                    let (connection, handle, _) = rtnetlink::new_connection().map_err(|err| {
                        NetworkError::Other(format!("netlink connection failed: {err}"))
                    })?;
                    tokio::spawn(connection);

                    handle
                        .link()
                        .add(LinkDummy::new("dummy0").build())
                        .execute()
                        .await
                        .map_err(|err| {
                            NetworkError::Other(format!("failed to add dummy0: {err}"))
                        })?;

                    let mut links = handle
                        .link()
                        .get()
                        .match_name("dummy0".to_string())
                        .execute();
                    let link = links
                        .try_next()
                        .await
                        .map_err(|err| {
                            NetworkError::Other(format!("failed to query dummy0: {err}"))
                        })?
                        .ok_or_else(|| NetworkError::Other("dummy0 was not created".to_string()))?;

                    handle
                        .link()
                        .set(rtnetlink::LinkUnspec::new_with_name("dummy0").up().build())
                        .execute()
                        .await
                        .map_err(|err| {
                            NetworkError::Other(format!("failed to set dummy0 up: {err}"))
                        })?;

                    handle
                        .address()
                        .add(
                            link.header.index,
                            IpAddr::V4(Ipv4Addr::new(10, 123, 45, 6)),
                            24,
                        )
                        .execute()
                        .await
                        .map_err(|err| {
                            NetworkError::Other(format!("failed to add dummy0 address: {err}"))
                        })?;

                    Ok(())
                })
            })
        })
        .await
        .map_err(|err| NetworkError::Other(format!("dummy interface task failed: {err}")))?
    }

    fn can_create_network_namespace() -> bool {
        std::process::Command::new("unshare")
            .args(["-n", "true"])
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }
}
