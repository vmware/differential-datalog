//! A module providing ZooKeeper related functionality for `d3log`.

#[cfg(test)]
use std::env::var_os;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde_json::from_slice;

use zookeeper::Acl;
use zookeeper::CreateMode;
use zookeeper::ZkError;

use crate::schema::Members;
use crate::schema::SysCfg;
use crate::ReadConfig;
use crate::ReadMembers;

pub use zookeeper::WatchedEvent;
pub use zookeeper::Watcher;
pub use zookeeper::ZooKeeper;

#[cfg(test)]
const ENDPOINTS_ENV: &str = "ZOOKEEPER_ENDPOINTS";

const D3LOG_KEY: &str = "/d3log";
const CONFIG_KEY: &str = "/d3log/config";
const MEMBERS_KEY: &str = "/d3log/members";

/// Connect to a ZooKeeper instance comprised of the given set of servers.
///
/// Note that each server address should contain the port the
/// service is listening on.
pub fn connect<I, S, W>(mut servers: I, watcher: W) -> Result<ZooKeeper, String>
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
    W: Watcher + 'static,
{
    let first = servers
        .next()
        .ok_or("failed to connect to ZooKeeper: no servers supplied")?
        .as_ref()
        .to_string();
    let servers = servers.fold(first, |x, y| x + "," + y.as_ref());
    let timeout = Duration::from_secs(15);
    let zk = ZooKeeper::connect(&servers, timeout, watcher)
        .map_err(|e| format!("failed to connect to ZooKeeper: {}", e))?;

    // Create the d3log root key to be able to watch for updates to it.
    let result = zk.create(
        D3LOG_KEY,
        vec![b'\0'],
        Acl::open_unsafe().clone(),
        CreateMode::Persistent,
    );
    match result {
        Ok(_) | Err(ZkError::NodeExists) => (),
        Err(e) => return Err(format!("failed to create {} node: {}", D3LOG_KEY, e)),
    };

    renew_watch(&zk)?;
    Ok(zk)
}

/// Set or renew a watch on the d3log root node.
pub fn renew_watch(zookeeper: &ZooKeeper) -> Result<(), String> {
    let watch = true;
    let _ = zookeeper
        .get_children(D3LOG_KEY, watch)
        .map_err(|e| format!("failed to set watch on {}: {}", D3LOG_KEY, e))?;
    Ok(())
}

/// Connect to a ZooKeeper instance with information from the
/// environment.
#[cfg(test)]
fn from_env() -> Result<ZooKeeper, String> {
    let endpoints = var_os(ENDPOINTS_ENV)
        .ok_or_else(|| format!("environment variable {} not present", ENDPOINTS_ENV))?
        .into_string()
        .map_err(|e| format!("{} does not contain a valid string: {:?}", ENDPOINTS_ENV, e))?;

    fn watcher(_: WatchedEvent) {}

    connect(endpoints.split(','), watcher)
}

/// Read and deserialize some data item from a ZooKeeper instance.
fn read<D>(zk: &ZooKeeper, key: &str) -> Result<D, String>
where
    D: Default + DeserializeOwned,
{
    // We always install a watch here. Clients can always opt to ignore
    // the path.
    let watch = true;

    match zk.get_data(key, watch) {
        Ok((json, _)) => {
            from_slice(&json).map_err(|e| format!("failed to deserialize data from {}: {}", key, e))
        }
        Err(ZkError::NoNode) => Ok(D::default()),
        Err(e) => Err(format!("failed to retrieve data from {}: {}", key, e)),
    }
}

impl ReadMembers for ZooKeeper {
    fn members(&self) -> Result<Members, String> {
        read(self, MEMBERS_KEY)
    }
}

impl ReadConfig for ZooKeeper {
    fn config(&self) -> Result<SysCfg, String> {
        read(self, CONFIG_KEY)
    }
}

#[cfg(test)]
mod tests {
    use serial_test_derive::serial;

    use super::*;

    #[test]
    #[serial(zookeeper)]
    fn no_members() {
        // TODO: This construct will appear everywhere we use zookeeper.
        //       Find a way to deduplicate.
        let zk = match from_env() {
            Ok(zk) => zk,
            Err(_) if var_os("IS_CI_RUN").is_none() => return,
            Err(e) => panic!(e),
        };

        match zk.delete(MEMBERS_KEY, None) {
            Ok(()) | Err(ZkError::NoNode) => (),
            Err(e) => panic!("failed to remove {}: {}", MEMBERS_KEY, e),
        }
        assert_eq!(zk.members().unwrap(), Members::new())
    }
}
