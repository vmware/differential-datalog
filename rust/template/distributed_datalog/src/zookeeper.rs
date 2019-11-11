use std::collections::BTreeSet;
use std::env::var_os;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde_json::from_slice;

use zookeeper::WatchedEvent;
use zookeeper::ZkError;
use zookeeper::ZooKeeper;

use crate::members::ReadMembers;
use crate::schema::Member;

const ENDPOINTS_ENV: &str = "ZOOKEEPER_ENDPOINTS";
const MEMBERS_KEY: &str = "/d3log/members";

/// Connect to a ZooKeeper instance comprised of the given set of servers.
///
/// Note that each server address should contain the port the
/// service is listening on.
pub fn connect<I, S>(mut servers: I) -> Result<ZooKeeper, String>
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    fn watcher(_: WatchedEvent) {};

    let first = servers
        .next()
        .ok_or_else(|| "failed to connect to ZooKeeper: no servers supplied")?
        .as_ref()
        .to_string();
    let servers = servers.fold(first, |x, y| x + "," + y.as_ref());
    let timeout = Duration::from_secs(15);
    let zk = ZooKeeper::connect(&servers, timeout, watcher)
        .map_err(|e| format!("failed to connect to ZooKeeper: {}", e))?;

    Ok(zk)
}

/// Connect to a ZooKeeper instance with information from the
/// environment.
fn from_env() -> Result<ZooKeeper, String> {
    let endpoints = var_os(ENDPOINTS_ENV)
        .ok_or_else(|| format!("environment variable {} not present", ENDPOINTS_ENV))?
        .into_string()
        .map_err(|e| format!("{} does not contain a valid string: {:?}", ENDPOINTS_ENV, e))?;

    connect(endpoints.split(','))
}

/// Read and deserialize some data item from a ZooKeeper instance.
fn read<D>(zk: &ZooKeeper, key: &str) -> Result<D, String>
where
    D: Default + DeserializeOwned,
{
    match zk.get_data(MEMBERS_KEY, false) {
        Ok((json, _)) => {
            from_slice(&json).map_err(|e| format!("failed to deserialize data for {}: {}", key, e))
        }
        Err(ZkError::NoNode) => Ok(D::default()),
        Err(e) => Err(format!("failed to retrieve data for {}: {}", key, e)),
    }
}

impl ReadMembers for ZooKeeper {
    fn members(&self) -> Result<BTreeSet<Member>, String> {
        read(self, MEMBERS_KEY)
    }
}

#[cfg(test)]
mod tests {
    use serial_test_derive::serial;

    use zookeeper::Acl;
    use zookeeper::CreateMode;

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
        assert_eq!(zk.members().unwrap(), BTreeSet::new())
    }
}
