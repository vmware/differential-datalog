//! Schema definitions for d3log. Most of this data is to be stored in
//! serialized form on some entity accessible from all nodes in the
//! system.
//!
//! As a general note: we use BTree based maps & sets here over hashing
//! based ones for two reasons:
//! - they guarantee a stable order of elements which makes our
//!   resulting computation predictable (node assignment based on data
//!   in a hash map could yield different results on different nodes due
//!   to randomness induced into the hashing part on purpose)
//! - BTree based data structures can be nested nicely because `Ord` and
//!   `PartialOrd` are defined for `BTreeMap` & `BTreeSet`, whereas hash
//!   based ones need custom `Hash` definitions
//!
//! We have the following components:
//! - Membership:
//!   Nodes participating in the distributed computation are represented
//!   by the `Member` struct.
//! - Configuration:
//!   From top to bottom we have a configuration describing the desired
//!   state of the system, `SysCfg`. This state is essentially a
//!   description of the required compute node configurations.
//!   Each compute node configuration (`NodeCfg`) is comprised of a map
//!   from relation IDs (the relations "belonging" to this node) to a
//!   set of relation configurations, `RelCfg`. It's a set because a
//!   relation can have multiple purposes. A relation configuration in
//!   turn describes one property of the relation: it could have a set
//!   of input relations and/or be fed directly from a source.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;
use std::io::Error;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::option::IntoIter;
use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;

use uuid::Uuid;

use differential_datalog::program::RelId;

/// An abstract representation of a node.
///
/// Nodes will eventually have to be assigned to members in order for a
/// computation to happen.
pub type Node = Uuid;

/// An address of an actual member in the system.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum Addr {
    /// A (ip-addr:port) pair describing how a member can be reached.
    Ip(SocketAddr),
}

impl Display for Addr {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Addr::Ip(addr) => addr.fmt(f),
        }
    }
}

impl Ord for Addr {
    fn cmp(&self, other: &Addr) -> Ordering {
        match self {
            Addr::Ip(addr) => match other {
                Addr::Ip(other_addr) => addr.to_string().cmp(&other_addr.to_string()),
            },
        }
    }
}

impl PartialOrd for Addr {
    fn partial_cmp(&self, other: &Addr) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl ToSocketAddrs for Addr {
    type Iter = IntoIter<SocketAddr>;

    fn to_socket_addrs(&self) -> Result<Self::Iter, Error> {
        match self {
            Addr::Ip(addr) => addr.to_socket_addrs(),
        }
    }
}

/// A struct representing an individual member participating in the
/// distributed system.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct Member {
    /// The address of the member.
    addr: Addr,
    // For now a member is nothing more but the address. In the future
    // we may want to add additional information describing it that
    // could help decide where to place a certain computation.
}

impl Member {
    /// Create a new member represented by the given node.
    pub fn new(addr: Addr) -> Self {
        Self { addr }
    }

    /// Retrieve the member's node.
    pub fn addr(&self) -> &Addr {
        &self.addr
    }
}

/// A type for representing a set of members.
pub type Members = BTreeSet<Member>;

/// A set of relations used in various contexts.
///
/// # Important:
///
/// We currently assume globally unique relation IDs, which also means
/// we require the exact same program to be present on all nodes in the
/// system. This is a simplifying assumption made for the time being.
/// Ideally, a relation ID really would be a `(Node, RelId)` tuple.
pub type Relations = BTreeSet<RelId>;

/// All the input sources we support.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub enum Source {
    /// Input is coming from a file.
    File(PathBuf),
}

/// All the output sinks we support.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub enum Sink {
    /// Output is emitted into a file.
    File(PathBuf),
}

/// A description of inputs and outputs of a relation.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub enum RelCfg {
    /// A relation that is directly fed from a source.
    Source(Source),
    /// This relation is fed from the given set of relations.
    Input(Relations),
    /// A relation that outputs directly into a sink.
    Sink(Sink),
}

/// Configuration of input/output relationships for a single node.
pub type NodeCfg = BTreeMap<RelId, BTreeSet<RelCfg>>;

/// The configuration of the system is comprised of input/output
/// configurations for a certain set of nodes.
///
/// Each value in the map represents the configuration of a single node.
/// In order to reason about nodes we assign a UUID to each.
pub type SysCfg = BTreeMap<Node, NodeCfg>;

#[cfg(test)]
mod tests {
    use super::*;

    use maplit::btreemap;
    use maplit::btreeset;
    use serde_json::to_string as to_json;

    #[test]
    fn compare_addrs() {
        let addr0 = Addr::Ip("127.0.0.1:1".parse().unwrap());
        let addr1 = Addr::Ip("127.0.0.1:2".parse().unwrap());

        assert!(addr0 < addr1);
    }

    #[test]
    fn serialize_relations() {
        let relations = btreeset! {1, 2, 3};
        let serialized = to_json(&relations).unwrap();
        let expected = r#"[1,2,3]"#;

        assert_eq!(serialized, expected);
    }

    #[test]
    fn serialize_config() {
        let uuid0 = Uuid::new_v4();
        let uuid1 = Uuid::new_v4();
        let (uuid0, uuid1) = if uuid0 <= uuid1 {
            (uuid0, uuid1)
        } else {
            (uuid1, uuid0)
        };

        let config = btreemap! {
            uuid0 => btreemap! {
                2usize => btreeset!{ RelCfg::Source(Source::File(PathBuf::from("input.cmd"))) },
            },
            uuid1 => btreemap! {
                3usize => btreeset!{ RelCfg::Input(btreeset! {2}) },
            },
        } as SysCfg;

        let serialized = to_json(&config).unwrap();
        let expected = r#"{""#.to_string()
            + &uuid0.to_hyphenated_ref().to_string()
            + r#"":{"2":[{"Source":{"File":"input.cmd"}}]},""#
            + &uuid1.to_hyphenated_ref().to_string()
            + r#"":{"3":[{"Input":[2]}]}}"#;

        assert_eq!(serialized, expected);
    }
}
