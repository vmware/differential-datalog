use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::time::Duration;

use differential_datalog::program::RelId;
use differential_datalog::program::Update;
use differential_datalog::record::Record;
use differential_datalog::DDlog;

use crate::observe::Observable;
use crate::observe::UpdatesObservable;
use crate::schema::Addr;
use crate::schema::Member;
use crate::schema::Node;
use crate::schema::NodeCfg;
use crate::schema::RelCfg;
use crate::schema::SysCfg;
use crate::tcp_channel::TcpReceiver;
use crate::tcp_channel::TcpSender;
use crate::DDlogServer;

type Outputs = BTreeMap<Addr, HashSet<RelId>>;
/// A mapping from abstract nodes to actual members in the system.
type Assignment = BTreeMap<Node, Addr>;

/// Deduce the output streaming relations we require.
///
/// In a nutshell, this function deduces a mapping from all relations on
/// a node to other nodes that have relations that have this relation as
/// input. Unfortunately doing so is rather costly, as we ultimately
/// have to visit pretty much all relations in the assignment and check
/// them.
fn deduce_outputs(
    addr: &Addr,
    node_cfg: &NodeCfg,
    sys_cfg: &SysCfg,
    assignment: &Assignment,
) -> Outputs {
    node_cfg.keys().fold(Outputs::new(), |mut outputs, rel| {
        sys_cfg
            .iter()
            .filter_map(|(uuid, node_cfg)| {
                assignment.get(uuid).and_then(|other_addr| {
                    if other_addr != addr {
                        Some((other_addr, node_cfg))
                    } else {
                        None
                    }
                })
            })
            .for_each(|(addr, node_cfg)| {
                node_cfg.values().for_each(|rel_cfgs| {
                    rel_cfgs.iter().for_each(|rel_cfg| match rel_cfg {
                        RelCfg::Input(inputs) => inputs.iter().for_each(|input| {
                            if input == rel {
                                let rels = outputs.entry(addr.clone()).or_default();
                                let _ = rels.insert(*input);
                            }
                        }),
                        RelCfg::Source(_) | RelCfg::Sink(_) => (),
                    })
                })
            });

        outputs
    })
}

/// Assign nodes to actual members in the system.
///
/// Right now the assignment is the simplest possible where we just take
/// members in "some" order and assign them to nodes until we have
/// covered all.
// TODO: We very likely want to have some more brains in here (such as
//       a consistent hashing algorithm) or we will incur potentially
//       excessive reconfigurations in the system whenever a node is
//       added or removed.
pub fn simple_assign<'n, 'm, N, M>(nodes: N, mut members: M) -> Option<Assignment>
where
    N: Iterator<Item = &'n Node> + ExactSizeIterator,
    M: Iterator<Item = &'m Member> + ExactSizeIterator,
{
    // If the configuration prescribes more nodes than we have members
    // in the system we can't find an assignment.
    // TODO: In theory we could map two or more nodes to a single
    //       member.
    if members.len() < nodes.len() {
        return None;
    }

    let assignment = BTreeMap::new();

    Some(nodes.fold(assignment, |mut assignment, uuid| {
        match members.next() {
            Some(member) => {
                let node = member.addr().clone();
                let _result = assignment.insert(*uuid, node);
                debug_assert_eq!(_result, None);
            }
            None => unreachable!(),
        }
        assignment
    }))
}

/// Deduce the required redirections for a given input/output
/// configuration.
fn deduce_redirects(config: &NodeCfg) -> HashMap<RelId, RelId> {
    config.iter().fold(HashMap::new(), |redirects, (rel, cfg)| {
        cfg.iter().fold(redirects, |mut redirects, node_cfg| {
            match node_cfg {
                RelCfg::Input(inputs) => {
                    // Each of the inputs needs to be redirected to the
                    // relation it feeds into.
                    inputs.iter().for_each(|input| {
                        let _ = redirects.insert(*input, *rel);
                    })
                }
                RelCfg::Source(_) | RelCfg::Sink(_) => (),
            }
            redirects
        })
    })
}

/// Realize the given configuration locally.
// TODO: Right now this function assumes a pristine state (i.e., nothing
//       had been created previously), however we really would want to
//       transition from a previously created state (which happens to be
//       "empty" initially) to the given one.
fn realize<P>(
    addr: &Addr,
    node_cfg: &NodeCfg,
    outputs: Outputs,
) -> Result<Realization<P::Value>, String>
where
    P: DDlog,
{
    let redirects = deduce_redirects(node_cfg);
    let has_inputs = !redirects.is_empty();
    // TODO: Should the number of workers be made configurable?
    let program = P::run(2, false, |_, _: &Record, _| {})?;
    let mut server = DDlogServer::new(program, redirects);

    let receiver = if has_inputs {
        let receiver = TcpReceiver::<Update<P::Value>>::new(addr)
            .map_err(|e| format!("failed to create TcpReceiver: {}", e))?;
        Some(receiver)
    } else {
        None
    };

    // TODO: Instantiate file based sources.

    let mut streams = Vec::with_capacity(outputs.len());
    // Create streams for the deduced output relations.
    for (addr, relations) in outputs.into_iter() {
        let mut stream = server.add_stream(relations);
        let timeout = Duration::from_secs(30);
        let interval = Duration::from_millis(500);
        let sender = TcpSender::with_retry(&addr, timeout, interval)
            .map_err(|e| format!("failed to connect to node {}: {}", addr, e))?;

        // TODO: What should we really do if we can't subscribe?
        stream
            .subscribe(Box::new(sender))
            .map_err(|_| "failed to subscribe TCP sender".to_string())?;

        streams.push(stream)
    }

    Ok(Realization { receiver, streams })
}

/// XXX
#[derive(Debug)]
pub struct Realization<V>
where
    V: Debug + Send,
{
    /// The receiver dispatching incoming updates.
    receiver: Option<TcpReceiver<Update<V>>>,
    /// The stream used by output relations.
    streams: Vec<UpdatesObservable<Update<V>, String>>,
}

/// Instantiate a configuration on a particular node under the given
/// assignment.
pub fn instantiate<P>(
    sys_cfg: SysCfg,
    addr: &Addr,
    assignment: &Assignment,
) -> Result<Vec<Realization<P::Value>>, String>
where
    P: DDlog,
{
    assignment
        .iter()
        .filter_map(|(uuid, assigned_addr)| {
            if assigned_addr == addr {
                sys_cfg.get(uuid)
            } else {
                None
            }
        })
        .try_fold(Vec::new(), |mut accumulator, node_cfg| {
            // The supplied configuration by design does not
            // include information about output streaming
            // relations, because these can be inferred by
            // looking at the input relations of other nodes.
            // Start by doing exactly that such that we have
            // enough information to fully configure a node
            // locally.
            let outputs = deduce_outputs(&addr, node_cfg, &sys_cfg, assignment);
            realize::<P>(addr, node_cfg, outputs).map(|realization| {
                accumulator.push(realization);
                accumulator
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cmp::max;
    use std::cmp::min;
    use std::path::PathBuf;

    use maplit::btreemap;
    use maplit::btreeset;
    use maplit::hashset;

    use uuid::Uuid;

    use crate::schema::Member;
    use crate::schema::Source;

    #[test]
    fn assign_insufficient_members() {
        let uuid0 = Uuid::new_v4();
        let uuid1 = Uuid::new_v4();
        let node0 = Addr::Ip("127.0.0.1:2000".parse().unwrap());

        let config = btreemap! {
            uuid0 => btreemap! { 0 => btreeset! {} },
            uuid1 => btreemap! {
                1 => btreeset! {
                    RelCfg::Source(Source::File(PathBuf::from("input.cmd")))
                }
            },
        };
        let members = btreeset! {
            Member::new(node0),
        };

        let assignment = simple_assign(config.keys(), members.iter());
        assert_eq!(assignment, None);
    }

    #[test]
    fn assign_members() {
        let uuid0 = Uuid::new_v4();
        let uuid1 = Uuid::new_v4();
        let uuid0 = min(uuid0, uuid1);
        let uuid1 = max(uuid0, uuid1);
        let node0 = Addr::Ip("127.0.0.1:2000".parse().unwrap());
        let node1 = Addr::Ip("127.0.0.1:2001".parse().unwrap());

        let config = btreemap! {
            uuid0 => btreemap! {},
            uuid1 => btreemap! {
                1 => btreeset! {
                    RelCfg::Input(btreeset! {0}),
                }
            },
        };
        let members = btreeset! {
            Member::new(node0.clone()),
            Member::new(node1.clone()),
        };

        let assignment = simple_assign(config.keys(), members.iter()).unwrap();
        assert_eq!(
            assignment,
            btreemap! {
                uuid0 => node0,
                uuid1 => node1,
            }
        );
    }

    #[test]
    fn output_deduction_simple() {
        let uuid0 = Uuid::new_v4();
        let uuid1 = Uuid::new_v4();
        let node0 = Addr::Ip("127.0.0.1:2000".parse().unwrap());
        let node1 = Addr::Ip("127.0.0.1:2001".parse().unwrap());

        let node0_cfg = btreemap! {
            0 => btreeset! {
                RelCfg::Source(Source::File(PathBuf::from("input.cmd"))),
            },
        };
        let node1_cfg = btreemap! {
            1 => btreeset! {
                RelCfg::Input(btreeset! {0}),
            },
        };
        let config = btreemap! {
            uuid0 => node0_cfg.clone(),
            uuid1 => node1_cfg.clone(),
        };
        let assignment = btreemap! {
            uuid0 => node0.clone(),
            uuid1 => node1.clone(),
        };

        let outputs = deduce_outputs(&node0, &node0_cfg, &config, &assignment);
        let expected = btreemap! {
            node1.clone() => hashset! { 0 },
        };
        assert_eq!(outputs, expected);

        let outputs = deduce_outputs(&node1, &node1_cfg, &config, &assignment);
        assert_eq!(outputs, Outputs::new());
    }
}
