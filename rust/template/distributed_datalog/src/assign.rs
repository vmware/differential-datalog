use std::collections::BTreeMap;

use crate::instantiate::Assignment;
use crate::schema::Member;
use crate::schema::Node;

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
                let node = *member.addr();
                let _result = assignment.insert(*uuid, node);
                debug_assert_eq!(_result, None);
            }
            None => unreachable!(),
        }
        assignment
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    use maplit::btreemap;
    use maplit::btreeset;

    use uuid::Uuid;

    use crate::schema::Addr;
    use crate::schema::Member;
    use crate::schema::RelCfg;
    use crate::schema::Source;

    #[test]
    fn assign_insufficient_members() {
        let uuid0 = Uuid::new_v4();
        let uuid1 = Uuid::new_v4();
        let node0 = Addr::Ip("127.0.0.1:1".parse().unwrap());

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
        let mut uuids = vec![Uuid::new_v4(), Uuid::new_v4()];
        uuids.sort();
        let node0 = Addr::Ip("127.0.0.1:1".parse().unwrap());
        let node1 = Addr::Ip("127.0.0.1:2".parse().unwrap());

        let config = btreemap! {
            uuids[0] => btreemap! {},
            uuids[1] => btreemap! {
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
        let expected = btreemap! {
            uuids[0] => node0,
            uuids[1] => node1,
        };
        assert_eq!(assignment, expected);
    }
}
