#[cfg(test)]
mod tests {
    use distributed_datalog::{Addr, DDlogServer, Realization, Sink, Source};
    use server_api_ddlog::{DDlogConverter, UpdateSerializer};
    use server_api_ddlog::api::{Inventory, HDDlog};
    use std::collections::{BTreeSet, HashMap};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    #[test]
    fn add_and_then_remove_file_source_from_realization() {
        let mut realization = Realization::<DDlogConverter, UpdateSerializer>::new();

        let file = NamedTempFile::new().unwrap();
        let path = file.path();
        // Check that adding the file succeeds.
        assert_eq!(realization.add_file_source(path), Ok(()));

        let pathbuf = PathBuf::from(path);
        // Check that realization has the file as a source.
        assert!(realization.contains_file_source(pathbuf.clone()));
        let source_id = realization.get_source_id((&path).to_path_buf());
        // Check TxnMux has subscription to source.
        assert!(realization.txn_subscription_exists(source_id));

        // Check that removing the file sink succeeds.
        assert_eq!(
            realization.remove_source(&Source::File(pathbuf.clone())),
            Ok(())
        );
        // Check that the realization no longer contains the file sink.
        assert!(!realization.contains_file_source(pathbuf));
    }

    #[test]
    fn add_and_then_remove_tcp_receiver_from_realization() {
        let mut realization = Realization::<DDlogConverter, UpdateSerializer>::new();

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5000);
        // Check that realization has tcp receiver.
        assert_eq!(realization.add_tcp_receiver(&addr), Ok(()));

        let source_id = realization.get_tcp_receiver_id();
        // Check that TxnMux is subscribed to tcp receiver.
        assert!(realization.txn_subscription_exists(source_id));
        // Check that Realization contains the tcp receiver.
        assert!(realization.contains_tcp_receiver());

        // Check that removing the tcp receiver succeeds.
        assert_eq!(realization.remove_source(&Source::TcpReceiver), Ok(()));
        // Check that the realization no longer contains the tcp receiver.
        assert!(!realization.contains_tcp_receiver());
    }

    #[test]
    fn add_and_remove_tcp_sender_sink() {
        let mut realization = Realization::<DDlogConverter, UpdateSerializer>::new();

        // Set up dummy realization.
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5000);
        let mut rel_ids = BTreeSet::new();
        rel_ids.insert(1);
        rel_ids.insert(2);
        rel_ids.insert(3);

        let mut server = DDlogServer::<HDDlog>::new(None, HashMap::new());
        let sink = Sink::TcpSender(Addr::Ip(addr));

        // Check that realization successfully adds tcp sender sink.
        assert_eq!(
            realization.add_sink(&sink, rel_ids.clone(), &mut server, Arc::new(Inventory)),
            Ok(())
        );
        // Check that realization has an accumulator for the sink and that the
        // subscription exists.
        assert!(realization.accumulator_is_subscribed_to_sink(rel_ids.clone(), &sink));

        // Check that realization successfully removes the tcp sender sink.
        assert_eq!(realization.remove_sink(&sink), Ok(()));
        assert!(!realization.accumulator_is_subscribed_to_sink(rel_ids.clone(), &sink));

        // Check that the sink accumulator can be successfully removed.
        assert_eq!(
            realization.remove_sink_accumulator(rel_ids, &mut server),
            Ok(())
        );
    }

    #[test]
    fn add_and_remove_file_sink() {
        // Set up dummy realization.
        let mut realization = Realization::<DDlogConverter, UpdateSerializer>::new();

        let file = NamedTempFile::new().unwrap();
        let path = file.path();

        let mut rel_ids = BTreeSet::new();
        rel_ids.insert(1);
        rel_ids.insert(2);
        rel_ids.insert(3);

        let pathbuf = PathBuf::from(path);
        let mut server = DDlogServer::<HDDlog>::new(None, HashMap::new());
        let sink = Sink::File(pathbuf);

        // Check that realization successfully adds file sink.
        assert_eq!(
            realization.add_sink(&sink, rel_ids.clone(), &mut server, Arc::new(Inventory)),
            Ok(())
        );
        // Check that realization has an accumulator for the sink and that the
        // subscription exists.
        assert!(realization.accumulator_is_subscribed_to_sink(rel_ids.clone(), &sink));

        // Check that realization successfully removes the file sink.
        assert_eq!(realization.remove_sink(&sink), Ok(()));
        assert!(!realization.accumulator_is_subscribed_to_sink(rel_ids.clone(), &sink));

        // Check that the sink accumulator can be successfully removed.
        assert_eq!(
            realization.remove_sink_accumulator(rel_ids, &mut server),
            Ok(())
        );
    }

    #[test]
    fn remove_sink_accumulator_prematurely() {
        // Should error if the accumulator to be removed still has sinks.

        let mut realization = Realization::<DDlogConverter, UpdateSerializer>::new();

        let file = NamedTempFile::new().unwrap();
        let path = file.path();

        let mut rel_ids = BTreeSet::new();
        rel_ids.insert(1);
        rel_ids.insert(2);
        rel_ids.insert(3);

        let pathbuf = PathBuf::from(path);
        let mut server = DDlogServer::<HDDlog>::new(None, HashMap::new());
        let sink = Sink::File(pathbuf);

        // Check that the sink is added successfully.
        assert_eq!(
            realization.add_sink(&sink, rel_ids.clone(), &mut server, Arc::new(Inventory)),
            Ok(())
        );
        // Check that removing the sink accumulator returns an error.
        assert!(realization
            .remove_sink_accumulator(rel_ids, &mut server)
            .is_err());
    }
}
