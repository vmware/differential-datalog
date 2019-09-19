#[cfg(test)]
mod tests {
    use differential_datalog::record::{Record, RelIdentifier, UpdCmd};
    use observe::{Observable, Observer, SharedObserver};

    use server_api_ddlog::api::*;
    use server_api_ddlog::server;
    use server_api_ddlog::Relations::*;

    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    #[test]
    fn subscribe() -> Result<(), String> {
        // Construct left server with no redirect
        let prog1 = HDDlog::run(1, false, |_, _: &Record, _| {});
        let mut s1 = server::DDlogServer::new(prog1, HashMap::new());

        // Construct right server, redirect Up table
        let prog2 = HDDlog::run(1, false, |_, _: &Record, _| {});
        let mut redirect2 = HashMap::new();
        redirect2.insert(P1Out, P2In);
        let s2 = server::DDlogServer::new(prog2, redirect2);

        // Stream Up table from left server
        let mut tables = HashSet::new();
        tables.insert(P1In);
        let mut stream = s1.add_stream(tables);

        // Right server subscribes to the stream
        let s2 = Arc::new(Mutex::new(s2));
        let sub = {
            let s2_a = SharedObserver(s2.clone());
            stream.subscribe(Box::new(s2_a))
        };

        // Insert `true` to Left in left server
        let updates = &[UpdCmd::Insert(
            RelIdentifier::RelId(P1In as usize),
            Record::String("test".to_string()),
        )];

        // Execute and transmit the update
        s1.on_start()?;
        s1.on_updates(Box::new(
            updates.into_iter().map(|cmd| updcmd2upd(cmd).unwrap()),
        ))?;
        s1.on_commit()?;
        s1.on_completed()?;

        // Test `unsubscribe`
        sub.unsubscribe();

        let updates2 = &[UpdCmd::Delete(
            RelIdentifier::RelId(P1In as usize),
            Record::String("test".to_string()),
        )];

        s1.on_start()?;
        s1.on_updates(Box::new(
            updates2.into_iter().map(|cmd| updcmd2upd(cmd).unwrap()),
        ))?;
        s1.on_commit()?;
        s1.on_completed()?;

        // Shutdown and clean up resources
        s1.remove_stream(stream);
        s1.shutdown()?;
        s2.lock().unwrap().shutdown()?;
        Ok(())
    }
}
