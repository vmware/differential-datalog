use channel::*;
use differential_datalog::program::RelId;
use differential_datalog::record::Record;
use api::*;

pub struct DDlogServer
{
    prog: HDDlog,
    observer: Box<dyn Observer<(RelId, Record, bool), String>>
}

impl Observable<(RelId, Record, bool), String> for DDlogServer
{
    fn subscribe(&mut self, observer: Box<dyn Observer<(RelId, Record, bool), String>>) {
        self.observer = observer;
    }
}
