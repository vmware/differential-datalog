use differential_datalog::record::Record;

// TODO decide on using usize / RelID / etc.
// TODO trait constraint on T
// TODO use fn or closure as the arguments?

// A channel to communicate DDlog deltas.
// The channel is identified by an ID of type T.
// For example, the ID can be a socket address.
pub trait Channel<T> {

    // Create a channel with an ID
    fn new(T);

    // Subscribe to a subset of the table updates to
    // transmit.
    fn subscribe(fn(usize)->bool);

    // Transmit an update through the channel. The update
    // is represented by a table ID (usize), a record,
    // and a polarity (bool). Only the updates from
    // subscribed tables are transmitted.
    fn transmit(usize, &Record, bool);

    // Emit an update to a callback. The update
    // is represented by a table ID (usize), a record,
    // and a polarity (bool)
    fn emit(fn(usize, &Record, bool));
}
