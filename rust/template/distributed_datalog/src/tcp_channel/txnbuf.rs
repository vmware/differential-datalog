//! A module providing functionality for buffering of transactions. Such
//! buffering comes in handy in scenarios where the future receiver of a
//! transaction is not yet available to process it.

use std::collections::LinkedList;
use std::fmt::Debug;
use std::io::Write;
use std::mem::replace;

use bincode::serialize_into;
use serde::Serialize;

use crate::observe::Observer;
use crate::tcp_channel::message::Message;

/// A type representing the updates of a transaction.
type Transaction<T> = LinkedList<Vec<T>>;

/// A buffer for transactions.
#[derive(Debug)]
pub enum TxnBuf<W, T>
where
    W: Debug,
    T: Debug,
{
    /// A list of buffered transactions.
    Updates {
        /// We merge all "completed" transactions (i.e., those for which
        /// we have seen a commit) into a single one.
        complete: Transaction<T>,
        /// The transaction currently in progress.
        ongoing: Option<Transaction<T>>,
        /// We have received an `on_completed` event.
        on_completed: bool,
    },
    /// A writer is present and we no longer need to buffer
    /// transactions.
    Writer(W),
}

impl<W, T> TxnBuf<W, T>
where
    W: Debug + Send + Write,
    T: Debug + Send + Serialize,
{
    /// Convert the `TxnBuf` into the `Writer` variant.
    ///
    /// An error return indicates a failure to flush all buffered
    /// transactions. The objects is in an undefined state afterwards.
    pub fn set_mode_passthrough(&mut self, mut writer: W) -> Result<(), String> {
        match self {
            TxnBuf::Updates {
                complete,
                ongoing,
                on_completed,
            } => {
                Self::handle_txn(&mut writer, replace(complete, LinkedList::new()))?;
                Self::handle_partial_txn(&mut writer, ongoing.take())?;
                if *on_completed {
                    Self::handle_msg(&mut writer, &Message::<T>::Complete)?;
                }
                writer.flush().map_err(|e| e.to_string())?;
                *self = TxnBuf::Writer(writer);
                Ok(())
            }
            TxnBuf::Writer(..) => panic!("TxnBuf is already a Writer variant"),
        }
    }

    /// Send a full transaction.
    fn handle_txn(writer: &mut W, txn: Transaction<T>) -> Result<(), String> {
        if !txn.is_empty() {
            Self::handle_msg(writer, &Message::<T>::Start)?;
            Self::handle_msg(writer, &Message::UpdateList(txn))?;
            Self::handle_msg(writer, &Message::<T>::Commit)?;
        }
        Ok(())
    }

    /// Send a partial transaction.
    fn handle_partial_txn(writer: &mut W, txn: Option<Transaction<T>>) -> Result<(), String> {
        if let Some(updates) = txn {
            // If there is a partial transaction that means that we
            // received a transaction start and potentially updates, but
            // no commit yet.
            Self::handle_msg(writer, &Message::<T>::Start)?;
            if !updates.is_empty() {
                Self::handle_msg(writer, &Message::UpdateList(updates))?;
            }
        }
        Ok(())
    }

    /// Send a single message.
    fn handle_msg(writer: &mut W, msg: &Message<T>) -> Result<(), String> {
        serialize_into(writer, msg).map_err(|e| e.to_string())
    }
}

impl<W, T> Default for TxnBuf<W, T>
where
    W: Debug,
    T: Debug,
{
    fn default() -> Self {
        TxnBuf::Updates {
            complete: LinkedList::default(),
            ongoing: None,
            on_completed: false,
        }
    }
}

impl<W, T> Observer<T, String> for TxnBuf<W, T>
where
    W: Debug + Send + Write,
    T: Debug + Send + Serialize,
{
    /// Perform some action before data starts coming in.
    fn on_start(&mut self) -> Result<(), String> {
        match self {
            TxnBuf::Updates { ongoing, .. } => {
                if ongoing.is_none() {
                    *ongoing = Some(LinkedList::new());
                } else {
                    panic!("received multiple on_start events")
                }
            }
            TxnBuf::Writer(writer) => Self::handle_msg(writer, &Message::<T>::Start)?,
        }
        Ok(())
    }

    /// Send a series of items over the TCP channel.
    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Result<(), String> {
        match self {
            TxnBuf::Updates { ongoing, .. } => {
                if let Some(transaction) = ongoing {
                    transaction.push_back(updates.collect())
                } else {
                    panic!("on_updates was not preceded by an on_start event")
                }
            }
            TxnBuf::Writer(writer) => {
                Self::handle_msg(writer, &Message::Updates(updates.collect()))?
            }
        }
        Ok(())
    }

    /// Flush the TCP stream and signal the commit.
    fn on_commit(&mut self) -> Result<(), String> {
        match self {
            TxnBuf::Updates {
                complete, ongoing, ..
            } => {
                if let Some(transaction) = ongoing {
                    complete.append(transaction);
                    *ongoing = None
                } else {
                    panic!("on_commit was not preceded by an on_start event")
                }
            }
            TxnBuf::Writer(writer) => {
                Self::handle_msg(writer, &Message::<T>::Commit)?;
                writer.flush().map_err(|e| e.to_string())?
            }
        }
        Ok(())
    }

    fn on_completed(&mut self) -> Result<(), String> {
        match self {
            TxnBuf::Updates { on_completed, .. } => *on_completed = true,
            TxnBuf::Writer(writer) => {
                Self::handle_msg(writer, &Message::<T>::Complete)?;
                writer.flush().map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bincode::deserialize_from;

    /// Test caching of transactions in a `TxnBuf`.
    #[test]
    fn transaction_caching() {
        fn test<F>(expected: Vec<Message<u64>>, f: F)
        where
            F: FnOnce(&mut TxnBuf<Vec<u8>, u64>) -> Result<(), String>,
        {
            let mut buffer = TxnBuf::default();
            f(&mut buffer).unwrap();
            buffer.set_mode_passthrough(Vec::new()).unwrap();

            match buffer {
                TxnBuf::Writer(buf) => {
                    let mut slice = buf.as_slice();
                    for expected in expected {
                        let msg = deserialize_from::<_, Message<u64>>(&mut slice).unwrap();
                        assert_eq!(msg, expected);
                    }

                    // Make sure we did not have any additional messages
                    // in the reader.
                    let result = deserialize_from::<_, Message<u64>>(&mut slice);
                    assert!(result.is_err(), result)
                }
                TxnBuf::Updates { .. } => unreachable!(),
            }
        }

        test(vec![Message::Start], |buffer| buffer.on_start());

        test(vec![], |buffer| {
            buffer.on_start()?;
            buffer.on_commit()?;
            Ok(())
        });

        let updates = vec![vec![1, 2], vec![3], vec![4, 5, 6]]
            .into_iter()
            .collect();
        let expected = vec![
            Message::Start,
            Message::UpdateList(updates),
            Message::Commit,
            Message::Start,
        ];
        test(expected, |buffer| {
            buffer.on_start()?;
            buffer.on_updates(Box::new(vec![1, 2].into_iter()))?;
            buffer.on_updates(Box::new(vec![3].into_iter()))?;
            buffer.on_commit()?;

            buffer.on_start()?;
            buffer.on_updates(Box::new(vec![4, 5, 6].into_iter()))?;
            buffer.on_commit()?;

            buffer.on_start()?;
            Ok(())
        });
    }
}
