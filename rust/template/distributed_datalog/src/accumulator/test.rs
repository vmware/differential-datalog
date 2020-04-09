use log::trace;

use std::fmt::Debug;

use crate::Observer;

/// A variant of the `MockObserver` that also keeps track of the updates it received.
#[derive(Debug, Default)]
pub struct UpdatesMockObserver<T>
    where
        T: Debug,
{
    /// The number of `on_start` calls the observer has seen.
    pub called_on_start: usize,
    /// The number of `on_commit` calls the observer has seen.
    pub called_on_commit: usize,
    /// The number of updates the observer has received.
    pub called_on_updates: usize,
    /// The number of `on_completed` calls the observer has seen.
    pub called_on_completed: usize,
    /// The updates the observer has seen.
    pub received_updates: Vec<T>,
}

impl<T> UpdatesMockObserver<T>
    where
        T: Debug,
{
    /// Create a new `MockObserver`.
    pub fn new() -> Self {
        Self {
            called_on_start: 0,
            called_on_commit: 0,
            called_on_updates: 0,
            called_on_completed: 0,
            received_updates: vec!(),
        }
    }
}

impl<T, E> Observer<T, E> for UpdatesMockObserver<T>
    where
        T: Debug + Send,
        E: Send,
{
    fn on_start(&mut self) -> Result<(), E> {
        trace!("MockObserver::on_start");
        self.called_on_start += 1;
        Ok(())
    }

    fn on_commit(&mut self) -> Result<(), E> {
        trace!("MockObserver::on_commit");
        self.called_on_commit += 1;
        Ok(())
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item=T> + 'a>) -> Result<(), E> {
        trace!("MockObserver::on_updates");
        let mut updates = updates.collect::<Vec<_>>();
        self.called_on_updates += updates.len();
        self.received_updates.append(&mut updates);
        Ok(())
    }

    fn on_completed(&mut self) -> Result<(), E> {
        trace!("MockObserver::on_completed");
        self.called_on_completed += 1;
        Ok(())
    }
}
