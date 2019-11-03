use crate::Observer;

/// A dummy observer merely counting method invocations.
#[derive(Copy, Clone, Debug, Default)]
pub struct MockObserver {
    /// The number of `on_start` calls the observer has seen.
    pub called_on_start: usize,
    /// The number of `on_commit` calls the observer has seen.
    pub called_on_commit: usize,
    /// The number of updates the observer has received.
    pub called_on_updates: usize,
    /// The number of `on_completed` calls the observer has seen.
    pub called_on_completed: usize,
}

impl MockObserver {
    /// Create a new `MockObserver`.
    pub fn new() -> Self {
        Self {
            called_on_start: 0,
            called_on_commit: 0,
            called_on_updates: 0,
            called_on_completed: 0,
        }
    }
}

impl<T, E> Observer<T, E> for MockObserver
where
    T: Send,
    E: Send,
{
    fn on_start(&mut self) -> Result<(), E> {
        self.called_on_start += 1;
        Ok(())
    }

    fn on_commit(&mut self) -> Result<(), E> {
        self.called_on_commit += 1;
        Ok(())
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Result<(), E> {
        self.called_on_updates += updates.count();
        Ok(())
    }

    fn on_completed(&mut self) -> Result<(), E> {
        self.called_on_completed += 1;
        Ok(())
    }
}
