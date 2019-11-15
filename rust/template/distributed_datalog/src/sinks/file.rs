use std::fmt::Debug;
use std::fs::File as FsFile;
use std::fs::OpenOptions;
use std::io::Error;
use std::io::Write;
use std::path::Path;

use differential_datalog::program::Update;
use differential_datalog::RecordReplay;

use crate::Observer;

/// An object implementing the `Observer` interface and dumping
/// transaction traces into a file.
#[derive(Debug)]
pub struct File {
    /// The file we dump our transaction traces into.
    file: FsFile,
}

impl File {
    /// Create a new file based observer using the given file.
    pub fn new(file: FsFile) -> Self {
        Self { file }
    }

    /// Create a new file based observer using a file at the given path
    /// that will be opened and appended to (if it exists) or newly
    /// created.
    pub fn open<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        Ok(Self {
            file: OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(path)?,
        })
    }
}

impl<P> Observer<Update<P::Value>, String> for File
where
    P: DDlog,
{
    fn on_start(&mut self) -> Result<(), String> {
        self.file
            .record_start::<P::Convert, _>()
            .map_err(|e| format!("failed to record 'on_start' event: {}", e))
    }

    fn on_commit(&mut self) -> Result<(), String> {
        self.file
            .record_commit::<P::Convert, _>()
            .map_err(|e| format!("failed to record 'on_commit' event: {}", e))
    }

    fn on_updates<'a>(
        &mut self,
        updates: Box<dyn Iterator<Item = Update<V>> + 'a>,
    ) -> Result<(), String> {
        unimplemented!()
    }

    fn on_completed(&mut self) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::NamedTempFile;

    fn dump_events() {
        let file = NamedTempFile::new().unwrap();
    }
}
