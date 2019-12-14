//! A module containing traits that abstract away the retrieval of
//! a configuration.

use crate::schema::Members;
use crate::schema::SysCfg;

/// A client to some form of membership service.
///
/// Right now only read-only access is supported.
pub trait ReadMembers {
    /// Retrieve the current set of members.
    fn members(&self) -> Result<Members, String>;
}

/// A client to some form of configuration manager.
///
/// Right now only read-only access is supported.
pub trait ReadConfig {
    /// Retrieve the current system configuration.
    fn config(&self) -> Result<SysCfg, String>;
}
