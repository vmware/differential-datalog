use crate::schema::Members;

/// A client to some form of membership service.
///
/// Right now only read-only access is supported.
pub trait ReadMembers {
    /// Retrieve the current set of members.
    fn members(&self) -> Result<Members, String>;
}
