use crate::{program::RelId, record::Mutator};
use std::{
    fmt::{self, Debug},
    sync::Arc,
};

/// A data type to represent insert and delete commands. A unified type lets us
/// combine many updates in one message.
///
/// `DeleteValue` takes a complete value to be deleted;
/// `DeleteKey` takes key only and is only defined for relations with 'key_func';
/// `Modify` takes a key and a `Mutator` trait object that represents an update
/// to be applied to the given key.
// TODO: Rename `v`, `k` and `m` to meaningful names, `v` -> `value`, `k` -> `key`
//       and `m` -> `mutator`
#[derive(Clone)]
pub enum Update<V> {
    Insert {
        relid: RelId,
        v: V,
    },
    InsertOrUpdate {
        relid: RelId,
        v: V,
    },
    DeleteValue {
        relid: RelId,
        v: V,
    },
    DeleteKey {
        relid: RelId,
        k: V,
    },
    Modify {
        relid: RelId,
        k: V,
        m: Arc<dyn Mutator<V> + Send + Sync>,
    },
}

/// Creates functions to check the type of an update
macro_rules! is_update_kind {
    ($($function:ident -> $update_kind:ident),* $(,)?) => {
        $(
            is_update_kind!(@doc
                #[doc = concat!("Returns `true` if the the current update is an `", stringify!($update_kind), "`")]
                pub fn $function(&self) -> bool {
                    matches!(self, Update::$update_kind { .. })
                }
            );
        )*
    };

    // Little bit of a hack around rust's macro system, putting a `concat!(..)` directly
    // within a macro doesn't work, but coercing the concatenated string into a literal
    // before putting it inside the doc attribute does
    (@doc #[doc = $doc:expr] $item:item) => {
        #[doc = $doc]
        $item
    };
}

impl<V> Update<V> {
    /// Get the relationship id of the current update
    pub fn relid(&self) -> RelId {
        match self {
            Update::Insert { relid, .. } => *relid,
            Update::InsertOrUpdate { relid, .. } => *relid,
            Update::DeleteValue { relid, .. } => *relid,
            Update::DeleteKey { relid, .. } => *relid,
            Update::Modify { relid, .. } => *relid,
        }
    }

    is_update_kind! {
        is_insert           -> Insert,
        is_insert_or_update -> InsertOrUpdate,
        is_delete_value     -> DeleteValue,
        is_delete_key       -> DeleteKey,
        is_modify           -> Modify,
    }

    /// Returns whether the current update has a key of some sort
    ///
    /// Returns `true` if the update is a `DeleteKey` or a `Modify`
    pub fn has_key(&self) -> bool {
        matches!(self, Update::DeleteKey { .. } | Update::Modify { .. })
    }

    /// Attempts to get the key of the current update
    ///
    /// Returns `Some` if the update is a `DeleteKey` or a `Modify` and `None` otherwise
    pub fn get_key(&self) -> Option<&V> {
        match self {
            Update::DeleteKey { k, .. } => Some(k),
            Update::Modify { k, .. } => Some(k),

            _ => None,
        }
    }

    /// Attempts to get the key of the current update
    ///
    /// # Panics
    ///
    /// Panics if the update is not a `DeleteKey` or a `Modify`, use `Update::get_key()`
    /// for a fallible version
    pub fn key(&self) -> &V {
        match self {
            Update::DeleteKey { k, .. } => k,
            Update::Modify { k, .. } => k,

            _ => panic!("Update::key: not a DeleteKey command"),
        }
    }
}

// Manual implementation of `Debug` for `Update` because the latter
// contains a member that is not auto-derivable.
impl<V> Debug for Update<V>
where
    V: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Update::Insert { relid, v } => f
                .debug_struct("Insert")
                .field("relid", relid)
                .field("v", v)
                .finish(),

            Update::InsertOrUpdate { relid, v } => f
                .debug_struct("InsertOrUpdate")
                .field("relid", relid)
                .field("v", v)
                .finish(),

            Update::DeleteValue { relid, v } => f
                .debug_struct("DeleteValue")
                .field("relid", relid)
                .field("v", v)
                .finish(),

            Update::DeleteKey { relid, k } => f
                .debug_struct("DeleteKey")
                .field("relid", relid)
                .field("k", k)
                .finish(),

            Update::Modify { relid, k, m } => f
                .debug_struct("Modify")
                .field("relid", relid)
                .field("k", k)
                .field("m", &m.to_string())
                .finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Record;

    #[test]
    fn get_update_relid() {
        let relid = 0xF00DBEEF;
        let (v, k): (u32, u32) = (0, 0);

        let updates = &[
            Update::Insert { relid, v },
            Update::InsertOrUpdate { relid, v },
            Update::DeleteValue { relid, v },
            Update::DeleteKey { relid, k },
            Update::Modify {
                relid,
                k,
                m: Arc::new(Record::Bool(false)),
            },
        ];

        for update in updates {
            assert_eq!(update.relid(), relid);
        }
    }

    #[test]
    fn is_update_kind() {
        let relid = 0;
        let (v, k): (u32, u32) = (0, 0);

        let insert = Update::Insert { relid, v };
        assert!(insert.is_insert());

        let insert_or_update = Update::InsertOrUpdate { relid, v };
        assert!(insert_or_update.is_insert_or_update());

        let delete_value = Update::DeleteValue { relid, v };
        assert!(delete_value.is_delete_value());

        let delete_key = Update::DeleteKey { relid, k };
        assert!(delete_key.is_delete_key());

        let modify = Update::Modify {
            relid,
            k,
            m: Arc::new(Record::Bool(false)),
        };
        assert!(modify.is_modify());
    }

    #[test]
    fn update_has_key() {
        let relid = 0;
        let (v, k): (u32, u32) = (0, 0);

        let no_keys = &[
            Update::Insert { relid, v },
            Update::InsertOrUpdate { relid, v },
            Update::DeleteValue { relid, v },
        ];

        for update in no_keys {
            assert!(!update.has_key());
            assert!(update.get_key().is_none());
        }

        let have_keys = &[
            Update::DeleteKey { relid, k },
            Update::Modify {
                relid,
                k,
                m: Arc::new(Record::Bool(false)),
            },
        ];

        for update in have_keys {
            assert!(update.has_key());
            assert!(update.get_key().is_some());
            // Making sure they don't panic
            update.key();
        }
    }
}
