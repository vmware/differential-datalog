//! Various utilities

use std::{
    collections::{HashMap, HashSet},
    hash::BuildHasherDefault,
};
use xxhash_rust::xxh3::Xxh3;

/// The default [`Xxh3`] hasher
pub type Xxh3Hasher = BuildHasherDefault<Xxh3>;

/// A [`HashMap`] backed by the [`Xxh3`] hashing algorithm
pub type XxHashMap<K, V> = HashMap<K, V, Xxh3Hasher>;

/// A [`HashSet`] backed by the [`Xxh3`] hashing algorithm
pub type XxHashSet<K> = HashSet<K, Xxh3Hasher>;
