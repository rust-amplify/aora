// SPDX-License-Identifier: Apache-2.0

//! AORA: Append-only random-accessed data persistence.

mod providers;

use core::borrow::Borrow;

pub use providers::*;

/// Trait for providers of append-only random-access log functionality.
pub trait Aora<K, V, const LEN: usize = 32>
where K: Into<[u8; LEN]> + From<[u8; LEN]>
{
    /// Inserts item to the append-only log. If the item is already in the log, does noting.
    ///
    /// # Panic
    ///
    /// Panics if item under the given id is different from another item under the same id already
    /// present in the log
    fn insert(&mut self, key: K, item: impl Borrow<V>);

    /// Appends items from an iterator.
    ///
    /// # Panic
    ///
    /// Panics if item under the given id is different from another item under the same id already
    /// present in the log
    fn extend(&mut self, iter: impl IntoIterator<Item = (K, impl Borrow<V>)>) {
        for (key, item) in iter {
            self.insert(key, item.borrow());
        }
    }

    /// Checks whether given value is present in the log.
    fn contains_key(&self, key: &K) -> bool;

    /// Retrieves value from the log.
    ///
    /// # Panics
    ///
    /// Panics if the item under the provided key is not present.
    fn get(&self, key: &K) -> Option<V>;

    /// Retrieves value from the log.
    ///
    /// # Panics
    ///
    /// Panics if the item under the provided key is not present.
    fn get_expect(&self, key: &K) -> V;

    /// Returns an iterator over the key and value pairs.
    fn iter(&self) -> impl Iterator<Item = (K, V)>;
}
