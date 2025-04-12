// SPDX-License-Identifier: Apache-2.0

//! AORA: Append-only random-accessed data persistence.

mod providers;

use core::borrow::Borrow;

pub use providers::*;

/// Trait for providers of append-only random-access log functionality.
pub trait Aora<K: Into<[u8; LEN]> + From<[u8; LEN]>, V, const LEN: usize = 32> {
    /// Adds item to the append-only log. If the item is already in the log, does noting.
    ///
    /// # Panic
    ///
    /// Panics if item under the given id is different from another item under the same id already
    /// present in the log
    fn append(&mut self, key: K, item: impl Borrow<V>);
    fn extend(&mut self, iter: impl IntoIterator<Item = (K, impl Borrow<V>)>) {
        for (key, item) in iter {
            self.append(key, item.borrow());
        }
    }
    fn has(&self, key: &K) -> bool;
    fn read(&mut self, key: &K) -> V;
    fn iter(&mut self) -> impl Iterator<Item = (K, V)>;
}
