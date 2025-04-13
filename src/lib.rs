// SPDX-License-Identifier: Apache-2.0

//! AORA: Append-only random-accessed data persistence, made in `BTreeMap`-like fashion.

mod providers;

use core::borrow::Borrow;

pub use providers::*;

/// Trait for providers of append-only key-value maps.
pub trait AoraMap<K, V, const KEY_LEN: usize = 32>
where
    K: Ord + Into<[u8; KEY_LEN]> + From<[u8; KEY_LEN]>,
    V: Eq,
{
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

    /// Inserts (appends) an item to the append-only log. If the item is already in the log, does
    /// noting.
    ///
    /// # Panic
    ///
    /// Panics if item under the given id is different from another item under the same id already
    /// present in the log.
    fn insert(&mut self, key: K, item: impl Borrow<V>);

    /// Inserts (appends) all items from an iterator to the append-only log.
    ///
    /// # Panic
    ///
    /// Panics if any of the items is different from an item under the same id already present in
    /// the log.
    fn extend(&mut self, iter: impl IntoIterator<Item = (K, impl Borrow<V>)>) {
        for (key, item) in iter {
            self.insert(key, item.borrow());
        }
    }

    /// Returns an iterator over the key and value pairs.
    fn iter(&self) -> impl Iterator<Item = (K, V)>;
}

/// Append-only key to vectorized values map.
pub trait AoraVecMap<K, V, const KEY_LEN: usize = 32>
where K: Ord + Into<[u8; KEY_LEN]> + From<[u8; KEY_LEN]>
{
    /// Returns iterator over all known keys.
    fn keys(&self) -> impl Iterator<Item = K>;

    /// Checks whether given value is present in the log.
    fn contains_key(&self, key: &K) -> bool { self.value_len(key) > 0 }

    /// Measures length of the value vector for the given key.
    fn value_len(&self, key: &K) -> usize;

    /// Retrieves value vector from the log. If the key is not present, returns an empty iterator.
    fn get(&self, key: K) -> impl ExactSizeIterator<Item = V>;

    /// Pushes a new value into the value array for the given key.
    fn push(&mut self, key: K, val: impl Borrow<V>);
}

/// Append-update key-value map.
///
/// Requires value to be encodable as a fixed-size array.
pub trait AuraMap<K, V, const KEY_LEN: usize = 32, const VAL_LEN: usize = 32>
where
    K: Ord + Into<[u8; KEY_LEN]> + From<[u8; KEY_LEN]>,
    V: Eq + Into<[u8; VAL_LEN]> + From<[u8; VAL_LEN]>,
{
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

    /// Inserts item to the append-only log if the key is not yet present.
    ///
    /// # Panic
    ///
    /// Panics if item under the given id is different from another item under the same id already
    /// present in the log.
    fn insert_only(&mut self, key: K, val: impl Borrow<V>);

    /// Inserts item to the append-only log or updates its value.
    fn insert_or_update(&mut self, key: K, val: impl Borrow<V>);

    /// Updates the value for a given key.
    ///
    /// # Panics
    ///
    /// If the key is not present in the log.
    fn update_only(&mut self, key: K, val: impl Borrow<V>);
}

/// Transaction interface for append-only logs.
pub trait TransactionalMap<K> {
    /// Starts new transaction.
    fn begin_transaction(&mut self);

    /// Commits transaction, returning transaction number.
    ///
    /// Transaction numbers are always sequential.
    fn commit_transaction(&mut self) -> u64;

    /// Iterates over keys added to the log as a part of a specific transaction number.
    ///
    /// If the transaction number is not known returns an empty iterator.
    fn transaction_keys(&self, txno: u64) -> impl ExactSizeIterator<Item = K>;

    /// Returns number of transactions.
    fn transaction_count(&self) -> u64;
}
