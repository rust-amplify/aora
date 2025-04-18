// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(not(feature = "std"), no_std)]

//! AORA: Append-only random-accessed data persistence, made in `BTreeMap`-like fashion.

#[macro_use]
extern crate amplify;

mod providers;
mod types;

#[allow(unused_imports)]
pub use providers::*;
pub use types::*;

/// Trait for providers of append-only key-value maps.
pub trait AoraMap<K, V, const KEY_LEN: usize = 32>
where K: Into<[u8; KEY_LEN]> + From<[u8; KEY_LEN]>
{
    /// Checks whether a given value is present in the log.
    fn contains_key(&self, key: K) -> bool;

    /// Retrieves value from the log.
    ///
    /// # Panics
    ///
    /// Panics if the item under the provided key is not present.
    fn get(&self, key: K) -> Option<V>;

    /// Retrieves value from the log.
    ///
    /// # Panics
    ///
    /// Panics if the item under the provided key is not present.
    fn get_expect(&self, key: K) -> V { self.get(key).expect("key not found") }

    /// Inserts (appends) an item to the append-only log. If the item is already in the log, does
    /// noting.
    ///
    /// # Panic
    ///
    /// Panics if item under the given id is different from another item under the same id already
    /// present in the log.
    fn insert(&mut self, key: K, item: &V);

    /// Inserts (appends) all items from an iterator to the append-only log.
    ///
    /// # Panic
    ///
    /// Panics if any of the items is different from an item under the same id already present in
    /// the log.
    fn extend<'a>(&mut self, iter: impl IntoIterator<Item = (K, &'a V)>)
    where V: 'a {
        for (key, item) in iter {
            self.insert(key, item);
        }
    }

    /// Returns an iterator over the key and value pairs.
    fn iter(&self) -> impl Iterator<Item = (K, V)>;
}

/// Append-only log mapping keys to value sets, which is useful for building one-to-many key
/// indexes. The values in the index are kept in the order they were added.
pub trait AoraIndex<K, V, const KEY_LEN: usize = 32, const VAL_LEN: usize = 32>
where
    K: Into<[u8; KEY_LEN]> + From<[u8; KEY_LEN]>,
    V: Into<[u8; VAL_LEN]> + From<[u8; VAL_LEN]>,
{
    /// Returns iterator over all known keys.
    fn keys(&self) -> impl Iterator<Item = K>;

    /// Checks whether a given value is present in the log.
    fn contains_key(&self, key: K) -> bool { self.value_len(key) > 0 }

    /// Measures length of the value vector for the given key.
    fn value_len(&self, key: K) -> usize;

    /// Retrieves value vector from the log. If the key is not present, returns an empty iterator.
    fn get(&self, key: K) -> impl ExactSizeIterator<Item = V>;

    /// Pushes a new value into the value array for the given key.
    fn push(&mut self, key: K, val: V);
}

/// Append-update key-value map.
///
/// Requires value to be encodable as a fixed-size array.
pub trait AuraMap<K, V, const KEY_LEN: usize = 32, const VAL_LEN: usize = 32>
where
    K: Into<[u8; KEY_LEN]> + From<[u8; KEY_LEN]>,
    V: Into<[u8; VAL_LEN]> + From<[u8; VAL_LEN]>,
{
    /// Returns iterator over all known keys.
    fn keys(&self) -> impl Iterator<Item = K>;

    /// Checks whether a given value is present in the log.
    fn contains_key(&self, key: K) -> bool;

    /// Retrieves value from the log.
    ///
    /// # Panics
    ///
    /// Panics if the item under the provided key is not present.
    fn get(&self, key: K) -> Option<V>;

    /// Retrieves value from the log.
    ///
    /// # Panics
    ///
    /// Panics if the item under the provided key is not present.
    fn get_expect(&self, key: K) -> V { self.get(key).expect("key is absent") }

    /// Inserts item to the append-only log if the key is not yet present.
    ///
    /// # Panic
    ///
    /// Panics if item under the given id is different from another item under the same id already
    /// present in the log.
    fn insert_only(&mut self, key: K, val: V)
    where K: Copy {
        if let Some(v) = self.get(key) {
            if v.into() != val.into() {
                panic!("key is already inserted");
            }
            return;
        }
        self.insert_or_update(key, val);
    }

    /// Inserts item to the append-only log or updates its value.
    fn insert_or_update(&mut self, key: K, val: V);

    /// Updates the value for a given key.
    ///
    /// # Panics
    ///
    /// If the key is not present in the log.
    fn update_only(&mut self, key: K, val: V)
    where K: Copy {
        if !self.contains_key(key) {
            panic!("the key is not known");
        }
        self.insert_or_update(key, val);
    }
}

/// Transaction interface for append-only logs.
///
/// If an AORA log supports transactions, it should start a transaction on database open - and panic
/// if there is a non-commited transaction on a drop.
pub trait TransactionalMap<K> {
    /// Commits transaction, returning transaction number.
    ///
    /// Transaction numbers are always sequential.
    ///
    /// # Panics
    ///
    /// Panics if another transaction is already taking place.
    fn commit_transaction(&mut self) -> u64;

    /// Aborts latest transaction.
    fn abort_transaction(&mut self);

    /// Iterates over keys added to the log as a part of a specific transaction number.
    ///
    /// # Panics
    ///
    /// If the transaction number is not known.
    fn transaction_keys(&self, txno: u64) -> impl ExactSizeIterator<Item = K>;

    /// Returns number of transactions.
    fn transaction_count(&self) -> u64;
}
