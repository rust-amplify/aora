// SPDX-License-Identifier: Apache-2.0

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fs::File;
use std::hash::Hash;
use std::io::{self, Read, Write};
use std::path::PathBuf;

use crate::AuraMap;

// For now, this is just an in-memory read BTree. In the next releases we need to change this.
#[derive(Clone, Debug)]
pub struct FileAuraMap<K, V, const KEY_LEN: usize = 32, const VAL_LEN: usize = 32>
where
    K: Ord + From<[u8; KEY_LEN]> + Into<[u8; KEY_LEN]>,
    V: Eq + From<[u8; VAL_LEN]> + Into<[u8; VAL_LEN]>,
{
    path: PathBuf,
    cache: BTreeMap<K, V>,
}

impl<K, V, const KEY_LEN: usize, const VAL_LEN: usize> FileAuraMap<K, V, KEY_LEN, VAL_LEN>
where
    K: Ord + From<[u8; KEY_LEN]> + Into<[u8; KEY_LEN]>,
    V: Eq + From<[u8; VAL_LEN]> + Into<[u8; VAL_LEN]>,
{
    pub fn create(path: PathBuf) -> io::Result<Self> {
        File::create_new(&path)?;
        Ok(Self { cache: BTreeMap::new(), path })
    }

    pub fn open(path: PathBuf) -> io::Result<Self>
    where V: Hash {
        let mut cache = BTreeMap::new();
        let mut file = File::open(&path)?;
        let mut key_buf = [0u8; KEY_LEN];
        let mut val_buf = [0u8; VAL_LEN];
        while file.read_exact(&mut key_buf).is_ok() {
            let key = K::from(key_buf);
            file.read_exact(&mut val_buf).expect("cannot read log file");
            cache.insert(key, val_buf.into());
        }
        Ok(Self { path, cache })
    }

    pub fn save(&self) -> io::Result<()>
    where
        K: Copy,
        V: Copy,
    {
        let mut index_file = File::create(&self.path)?;
        for (key, value) in &self.cache {
            index_file.write_all(&(*key).into())?;
            index_file.write_all(&(*value).into())?;
        }
        Ok(())
    }
}

impl<K, V, const KEY_LEN: usize, const VAL_LEN: usize> AuraMap<K, V, KEY_LEN, VAL_LEN>
    for FileAuraMap<K, V, KEY_LEN, VAL_LEN>
where
    K: Copy + Ord + From<[u8; KEY_LEN]> + Into<[u8; KEY_LEN]>,
    V: Copy + Eq + Hash + From<[u8; VAL_LEN]> + Into<[u8; VAL_LEN]>,
{
    fn keys(&self) -> impl Iterator<Item = impl Borrow<K>> { self.cache.keys() }

    fn contains_key(&self, key: &K) -> bool { self.cache.contains_key(&key) }

    fn get(&self, key: &K) -> Option<impl Borrow<V>> { self.cache.get(key) }

    fn insert_or_update(&mut self, key: K, val: V) {
        *self.cache.entry(key).or_insert(val) = val;
        self.save().expect("Cannot save log file");
    }
}
