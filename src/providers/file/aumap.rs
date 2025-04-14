// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::fs::File;
use std::hash::Hash;
use std::io::{self, Read, Write};
use std::marker::PhantomData;
use std::path::PathBuf;

use crate::AuraMap;

// For now, this is just an in-memory read BTree. In the next releases we need to change this.
#[derive(Debug)]
pub struct FileAuraMap<K, V, const KEY_LEN: usize = 32, const VAL_LEN: usize = 32>
where
    K: From<[u8; KEY_LEN]> + Into<[u8; KEY_LEN]>,
    V: From<[u8; VAL_LEN]> + Into<[u8; VAL_LEN]>,
{
    path: PathBuf,
    cache: BTreeMap<[u8; KEY_LEN], [u8; VAL_LEN]>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V, const KEY_LEN: usize, const VAL_LEN: usize> FileAuraMap<K, V, KEY_LEN, VAL_LEN>
where
    K: From<[u8; KEY_LEN]> + Into<[u8; KEY_LEN]>,
    V: From<[u8; VAL_LEN]> + Into<[u8; VAL_LEN]>,
{
    pub fn create(path: PathBuf) -> io::Result<Self> {
        File::create_new(&path)?;
        Ok(Self { cache: BTreeMap::new(), path, _phantom: PhantomData })
    }

    pub fn open(path: PathBuf) -> io::Result<Self>
    where V: Hash {
        let mut cache = BTreeMap::new();
        let mut file = File::open(&path)?;
        let mut key_buf = [0u8; KEY_LEN];
        let mut val_buf = [0u8; VAL_LEN];
        while file.read_exact(&mut key_buf).is_ok() {
            file.read_exact(&mut val_buf).expect("cannot read log file");
            cache.insert(key_buf, val_buf);
        }
        Ok(Self { path, cache, _phantom: PhantomData })
    }

    pub fn save(&self) -> io::Result<()> {
        let mut index_file = File::create(&self.path)?;
        for (key, value) in &self.cache {
            index_file.write_all(key)?;
            index_file.write_all(value)?;
        }
        Ok(())
    }
}

impl<K, V, const KEY_LEN: usize, const VAL_LEN: usize> AuraMap<K, V, KEY_LEN, VAL_LEN>
    for FileAuraMap<K, V, KEY_LEN, VAL_LEN>
where
    K: From<[u8; KEY_LEN]> + Into<[u8; KEY_LEN]>,
    V: From<[u8; VAL_LEN]> + Into<[u8; VAL_LEN]>,
{
    fn keys(&self) -> impl Iterator<Item = K> { self.cache.keys().copied().map(K::from) }

    fn contains_key(&self, key: K) -> bool { self.cache.contains_key(&key.into()) }

    fn get(&self, key: K) -> Option<V> { self.cache.get(&key.into()).copied().map(V::from) }

    fn insert_or_update(&mut self, key: K, val: V) {
        let val = val.into();
        *self.cache.entry(key.into()).or_insert(val) = val;
        self.save().expect("Cannot save log file");
    }
}
