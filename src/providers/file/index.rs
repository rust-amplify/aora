// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Write};
use std::marker::PhantomData;
use std::path::PathBuf;

use indexmap::IndexSet;

use crate::AoraIndex;

// For now, this is just an in-memory read BTree. In the next releases we need to change this.
#[derive(Debug)]
pub struct FileAoraIndex<K, V, const KEY_LEN: usize = 32, const VAL_LEN: usize = 32>
where
    K: From<[u8; KEY_LEN]> + Into<[u8; KEY_LEN]>,
    V: From<[u8; VAL_LEN]> + Into<[u8; VAL_LEN]>,
{
    path: PathBuf,
    cache: HashMap<[u8; KEY_LEN], IndexSet<[u8; VAL_LEN]>>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V, const KEY_LEN: usize, const VAL_LEN: usize> FileAoraIndex<K, V, KEY_LEN, VAL_LEN>
where
    K: From<[u8; KEY_LEN]> + Into<[u8; KEY_LEN]>,
    V: From<[u8; VAL_LEN]> + Into<[u8; VAL_LEN]>,
{
    pub fn create(path: PathBuf) -> io::Result<Self> {
        File::create_new(&path)?;
        Ok(Self { cache: HashMap::new(), path, _phantom: PhantomData })
    }

    pub fn open(path: PathBuf) -> io::Result<Self> {
        let mut cache = HashMap::new();
        let mut file = File::open(&path)?;
        let mut key_buf = [0u8; KEY_LEN];
        let mut val_buf = [0u8; VAL_LEN];
        while file.read_exact(&mut key_buf).is_ok() {
            let mut values = IndexSet::new();
            let mut len = [0u8; 4];
            file.read_exact(&mut len).expect("cannot read index file");
            let mut len = u32::from_le_bytes(len);
            while len > 0 {
                file.read_exact(&mut val_buf)
                    .expect("cannot read index file");
                let res = values.insert(val_buf);
                debug_assert!(res, "duplicate id in index file");
                len -= 1;
            }
            cache.insert(key_buf, values);
        }
        Ok(Self { path, cache, _phantom: PhantomData })
    }

    pub fn save(&self) -> io::Result<()> {
        let mut index_file = File::create(&self.path)?;
        for (key, values) in &self.cache {
            index_file.write_all(key)?;
            let len = values.len() as u32;
            index_file.write_all(&len.to_le_bytes())?;
            for value in values {
                index_file.write_all(value)?;
            }
        }
        Ok(())
    }
}

impl<K, V, const KEY_LEN: usize, const VAL_LEN: usize> AoraIndex<K, V, KEY_LEN, VAL_LEN>
    for FileAoraIndex<K, V, KEY_LEN, VAL_LEN>
where
    K: From<[u8; KEY_LEN]> + Into<[u8; KEY_LEN]>,
    V: From<[u8; VAL_LEN]> + Into<[u8; VAL_LEN]>,
{
    fn keys(&self) -> impl Iterator<Item = K> { self.cache.keys().copied().map(K::from) }

    fn contains_key(&self, key: K) -> bool { self.cache.contains_key(&key.into()) }

    fn value_len(&self, key: K) -> usize {
        self.cache
            .get(&key.into())
            .map(|ids| ids.len())
            .unwrap_or(0)
    }

    fn get(&self, key: K) -> impl ExactSizeIterator<Item = V> {
        match self.cache.get(&key.into()) {
            Some(ids) => ids.clone().into_iter().map(V::from),
            None => IndexSet::new().into_iter().map(V::from),
        }
    }

    fn push(&mut self, key: K, val: V) {
        self.cache.entry(key.into()).or_default().insert(val.into());
        self.save().expect("Cannot save index file");
    }
}
