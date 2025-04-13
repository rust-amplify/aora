// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::fs::File;
use std::hash::Hash;
use std::io;
use std::io::{Read, Write};
use std::path::PathBuf;

use indexmap::IndexSet;

use crate::AoraIndex;

// For now, this is just an in-memory read BTree. In the next releases we need to change this.
#[derive(Clone, Debug)]
pub struct FileAoraIndex<K, V, const KEY_LEN: usize = 32, const VAL_LEN: usize = 32>
where
    K: Ord + From<[u8; KEY_LEN]> + Into<[u8; KEY_LEN]>,
    V: Eq + From<[u8; VAL_LEN]> + Into<[u8; VAL_LEN]>,
{
    path: PathBuf,
    cache: BTreeMap<K, IndexSet<V>>,
}

impl<K, V, const KEY_LEN: usize, const VAL_LEN: usize> FileAoraIndex<K, V, KEY_LEN, VAL_LEN>
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
            let opid = K::from(key_buf);
            let mut ids = IndexSet::new();
            let mut len = [0u8; 4];
            file.read_exact(&mut len).expect("cannot read index file");
            let mut len = u32::from_le_bytes(len);
            while len > 0 {
                file.read_exact(&mut val_buf)
                    .expect("cannot read index file");
                let res = ids.insert(val_buf.into());
                debug_assert!(res, "duplicate id in index file");
                len -= 1;
            }
            cache.insert(opid, ids);
        }
        Ok(Self { path, cache })
    }

    pub fn save(&self) -> io::Result<()>
    where
        K: Copy,
        V: Copy,
    {
        let mut index_file = File::create(&self.path)?;
        for (key, values) in &self.cache {
            index_file.write_all(&(*key).into())?;
            let len = values.len() as u32;
            index_file.write_all(&len.to_le_bytes())?;
            for value in values {
                index_file.write_all(&(*value).into())?;
            }
        }
        Ok(())
    }
}

impl<K, V, const KEY_LEN: usize, const VAL_LEN: usize> AoraIndex<K, V, KEY_LEN, VAL_LEN>
    for FileAoraIndex<K, V, KEY_LEN, VAL_LEN>
where
    K: Copy + Ord + From<[u8; KEY_LEN]> + Into<[u8; KEY_LEN]>,
    V: Copy + Eq + Hash + From<[u8; VAL_LEN]> + Into<[u8; VAL_LEN]>,
{
    fn keys(&self) -> impl Iterator<Item = K> { self.cache.keys().copied() }

    fn contains_key(&self, key: &K) -> bool { self.cache.contains_key(&key) }

    fn value_len(&self, key: &K) -> usize { self.cache.get(key).map(|ids| ids.len()).unwrap_or(0) }

    fn get(&self, key: &K) -> impl ExactSizeIterator<Item = V> {
        match self.cache.get(key) {
            Some(ids) => ids.clone().into_iter(),
            None => IndexSet::new().into_iter(),
        }
    }

    fn push(&mut self, key: K, val: V) {
        self.cache.entry(key).or_default().insert(val);
        self.save().expect("Cannot save index file");
    }
}
