// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::marker::PhantomData;
use std::mem;
use std::path::PathBuf;

use binfile::BinFile;

use crate::{AuraMap, TransactionalMap};

// For now, this is just an in-memory read BTree. In the next releases we need to change this.
#[derive(Debug)]
pub struct FileAuraMap<
    K,
    V,
    const MAGIC: u64,
    const VER: u16 = 1,
    const KEY_LEN: usize = 32,
    const VAL_LEN: usize = 32,
> where
    K: From<[u8; KEY_LEN]> + Into<[u8; KEY_LEN]>,
    V: From<[u8; VAL_LEN]> + Into<[u8; VAL_LEN]>,
{
    path: PathBuf,
    cache: Vec<HashMap<[u8; KEY_LEN], [u8; VAL_LEN]>>,
    pending: HashMap<[u8; KEY_LEN], [u8; VAL_LEN]>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V, const MAGIC: u64, const VER: u16, const KEY_LEN: usize, const VAL_LEN: usize>
    FileAuraMap<K, V, MAGIC, VER, KEY_LEN, VAL_LEN>
where
    K: From<[u8; KEY_LEN]> + Into<[u8; KEY_LEN]>,
    V: From<[u8; VAL_LEN]> + Into<[u8; VAL_LEN]>,
{
    pub fn create(path: PathBuf) -> io::Result<Self> {
        BinFile::<MAGIC, VER>::create_new(&path)?;
        Ok(Self {
            cache: Vec::new(),
            pending: HashMap::new(),
            path,
            _phantom: PhantomData,
        })
    }

    pub fn open(path: PathBuf) -> io::Result<Self> {
        let mut file = BinFile::<MAGIC, VER>::open(&path)?;

        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let num_pages = u64::from_le_bytes(buf);

        let mut key_buf = [0u8; KEY_LEN];
        let mut val_buf = [0u8; VAL_LEN];
        let mut cache = Vec::with_capacity(num_pages as usize);
        for _ in 0..num_pages {
            let mut page = HashMap::new();
            while file.read_exact(&mut key_buf).is_ok() {
                file.read_exact(&mut val_buf).expect("cannot read log file");
                page.insert(key_buf, val_buf);
            }
            cache.push(page);
        }

        Ok(Self { path, cache, pending: HashMap::new(), _phantom: PhantomData })
    }

    pub fn save(&self) -> io::Result<()> {
        let mut index_file = BinFile::<MAGIC, VER>::create(&self.path)?;

        let num_pages = self.cache.len() as u64;
        index_file.write_all(&num_pages.to_le_bytes())?;

        for page in &self.cache {
            for (key, value) in page {
                index_file.write_all(key)?;
                index_file.write_all(value)?;
            }
        }

        Ok(())
    }

    fn keys_internal(&self) -> impl Iterator<Item = &[u8; KEY_LEN]> {
        self.cache.iter().flat_map(|page| page.keys())
    }
}

impl<K, V, const MAGIC: u64, const VER: u16, const KEY_LEN: usize, const VAL_LEN: usize>
    AuraMap<K, V, KEY_LEN, VAL_LEN> for FileAuraMap<K, V, MAGIC, VER, KEY_LEN, VAL_LEN>
where
    K: From<[u8; KEY_LEN]> + Into<[u8; KEY_LEN]>,
    V: From<[u8; VAL_LEN]> + Into<[u8; VAL_LEN]>,
{
    fn keys(&self) -> impl Iterator<Item = K> { self.keys_internal().copied().map(K::from) }

    fn contains_key(&self, key: K) -> bool {
        let key = key.into();
        self.keys_internal().any(|k| *k == key)
    }

    fn get(&self, key: K) -> Option<V> {
        let key = key.into();
        self.cache
            .iter()
            .find_map(|page| page.get(&key))
            .copied()
            .map(V::from)
    }

    fn insert_or_update(&mut self, key: K, val: V) {
        let val = val.into();
        *self.pending.entry(key.into()).or_insert(val) = val;
    }
}

impl<K, V, const MAGIC: u64, const VER: u16, const KEY_LEN: usize, const VAL_LEN: usize>
    TransactionalMap<K> for FileAuraMap<K, V, MAGIC, VER, KEY_LEN, VAL_LEN>
where
    K: From<[u8; KEY_LEN]> + Into<[u8; KEY_LEN]>,
    V: From<[u8; VAL_LEN]> + Into<[u8; VAL_LEN]>,
{
    fn commit_transaction(&mut self) -> u64 {
        if !self.pending.is_empty() {
            self.cache.push(mem::take(&mut self.pending));
            self.save().expect("Cannot save log file");
        }
        self.cache.len() as u64 - 1
    }

    fn abort_transaction(&mut self) { self.pending.clear(); }

    fn transaction_keys(&self, txno: u64) -> impl ExactSizeIterator<Item = K> {
        self.cache[txno as usize].keys().copied().map(K::from)
    }

    fn transaction_count(&self) -> u64 { self.cache.len() as u64 }
}

impl<K, V, const MAGIC: u64, const VER: u16, const KEY_LEN: usize, const VAL_LEN: usize> Drop
    for FileAuraMap<K, V, MAGIC, VER, KEY_LEN, VAL_LEN>
where
    K: From<[u8; KEY_LEN]> + Into<[u8; KEY_LEN]>,
    V: From<[u8; VAL_LEN]> + Into<[u8; VAL_LEN]>,
{
    fn drop(&mut self) {
        assert!(
            !self.pending.is_empty(),
            "the latest transaction must be committed before dropping"
        );
    }
}
