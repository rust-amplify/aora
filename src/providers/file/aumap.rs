// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::{fs, mem};

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
    on_disk: Vec<HashMap<[u8; KEY_LEN], [u8; VAL_LEN]>>,
    dirty: Vec<HashMap<[u8; KEY_LEN], [u8; VAL_LEN]>>,
    pending: HashMap<[u8; KEY_LEN], [u8; VAL_LEN]>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V, const MAGIC: u64, const VER: u16, const KEY_LEN: usize, const VAL_LEN: usize>
    FileAuraMap<K, V, MAGIC, VER, KEY_LEN, VAL_LEN>
where
    K: From<[u8; KEY_LEN]> + Into<[u8; KEY_LEN]>,
    V: From<[u8; VAL_LEN]> + Into<[u8; VAL_LEN]>,
{
    fn prepare(path: impl AsRef<Path>, name: &str) -> PathBuf {
        let path = path.as_ref();
        path.join(name).with_extension("log")
    }

    pub fn create_new(path: impl AsRef<Path>, name: &str) -> io::Result<Self> {
        let path = Self::prepare(path, name);
        if fs::exists(&path)? {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("append-update log file '{}' already exists", path.display()),
            ));
        }
        let mut file = BinFile::<MAGIC, VER>::create_new(&path)
            .map_err(|e| io::Error::new(e.kind(), format!("at path '{}'", path.display())))?;
        file.write_all(&[0u8; 8])?;
        Ok(Self {
            on_disk: Vec::new(),
            dirty: Vec::new(),
            pending: HashMap::new(),
            path,
            _phantom: PhantomData,
        })
    }

    pub fn open_or_create(path: impl AsRef<Path>, name: &str) -> io::Result<Self> {
        let path = Self::prepare(path, name);
        if !fs::exists(&path)? { Self::create_new(path, name) } else { Self::open(path, name) }
    }

    pub fn open(path: impl AsRef<Path>, name: &str) -> io::Result<Self> {
        let path = Self::prepare(path, name);

        if !fs::exists(&path)? {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("append-update log file '{}' does not exist", path.display()),
            ));
        }
        let mut file = BinFile::<MAGIC, VER>::open(&path)?;

        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let num_pages = u64::from_le_bytes(buf);
        eprintln!("totlal {num_pages} pages in {path:?}");

        let mut key_buf = [0u8; KEY_LEN];
        let mut val_buf = [0u8; VAL_LEN];
        let mut cache = Vec::with_capacity(num_pages as usize);
        for _ in 0..num_pages {
            file.read_exact(&mut buf).unwrap();
            let num_keys = u64::from_le_bytes(buf);
            let mut page = HashMap::with_capacity(num_keys as usize);
            eprintln!("page {} has {} keys", cache.len(), num_keys);
            for _ in 0..num_keys {
                file.read_exact(&mut key_buf).unwrap();
                file.read_exact(&mut val_buf).unwrap();
                page.insert(key_buf, val_buf);
            }
            cache.push(page);
        }

        if file.stream_position()? != file.metadata()?.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("append-update log file '{}' is corrupted", path.display()),
            ));
        }

        Ok(Self {
            path,
            on_disk: cache,
            dirty: Vec::new(),
            pending: HashMap::new(),
            _phantom: PhantomData,
        })
    }

    pub fn save(&mut self) -> io::Result<()> {
        let mut index_file = BinFile::<MAGIC, VER>::open_rw(&self.path)
            .map_err(|e| io::Error::new(e.kind(), format!("at path '{}'", self.path.display())))?;

        let offset = index_file.stream_position()?;
        debug_assert_eq!(offset, 10);

        let mut num_pages = self.on_disk.len() as u64;
        #[cfg(debug_assertions)]
        {
            let mut buf = [0u8; 8];
            index_file.read_exact(&mut buf)?;
            index_file.seek(SeekFrom::Current(-8))?;
            let prev_num_pages = u64::from_le_bytes(buf);
            debug_assert_eq!(prev_num_pages, num_pages);
        }

        for page in &self.dirty {
            index_file.seek(SeekFrom::End(0))?;

            let num_keys = page.len() as u64;
            index_file.write_all(&num_keys.to_le_bytes())?;
            for (key, value) in page {
                index_file.write_all(key)?;
                index_file.write_all(value)?;
            }

            num_pages += 1;
            index_file.seek(SeekFrom::Start(offset))?;
            index_file.write_all(&num_pages.to_le_bytes())?;
        }
        debug_assert_eq!(num_pages as usize, self.on_disk.len() + self.dirty.len());

        self.on_disk.append(&mut self.dirty);

        Ok(())
    }

    fn keys_internal(&self) -> impl Iterator<Item = &[u8; KEY_LEN]> {
        self.on_disk
            .iter()
            .flat_map(|page| page.keys())
            .chain(self.pending.keys())
    }

    pub fn path(&self) -> &Path { &self.path }
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
        self.on_disk
            .iter()
            .find_map(|page| page.get(&key))
            .or_else(|| self.pending.get(&key))
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
            self.dirty.push(mem::take(&mut self.pending));
            self.save().expect("Cannot save log file");
        }
        self.transaction_count() - 1
    }

    fn abort_transaction(&mut self) { self.pending.clear(); }

    fn transaction_keys(&self, txno: u64) -> impl ExactSizeIterator<Item = K> {
        self.on_disk[txno as usize].keys().copied().map(K::from)
    }

    fn transaction_count(&self) -> u64 { (self.on_disk.len() + self.pending.len()) as u64 }
}

impl<K, V, const MAGIC: u64, const VER: u16, const KEY_LEN: usize, const VAL_LEN: usize> Drop
    for FileAuraMap<K, V, MAGIC, VER, KEY_LEN, VAL_LEN>
where
    K: From<[u8; KEY_LEN]> + Into<[u8; KEY_LEN]>,
    V: From<[u8; VAL_LEN]> + Into<[u8; VAL_LEN]>,
{
    fn drop(&mut self) {
        assert!(
            self.pending.is_empty(),
            "the latest transaction must be committed before dropping"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::U64Le;

    type Db = FileAuraMap<U64Le, U64Le, { u64::from_be_bytes(*b"DUMBTEST") }, 1, 8, 8>;

    fn normal_ops(db: &mut Db) {
        // Newly created db is empty
        assert_eq!(db.keys().count(), 0);

        // No unknown keys
        assert_eq!(db.get(1.into()), None);

        // Insert op
        db.insert_only(0.into(), 1.into());
        // It got there
        assert_eq!(db.get_expect(0.into()).0, 1);
        // Idempotence
        assert_eq!(db.get_expect(0.into()).0, 1);

        // Still no unknown keys
        assert_eq!(db.get(1.into()), None);

        // Update op
        db.update_only(0.into(), 2.into());
        // It got updated
        assert_eq!(db.get_expect(0.into()).0, 2);

        // Update or insert op
        db.insert_or_update(0.into(), 3.into());
        // It got updated
        assert_eq!(db.get_expect(0.into()).0, 3);

        // Still no unknown keys
        assert_eq!(db.get(1.into()), None);

        // Update or insert op with a new key
        db.insert_or_update(1.into(), 4.into());
        // It got there
        assert_eq!(db.get_expect(1.into()).0, 4);
        // The previous key hasn't gone
        assert_eq!(db.get_expect(0.into()).0, 3);

        // We have two keys at the end
        assert_eq!(db.keys().count(), 2);
    }

    #[test]
    fn abort() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create_new(dir.path(), "happy_path").unwrap();

        normal_ops(&mut db);
        db.abort_transaction();

        // Check that now we are empty
        assert_eq!(db.get(1.into()), None);
        assert_eq!(db.get(0.into()), None);
        assert_eq!(db.keys().count(), 0);
        assert_eq!(db.transaction_count(), 0);

        let data = fs::read(dir.path().join("happy_path.log")).unwrap();
        assert_eq!(data, b"DUMBTEST\0\x01\0\0\0\0\0\0\0\0");
    }

    #[test]
    fn commit() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create_new(dir.path(), "happy_transactions").unwrap();

        normal_ops(&mut db);
        assert_eq!(db.commit_transaction(), 0);

        // Check that commitment hasn't changed anything
        assert_eq!(db.get_expect(1.into()).0, 4);
        assert_eq!(db.get_expect(0.into()).0, 3);
        assert_eq!(db.keys().collect::<HashSet<_>>(), set![0.into(), 1.into()]);

        // Check that transaction information is value
        assert_eq!(db.transaction_count(), 1);
        assert_eq!(db.transaction_keys(0).collect::<HashSet<_>>(), set![0.into(), 1.into()]);

        // Insert another item
        db.insert_only(3.into(), 5.into());
        assert_eq!(db.commit_transaction(), 1);
        assert_eq!(db.transaction_count(), 2);
        assert_eq!(db.transaction_keys(0).collect::<HashSet<_>>(), set![0.into(), 1.into()]);
        assert_eq!(db.transaction_keys(1).collect::<HashSet<_>>(), set![3.into()]);

        let db = Db::open(dir.path(), "happy_transactions").unwrap();

        // Check that commitment hasn't changed anything
        assert_eq!(db.get_expect(1.into()).0, 4);
        assert_eq!(db.get_expect(0.into()).0, 3);
        assert_eq!(db.keys().collect::<HashSet<_>>(), set![0.into(), 1.into(), 3.into()]);

        // Check that transaction information is value
        assert_eq!(db.transaction_count(), 2);
        assert_eq!(db.transaction_keys(0).collect::<HashSet<_>>(), set![0.into(), 1.into()]);
        assert_eq!(db.transaction_keys(1).collect::<HashSet<_>>(), set![3.into()]);
    }

    #[test]
    fn insert_same() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create_new(dir.path(), "insert_same").unwrap();

        db.insert_only(0.into(), 1.into());
        db.insert_only(0.into(), 1.into());
        assert_eq!(db.commit_transaction(), 0);

        db.insert_only(0.into(), 1.into());
        assert_eq!(db.commit_transaction(), 0);

        assert_eq!(db.transaction_count(), 1);
    }

    #[test]
    #[should_panic(expected = "key is already inserted")]
    fn unique_keys() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Db::create_new(dir.path(), "unique_keys").unwrap();

        db.insert_only(0.into(), 1.into());
        assert_eq!(db.commit_transaction(), 0);

        db.insert_only(0.into(), 2.into());
        assert_eq!(db.commit_transaction(), 1);
    }

    #[test]
    #[should_panic(expected = "the latest transaction must be committed before dropping")]
    fn drop_uncommitted() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut db = Db::create_new(dir.path(), "drop_uncommitted").unwrap();
            normal_ops(&mut db);
            drop(db);
        }
        // we panic at the end of the scope
    }
}
