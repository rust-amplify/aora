// SPDX-License-Identifier: Apache-2.0

use std::cell::{RefCell, RefMut};
use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use binfile::BinFile;
use indexmap::IndexMap;
use strict_encoding::{
    StreamReader, StreamWriter, StrictDecode, StrictEncode, StrictReader, StrictWriter,
};

use crate::AoraMap;

#[derive(Clone, Debug, Display, Error)]
#[display(doc_comments)]
pub enum AoraMapError {
    /// AORA log database '{name}' can't be created since it already exists at '{path}'.
    Exists { name: String, path: String },

    /// AORA log database '{name}' at '{path}' exists, but some files are missing. A manual fix is
    /// required.
    PartiallyExists { name: String, path: String },

    /// AORA log database '{name}' does not exist at '{path}'. You need to initialize it first with
    /// either `create_new` or `open_or_create` methods.
    NotExists { name: String, path: String },
}

/// NB: This is blocking
// TODO: Make unblocking with a separate thread reading and writing to the disk, communicated
//       through a channel
#[derive(Debug)]
pub struct FileAoraMap<K, V, const MAGIC: u64, const VER: u16 = 1, const KEY_LEN: usize = 32>
where K: Into<[u8; KEY_LEN]> + From<[u8; KEY_LEN]>
{
    log: RefCell<BinFile<MAGIC, VER>>,
    idx: RefCell<BinFile<MAGIC, VER>>,
    index: RefCell<IndexMap<[u8; KEY_LEN], u64>>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V, const MAGIC: u64, const VER: u16, const KEY_LEN: usize>
    FileAoraMap<K, V, MAGIC, VER, KEY_LEN>
where K: Into<[u8; KEY_LEN]> + From<[u8; KEY_LEN]>
{
    fn prepare(path: impl AsRef<Path>, name: &str) -> (PathBuf, PathBuf) {
        let path = path.as_ref();
        let log = path.join(name).with_extension("log");
        let idx = path.join(name).with_extension("idx");
        (log, idx)
    }

    pub fn create_new(path: impl AsRef<Path>, name: &str) -> io::Result<Self> {
        let path = path.as_ref();
        let (log, idx) = Self::prepare(path, name);
        let log_exists = fs::exists(&log)?;
        let idx_exists = fs::exists(&idx)?;
        if log_exists && idx_exists {
            return Err(io::Error::other(AoraMapError::Exists {
                name: name.to_string(),
                path: path.display().to_string(),
            }));
        }
        if log_exists || idx_exists {
            return Err(io::Error::other(AoraMapError::PartiallyExists {
                name: name.to_string(),
                path: path.display().to_string(),
            }));
        }
        let log = BinFile::create_new(&log)
            .map_err(|err| io::Error::new(err.kind(), format!("log file '{}'", log.display())))?;
        let idx = BinFile::create_new(&idx)
            .map_err(|err| io::Error::new(err.kind(), format!("index file '{}'", idx.display())))?;
        Ok(Self {
            log: RefCell::new(log),
            idx: RefCell::new(idx),
            index: RefCell::new(IndexMap::new()),
            _phantom: PhantomData,
        })
    }

    pub fn open_or_create(path: impl AsRef<Path>, name: &str) -> io::Result<Self> {
        let path = path.as_ref();
        let (log, idx) = Self::prepare(path, name);
        let log_exists = fs::exists(&log)?;
        let idx_exists = fs::exists(&idx)?;
        if log_exists || idx_exists {
            return Err(io::Error::other(AoraMapError::PartiallyExists {
                name: name.to_string(),
                path: path.display().to_string(),
            }));
        }

        let (log, idx) = if log_exists && idx_exists {
            let log = BinFile::create_new(&log).map_err(|err| {
                io::Error::new(err.kind(), format!("log file '{}'", log.display()))
            })?;

            let idx = BinFile::create_new(&idx).map_err(|err| {
                io::Error::new(err.kind(), format!("index file '{}'", idx.display()))
            })?;

            (log, idx)
        } else {
            let log = BinFile::open_rw(&log).map_err(|err| {
                io::Error::new(err.kind(), format!("log file '{}'", log.display()))
            })?;

            let idx = BinFile::open_rw(&idx).map_err(|err| {
                io::Error::new(err.kind(), format!("index file '{}'", idx.display()))
            })?;

            (log, idx)
        };

        Ok(Self {
            log: RefCell::new(log),
            idx: RefCell::new(idx),
            index: RefCell::new(IndexMap::new()),
            _phantom: PhantomData,
        })
    }

    pub fn open(path: impl AsRef<Path>, name: &str) -> io::Result<Self> {
        let path = path.as_ref();
        let (log, idx) = Self::prepare(path, name);
        let log_exists = fs::exists(&log)?;
        let idx_exists = fs::exists(&idx)?;
        if log_exists && idx_exists {
            return Err(io::Error::other(AoraMapError::NotExists {
                name: name.to_string(),
                path: path.display().to_string(),
            }));
        }
        if log_exists || idx_exists {
            return Err(io::Error::other(AoraMapError::PartiallyExists {
                name: name.to_string(),
                path: path.display().to_string(),
            }));
        }

        let mut log = BinFile::open_rw(&log)
            .map_err(|err| io::Error::new(err.kind(), format!("log file '{}'", log.display())))?;
        let mut idx = BinFile::open_rw(&idx)
            .map_err(|err| io::Error::new(err.kind(), format!("index file '{}'", idx.display())))?;

        let mut index = IndexMap::new();
        loop {
            let mut key_buf = [0u8; KEY_LEN];
            let res = idx.read_exact(&mut key_buf);
            if matches!(res, Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof) {
                break;
            } else {
                res.expect("unable to read item ID");
            }

            let mut buf = [0u8; 8];
            idx.read_exact(&mut buf)
                .expect("unable to read index entry");
            let pos = u64::from_le_bytes(buf);

            index.insert(key_buf, pos);
        }

        log.seek(SeekFrom::End(0))
            .expect("unable to seek to the end of the log");
        idx.seek(SeekFrom::End(0))
            .expect("unable to seek to the end of the index");

        Ok(Self {
            log: RefCell::new(log),
            idx: RefCell::new(idx),
            index: RefCell::new(index),
            _phantom: PhantomData,
        })
    }
}

impl<K, V, const MAGIC: u64, const VER: u16, const KEY_LEN: usize> AoraMap<K, V, KEY_LEN>
    for FileAoraMap<K, V, MAGIC, VER, KEY_LEN>
where
    K: Into<[u8; KEY_LEN]> + From<[u8; KEY_LEN]>,
    V: Eq + StrictEncode + StrictDecode,
{
    fn contains_key(&self, key: K) -> bool { self.index.borrow().contains_key(&key.into()) }

    fn get(&self, key: K) -> Option<V> {
        let index = self.index.borrow();
        let pos = index.get(&key.into())?;

        let mut log = self.log.borrow_mut();
        log.seek(SeekFrom::Start(*pos))
            .expect("unable to seek to the item");
        let mut reader = StrictReader::with(StreamReader::new::<{ usize::MAX }>(&mut *log));
        let value = V::strict_decode(&mut reader).expect("unable to read item");
        Some(value)
    }

    fn insert(&mut self, key: K, value: &V) {
        let key = key.into();
        if self.index.borrow().contains_key(&key) {
            let old = self.get(key.into());
            if old.as_ref() != Some(value) {
                panic!(
                    "item under the given id is different from another item under the same id \
                     already present in the log"
                );
            }
            return;
        }
        let log = self.log.get_mut();
        let idx = self.idx.get_mut();

        log.seek(SeekFrom::End(0))
            .expect("unable to seek to the end of the log");
        let pos = log.stream_position().expect("unable to get log position");
        let writer = StrictWriter::with(StreamWriter::new::<{ usize::MAX }>(log));
        value.strict_encode(writer).unwrap();

        idx.seek(SeekFrom::End(0))
            .expect("unable to seek to the end of the index");
        idx.write_all(&key).expect("unable to write to index");
        idx.write_all(&pos.to_le_bytes())
            .expect("unable to write to index");

        self.index.borrow_mut().insert(key, pos);
    }

    fn iter(&self) -> impl Iterator<Item = (K, V)> {
        let index = self.index.borrow().clone();
        Iter {
            log: self.log.borrow_mut(),
            index: index.into_iter(),
            _phantom: PhantomData,
        }
    }
}

pub struct Iter<
    'file,
    K: From<[u8; KEY_LEN]>,
    V: StrictDecode,
    const MAGIC: u64,
    const VER: u16,
    const KEY_LEN: usize,
> {
    log: RefMut<'file, BinFile<MAGIC, VER>>,
    index: indexmap::map::IntoIter<[u8; KEY_LEN], u64>,
    _phantom: PhantomData<(K, V)>,
}

impl<
    K: From<[u8; KEY_LEN]>,
    V: StrictDecode,
    const MAGIC: u64,
    const VER: u16,
    const KEY_LEN: usize,
> Iterator for Iter<'_, K, V, MAGIC, VER, KEY_LEN>
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let (id, pos) = self.index.next()?;
        self.log
            .seek(SeekFrom::Start(pos))
            .expect("unable to seek to the iterator position");

        let mut reader = StrictReader::with(StreamReader::new::<{ usize::MAX }>(&mut *self.log));
        let item = V::strict_decode(&mut reader).ok()?;

        Some((id.into(), item))
    }
}
