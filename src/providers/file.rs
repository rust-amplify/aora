// SPDX-License-Identifier: Apache-2.0

use std::borrow::Borrow;
use std::cell::{RefCell, RefMut};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use strict_encoding::{
    StreamReader, StreamWriter, StrictDecode, StrictEncode, StrictReader, StrictWriter,
};

use crate::Aora;

pub struct FileAora<K, V, const LEN: usize = 32>
where K: Ord + Into<[u8; LEN]> + From<[u8; LEN]>
{
    log: RefCell<File>,
    idx: RefCell<File>,
    index: RefCell<BTreeMap<K, u64>>,
    _phantom: PhantomData<V>,
}

impl<K, V, const LEN: usize> FileAora<K, V, LEN>
where K: Ord + Into<[u8; LEN]> + From<[u8; LEN]>
{
    fn prepare(path: impl AsRef<Path>, name: &str) -> (PathBuf, PathBuf) {
        let path = path.as_ref();
        let log = path.join(format!("{name}.log"));
        let idx = path.join(format!("{name}.idx"));
        (log, idx)
    }

    pub fn new(path: impl AsRef<Path>, name: &str) -> Self {
        let (log, idx) = Self::prepare(path, name);
        let log = File::create_new(&log).unwrap_or_else(|_| {
            panic!("unable to create append-only log file `{}`", log.display())
        });
        let idx = File::create_new(&idx).unwrap_or_else(|_| {
            panic!("unable to create random-access index file `{}`", idx.display())
        });
        Self {
            log: RefCell::new(log),
            idx: RefCell::new(idx),
            index: RefCell::new(BTreeMap::new()),
            _phantom: PhantomData,
        }
    }

    pub fn open(path: impl AsRef<Path>, name: &str) -> Self {
        let (log, idx) = Self::prepare(path, name);
        let mut log = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&log)
            .unwrap_or_else(|_| {
                panic!("unable to create append-only log file `{}`", log.display())
            });
        let mut idx = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&idx)
            .unwrap_or_else(|_| {
                panic!("unable to create random-access index file `{}`", idx.display())
            });

        let mut index = BTreeMap::new();
        loop {
            let mut id = [0u8; LEN];
            let res = idx.read_exact(&mut id);
            if matches!(res, Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof) {
                break;
            } else {
                res.expect("unable to read item ID");
            }

            let mut buf = [0u8; 8];
            idx.read_exact(&mut buf)
                .expect("unable to read index entry");
            let pos = u64::from_le_bytes(buf);

            index.insert(id.into(), pos);
        }

        log.seek(SeekFrom::End(0))
            .expect("unable to seek to the end of the log");
        idx.seek(SeekFrom::End(0))
            .expect("unable to seek to the end of the index");

        Self {
            log: RefCell::new(log),
            idx: RefCell::new(idx),
            index: RefCell::new(index),
            _phantom: PhantomData,
        }
    }
}

impl<K, V, const LEN: usize> Aora<K, V, LEN> for FileAora<K, V, LEN>
where
    K: Ord + Into<[u8; LEN]> + From<[u8; LEN]>,
    V: Eq + StrictEncode + StrictDecode,
{
    fn append(&mut self, key: K, value: impl Borrow<V>) {
        let value = value.borrow();

        if self.contains(&key) {
            let old = self.read(&key);
            if &old != value {
                panic!(
                    "item under the given id is different from another item under the same id \
                     already present in the log"
                );
            }
            return;
        }
        let id = key.into();

        let log = self.log.get_mut();
        let idx = self.idx.get_mut();

        log.seek(SeekFrom::End(0))
            .expect("unable to seek to the end of the log");
        let pos = log.stream_position().expect("unable to get log position");
        let writer = StrictWriter::with(StreamWriter::new::<{ usize::MAX }>(log));
        value.strict_encode(writer).unwrap();

        idx.seek(SeekFrom::End(0))
            .expect("unable to seek to the end of the index");
        idx.write_all(&id).expect("unable to write to index");
        idx.write_all(&pos.to_le_bytes())
            .expect("unable to write to index");

        self.index.get_mut().insert(id.into(), pos);
    }

    fn contains(&self, key: &K) -> bool { self.index.borrow().contains_key(key) }

    fn read(&self, key: &K) -> V {
        let index = self.index.borrow();
        let pos = index.get(&key).expect("unknown item");

        let mut log = self.log.borrow_mut();
        log.seek(SeekFrom::Start(*pos))
            .expect("unable to seek to the item");
        let mut reader = StrictReader::with(StreamReader::new::<{ usize::MAX }>(&*log));
        V::strict_decode(&mut reader).expect("unable to read item")
    }

    fn iter(&self) -> impl Iterator<Item = (K, V)> {
        let mut log = self.log.borrow_mut();
        log.seek(SeekFrom::Start(0))
            .expect("unable to seek to the start of the log file");
        log.seek(SeekFrom::Start(0))
            .expect("unable to seek to the start of the index file");

        Iter {
            log,
            idx: self.idx.borrow_mut(),
            pos: 0,
            _phantom: PhantomData,
        }
    }
}

pub struct Iter<'file, K: From<[u8; LEN]>, V: StrictDecode, const LEN: usize> {
    log: RefMut<'file, File>,
    idx: RefMut<'file, File>,
    pos: u64,
    _phantom: PhantomData<(K, V)>,
}

impl<K: From<[u8; LEN]>, V: StrictDecode, const LEN: usize> Iterator for Iter<'_, K, V, LEN> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let mut id = [0u8; LEN];
        self.idx.read_exact(&mut id).ok()?;
        self.idx
            .seek(SeekFrom::Current(8))
            .expect("broken index file");

        self.log
            .seek(SeekFrom::Start(self.pos))
            .expect("unable to seek to the iterator position");

        let mut reader = StrictReader::with(StreamReader::new::<{ usize::MAX }>(&*self.log));
        let item = V::strict_decode(&mut reader).ok()?;

        self.pos = self
            .log
            .stream_position()
            .expect("unable to retrieve log position");

        Some((id.into(), item))
    }
}
