// SPDX-License-Identifier: Apache-2.0

//! AORA: Append-only random-accessed data persistence.

use core::borrow::Borrow;

/// Trait for providers of append-only random-access log functionality.
pub trait Aora {
    type Item: Sized;
    type Id: Into<[u8; 32]> + From<[u8; 32]>;

    /// Adds item to the append-only log. If the item is already in the log, does noting.
    ///
    /// # Panic
    ///
    /// Panics if item under the given id is different from another item under the same id already
    /// present in the log
    fn append(&mut self, id: Self::Id, item: &Self::Item);
    fn extend(&mut self, iter: impl IntoIterator<Item = (Self::Id, impl Borrow<Self::Item>)>) {
        for (id, item) in iter {
            self.append(id, item.borrow());
        }
    }
    fn has(&self, id: &Self::Id) -> bool;
    fn read(&mut self, id: Self::Id) -> Self::Item;
    fn iter(&mut self) -> impl Iterator<Item = (Self::Id, Self::Item)>;
}

#[cfg(feature = "file-strict")]
pub mod file {
    use std::collections::BTreeMap;
    use std::fs::{File, OpenOptions};
    use std::io;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::marker::PhantomData;
    use std::path::{Path, PathBuf};

    use strict_encoding::{
        StreamReader, StreamWriter, StrictDecode, StrictEncode, StrictReader, StrictWriter,
    };

    use super::*;

    pub struct FileAora<Id: Ord + From<[u8; 32]>, T> {
        log: File,
        idx: File,
        index: BTreeMap<Id, u64>,
        _phantom: PhantomData<T>,
    }

    impl<Id: Ord + From<[u8; 32]>, T> FileAora<Id, T> {
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
            Self { log, idx, index: BTreeMap::new(), _phantom: PhantomData }
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
                let mut id = [0u8; 32];
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

            Self { log, idx, index, _phantom: PhantomData }
        }
    }

    impl<Id: Ord + From<[u8; 32]> + Into<[u8; 32]>, T: Eq + StrictEncode + StrictDecode> Aora
        for FileAora<Id, T>
    {
        type Item = T;
        type Id = Id;

        fn append(&mut self, id: Self::Id, item: &T) {
            if self.has(&id) {
                let old = self.read(id);
                if &old != item {
                    panic!(
                        "item under the given id is different from another item under the same id \
                         already present in the log"
                    );
                }
                return;
            }
            let id = id.into();
            self.log
                .seek(SeekFrom::End(0))
                .expect("unable to seek to the end of the log");
            let pos = self
                .log
                .stream_position()
                .expect("unable to get log position");
            let writer = StrictWriter::with(StreamWriter::new::<{ usize::MAX }>(&mut self.log));
            item.strict_encode(writer).unwrap();
            self.idx
                .seek(SeekFrom::End(0))
                .expect("unable to seek to the end of the index");
            self.idx.write_all(&id).expect("unable to write to index");
            self.idx
                .write_all(&pos.to_le_bytes())
                .expect("unable to write to index");
            self.index.insert(id.into(), pos);
        }

        fn has(&self, id: &Self::Id) -> bool { self.index.contains_key(id) }

        fn read(&mut self, id: Self::Id) -> T {
            let pos = self.index.get(&id).expect("unknown item");

            self.log
                .seek(SeekFrom::Start(*pos))
                .expect("unable to seek to the item");
            let mut reader = StrictReader::with(StreamReader::new::<{ usize::MAX }>(&self.log));
            T::strict_decode(&mut reader).expect("unable to read item")
        }

        fn iter(&mut self) -> impl Iterator<Item = (Self::Id, T)> {
            self.log
                .seek(SeekFrom::Start(0))
                .expect("unable to seek to the start of the log file");
            self.idx
                .seek(SeekFrom::Start(0))
                .expect("unable to seek to the start of the index file");

            let reader = StrictReader::with(StreamReader::new::<{ usize::MAX }>(&self.log));
            Iter { log: reader, idx: &self.idx, _phantom: PhantomData }
        }
    }

    pub struct Iter<'file, Id: From<[u8; 32]>, T: StrictDecode> {
        log: StrictReader<StreamReader<&'file File>>,
        idx: &'file File,
        _phantom: PhantomData<(Id, T)>,
    }

    impl<Id: From<[u8; 32]>, T: StrictDecode> Iterator for Iter<'_, Id, T> {
        type Item = (Id, T);

        fn next(&mut self) -> Option<Self::Item> {
            let mut id = [0u8; 32];
            self.idx.read_exact(&mut id).ok()?;
            self.idx
                .seek(SeekFrom::Current(8))
                .expect("broken index file");
            let item = T::strict_decode(&mut self.log).ok()?;
            Some((id.into(), item))
        }
    }
}
