#![feature(test)]

extern crate test;

use amplify::confinement::SmallVec;
use amplify::num::u256;
use aora::AoraMap;
use aora::file::FileAoraMap;
use test::Bencher;

type Db = FileAoraMap<[u8; 32], SmallVec<u8>, { u64::from_be_bytes(*b"DUMBTEST") }, 8>;

#[bench]
fn get(bench: &mut Bencher) {
    const NAME: &str = "bench_get";
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::create_new(dir.path(), NAME).unwrap();

    let key = [0xFD; 32];
    let val = SmallVec::from_checked(vec![0xA8; 1024]);
    db.insert(key, &val);

    bench.iter(|| {
        db.get(key);
    });
}

#[bench]
fn insert(bench: &mut Bencher) {
    const NAME: &str = "bench_insert";
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::create_new(dir.path(), NAME).unwrap();

    let mut key = u256::ZERO;
    let val = SmallVec::from_checked(vec![0xA8; 1024]);
    bench.iter(|| {
        db.insert(key.to_be_bytes(), &val);
        key += u256::ONE;
    });
}
