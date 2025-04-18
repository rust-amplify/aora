#![feature(test)]

extern crate test;

use std::fs;

use aora::file::FileAuraMap;
use aora::{AuraMap, TransactionalMap, U64Le};
use test::Bencher;

type Db = FileAuraMap<U64Le, U64Le, { u64::from_be_bytes(*b"DUMBTEST") }, 1, 8, 8>;

fn print_db_size(db: Db) {
    let file = fs::File::open(db.path()).unwrap();
    let size = file.metadata().unwrap().len() as f64 / (1024.0 * 1024.0);
    eprintln!("Generated DB size is {size:.2} MB");
}

#[bench]
fn get(bench: &mut Bencher) {
    const NAME: &str = "bench_get";
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::create_new(dir.path(), NAME).unwrap();

    let key = U64Le(0u64);
    db.insert_only(key, U64Le(1u64));
    db.commit_transaction();

    bench.iter(|| {
        db.get(U64Le(0u64));
    });
}

#[bench]
fn insert_only(bench: &mut Bencher) {
    const NAME: &str = "bench_insert_only";
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::create_new(dir.path(), NAME).unwrap();

    let mut key = U64Le(0u64);
    bench.iter(|| {
        db.insert_only(key, 1.into());
        key.0 += 1;
    });

    db.commit_transaction();

    print_db_size(db);
}

#[bench]
fn insert_commit(bench: &mut Bencher) {
    const NAME: &str = "bench_insert_commit";
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::create_new(dir.path(), NAME).unwrap();

    let mut key = U64Le(0u64);
    bench.iter(|| {
        db.insert_only(key, 1.into());
        key = U64Le(key.0 + 1);
        db.commit_transaction();
    });

    print_db_size(db);
}

#[bench]
fn update_only(bench: &mut Bencher) {
    const NAME: &str = "bench_update_only";
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::create_new(dir.path(), NAME).unwrap();

    let key = U64Le(0u64);
    let mut val = U64Le(0u64);
    db.insert_only(key, 1.into());
    bench.iter(|| {
        db.update_only(key, val);
        val = U64Le(val.0 + 1);
    });

    db.commit_transaction();

    print_db_size(db);
}

#[bench]
fn update_commit(bench: &mut Bencher) {
    const NAME: &str = "bench_update_commit";
    let dir = tempfile::tempdir().unwrap();
    let mut db = Db::create_new(dir.path(), NAME).unwrap();

    let key = U64Le(0u64);
    let mut val = U64Le(0u64);
    db.insert_only(key, 1.into());
    bench.iter(|| {
        db.update_only(key, val);
        val = U64Le(val.0 + 1);
        db.commit_transaction();
    });

    print_db_size(db);
}
