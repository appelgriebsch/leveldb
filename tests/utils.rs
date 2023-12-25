#![allow(dead_code)]

use leveldb::database::Database;
use leveldb::options::{Options,WriteOptions};
use leveldb::key::IntoLevelDBKey;
use std::path::Path;
use tempdir::TempDir;

pub fn temp_dir(name: &str) -> TempDir {
  TempDir::new(name).unwrap()
}

pub fn open_database(path: &Path, create_if_missing: bool) -> Database {
  let mut opts = Options::new();
  opts.create_if_missing = create_if_missing;

  match Database::open(path, &opts) {
    Ok(db) => { db },
    Err(e) => { panic!("failed to open database: {:?}", e) }
  }
}

pub fn db_put_u8_simple(database: &Database, key: &[u8], val: &[u8]) {
  let write_opts = WriteOptions::new();

  match database.put_u8(&write_opts, key, val) {
    Ok(_) => {  },
    Err(e) => { panic!("failed to write to database: {:?}", e) }
  }
}

pub fn db_put_simple(database: &Database, key: &dyn IntoLevelDBKey, val: &[u8]) {
  let write_opts = WriteOptions::new();

  match database.put(&write_opts, key, val) {
    Ok(_) => {  },
    Err(e) => { panic!("failed to write to database: {:?}", e) }
  }
}

