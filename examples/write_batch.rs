use leveldb::database::batch::{Batch, WriteBatch};
use leveldb::database::Database;
use leveldb::error::Error;
use leveldb::options::{Options, ReadOptions, WriteOptions};
use std::path::Path;

fn main() -> Result<(), Error> {
    let path = Path::new("temp_batch_ldb");
    let mut options = Options::new();
    options.create_if_missing = true;

    let database = Database::open(&path, &options)?;
    let batch = WriteBatch::new();
    let write_ops = WriteOptions::new();
    let read_ops = ReadOptions::new();

    batch.put(&1, &[1]);
    batch.put(&2, &[2]);
    batch.put(&3, &[3]);
    batch.delete(&2);

    database.write(&write_ops, &batch)?;

    let value = database.get(&read_ops, &3)?;
    assert!(value.is_some());
    assert_eq!(value.unwrap().as_slice(), &[3u8][..]);

    let value = database.get(&read_ops, &2)?;
    assert!(value.is_none());

    Ok(())
}
