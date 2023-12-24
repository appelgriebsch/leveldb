use leveldb::db::Database;
use leveldb::error::Error;
use leveldb::iterator::Iterable;
use leveldb::options::{Options, ReadOptions, WriteOptions};
use leveldb::util::FromU8;
use std::path::Path;

fn main() -> Result<(), Error> {
    let path = Path::new("temp_ldb");
    let mut options = Options::new();
    options.create_if_missing = true;

    let database = Database::open(&path, &options)?;

    let write_ops = WriteOptions::new();
    let read_ops = ReadOptions::new();

    // key of &[u8] type, it's the world 'name';
    let key = &[110, 97, 109, 101][..];
    database.put(&write_ops, &key, &b"tom"[..])?;

    let value = database.get(&read_ops, &key)?;

    assert!(value.is_some());
    assert_eq!(Vec::from(&b"tom"[..]), value.unwrap());

    // key of &str type
    let key = "age";
    database.put(&write_ops, &key, &b"5"[..])?;

    let value = database.get(&read_ops, &key)?;
    assert!(value.is_some());
    assert_eq!(Vec::from(&b"5"[..]), value.unwrap());

    // key of String type
    let key = "from".to_string();
    database.put(&write_ops, &key, &b"mars"[..])?;

    let value = database.get(&read_ops, &key)?;
    assert!(value.is_some());
    assert_eq!(Vec::from(&b"mars"[..]), value.unwrap());

    // key of integer type
    database.put(&write_ops, &1000, &10000i32.to_be_bytes()[..])?;

    let value = database.get(&read_ops, &1000)?;
    assert!(value.is_some());
    assert_eq!(10000, i32::from_u8(value.unwrap().as_slice()));

    // use put_u8 and get_u8
    let key = &b"temp"[..];
    database.put_u8(&write_ops, key, &b"temp"[..])?;

    let value = database.get_u8(&read_ops, key)?;
    assert!(value.is_some());
    assert_eq!(Vec::from(&b"temp"[..]), value.unwrap());

    // delete use key of integer, &str, String
    database.delete(&write_ops, &1000)?;
    let value = database.get(&read_ops, &1000)?;
    assert!(value.is_none());

    // delete use key of type &[u8]
    database.delete_u8(&write_ops, &b"temp"[..])?;
    let value = database.get(&read_ops, &&b"key"[..])?;
    assert!(value.is_none());

    // iterator
    let iter = database.iter(&read_ops);

    let mut key_and_values = vec![("name", "tom"), ("age", "5"), ("from", "mars")];
    key_and_values.sort();

    for entry in iter.enumerate() {
        let (i, (key, value)) = entry;
        let key_str = String::from_utf8_lossy(key.as_slice());
        let value_str = String::from_utf8_lossy(value.as_slice());

        let (expected_key, expected_value) = key_and_values.get(i).unwrap();

        assert_eq!(*expected_key, &key_str.to_string());
        assert_eq!(*expected_value, &value_str.to_string());
    }

    Ok(())
}
