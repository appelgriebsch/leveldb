use crate::utils::{db_put_simple, db_put_u8_simple, open_database, temp_dir};
use leveldb::iterator::Iterable;
use leveldb::iterator::LevelDBIterator;
use leveldb::options::ReadOptions;
use leveldb::util::FromU8;

#[test]
fn test_iterator() {
    let tmp = temp_dir("iter");
    let database = &mut open_database(tmp.path(), true);
    db_put_simple(database, &1, &[1]);
    db_put_simple(database, &2, &[2]);

    let read_opts = ReadOptions::new();
    let mut iter = database.iter(&read_opts);

    let entry = iter.next();
    assert!(entry.is_some());
    let (key_u8, value) = entry.unwrap();
    let key = i32::from_u8(&key_u8);
    assert_eq!((key, value), (1, vec![1]));

    let entry2 = iter.next();
    assert!(entry2.is_some());
    let (key_u8, value) = entry2.unwrap();
    let key = i32::from_u8(&key_u8);

    assert_eq!((key, value), (2, vec![2]));
    assert!(iter.next().is_none());
}

#[test]
fn test_iterator_reverse() {
    let tmp = temp_dir("iter");
    let database = &mut open_database(tmp.path(), true);
    db_put_simple(database, &99, &[1]);
    db_put_simple(database, &100, &[2]);

    let read_opts = ReadOptions::new();
    let mut iter = database.iter(&read_opts).reverse();

    let entry = iter.next();
    assert!(entry.is_some());
    let (key_u8, value) = entry.unwrap();
    let key = i32::from_u8(&key_u8);
    assert_eq!((key, value), (100, vec![2]));

    let entry2 = iter.next();
    assert!(entry2.is_some());
    let (key_u8, value) = entry2.unwrap();
    let key = i32::from_u8(&key_u8);

    assert_eq!((key, value), (99, vec![1]));
    assert!(iter.next().is_none());
}

#[test]
fn test_iterator_last() {
    let tmp = temp_dir("iter_last");
    let database = &mut open_database(tmp.path(), true);
    db_put_u8_simple(database, &[1], &[1]);
    db_put_u8_simple(database, &[2], &[2]);

    let read_opts = ReadOptions::new();
    let iter = database.iter(&read_opts);

    assert!(iter.last().is_some());
}

#[test]
fn test_iterator_seek() {
    let tmp = temp_dir("from_to");
    let database = &mut open_database(tmp.path(), true);
    db_put_u8_simple(database, &[1], &[1]);
    db_put_u8_simple(database, &[2], &[2]);
    db_put_u8_simple(database, &[3], &[3]);
    db_put_u8_simple(database, &[4], &[4]);
    db_put_u8_simple(database, &[5], &[5]);

    let read_opts = ReadOptions::new();
    let mut iter = database.iter(&read_opts);

    iter.seek(&[2]);

    assert_eq!(iter.next().unwrap(), (vec![2], vec![2]));
    assert_eq!(iter.next().unwrap(), (vec![3], vec![3]));
}

#[test]
fn test_key_iterator() {
    let tmp = temp_dir("key_iter");
    let database = &mut open_database(tmp.path(), true);
    db_put_u8_simple(database, &[1], &[1]);
    db_put_u8_simple(database, &[2], &[2]);

    let read_opts = ReadOptions::new();
    let mut iter = database.keys_iter(&read_opts);
    let value = iter.next().unwrap();

    assert_eq!(value, vec![1]);
}

#[test]
fn test_value_iterator() {
    let tmp = temp_dir("value_iter");
    let database = &mut open_database(tmp.path(), true);
    db_put_u8_simple(database, &[1], &[1]);
    db_put_u8_simple(database, &[2], &[2]);

    let read_opts = ReadOptions::new();
    let mut iter = database.value_iter(&read_opts);
    let value = iter.next().unwrap();
    assert_eq!(value, vec![1]);
}
