use crate::utils::{db_put_simple, open_database, temp_dir};
use leveldb::iterator::Iterable;
use leveldb::options::ReadOptions;
use leveldb::snapshots::Snapshots;
use leveldb::util::FromU8;

#[test]
fn test_snapshots() {
    let tmp = temp_dir("snapshots");
    let database = &mut open_database(tmp.path(), true);

    db_put_simple(database, &1, &[1]);
    let snapshot = database.snapshot();

    db_put_simple(database, &2, &[2]);

    let mut read_opts = ReadOptions::new();
    let res = snapshot.get(&mut read_opts, &2);

    assert!(res.is_ok());
    assert_eq!(None, res.unwrap());
}

#[test]
fn test_snapshot_iterator() {
    let tmp = temp_dir("snap_iterator");
    let database = &mut open_database(tmp.path(), true);
    db_put_simple(database, &1, &[1]);
    let snapshot = database.snapshot();

    db_put_simple(database, &2, &[2]);
    let read_opts = ReadOptions::new();

    let mut iter = snapshot.keys_iter(&read_opts);

    let key = iter.next();

    assert!(key.is_some());
    let key_value = key.unwrap();
    let key_value = i32::from_u8(&key_value);

    assert_eq!(key_value, 1);

    let next = iter.next();
    assert_eq!(None, next);
}
