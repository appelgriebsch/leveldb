use crate::utils::{db_put_u8_simple, open_database, temp_dir};
use leveldb::compaction::Compaction;

#[test]
fn test_iterator_from_to() {
    let tmp = temp_dir("compact");
    let database = &mut open_database(tmp.path(), true);
    db_put_u8_simple(database, &[1], &[1]);
    db_put_u8_simple(database, &[2], &[2]);
    db_put_u8_simple(database, &[3], &[3]);
    db_put_u8_simple(database, &[4], &[4]);
    db_put_u8_simple(database, &[5], &[5]);

    database.compact(&[2], &[4]);
}
