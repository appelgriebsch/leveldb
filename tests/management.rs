use crate::utils::{open_database, temp_dir};
use leveldb::management::*;
use leveldb::options::*;

#[test]
fn test_destroy_database() {
    let tmp = temp_dir("destroy");
    let database = open_database(tmp.path(), true);

    drop(database);

    let options = Options::new();
    let res = destroy(tmp.path(), &options);
    assert!(res.is_ok());
}

#[test]
fn test_repair_database() {
    let tmp = temp_dir("repair");
    let database = open_database(tmp.path(), true);

    drop(database);

    let options = Options::new();
    let res = repair(tmp.path(), &options);
    assert!(res.is_ok());
}
