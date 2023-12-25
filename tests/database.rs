use leveldb::database::Database;
use leveldb::options::Options;

use crate::utils::temp_dir;

#[test]
fn test_open_database() {
    let mut opts = Options::new();
    opts.create_if_missing = true;

    let tmp = temp_dir("create_if_missing");

    let res: Result<Database, _> = Database::open(tmp.path(), &opts);

    assert!(res.is_ok());
}

#[test]
fn test_open_nonexistent_database_without_create() {
    let mut opts = Options::new();
    opts.create_if_missing = false;
    let tmp = temp_dir("failed_if_missing");

    let res: Result<Database, _> = Database::open(tmp.path(), &opts);
    assert!(res.is_err());
}
