use crate::utils::{db_put_u8_simple, temp_dir};
use leveldb::comparator::Comparator;
use leveldb::database::Database;
use leveldb::iterator::Iterable;
use leveldb::options::{Options, ReadOptions};
use libc::c_char;
use std::cmp::Ordering;

struct ReverseComparator;

impl Comparator for ReverseComparator {
    fn name(&self) -> *const c_char {
        "reverse".as_ptr() as *const c_char
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        b.cmp(a)
    }
}

#[test]
fn test_comparator() {
    let comparator: ReverseComparator = ReverseComparator {};
    let mut opts = Options::new();
    opts.create_if_missing = true;

    let tmp = temp_dir("reverse_comparator");
    let database = &mut Database::open_with_comparator(tmp.path(), &opts, comparator).unwrap();
    // let database = &mut Database::open(tmp.path(), &opts).unwrap();

    db_put_u8_simple(database, &[1], &[1]);
    db_put_u8_simple(database, &[2], &[2]);

    let read_opts = ReadOptions::new();
    let mut iter = database.iter(&read_opts);

    assert_eq!((vec![2], vec![2]), iter.next().unwrap());
    assert_eq!((vec![1], vec![1]), iter.next().unwrap());
}
