//! leveldb snapshots
//!
//! Snapshots give you a reference to the database at a certain
//! point in time and won't change while you work with them.
use cruzbit_leveldb_sys::*;

use super::bytes::Bytes;
use super::db::{Database, DatabaseReader};
use super::error::Error;
use super::iterator::{Iterable, Iterator, KeyIterator, ValueIterator};
use super::key::IntoLevelDBKey;
use super::options::{c_readoptions, ReadOptions};
use libc::{c_char, size_t};
use std::ptr;

#[allow(missing_docs)]
struct RawSnapshot {
    db_ptr: *mut leveldb_t,
    ptr: *mut leveldb_snapshot_t,
}

impl Drop for RawSnapshot {
    fn drop(&mut self) {
        unsafe { leveldb_release_snapshot(self.db_ptr, self.ptr) };
    }
}

/// A database snapshot
///
/// Represents a database at a certain point in time,
/// and allows for all read operations (get and iteration).
pub struct Snapshot<'a> {
    raw: RawSnapshot,
    database: &'a Database,
}

impl<'a> Snapshot<'a> {
    /// fetches a key from the database
    ///
    /// Inserts this snapshot into ReadOptions before reading

    pub fn get(
        &self,
        options: &ReadOptions,
        key: &dyn IntoLevelDBKey,
    ) -> Result<Option<Vec<u8>>, Error> {
        key.as_u8_slice_for_get(&|k| self.get_u8(options, k))
    }

    /// override the get_u8 of Database. Overriding is for avoiding the snapshot field of ReadOption,
    /// if so, a lifetime parameter must be added for ReadOption.
    pub fn get_u8(&self, options: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        unsafe {
            let mut error = ptr::null_mut();
            let mut length: size_t = 0;
            let c_readoptions = c_readoptions(options);

            // add the extra snapshot information to c_readoptions
            leveldb_readoptions_set_snapshot(c_readoptions, self.raw_ptr());

            let result = leveldb_get(
                self.database.database.ptr,
                c_readoptions,
                key.as_ptr() as *mut c_char,
                key.len() as size_t,
                &mut length,
                &mut error,
            );

            leveldb_readoptions_destroy(c_readoptions);

            if error.is_null() {
                let bytes_opt = Bytes::from_raw(result as *mut u8, length);

                Ok(bytes_opt.map(|val| val.into()))
            } else {
                Err(Error::new_from_char(error))
            }
        }
    }

    #[inline]
    #[allow(missing_docs)]
    pub fn raw_ptr(&self) -> *mut leveldb_snapshot_t {
        self.raw.ptr
    }
}

/// Structs implementing the Snapshots trait can be
/// snapshotted.
pub trait Snapshots {
    /// Creates a snapshot and returns a struct
    /// representing it.
    fn snapshot(&self) -> Snapshot;
}

impl Snapshots for Database {
    fn snapshot(&self) -> Snapshot {
        let db_str = self.database.ptr;
        let snap = unsafe { leveldb_create_snapshot(db_str) };

        let raw = RawSnapshot {
            db_ptr: db_str,
            ptr: snap,
        };

        Snapshot {
            raw,
            database: self,
        }
    }
}

impl<'a> Iterable<'a> for Snapshot<'a> {
    fn iter(&'a self, options: &ReadOptions) -> Iterator<'a> {
        Iterator::new(self.database, options, Some(self))
    }

    fn keys_iter(&'a self, options: &ReadOptions) -> KeyIterator<'a> {
        KeyIterator::new(self.database, options, Some(self))
    }

    fn value_iter(&'a self, options: &ReadOptions) -> ValueIterator<'a> {
        ValueIterator::new(self.database, options, Some(self))
    }
}

impl DatabaseReader for Snapshot<'_> {
    fn get(
        &self,
        options: &ReadOptions,
        key: &dyn IntoLevelDBKey,
    ) -> Result<Option<Vec<u8>>, Error> {
        self.get(options, key)
    }

    fn get_u8(
        &self,
        options: &ReadOptions,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        self.get_u8(options, key)
    }
}
