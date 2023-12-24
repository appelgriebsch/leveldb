use libc::{c_char, size_t, c_void};
use std::{slice, ptr};
use super::options::{WriteOptions, c_writeoptions};
use super::error::Error;
use super::db::Database;
use super::key::IntoLevelDBKey;
use cruzbit_leveldb_sys::*;

pub(crate) struct RawWriteBatch {
    pub(crate) ptr: *mut leveldb_writebatch_t,
}

impl Drop for RawWriteBatch {
    fn drop(&mut self) {
        unsafe {
            leveldb_writebatch_destroy(self.ptr);
        }
    }
}

pub struct WriteBatch {
    pub(crate) write_batch: RawWriteBatch,
}

/// Batch access to the database
pub trait Batch {
    /// Write a batch to the database, ensuring success for all items or an error
    fn write(&self, options: &WriteOptions, batch: &WriteBatch) -> Result<(), Error>;
}

impl Batch for Database {
    fn write(&self, options: &WriteOptions, batch: &WriteBatch) -> Result<(), Error> {
        unsafe {
            let mut error = ptr::null_mut();
            let c_write_options = c_writeoptions(options);

            leveldb_write(
                self.database.ptr,
                c_write_options,
                batch.write_batch.ptr,
                &mut error
            );

            if error == ptr::null_mut() {
                Ok(())
            } else {
                Err(Error::new_from_char(error))
            }
        }
    }
}

impl WriteBatch {
    pub fn new() -> WriteBatch {
        let ptr = unsafe { leveldb_writebatch_create() };
        let raw = RawWriteBatch { ptr };

        WriteBatch {
            write_batch: raw,
        }
    }

    /// Clear the writebatch
    pub fn clear(&self) {
        unsafe { leveldb_writebatch_clear(self.write_batch.ptr) };
    }

    /// Batch a put operation
    pub fn put(&self, key: &dyn IntoLevelDBKey, value: &[u8]) {
        let _ = key.as_u8_slice_for_write(&|k| {
            self.put_u8(k, value);

            Ok(())
        });
    }


    pub fn put_u8(&self, key: &[u8], value: &[u8]) {
        unsafe {
            leveldb_writebatch_put(self.write_batch.ptr,
                                   key.as_ptr() as *mut c_char,
                                   key.len() as size_t,
                                   value.as_ptr() as *mut c_char,
                                   value.len() as size_t);
        }
    }

    /// Batch a delete operation
    pub fn delete(&self, key: &dyn IntoLevelDBKey) {
        let _ = key.as_u8_slice_for_write(&|k| {
            self.delete_u8(k);

            Ok(())
        });
    }

    pub fn delete_u8(&self, key: &[u8]) {
        unsafe {
            leveldb_writebatch_delete(self.write_batch.ptr,
                                      key.as_ptr() as *mut c_char,
                                      key.len() as size_t);
        }
    }

    /// Iterate over the writeBatch, returning the resulting iterator
    pub fn iterate<T: WriteBatchIterator>(&mut self, iterator: Box<T>) -> Box<T> {
        unsafe {
            let iter = Box::into_raw(iterator);
            leveldb_writebatch_iterate(self.write_batch.ptr,
                                       iter as *mut c_void,
                                       put_callback::<T>,
                                       deleted_callback::<T>);
            Box::from_raw(iter)
        }
    }
}

/// A trait for iterators to iterate over written batches and check their validity.
pub trait WriteBatchIterator {
    /// Callback for put items
    fn put_u8(&mut self, key: &[u8], value: &[u8]);

    /// Callback for deleted items
    fn deleted_u8(&mut self, key: &[u8]);
}

extern "C" fn put_callback<T: WriteBatchIterator>(
    state: *mut c_void,
    key: *const c_char,
    key_len: size_t,
    val: *const c_char,
    val_len: size_t) {

    unsafe {
        let iter: &mut T = &mut *(state as *mut T);
        let key_slice = slice::from_raw_parts::<u8>(key as *const u8, key_len as usize);
        let val_slice = slice::from_raw_parts::<u8>(val as *const u8, val_len as usize);

        iter.put_u8(key_slice, val_slice);
    }
}

extern "C" fn deleted_callback<T: WriteBatchIterator>(
    state: *mut c_void,
    key: *const c_char,
    key_len: size_t
) {
    unsafe {
        let iter: &mut T = &mut *(state as *mut T);
        let key_slice = slice::from_raw_parts::<u8>(key as *const u8, key_len as usize);

        iter.deleted_u8(key_slice);
    }
}