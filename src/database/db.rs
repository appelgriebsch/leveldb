use super::bytes::Bytes;
use super::comparator::{create_comparator, Comparator};
use super::error::Error;
use super::key::IntoLevelDBKey;
use super::options::*;
use cruzbit_leveldb_sys::*;
use libc::{c_char, size_t};
use std::ffi::CString;
use std::path::Path;
use std::ptr;

#[allow(missing_docs)]
#[derive(Debug)]
pub(crate) struct RawDB {
    pub(crate) ptr: *mut leveldb_t,
}

#[allow(missing_docs)]
impl Drop for RawDB {
    fn drop(&mut self) {
        unsafe {
            leveldb_close(self.ptr);
        }
    }
}

#[allow(missing_docs)]
#[derive(Debug)]
pub(crate) struct RawComparator {
    ptr: *mut leveldb_comparator_t,
}

#[allow(missing_docs)]
impl Drop for RawComparator {
    fn drop(&mut self) {
        unsafe {
            leveldb_comparator_destroy(self.ptr);
        }
    }
}

#[derive(Debug)]
pub struct Database {
    pub(crate) database: RawDB,
    // this holds a reference passed into leveldb
    // it is never read from Rust, but must be kept around
    #[allow(dead_code)]
    pub(crate) comparator: Option<RawComparator>,
}

unsafe impl Sync for Database {}
unsafe impl Send for Database {}

impl Database {
    fn new(database: *mut leveldb_t, comparator: Option<*mut leveldb_comparator_t>) -> Database {
        let raw_comp = match comparator {
            Some(p) => Some(RawComparator { ptr: p }),
            None => None,
        };

        Database {
            database: RawDB { ptr: database },
            comparator: raw_comp,
        }
    }

    /// Open a new database
    ///
    /// If the database is missing, the behaviour depends on `options.create_if_missing`.
    /// The database will be created using the settings given in `options`.
    pub fn open(name: &Path, options: &Options) -> Result<Database, Error> {
        let mut error = ptr::null_mut();

        unsafe {
            let c_string = CString::new(name.to_str().unwrap()).unwrap();
            let c_options = c_options(options, None);
            let db = leveldb_open(
                c_options as *const leveldb_options_t,
                c_string.as_bytes_with_nul().as_ptr() as *const c_char,
                &mut error,
            );
            leveldb_options_destroy(c_options);

            if error == ptr::null_mut() {
                Ok(Database::new(db, None))
            } else {
                Err(Error::new_from_char(error))
            }
        }
    }

    /// Open a new database with a custom comparator
    ///
    /// If the database is missing, the behaviour depends on `options.create_if_missing`.
    /// The database will be created using the settings given in `options`.
    ///
    /// The comparator must implement a total ordering over the keyspace.
    ///
    /// For keys that implement Ord, consider the `OrdComparator`.
    pub fn open_with_comparator<C: Comparator>(
        name: &Path,
        options: &Options,
        comparator: C,
    ) -> Result<Database, Error> {
        let mut error = ptr::null_mut();
        let comp_ptr = create_comparator(Box::new(comparator));
        unsafe {
            let c_string = CString::new(name.to_str().unwrap()).unwrap();
            let c_options = c_options(options, Some(comp_ptr));
            let db = leveldb_open(
                c_options as *const leveldb_options_t,
                c_string.as_bytes_with_nul().as_ptr() as *const c_char,
                &mut error,
            );
            leveldb_options_destroy(c_options);

            if error == ptr::null_mut() {
                Ok(Database::new(db, Some(comp_ptr)))
            } else {
                Err(Error::new_from_char(error))
            }
        }
    }

    pub fn put(
        &self,
        options: &WriteOptions,
        key: &dyn IntoLevelDBKey,
        value: &[u8],
    ) -> Result<(), Error> {
        key.as_u8_slice_for_write(&|k| self.put_u8(options, k, value))
    }

    pub fn put_u8(&self, options: &WriteOptions, key: &[u8], value: &[u8]) -> Result<(), Error> {
        unsafe {
            let mut error = ptr::null_mut();
            let c_writeoptions = c_writeoptions(options);

            leveldb_put(
                self.database.ptr,
                c_writeoptions,
                key.as_ptr() as *mut c_char,
                key.len() as size_t,
                value.as_ptr() as *mut c_char,
                value.len() as size_t,
                &mut error,
            );

            leveldb_writeoptions_destroy(c_writeoptions);

            if error == ptr::null_mut() {
                Ok(())
            } else {
                Err(Error::new_from_char(error))
            }
        }
    }

    pub fn get(
        &self,
        options: &ReadOptions,
        key: &dyn IntoLevelDBKey,
    ) -> Result<Option<Vec<u8>>, Error> {
        key.as_u8_slice_for_get(&|k| self.get_u8(options, k))
    }

    pub fn get_u8(&self, options: &ReadOptions, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        unsafe {
            let mut error = ptr::null_mut();
            let mut length: size_t = 0;
            let c_readoptions = c_readoptions(options);
            let result = leveldb_get(
                self.database.ptr,
                c_readoptions,
                key.as_ptr() as *mut c_char,
                key.len() as size_t,
                &mut length,
                &mut error,
            );
            leveldb_readoptions_destroy(c_readoptions);

            if error == ptr::null_mut() {
                let bytes_opt = Bytes::from_raw(result as *mut u8, length);

                Ok(bytes_opt.map(|val| val.into()))
            } else {
                Err(Error::new_from_char(error))
            }
        }
    }

    pub fn delete(&self, options: &WriteOptions, key: &dyn IntoLevelDBKey) -> Result<(), Error> {
        key.as_u8_slice_for_write(&|k| self.delete_u8(options, k))
    }

    pub fn delete_u8(&self, options: &WriteOptions, key: &[u8]) -> Result<(), Error> {
        unsafe {
            let mut error = ptr::null_mut();
            let c_writeoptions = c_writeoptions(&options);

            leveldb_delete(
                self.database.ptr,
                c_writeoptions,
                key.as_ptr() as *mut c_char,
                key.len() as size_t,
                &mut error,
            );

            leveldb_writeoptions_destroy(c_writeoptions);

            if error == ptr::null_mut() {
                Ok(())
            } else {
                Err(Error::new_from_char(error))
            }
        }
    }
}
