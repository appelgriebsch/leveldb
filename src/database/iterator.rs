//! leveldb iterators
//!
//! Iteration is one of the most important parts of leveldb. This module provides
//! Iterators to iterate over key, values and pairs of both.
use libc::{size_t, c_char};
use std::iter;
use super::Database;
use super::options::{ReadOptions, c_readoptions};
use std::slice::from_raw_parts;
use std::marker::PhantomData;
use crate::database::snapshots::Snapshot;
use cruzbit_leveldb_sys::*;

#[allow(missing_docs)]
struct RawIterator {
    ptr: *mut leveldb_iterator_t,
}

#[allow(missing_docs)]
impl Drop for RawIterator {
    fn drop(&mut self) {
        unsafe { leveldb_iter_destroy(self.ptr) }
    }
}

/// An iterator over the leveldb keyspace.
///
/// Returns key and value as a tuple.
pub struct Iterator<'a> {
    iter: RawIterator,
    // Iterator accesses the Database through a leveldb_iter_t pointer
    // but needs to hold the reference for lifetime tracking
    #[allow(dead_code)]
    database: PhantomData<&'a Database>,

}

/// An iterator over the leveldb keyspace  that browses the keys backwards.
///
/// Returns key and value as a tuple.
pub struct RevIterator<'a> {
    iter: RawIterator,
    // Iterator accesses the Database through a leveldb_iter_t pointer
    // but needs to hold the reference for lifetime tracking
    #[allow(dead_code)]
    database: PhantomData<&'a Database>,
}

/// An iterator over the leveldb keyspace.
///
/// Returns just the keys.
pub struct KeyIterator<'a> {
    inner: Iterator<'a>,
}

/// An iterator over the leveldb keyspace that browses the keys backwards.
///
/// Returns just the keys.
pub struct RevKeyIterator<'a> {
    inner: RevIterator<'a>,
}

/// An iterator over the leveldb keyspace.
///
/// Returns just the value.
pub struct ValueIterator<'a> {
    inner: Iterator<'a>,
}

/// An iterator over the leveldb keyspace that browses the keys backwards.
///
/// Returns just the value.
pub struct RevValueIterator<'a> {
    inner: RevIterator<'a>,
}

/// A trait to allow access to the three main iteration styles of leveldb.
pub trait Iterable<'a> {
    /// Return an Iterator iterating over (Key,Value) pairs
    fn iter(&'a self, options: &ReadOptions) -> Iterator<'a>;
    /// Returns an Iterator iterating over Keys only.
    fn keys_iter(&'a self, options: &ReadOptions) -> KeyIterator<'a>;
    /// Returns an Iterator iterating over Values only.
    fn value_iter(&'a self, options: &ReadOptions) -> ValueIterator<'a>;
}

impl<'a> Iterable<'a> for Database {
    fn iter(&'a self, options: &ReadOptions) -> Iterator<'a> {
        Iterator::new(self, options, None)
    }

    fn keys_iter(&'a self, options: &ReadOptions) -> KeyIterator<'a> {
        KeyIterator::new(self, options, None)
    }

    fn value_iter(&'a self, options: &ReadOptions) -> ValueIterator<'a> {
        ValueIterator::new(self, options, None)
    }
}

pub trait LevelDBIterator<'a> {
    type RevIter: LevelDBIterator<'a>;

    fn raw_iterator(&self) -> *mut leveldb_iterator_t;

    fn reverse(self) -> Self::RevIter;

    fn valid(&self) -> bool {
        unsafe { leveldb_iter_valid(self.raw_iterator()) != 0 }
    }

    #[inline]
    unsafe fn advance_raw(&mut self) {
        leveldb_iter_next(self.raw_iterator());
    }

    fn advance(&mut self) {
        unsafe { self.advance_raw(); }
    }

    fn key(&self) -> Vec<u8> {
        unsafe {
            let length: size_t = 0;
            let value = leveldb_iter_key(self.raw_iterator(), &length) as *const u8;
            from_raw_parts(value, length as usize).to_vec()
        }
    }

    fn value(&self) -> Vec<u8> {
        unsafe {
            let length: size_t = 0;
            let value = leveldb_iter_value(self.raw_iterator(), &length) as *const u8;
            from_raw_parts(value, length as usize).to_vec()
        }
    }

    fn entry(&self) -> (Vec<u8>, Vec<u8>) {
        (self.key(), self.value())
    }

    fn seek_to_first(&self) {
        unsafe { leveldb_iter_seek_to_first(self.raw_iterator()); }
    }

    fn seek_to_last(&self) {
        unsafe {
                leveldb_iter_seek_to_last(self.raw_iterator());
        }
    }

    fn seek(&self, key: &[u8]) {
        unsafe {
            leveldb_iter_seek(self.raw_iterator(), key.as_ptr() as *mut c_char, key.len() as size_t);
        }
    }
}

impl<'a> Iterator<'a> {
    pub fn new(database: &'a Database, options: &ReadOptions, snapshot: Option<&'a Snapshot>) -> Iterator<'a> {
        unsafe {
            let c_read_options = c_readoptions(options);

            if let Some(snapshot) = snapshot {
                leveldb_readoptions_set_snapshot(c_read_options, snapshot.raw_ptr());
            }

            let ptr = leveldb_create_iterator(database.database.ptr, c_read_options);

            leveldb_readoptions_destroy(c_read_options);
            leveldb_iter_seek_to_first(ptr);

            Iterator {
                iter: RawIterator { ptr },
                database: PhantomData,
            }
        }
    }

    /// return the last element of the iterator
    pub fn last(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.seek_to_last();
        Some((self.key(), self.value()))
    }
}

impl<'a> LevelDBIterator<'a> for Iterator<'a> {
    type RevIter = RevIterator<'a>;

    #[inline]
    fn raw_iterator(&self) -> *mut leveldb_iterator_t {
        self.iter.ptr
    }

    fn reverse(self) -> Self::RevIter {
        self.seek_to_last();

        RevIterator {
            database: self.database,
            iter: self.iter,
        }
    }
}

impl<'a> LevelDBIterator<'a> for RevIterator<'a> {
    type RevIter = Iterator<'a>;

    #[inline]
    fn raw_iterator(&self) -> *mut leveldb_iterator_t {
        self.iter.ptr
    }

    fn reverse(self) -> Self::RevIter {
        self.seek_to_last();

        Iterator {
            database: self.database,
            iter: self.iter,
        }
    }

    #[inline]
    unsafe fn advance_raw(&mut self) {
        leveldb_iter_prev(self.raw_iterator());
    }
}

impl<'a> KeyIterator<'a> {
    pub fn new(database: &'a Database, options: &ReadOptions, snapshot: Option<&'a Snapshot>) -> KeyIterator<'a> {
        KeyIterator { inner: Iterator::new(database, options, snapshot) }
    }

    /// return the last element of the iterator
    pub fn last(self) -> Option<Vec<u8>> {
        self.seek_to_last();
        Some(self.key())
    }
}

impl<'a> ValueIterator<'a> {
    pub fn new(database: &'a Database, options: &ReadOptions, snapshot: Option<&'a Snapshot>) -> ValueIterator<'a> {
        ValueIterator { inner: Iterator::new(database, options, snapshot) }
    }

    /// return the last element of the iterator
    pub fn last(self) -> Option<Vec<u8>> {
        self.seek_to_last();
        Some(self.value())
    }
}

macro_rules! impl_leveldb_iterator {
    ($T:ty, $RevT:ty) => {
        impl<'a> LevelDBIterator<'a> for $T {
            type RevIter = $RevT;

            #[inline]
            fn raw_iterator(&self) -> *mut leveldb_iterator_t {
                self.inner.iter.ptr
            }

            #[inline]
            unsafe fn advance_raw(&mut self) {
                self.inner.advance_raw();
            }

            #[inline]
            fn reverse(self) -> Self::RevIter {
                Self::RevIter { inner: self.inner.reverse() }
            }
        }
    };
}

impl_leveldb_iterator!(KeyIterator<'a>, RevKeyIterator<'a>);
impl_leveldb_iterator!(RevKeyIterator<'a>, KeyIterator<'a>);
impl_leveldb_iterator!(ValueIterator<'a>, RevValueIterator<'a>);
impl_leveldb_iterator!(RevValueIterator<'a>, ValueIterator<'a>);

macro_rules! impl_iterator {
    ($T:ty, $Item:ty, $ItemMethod:ident) => {
        impl<'a> iter::Iterator for $T {
            type Item = $Item;

            fn next(&mut self) -> Option<Self::Item> {
                if (self.valid()) {
                    let ret = Some(self.$ItemMethod());
                    self.advance();

                    ret
                } else {
                    None
                }
            }
        }
    };
}

impl_iterator!(Iterator<'a>, (Vec<u8>,Vec<u8>), entry);
impl_iterator!(RevIterator<'a>, (Vec<u8>,Vec<u8>), entry);
impl_iterator!(KeyIterator<'a>, Vec<u8>, key);
impl_iterator!(RevKeyIterator<'a>, Vec<u8>, key);
impl_iterator!(ValueIterator<'a>, Vec<u8>, value);
impl_iterator!(RevValueIterator<'a>, Vec<u8>, key);
