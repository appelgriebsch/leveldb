//! leveldb iterators
//!
//! Iteration is one of the most important parts of leveldb. This module provides
//! Iterators to iterate over key, values and pairs of both.
use super::options::{c_readoptions, ReadOptions};
use super::Database;
use crate::database::snapshots::Snapshot;
use cruzbit_leveldb_sys::*;
use libc::{c_char, size_t};
use std::iter;
use std::marker::PhantomData;
use std::slice::from_raw_parts;

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
    start: bool,
    // Iterator accesses the Database through a leveldb_iter_t pointer
    // but needs to hold the reference for lifetime tracking
    #[allow(dead_code)]
    database: PhantomData<&'a Database>,
    from: Option<&'a [u8]>,
    to: Option<&'a [u8]>,
    prefix: Option<&'a [u8]>,
}

/// An iterator over the leveldb keyspace  that browses the keys backwards.
///
/// Returns key and value as a tuple.
pub struct RevIterator<'a> {
    iter: RawIterator,
    start: bool,
    // Iterator accesses the Database through a leveldb_iter_t pointer
    // but needs to hold the reference for lifetime tracking
    #[allow(dead_code)]
    database: PhantomData<&'a Database>,
    from: Option<&'a [u8]>,
    to: Option<&'a [u8]>,
    prefix: Option<&'a [u8]>,
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

    fn start(&self) -> bool;
    fn started(&mut self);

    fn reverse(self) -> Self::RevIter;

    fn from(self, key: &'a [u8]) -> Self;
    fn to(self, key: &'a [u8]) -> Self;
    fn prefix(self, key: &'a [u8]) -> Self;

    fn from_key(&self) -> Option<&'a [u8]>;
    fn to_key(&self) -> Option<&'a [u8]>;
    fn prefix_key(&self) -> Option<&'a [u8]>;

    fn valid(&self, reverse: bool) -> bool {
        if unsafe { leveldb_iter_valid(self.raw_iterator()) != 0 } {
            if let Some(k) = self.prefix_key() {
                // match the key with a byte prefix
                if self.key()[..].starts_with(k) {
                    return true;
                }
            } else {
                let from = if let Some(k) = self.from_key() {
                    let comparator: fn(&[u8], &[u8]) -> bool = if reverse {
                        |a: &[u8], b: &[u8]| -> bool { a <= b }
                    } else {
                        |a: &[u8], b: &[u8]| -> bool { a >= b }
                    };
                    comparator(&self.key()[..], k)
                } else {
                    true
                };
                let to = if let Some(k) = self.to_key() {
                    let comparator: fn(&[u8], &[u8]) -> bool = if reverse {
                        |a: &[u8], b: &[u8]| -> bool { a >= b }
                    } else {
                        |a: &[u8], b: &[u8]| -> bool { a <= b }
                    };
                    comparator(&self.key()[..], k)
                } else {
                    true
                };

                return from && to;
            }
        }

        false
    }

    unsafe fn advance_raw(&mut self);

    fn advance(&mut self, reverse: bool) -> bool {
        if !self.start() {
            unsafe {
                self.advance_raw();
            }
        } else {
            if let Some(k) = self.prefix_key() {
                self.seek(k)
            } else if let Some(k) = self.from_key() {
                self.seek(k);
                if !self.valid(reverse) && reverse {
                    // if from doesn't exist we seeked past it so go back
                    unsafe {
                        self.advance_raw();
                    }
                } else {
                    self.seek(k)
                }
            }
            self.started();
        }

        self.valid(reverse)
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
        unsafe {
            leveldb_iter_seek_to_first(self.raw_iterator());
        }
    }

    fn seek_to_last(&self) {
        if let Some(k) = self.to_key() {
            self.seek(k);
        } else {
            unsafe {
                leveldb_iter_seek_to_last(self.raw_iterator());
            }
        }
    }

    fn seek(&self, key: &[u8]) {
        unsafe {
            leveldb_iter_seek(
                self.raw_iterator(),
                key.as_ptr() as *mut c_char,
                key.len() as size_t,
            );
        }
    }
}

impl<'a> Iterator<'a> {
    pub fn new(
        database: &'a Database,
        options: &ReadOptions,
        snapshot: Option<&'a Snapshot>,
    ) -> Iterator<'a> {
        unsafe {
            let c_read_options = c_readoptions(options);

            if let Some(snapshot) = snapshot {
                leveldb_readoptions_set_snapshot(c_read_options, snapshot.raw_ptr());
            }

            let ptr = leveldb_create_iterator(database.database.ptr, c_read_options);

            leveldb_readoptions_destroy(c_read_options);
            leveldb_iter_seek_to_first(ptr);

            Iterator {
                start: true,
                iter: RawIterator { ptr },
                database: PhantomData,
                from: None,
                to: None,
                prefix: None,
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

    #[inline]
    fn start(&self) -> bool {
        self.start
    }

    #[inline]
    fn started(&mut self) {
        self.start = false
    }

    #[inline]
    unsafe fn advance_raw(&mut self) {
        leveldb_iter_next(self.raw_iterator());
    }

    #[inline]
    fn reverse(self) -> Self::RevIter {
        if self.start {
            unsafe {
                leveldb_iter_seek_to_last(self.iter.ptr);
            }
        }
        RevIterator {
            start: self.start,
            database: self.database,
            iter: self.iter,
            from: self.from,
            to: self.to,
            prefix: self.prefix,
        }
    }

    fn from(mut self, key: &'a [u8]) -> Self {
        self.from = Some(key);
        self
    }

    fn to(mut self, key: &'a [u8]) -> Self {
        self.to = Some(key);
        self
    }

    fn prefix(mut self, key: &'a [u8]) -> Self {
        self.prefix = Some(key);
        self
    }

    fn from_key(&self) -> Option<&'a [u8]> {
        self.from
    }

    fn to_key(&self) -> Option<&'a [u8]> {
        self.to
    }

    fn prefix_key(&self) -> Option<&'a [u8]> {
        self.prefix
    }
}

impl<'a> LevelDBIterator<'a> for RevIterator<'a> {
    type RevIter = Iterator<'a>;

    #[inline]
    fn raw_iterator(&self) -> *mut leveldb_iterator_t {
        self.iter.ptr
    }

    #[inline]
    fn start(&self) -> bool {
        self.start
    }

    #[inline]
    fn started(&mut self) {
        self.start = false
    }

    #[inline]
    unsafe fn advance_raw(&mut self) {
        leveldb_iter_prev(self.raw_iterator());
    }

    #[inline]
    fn reverse(self) -> Self::RevIter {
        if self.start {
            unsafe {
                leveldb_iter_seek_to_first(self.iter.ptr);
            }
        }
        Iterator {
            start: self.start,
            database: self.database,
            iter: self.iter,
            from: self.from,
            to: self.to,
            prefix: self.prefix,
        }
    }

    fn from(mut self, key: &'a [u8]) -> Self {
        self.from = Some(key);
        self
    }

    fn to(mut self, key: &'a [u8]) -> Self {
        self.to = Some(key);
        self
    }

    fn prefix(mut self, key: &'a [u8]) -> Self {
        self.prefix = Some(key);
        self
    }

    fn from_key(&self) -> Option<&'a [u8]> {
        self.from
    }

    fn to_key(&self) -> Option<&'a [u8]> {
        self.to
    }

    fn prefix_key(&self) -> Option<&'a [u8]> {
        self.prefix
    }
}

impl<'a> KeyIterator<'a> {
    pub fn new(
        database: &'a Database,
        options: &ReadOptions,
        snapshot: Option<&'a Snapshot>,
    ) -> KeyIterator<'a> {
        KeyIterator {
            inner: Iterator::new(database, options, snapshot),
        }
    }

    /// return the last element of the iterator
    pub fn last(self) -> Option<Vec<u8>> {
        self.seek_to_last();
        Some(self.key())
    }
}

impl<'a> ValueIterator<'a> {
    pub fn new(
        database: &'a Database,
        options: &ReadOptions,
        snapshot: Option<&'a Snapshot>,
    ) -> ValueIterator<'a> {
        ValueIterator {
            inner: Iterator::new(database, options, snapshot),
        }
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
            fn start(&self) -> bool {
                self.inner.start
            }

            #[inline]
            fn started(&mut self) {
                self.inner.start = false
            }

            #[inline]
            unsafe fn advance_raw(&mut self) {
                self.inner.advance_raw();
            }

            #[inline]
            fn reverse(self) -> Self::RevIter {
                Self::RevIter {
                    inner: self.inner.reverse(),
                }
            }

            fn from(mut self, key: &'a [u8]) -> Self {
                self.inner.from = Some(key);
                self
            }

            fn to(mut self, key: &'a [u8]) -> Self {
                self.inner.to = Some(key);
                self
            }

            fn prefix(mut self, key: &'a [u8]) -> Self {
                self.inner.prefix = Some(key);
                self
            }

            fn from_key(&self) -> Option<&'a [u8]> {
                self.inner.from
            }

            fn to_key(&self) -> Option<&'a [u8]> {
                self.inner.to
            }

            fn prefix_key(&self) -> Option<&'a [u8]> {
                self.inner.prefix
            }
        }
    };
}

impl_leveldb_iterator!(KeyIterator<'a>, RevKeyIterator<'a>);
impl_leveldb_iterator!(RevKeyIterator<'a>, KeyIterator<'a>);
impl_leveldb_iterator!(ValueIterator<'a>, RevValueIterator<'a>);
impl_leveldb_iterator!(RevValueIterator<'a>, ValueIterator<'a>);

macro_rules! impl_iterator {
    ($T:ty, $Item:ty, $ItemMethod:ident, $Rev:expr) => {
        impl<'a> iter::Iterator for $T {
            type Item = $Item;

            fn next(&mut self) -> Option<Self::Item> {
                if self.advance($Rev) {
                    Some(self.$ItemMethod())
                } else {
                    None
                }
            }
        }
    };
}

impl_iterator!(Iterator<'a>, (Vec<u8>, Vec<u8>), entry, false);
impl_iterator!(RevIterator<'a>, (Vec<u8>, Vec<u8>), entry, true);
impl_iterator!(KeyIterator<'a>, Vec<u8>, key, false);
impl_iterator!(RevKeyIterator<'a>, Vec<u8>, key, true);
impl_iterator!(ValueIterator<'a>, Vec<u8>, value, false);
impl_iterator!(RevValueIterator<'a>, Vec<u8>, key, true);
