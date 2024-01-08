# Rust leveldb bindings

Almost-complete bindings for leveldb for Rust.

# Fork

This is a fork of [leveldb](https://github.com/skade/leveldb) for use with [cruzbit](https://github.com/christian-smith/cruzbit). The fork has these main differences:

- &[u8] byte slices can be used as keys
- keys can be iterated using from, to, or a byte prefix
- DatabaseReader trait has been added to support reading from either Database or Snapshot
- leveldb-sys has been updated to the latest release of leveldb and snappy

# License

MIT, see `LICENSE`
