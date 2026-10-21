# xmip-core-persist-rocksdb

The runtime store's engine: RocksDB (ADR-0015, amendment 2026-09-25). A
technology of [xmip-core-persist](https://github.com/IlleNilsson/xmip-core-persist).

`RocksDb::open(directory)` is a `persist::Engine`, and
`persist::EncryptedStore::open(RocksDb::open(dir)?, &keys, &kek)` is the
runtime store. It keeps what the layer hands it — a keyed hash, or an index
entry's sortable key, as the key, a sealed record as the value — and encrypts
nothing itself; RocksDB's own encryption hook is not used (ADR-0063 clause 2).

- Every write is synced before it returns, but a deferred one, which waits
  in RocksDB's own buffer of the write-ahead log for the next sync to write
  and sync it (`manual_wal_flush`): it never waits on another thread's sync
  of the log, and goes with the process until synced.
- No compression: every value is ciphertext, which does not compress, and each
  codec is one more C library to build.
- A directory is opened by one process at a time, as RocksDB's lock file
  enforces; `write_new` is one step under a lock of its own within it.
- `scan` reads a range of keys, either way, as one iterator: how an index
  entry is found (`persist::EncryptedStore::scan_index`). The iterator is
  bounded at both ends, so it never steps past the range over keys taken
  out beyond it: a key taken out is a mark `RocksDB` passes, about 85 ns
  each, until a compaction drops it (measured 2026-10-21, the test
  `a_range_from_its_first_kept_key_steps_over_no_key_taken_out`). A read
  of what is due starts where a queue's first due entry is for the same
  reason (`persist`'s `storage/due.rs`).
- `flush` writes the memory table to its files for a clean close; what is
  written is durable in the write-ahead log either way.

## Building

RocksDB is C++. The `rocksdb` crate compiles it with the platform's C++
compiler and binds it through bindgen, which loads libclang while it builds:
`prerequisite.toml` declares both (`libclang`, and `cxx` on Linux). Cloned
alone, the first Windows build compiles it, some twenty minutes, and later
builds reuse it. Under the estate root every build links the one library the
root's `.cargo/config.toml` names instead, compiled once per RocksDB version
by `Install-XmipPrerequisite -Install`.

## Verification

`persist::fixture::conformance` over a real database directory, on Windows
and on the AlmaLinux guest: a record back after the database is closed and
reopened, index entries found by a value within a time, oldest or newest
first, their values in the clear in the files, and neither the record's key,
its kind nor its value anywhere in them (write-ahead log and tables alike), a tampered record refused with its scope, the store
refused under another key of the same name; and a directory opened twice is
refused. The workflow is manual-only and calls the versioned shared workflow
at `IlleNilsson/.github@v1`.
