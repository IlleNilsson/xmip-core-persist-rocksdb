# xmip-core-persist-rocksdb

The runtime store's engine: RocksDB (ADR-0015, amendment 2026-09-25). A
technology of [xmip-core-persist](https://github.com/IlleNilsson/xmip-core-persist).

`RocksDb::open(directory)` is a `persist::Engine`, and
`persist::EncryptedStore::open(RocksDb::open(dir)?, &keys, &kek)` is the
runtime store. It keeps what the layer hands it — a keyed hash as the key, a
sealed record as the value — and encrypts nothing itself; RocksDB's own
encryption hook is not used (ADR-0063 clause 2).

- Every write is synced before it returns.
- No compression: every value is ciphertext, which does not compress, and each
  codec is one more C library to build.
- A directory is opened by one process at a time, as RocksDB's lock file
  enforces; `write_new` is one step under a lock of its own within it.
- `flush` writes the memory table to its files for a clean close; what is
  written is durable in the write-ahead log either way.

## Building

RocksDB is C++. The `rocksdb` crate compiles it with the platform's C++
compiler and binds it through bindgen, which loads libclang while it builds:
`prerequisite.toml` declares both (`libclang`, and `cxx` on Linux). The first
Windows build takes some twenty minutes; later builds reuse it.

## Verification

`persist::fixture::conformance` over a real database directory, on Windows
and on the AlmaLinux guest: a record back after the database is closed and
reopened, neither the record's key, its kind nor its value anywhere in the
directory's files (write-ahead log and tables alike), a tampered record
refused with its scope, the store refused under another key of the same
name; and a directory opened twice is refused. The workflow is manual-only
and calls the versioned shared workflow at `IlleNilsson/.github@v1`.
