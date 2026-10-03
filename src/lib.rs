#![forbid(unsafe_code)]

//! The runtime store's engine: `RocksDB` (ADR-0015, amendment 2026-09-25).
//!
//! Messages, Journeys and checkpoints are written constantly and read by
//! key, which is what a log-structured engine is built for. [`RocksDb`] is a
//! [`persist::Engine`]: it keeps the bytes [`persist::EncryptedStore`] hands
//! it — a keyed hash as the key, a sealed record as the value — and knows
//! nothing of either. It encrypts nothing itself, and `RocksDB`'s own
//! encryption hook is not used: the layer above is the one way (ADR-0063
//! clause 2).
//!
//! Every write is synced before it returns, so a record a caller was told
//! is written survives the machine stopping — except a deferred one
//! ([`persist::Engine::apply_deferred`]), which goes into the write-ahead
//! log unsynced and is synced with the next synced write: how a Stream's
//! chunks wait for their Publication's one sync. A batch is one `WriteBatch`,
//! all of it or none, under one sync; and writers in several threads that
//! sync at once share one sync of the write-ahead log, `RocksDB`'s own
//! group commit (`deployment-model.md` section 7: *group commit shares one
//! sync among concurrent writes*). A directory is opened by one
//! process at a time, which `RocksDB`'s lock file enforces; within it,
//! [`persist::Engine::write_new`] is one step under a lock of its own.

use persist::{Change, Engine, PersistError};
use rocksdb::{DB, DBCompressionType, Options, WriteBatch, WriteOptions};
use std::path::Path;
use std::sync::Mutex;

/// What a failure's scope names this engine.
const ENGINE: &str = "rocksdb";

/// A `RocksDB` database in one directory.
pub struct RocksDb {
    db: DB,
    /// Held across the read and the write of `write_new`, so two callers in
    /// this process cannot both find a key free.
    claim: Mutex<()>,
}

impl RocksDb {
    /// The database in `directory`, created when there is none.
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] when `RocksDB` cannot open it — another
    /// process holding it among the reasons.
    pub fn open(directory: &Path) -> Result<Self, PersistError> {
        let mut options = Options::default();
        options.create_if_missing(true);
        // Ciphertext does not compress; trying costs time for nothing.
        options.set_compression_type(DBCompressionType::None);
        let db = DB::open(&options, directory).map_err(failed)?;
        Ok(Self {
            db,
            claim: Mutex::new(()),
        })
    }

    /// The memory table written to its files now, rather than when
    /// `RocksDB` chooses. What is written is durable either way, in the
    /// write-ahead log; this is for a clean close.
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] when the flush fails.
    pub fn flush(&self) -> Result<(), PersistError> {
        self.db.flush().map_err(failed)
    }
}

fn failed(error: rocksdb::Error) -> PersistError {
    PersistError::engine(ENGINE, error)
}

/// `batch` as one `RocksDB` write.
fn batched(batch: &[Change]) -> WriteBatch {
    let mut written = WriteBatch::default();
    for (key, value) in batch {
        match value {
            Some(value) => written.put(key, value),
            None => written.delete(key),
        }
    }
    written
}

/// Writes that are on disk when they return.
fn synced() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options
}

impl Engine for RocksDb {
    fn engine(&self) -> &'static str {
        ENGINE
    }

    fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, PersistError> {
        self.db.get(key).map_err(failed)
    }

    fn write(&self, key: &[u8], value: &[u8]) -> Result<(), PersistError> {
        self.db.put_opt(key, value, &synced()).map_err(failed)
    }

    fn write_new(&self, key: &[u8], value: &[u8]) -> Result<bool, PersistError> {
        let _claim = self
            .claim
            .lock()
            .map_err(|_| PersistError::engine(ENGINE, "the claim lock is poisoned"))?;
        if self.db.get_pinned(key).map_err(failed)?.is_some() {
            return Ok(false);
        }
        self.write(key, value)?;
        Ok(true)
    }

    fn remove(&self, key: &[u8]) -> Result<(), PersistError> {
        self.db.delete_opt(key, &synced()).map_err(failed)
    }

    fn apply(&self, batch: &[Change]) -> Result<(), PersistError> {
        self.db.write_opt(batched(batch), &synced()).map_err(failed)
    }

    /// Into the write-ahead log, unsynced: the next synced write syncs the
    /// log, and with it this.
    fn apply_deferred(&self, batch: &[Change]) -> Result<(), PersistError> {
        self.db
            .write_opt(batched(batch), &WriteOptions::default())
            .map_err(failed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use persist::fixture::{conformance, everything_in};
    use std::path::PathBuf;

    fn directory(test: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "xmip-persist-rocksdb-{test}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        path
    }

    #[test]
    fn rocksdb_keeps_only_sealed_records() {
        let dir = directory("conformance");
        let open = || {
            let db = RocksDb::open(&dir).expect("open");
            db.flush().expect("flushed");
            db
        };
        conformance(open, || everything_in(&dir));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_deferred_write_is_there_with_the_synced_write_after_it() {
        let dir = directory("deferred");
        let db = RocksDb::open(&dir).expect("open");
        db.apply_deferred(&[(b"chunk".to_vec(), Some(b"bytes".to_vec()))])
            .expect("written");
        assert_eq!(db.read(b"chunk").expect("read"), Some(b"bytes".to_vec()));
        db.apply(&[(b"message".to_vec(), Some(b"record".to_vec()))])
            .expect("synced");
        drop(db);
        let again = RocksDb::open(&dir).expect("open again");
        assert_eq!(again.read(b"chunk").expect("read"), Some(b"bytes".to_vec()));
        drop(again);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_directory_is_opened_once() {
        let dir = directory("once");
        let first = RocksDb::open(&dir).expect("open");
        assert!(matches!(
            RocksDb::open(&dir),
            Err(PersistError::Engine { .. })
        ));
        drop(first);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
