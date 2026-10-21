#![forbid(unsafe_code)]

//! The runtime store's engine: `RocksDB` (ADR-0015, amendment 2026-09-25).
//!
//! Messages, Journeys and checkpoints are written constantly and read by
//! key, which is what a log-structured engine is built for. [`RocksDb`] is a
//! [`persist::Engine`]: it keeps the bytes [`persist::EncryptedStore`] hands
//! it — a keyed hash, or an index entry's sortable key, as the key, a sealed
//! record as the value — and knows nothing of either; a range of keys is one
//! iterator ([`persist::Engine::scan`]). It encrypts nothing itself, and
//! `RocksDB`'s own encryption hook is not used: the layer above is the one
//! way (ADR-0063 clause 2).
//!
//! Every write is synced before it returns, so a record a caller was told
//! is written survives the machine stopping — except a deferred one
//! ([`persist::Engine::apply_deferred`]), which goes into the write-ahead
//! log unsynced and is synced with the next synced write: how a Stream's
//! chunks wait for their Publication's one sync. The log is written to its
//! file only by a sync (`RocksDB`'s `manual_wal_flush`): a deferred write
//! is kept in `RocksDB`'s own buffer of the log until then, so it never
//! waits on a sync another thread runs — on Windows a write to a file
//! waits while that file is flushed, and a Stream's chunks waited out the
//! previous Message's sync (ADR-0018, amendment 2026-10-20) — and a write
//! not yet synced goes with the process. A batch is one `WriteBatch`,
//! all of it or none, under one sync; and writers in several threads that
//! sync at once share one sync of the write-ahead log, `RocksDB`'s own
//! group commit (`deployment-model.md` section 7: *group commit shares one
//! sync among concurrent writes*). A directory is opened by one
//! process at a time, which `RocksDB`'s lock file enforces; within it,
//! [`persist::Engine::write_new`] is one step under a lock of its own.

use persist::{Change, Engine, Entry, PersistError};
use rocksdb::{
    DB, DBCompressionType, Direction, IteratorMode, Options, ReadOptions, WriteBatch, WriteOptions,
};
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
        // The log written to its file by a sync alone: a write never waits
        // on another thread's sync of it.
        options.set_manual_wal_flush(true);
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

    /// Into the write-ahead log's buffer, unsynced: the next synced write
    /// writes the log and syncs it, and with it this.
    fn apply_deferred(&self, batch: &[Change]) -> Result<(), PersistError> {
        self.db
            .write_opt(batched(batch), &WriteOptions::default())
            .map_err(failed)
    }

    /// The write-ahead log synced: every write before now durable, the
    /// deferred among them.
    fn sync(&self) -> Result<(), PersistError> {
        self.db.flush_wal(true).map_err(failed)
    }

    /// One iterator from one end of the range, stopped at the other or at
    /// `most`: a range in a log-structured engine is a seek and a walk.
    fn scan(
        &self,
        first: &[u8],
        last: &[u8],
        most: usize,
        reverse: bool,
    ) -> Result<Vec<Entry>, PersistError> {
        let (from, direction) = if reverse {
            (last, Direction::Reverse)
        } else {
            (first, Direction::Forward)
        };
        // Bounded at both ends, so the walk never steps past the range over
        // what was taken out beyond it: a key taken out is a mark the
        // iterator passes until a compaction drops it (2026-10-21).
        let mut bounds = ReadOptions::default();
        bounds.set_iterate_lower_bound(first.to_vec());
        bounds.set_iterate_upper_bound([last, &[0]].concat());
        let mut entries = Vec::new();
        for entry in self
            .db
            .iterator_opt(IteratorMode::From(from, direction), bounds)
        {
            let (key, value) = entry.map_err(failed)?;
            let past = if reverse { &*key < first } else { &*key > last };
            if past || entries.len() == most {
                break;
            }
            entries.push((key.into_vec(), value.into_vec()));
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use persist::fixture::{conformance, everything_in};
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

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

    /// The fastest of five scans from `first` to `last`, each finding
    /// `found`.
    fn fastest(db: &RocksDb, (first, last): (&[u8], &[u8]), found: usize) -> Duration {
        (0..5)
            .map(|_| {
                let started = Instant::now();
                let entries = db.scan(first, last, 64, false).expect("scanned");
                assert_eq!(entries.len(), found);
                started.elapsed()
            })
            .min()
            .unwrap_or_default()
    }

    /// A queue sent from: a hundred thousand keys of an index written and
    /// taken out, the marks `RocksDB` keeps of them until it compacts, then
    /// three kept after them and a hundred thousand more taken out after
    /// those. A range read
    /// through the marks steps over each; one from where the first kept key
    /// is — where the Storage node starts a read of what is due
    /// (`persist::storage::DueQueue`) — and bounded at its end steps over
    /// none, in the memory table and once flushed (2026-10-21).
    #[test]
    fn a_range_from_its_first_kept_key_steps_over_no_key_taken_out() {
        let dir = directory("tombstones");
        let db = RocksDb::open(&dir).expect("open");
        let key = |n: u64| [b"due".as_slice(), &n.to_be_bytes()].concat();
        let removed = 100_000u64;
        let gone = |range: std::ops::Range<u64>| {
            for chunk in range.collect::<Vec<_>>().chunks(1024) {
                let put: Vec<Change> = chunk.iter().map(|&n| (key(n), Some(vec![1]))).collect();
                db.apply(&put).expect("put");
                let out: Vec<Change> = chunk.iter().map(|&n| (key(n), None)).collect();
                db.apply(&out).expect("taken out");
            }
        };
        gone(0..removed);
        let kept: Vec<Change> = (0..3).map(|n| (key(removed + n), Some(vec![2]))).collect();
        db.apply(&kept).expect("kept");
        gone(removed + 3..2 * removed);
        let (whole, from_first) = ((key(0), key(u64::MAX)), (key(removed), key(removed + 2)));
        for held in ["in the memory table", "flushed"] {
            let through = fastest(&db, (&whole.0, &whole.1), 3);
            let first = fastest(&db, (&from_first.0, &from_first.1), 3);
            let marks = 2 * removed - 3;
            eprintln!("{held}: through {marks} marks {through:?}, from the first kept {first:?}");
            assert!(first < Duration::from_millis(1), "{held}: {first:?}");
            db.flush().expect("flushed");
        }
        drop(db);
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
