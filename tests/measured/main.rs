//! The module's timing program (`doc/governance/rust-style.md` section
//! 5c): `RocksDB` held to the millisecond rule as the Storage node reads it.
//! A plain `cargo test` leaves it out; the landing runs it after every other
//! test of the module, alone, one test at a time (`cargo test --test
//! measured --all-features -- --test-threads=1`), so what is measured is
//! Xmip's work and not its neighbors':
//!
//! - a range from its first kept key steps over no key taken out.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use persist::{Change, Engine};
use xmip_core_persist_rocksdb::RocksDb;

fn directory(test: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "xmip-persist-rocksdb-measured-{test}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    path
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
