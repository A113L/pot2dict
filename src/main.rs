use std::cmp::Ordering;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::Result;
use bumpalo::Bump;
use clap::Parser;
use dashmap::DashMap;
use hashbrown::HashMap;
use indicatif::{ProgressBar, ProgressStyle};
use memmap2::{Mmap, MmapMut};
use parking_lot::{Mutex, RwLock};
use rayon::prelude::*;
use rustc_hash::FxBuildHasher;
use smallvec::SmallVec;
use std::hash::BuildHasherDefault;
use crossbeam_channel::{bounded, Sender};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
use rustc_hash::FxHasher;
use sysinfo::{Disks, System};
use tempfile::NamedTempFile;

pub type FastMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;
pub type GlobalMap = DashMap<Vec<u8>, u64, FxBuildHasher>;
const GLOBAL_MAP_SHARDS: usize = 1024;

type PwKey = SmallVec<[u8; 24]>;

struct ArenaPool {
    arenas: Vec<Bump>,
}
unsafe impl Sync for ArenaPool {}

static ARENA_POOL: OnceLock<ArenaPool> = OnceLock::new();
static ARENA_ALLOCS: AtomicUsize = AtomicUsize::new(0);

fn arena_pool(num_threads: usize) -> &'static ArenaPool {
    ARENA_POOL.get_or_init(|| ArenaPool {
        arenas: (0..num_threads.max(1)).map(|_| Bump::new()).collect(),
    })
}

#[inline(always)]
fn arena_alloc(pool: &'static ArenaPool, bytes: &[u8]) -> &'static [u8] {
    let idx = rayon::current_thread_index().unwrap_or(0) % pool.arenas.len();
    let bump = &pool.arenas[idx];
    let slice = bump.alloc_slice_copy(bytes);
    ARENA_ALLOCS.fetch_add(1, AtomicOrdering::Relaxed);
    unsafe { std::mem::transmute::<&[u8], &'static [u8]>(slice) }
}

#[inline(always)]
fn trim_end(mut line: &[u8]) -> &[u8] {
    while let Some(&b) = line.last() {
        if b == b'\r' || b == b'\n' {
            line = &line[..line.len() - 1];
        } else {
            break;
        }
    }
    line
}

#[inline(always)]
fn extract_password(line: &[u8], keep_trailing_colon: bool) -> Option<&[u8]> {
    let line = trim_end(line);
    if line.is_empty() {
        return None;
    }
    if let Some(last_colon) = memchr::memrchr(b':', line) {
        let pw = &line[last_colon + 1..];
        if !pw.is_empty() {
            return Some(pw);
        }
        if keep_trailing_colon {
            return Some(line);
        }
        return None;
    }
    Some(line)
}

#[inline(always)]
fn bump_count(map: &mut FastMap<PwKey, u64>, pw: &[u8]) {
    match map.raw_entry_mut().from_key(pw) {
        hashbrown::hash_map::RawEntryMut::Occupied(mut e) => {
            *e.get_mut() += 1;
        }
        hashbrown::hash_map::RawEntryMut::Vacant(e) => {
            e.insert(PwKey::from_slice(pw), 1);
        }
    }
}

#[inline(always)]
fn bump_count_arena(map: &mut FastMap<&'static [u8], u64>, pool: &'static ArenaPool, pw: &[u8]) {
    match map.raw_entry_mut().from_key(pw) {
        hashbrown::hash_map::RawEntryMut::Occupied(mut e) => {
            *e.get_mut() += 1;
        }
        hashbrown::hash_map::RawEntryMut::Vacant(e) => {
            let arena_slice = arena_alloc(pool, pw);
            e.insert(arena_slice, 1);
        }
    }
}

fn count_chunk(chunk: &[u8], keep_trailing_colon: bool) -> FastMap<PwKey, u64> {
    let mut map = FastMap::default();
    map.reserve(chunk.len() / 12 + 16);
    let mut start = 0usize;
    while let Some(end) = memchr::memchr(b'\n', &chunk[start..]) {
        let line = &chunk[start..start + end];
        if let Some(pw) = extract_password(line, keep_trailing_colon) {
            bump_count(&mut map, pw);
        }
        start += end + 1;
    }
    if start < chunk.len() {
        if let Some(pw) = extract_password(&chunk[start..], keep_trailing_colon) {
            bump_count(&mut map, pw);
        }
    }
    map
}

fn count_chunk_arena(
    chunk: &[u8],
    keep_trailing_colon: bool,
    pool: &'static ArenaPool,
) -> FastMap<&'static [u8], u64> {
    let mut map: FastMap<&'static [u8], u64> = FastMap::default();
    map.reserve(chunk.len() / 12 + 16);
    let mut start = 0usize;
    while let Some(end) = memchr::memchr(b'\n', &chunk[start..]) {
        let line = &chunk[start..start + end];
        if let Some(pw) = extract_password(line, keep_trailing_colon) {
            bump_count_arena(&mut map, pool, pw);
        }
        start += end + 1;
    }
    if start < chunk.len() {
        if let Some(pw) = extract_password(&chunk[start..], keep_trailing_colon) {
            bump_count_arena(&mut map, pool, pw);
        }
    }
    map
}

const PER_ENTRY_OVERHEAD_BYTES: usize = 48;

struct DiskCache {
    disks: Disks,
    last_refresh: Instant,
}

static DISK_CACHE: OnceLock<Mutex<DiskCache>> = OnceLock::new();
const DISK_REFRESH_INTERVAL: Duration = Duration::from_secs(2);

fn with_refreshed_disks<T>(f: impl FnOnce(&Disks) -> T) -> T {
    let cache = DISK_CACHE.get_or_init(|| {
        Mutex::new(DiskCache {
            disks: Disks::new_with_refreshed_list(),
            last_refresh: Instant::now(),
        })
    });
    let mut guard = cache.lock();
    if guard.last_refresh.elapsed() >= DISK_REFRESH_INTERVAL {
        guard.disks.refresh();
        guard.last_refresh = Instant::now();
    }
    f(&guard.disks)
}

// Per-path cache for available_space_for(). ensure_disk_space() is called
// once per spill run, which with multiple concurrent spill-writer threads
// can happen many times a second; without this, every single call pays for
// a canonicalize() syscall plus a full scan/filter over the disk list. The
// underlying disk stats themselves are only refreshed at most once per
// DISK_REFRESH_INTERVAL anyway (see with_refreshed_disks), so caching the
// resolved per-path answer on the same interval loses no real freshness.
static AVAIL_SPACE_CACHE: OnceLock<Mutex<std::collections::HashMap<PathBuf, (Instant, Option<u64>)>>> =
    OnceLock::new();

fn available_space_for(path: &std::path::Path) -> Option<u64> {
    let cache = AVAIL_SPACE_CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()));

    if let Some((ts, val)) = cache.lock().get(path) {
        if ts.elapsed() < DISK_REFRESH_INTERVAL {
            return *val;
        }
    }

    let probe = if path.exists() {
        path.to_path_buf()
    } else {
        match path.ancestors().find(|p| p.exists()) {
            Some(p) => p.to_path_buf(),
            None => {
                cache.lock().insert(path.to_path_buf(), (Instant::now(), None));
                return None;
            }
        }
    };
    let Ok(probe) = probe.canonicalize() else {
        cache.lock().insert(path.to_path_buf(), (Instant::now(), None));
        return None;
    };

    let result = with_refreshed_disks(|disks| {
        disks
            .iter()
            .filter(|d| probe.starts_with(d.mount_point()))
            .max_by_key(|d| d.mount_point().as_os_str().len())
            .map(|d| d.available_space())
    });

    cache.lock().insert(path.to_path_buf(), (Instant::now(), result));
    result
}

fn ensure_disk_space(dir: &std::path::Path, estimated_bytes: u64, context: &str) -> Result<()> {
    const SAFETY_MARGIN: f64 = 1.15;
    const WARN_MULTIPLIER: f64 = 2.0;

    let Some(avail) = available_space_for(dir) else {
        return Ok(());
    };

    let required = (estimated_bytes as f64 * SAFETY_MARGIN) as u64;

    if avail < required {
        anyhow::bail!(
            "Not enough disk space in '{}' for {}: need ~{:.2} GB (with headroom), only {:.2} GB available. \
             Free up space, point --temp-dir at a larger volume, or lower --count-mem/--max-mem to spill smaller runs.",
            dir.display(),
            context,
            required as f64 / (1024.0 * 1024.0 * 1024.0),
            avail as f64 / (1024.0 * 1024.0 * 1024.0)
        );
    }

    if (avail as f64) < required as f64 * WARN_MULTIPLIER {
        eprintln!(
            "Warning: disk space in '{}' is getting low ({:.2} GB free; {} needs ~{:.2} GB).",
            dir.display(),
            avail as f64 / (1024.0 * 1024.0 * 1024.0),
            context,
            required as f64 / (1024.0 * 1024.0 * 1024.0)
        );
    }

    Ok(())
}

// Runs for the lifetime of the process, polling free space on the
// --temp-dir volume and printing a warning to stderr if it gets low.
// ensure_disk_space() already warns/fails at each individual spill
// checkpoint, but long stretches of work (e.g. the parallel pre-merge, or
// a big counting pass between spills) can run for a while without hitting
// one of those checkpoints, during which the temp volume could still be
// draining from other spill-writer threads. This gives a continuously
// live signal instead of the first sign of trouble being a hard failure.
// Intentionally not joined: it's a daemon-style thread that dies with the
// process on exit.
fn spawn_disk_space_watchdog(temp_dir: Option<PathBuf>, total_input_bytes: u64) {
    let watch_dir = temp_dir.unwrap_or_else(std::env::temp_dir);
    let warn_floor_bytes: u64 = 2 * 1024 * 1024 * 1024; // 2 GB
    // Scale the warning threshold with input size (spilling a bigger input
    // needs more headroom) but never below the floor, so small jobs don't
    // get spurious warnings from normal free-space fluctuation.
    let warn_threshold = (total_input_bytes / 10).max(warn_floor_bytes);

    let _ = std::thread::Builder::new()
        .name("disk-space-watchdog".to_string())
        .spawn(move || {
            let mut last_warned: Option<Instant> = None;
            loop {
                std::thread::sleep(Duration::from_secs(5));
                let Some(avail) = available_space_for(&watch_dir) else {
                    continue;
                };
                if avail < warn_threshold {
                    let should_warn = match last_warned {
                        None => true,
                        Some(t) => t.elapsed() >= Duration::from_secs(30),
                    };
                    if should_warn {
                        eprintln!(
                            "Warning: temp dir '{}' is running low on space ({:.2} GB free).",
                            watch_dir.display(),
                            avail as f64 / (1024.0 * 1024.0 * 1024.0)
                        );
                        last_warned = Some(Instant::now());
                    }
                }
            }
        });
}

pub enum FinalizedResult {
    InMemory(u64, Vec<Record>),
    Spilled {
        runs: Vec<tempfile::TempPath>,
        sort_mode: String,
    },
}

struct SpillJob {
    entries: Vec<(Vec<u8>, u64)>,
    run_number: usize,
}

pub struct CountingIndex {
    map: GlobalMap,
    approx_bytes: AtomicUsize,
    budget_bytes: usize,
    temp_dir: Option<PathBuf>,
    spilled_runs: Arc<Mutex<Vec<tempfile::TempPath>>>,
    spill_lock: RwLock<()>,
    spill_tx: Mutex<Option<Sender<SpillJob>>>,
    // Pool of spill-writer threads (see CountingIndex::new). Multiple
    // threads let sort+write for one spill run overlap with another,
    // instead of serializing all spills behind a single worker whenever
    // the temp disk is slow or contended.
    spill_threads: Mutex<Vec<std::thread::JoinHandle<Result<()>>>>,
    progress: ProgressBar,
    spill_count: AtomicUsize,
}

impl CountingIndex {
    fn new(budget_bytes: usize, temp_dir: Option<PathBuf>, progress: ProgressBar) -> Self {
        let spilled_runs = Arc::new(Mutex::new(Vec::new()));

        let (tx, rx) = bounded::<SpillJob>(8);
        let writer_temp_dir = temp_dir.clone();
        let writer_runs = Arc::clone(&spilled_runs);
        let writer_progress = progress.clone();
        // Use several spill-writer threads pulling from the same mpmc
        // channel (crossbeam_channel::Receiver is Clone and safe to share
        // across threads) so that sort+write for one spill run can overlap
        // with another, instead of serializing every spill behind a single
        // worker. This matters most once spill counts get large and/or the
        // temp disk is slow: with one writer thread, the whole counting
        // pipeline stalls the moment a single write is slow, because the
        // bounded channel fills up and producer threads block on send().
        //
        // Thread count scales off rayon::current_num_threads() (i.e. -p /
        // number of cores), not a fixed constant: a wider -p implies both
        // faster spill production (more producer threads feeding the
        // channel) and typically more concurrent I/O capacity available, so
        // a single-machine-sized magic number under- or over-provisions
        // depending on hardware. Clamped to a sane range since spill
        // writing is disk-bound, not CPU-bound — beyond a handful of
        // threads, more of them just contends for the same I/O queue
        // without adding throughput.
        let spill_writer_threads = (rayon::current_num_threads() / 4).clamp(2, 8);
        let mut spill_threads = Vec::with_capacity(spill_writer_threads);
        for worker_id in 0..spill_writer_threads {
            let rx = rx.clone();
            let writer_temp_dir = writer_temp_dir.clone();
            let writer_runs = Arc::clone(&writer_runs);
            let writer_progress = writer_progress.clone();
            let handle = std::thread::Builder::new()
                .name(format!("spill-writer-{worker_id}"))
                .spawn(move || -> Result<()> {
                    while let Ok(job) = rx.recv() {
                        let spill_dir = writer_temp_dir.clone().unwrap_or_else(std::env::temp_dir);
                        let approx_bytes: u64 = job
                            .entries
                            .iter()
                            .map(|(k, _)| (k.len() + PER_ENTRY_OVERHEAD_BYTES) as u64)
                            .sum();
                        ensure_disk_space(&spill_dir, approx_bytes, "counting-phase spill")?;

                        let mut entries = job.entries;
                        // Safe to use the parallel sort here: compressed-file
                        // support (which used to permanently pin every rayon
                        // worker thread) has been removed, so this can
                        // freely borrow CPU cores from the global pool even
                        // while multiple spill-writer threads are doing the
                        // same thing concurrently — rayon's work-stealing
                        // scheduler handles concurrent scope calls fine.
                        entries.par_sort_unstable_by(|a, b| a.0.cmp(&b.0));

                        let temp_path = write_entries_to_temp(&entries, &writer_temp_dir, false)?;
                        writer_runs.lock().push(temp_path);

                        // Notify that this spill run has been written to disk
                        writer_progress.set_message(format!(
                            "spill run #{} written to disk ({} entries)",
                            job.run_number,
                            entries.len()
                        ));
                    }
                    Ok(())
                })
                .expect("failed to spawn spill-writer thread");
            spill_threads.push(handle);
        }

        CountingIndex {
            map: DashMap::with_capacity_and_hasher_and_shard_amount(
                1 << 20,
                FxBuildHasher,
                GLOBAL_MAP_SHARDS,
            ),
            approx_bytes: AtomicUsize::new(0),
            budget_bytes,
            temp_dir,
            spilled_runs,
            spill_lock: RwLock::new(()),
            spill_tx: Mutex::new(Some(tx)),
            spill_threads: Mutex::new(spill_threads),
            progress,
            spill_count: AtomicUsize::new(0),
        }
    }

    fn spill_dir(&self) -> PathBuf {
        self.temp_dir.clone().unwrap_or_else(std::env::temp_dir)
    }

    fn maybe_spill(&self) -> Result<()> {
        if self.approx_bytes.load(AtomicOrdering::Relaxed) < self.budget_bytes {
            return Ok(());
        }

        // CRITICAL: Clone sender BEFORE taking write lock to avoid
        // holding spill_lock while blocking on channel send.
        let tx_opt = self.spill_tx.lock().clone();
        let tx = match tx_opt {
            Some(tx) => tx,
            None => return Ok(()), // Channel already closed, probably finalizing
        };

        let _guard = self.spill_lock.write();
        if self.approx_bytes.load(AtomicOrdering::Relaxed) < self.budget_bytes {
            return Ok(());
        }
        if self.map.is_empty() {
            return Ok(());
        }

        let approx_bytes_now = self.approx_bytes.load(AtomicOrdering::Relaxed) as u64;
        let approx_gb = approx_bytes_now as f64 / (1024.0 * 1024.0 * 1024.0);
        let spill_num = self.spill_count.fetch_add(1, AtomicOrdering::Relaxed) + 1;
        self.progress.set_message(format!(
            "spilling run #{} to disk (~{:.2} GB)...",
            spill_num,
            approx_gb
        ));

        // dashmap's "rayon" feature (enabled in Cargo.toml) gives us
        // par_iter(), so collect this in parallel instead of on one thread.
        let entries: Vec<(Vec<u8>, u64)> = self
            .map
            .par_iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect();
        self.map.clear();
        self.approx_bytes.store(0, AtomicOrdering::Relaxed);

        // Release write lock BEFORE blocking send
        drop(_guard);

        tx.send(SpillJob { entries, run_number: spill_num })
            .map_err(|_| anyhow::anyhow!("spill-writer thread terminated unexpectedly"))?;
        Ok(())
    }

    fn has_spilled(&self) -> bool {
        !self.spilled_runs.lock().is_empty()
    }

    // NOTE: &mut self because CountingIndex implements Drop, so we cannot
    // move fields out of a by-value self. We use std::mem::replace/take
    // to extract ownership of the map and spilled runs.
    fn finalize(&mut self, sort_mode: &str) -> Result<FinalizedResult> {
        let spill_dir = self.spill_dir();

        // Close channel and wait for background writers
        self.spill_tx.lock().take();
        for handle in self.spill_threads.lock().drain(..) {
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("spill-writer thread panicked"))??;
        }

        let mut spilled_runs = std::mem::take(&mut *self.spilled_runs.lock());

        if spilled_runs.is_empty() {
            let unique = self.map.len() as u64;
            let map = std::mem::replace(
                &mut self.map,
                DashMap::with_hasher_and_shard_amount(FxBuildHasher, GLOBAL_MAP_SHARDS),
            );
            let records: Vec<Record> = if sort_mode == "frequency" {
                map.into_iter()
                    .map(|(pw, freq)| Record {
                        key: -(freq as i64),
                        pw,
                    })
                    .collect()
            } else {
                map.into_iter()
                    .map(|(pw, _freq)| Record { key: 0, pw })
                    .collect()
            };
            return Ok(FinalizedResult::InMemory(unique, records));
        }

        if !self.map.is_empty() {
            let approx_bytes_now = self.approx_bytes.load(AtomicOrdering::Relaxed) as u64;
            ensure_disk_space(&spill_dir, approx_bytes_now, "final counting-phase spill")?;

            let mut entries: Vec<(Vec<u8>, u64)> = self
                .map
                .par_iter()
                .map(|entry| (entry.key().clone(), *entry.value()))
                .collect();
            entries.par_sort_unstable_by(|a, b| a.0.cmp(&b.0));
            let temp_path = write_entries_to_temp(&entries, &self.temp_dir, false)?;
            spilled_runs.push(temp_path);
        }

        let spilled_run_count = spilled_runs.len();
        eprintln!(
            "Merging {} spilled counting runs from disk...",
            spilled_run_count
        );

        // Parallel pre-pass: fan the N spilled runs out across cores,
        // merging+deduping groups of them concurrently, before handing the
        // (much smaller) result to the existing single-threaded merge. This
        // is what actually parallelizes "Merging N spilled counting runs
        // from disk..." -- without it that phase is a single thread walking
        // all N runs with one heap.
        let spilled_runs = parallel_merge_counting_runs(spilled_runs, &self.temp_dir)?;
        if spilled_runs.len() != spilled_run_count {
            eprintln!(
                "Parallel pre-merge done: {} runs -> {} runs; finishing merge...",
                spilled_run_count,
                spilled_runs.len()
            );
        }

        Ok(FinalizedResult::Spilled {
            runs: spilled_runs,
            sort_mode: sort_mode.to_string(),
        })
    }
}

impl Drop for CountingIndex {
    fn drop(&mut self) {
        // If finalize() already ran, spill_tx/spill_threads are already
        // empty/joined and this is a no-op. If we're here because an error
        // propagated out before finalize() was called, close the channel
        // (so rx.recv() unblocks) and join the writer threads.
        self.spill_tx.lock().take();
        for handle in self.spill_threads.lock().drain(..) {
            let _ = handle.join();
        }
    }
}

fn fold_into_dashmap(index: &CountingIndex, local: FastMap<PwKey, u64>) {
    let _guard = index.spill_lock.read();
    for (k, v) in local {
        // Fast path: probe with a borrowed key first to avoid allocating a
        // Vec<u8> on every fold when the key is already present (which is
        // the common case for password lists, since a small number of very
        // common passwords account for a large share of all lines).
        if let Some(mut slot) = index.map.get_mut(k.as_slice()) {
            *slot += v;
            continue;
        }
        match index.map.entry(k.to_vec()) {
            dashmap::mapref::entry::Entry::Occupied(mut e) => {
                // Lost the race between the probe and the entry() call.
                *e.get_mut() += v;
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                index
                    .approx_bytes
                    .fetch_add(e.key().len() + PER_ENTRY_OVERHEAD_BYTES, AtomicOrdering::Relaxed);
                e.insert(v);
            }
        }
    }
}

fn fold_into_dashmap_arena(index: &CountingIndex, local: FastMap<&'static [u8], u64>) {
    let _guard = index.spill_lock.read();
    for (k, v) in local {
        // Same borrowed-probe-first optimization as fold_into_dashmap.
        if let Some(mut slot) = index.map.get_mut(k) {
            *slot += v;
            continue;
        }
        match index.map.entry(k.to_vec()) {
            dashmap::mapref::entry::Entry::Occupied(mut e) => {
                *e.get_mut() += v;
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                index
                    .approx_bytes
                    .fetch_add(e.key().len() + PER_ENTRY_OVERHEAD_BYTES, AtomicOrdering::Relaxed);
                e.insert(v);
            }
        }
    }
}

/// Merges one group of sorted, spilled counting runs (same on-disk format as
/// produced by write_entries_to_temp: key = raw positive count, entries
/// sorted by password) into a single new sorted+deduped run. This is the
/// same heap-merge logic used by the final sequential merge, just scoped to
/// a subset of runs so it can be run concurrently with other groups.
fn merge_dedup_group(
    paths: &[tempfile::TempPath],
    temp_dir: &Option<PathBuf>,
) -> Result<tempfile::TempPath> {
    struct HeapEntry {
        pw: Vec<u8>,
        count: u64,
        idx: usize,
    }
    impl PartialEq for HeapEntry {
        fn eq(&self, other: &Self) -> bool {
            self.pw == other.pw
        }
    }
    impl Eq for HeapEntry {}
    impl Ord for HeapEntry {
        fn cmp(&self, other: &Self) -> Ordering {
            other.pw.cmp(&self.pw)
        }
    }
    impl PartialOrd for HeapEntry {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut readers: Vec<BufReader<File>> = paths
        .iter()
        .map(|p| -> Result<BufReader<File>> {
            Ok(BufReader::with_capacity(4 * 1024 * 1024, File::open(p)?))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut heap = std::collections::BinaryHeap::with_capacity(readers.len());
    for (idx, r) in readers.iter_mut().enumerate() {
        if let Some(rec) = read_record(r)? {
            heap.push(HeapEntry {
                pw: rec.pw,
                count: rec.key as u64,
                idx,
            });
        }
    }

    let temp = if let Some(dir) = temp_dir {
        NamedTempFile::new_in(dir)?
    } else {
        NamedTempFile::new()?
    };
    {
        let mut writer = BufWriter::with_capacity(4 * 1024 * 1024, &temp);
        while let Some(HeapEntry { pw, count, idx }) = heap.pop() {
            let mut total = count;
            if let Some(next) = read_record(&mut readers[idx])? {
                heap.push(HeapEntry {
                    pw: next.pw,
                    count: next.key as u64,
                    idx,
                });
            }
            while let Some(top) = heap.peek() {
                if top.pw != pw {
                    break;
                }
                let HeapEntry {
                    count: c2,
                    idx: idx2,
                    ..
                } = heap.pop().unwrap();
                total += c2;
                if let Some(next2) = read_record(&mut readers[idx2])? {
                    heap.push(HeapEntry {
                        pw: next2.pw,
                        count: next2.key as u64,
                        idx: idx2,
                    });
                }
            }
            // Keep the same key convention as write_entries_to_temp with
            // negate_key=false: key is the raw (positive) merged count.
            write_record(&mut writer, &Record { key: total as i64, pw })?;
        }
        writer.flush()?;
    }
    Ok(temp.into_temp_path())
}

/// Parallel merge-tree pre-pass: instead of one thread walking all N spilled
/// runs with a single heap, split the runs into ~num_threads groups, merge
/// each group (dedup + sum counts) concurrently on separate cores, and
/// return the much smaller set of intermediate runs. The existing
/// single-threaded merge (stream_merge_and_write / external_sort_spilled_runs)
/// then only has to walk that small set, so the expensive O(N) fan-in work
/// is parallelized while the final merge stays simple and correct.
///
/// Takes ownership of `runs` because each group's input files are consumed
/// (read once, then dropped/deleted) as soon as its intermediate run is
/// produced, which also keeps peak temp-disk usage down.
fn parallel_merge_counting_runs(
    runs: Vec<tempfile::TempPath>,
    temp_dir: &Option<PathBuf>,
) -> Result<Vec<tempfile::TempPath>> {
    let num_threads = rayon::current_num_threads().max(1);

    // Not enough runs for grouping to help (each group needs >=2 runs to
    // actually reduce anything), or only one core available: skip the
    // pre-pass and let the caller's sequential merge handle it directly.
    if num_threads <= 1 || runs.len() < num_threads * 2 {
        return Ok(runs);
    }

    let group_size = ((runs.len() + num_threads - 1) / num_threads).max(2);

    let mut groups: Vec<Vec<tempfile::TempPath>> = Vec::new();
    let mut iter = runs.into_iter();
    loop {
        let group: Vec<_> = iter.by_ref().take(group_size).collect();
        if group.is_empty() {
            break;
        }
        groups.push(group);
    }

    if groups.len() <= 1 {
        // Only formed a single group; flatten it back out, nothing to
        // parallelize.
        return Ok(groups.into_iter().flatten().collect());
    }

    let total_groups = groups.len();
    eprintln!(
        "Parallel pre-merge: {} runs -> {} groups across up to {} threads...",
        groups.iter().map(|g| g.len()).sum::<usize>(),
        total_groups,
        num_threads
    );

    // This stage can run for a long time on large inputs (each group is a
    // full k-way merge+dedup of its share of the spilled runs) with no
    // other output in between, which looks identical to a hang. A spinner
    // with a steady tick (ticks on a timer, independent of any group
    // actually finishing) plus a position that advances as groups complete
    // gives a continuously-live, cross-platform-safe indicator that the
    // program is still working. indicatif renders plain ASCII fallbacks on
    // terminals that don't support fancier glyphs, so this is safe on any
    // platform/terminal.
    let pb = ProgressBar::new(total_groups as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] pre-merge groups: {pos}/{len} ({percent}%)  {msg}",
        )
        .unwrap()
        .progress_chars("#>-"),
    );
    pb.enable_steady_tick(Duration::from_millis(120));
    pb.set_message("merging + deduping run groups...");

    // The merge groups themselves only call ensure_disk_space() once they
    // go to write their output run, so a temp volume that's draining
    // during this (potentially long) stage would otherwise go unnoticed
    // until a write fails outright. Poll free space on the side and surface
    // a warning through the same progress bar (pb.println keeps the
    // spinner line intact) instead of letting the failure be the first
    // sign anything was wrong.
    let watch_dir = temp_dir.clone().unwrap_or_else(std::env::temp_dir);
    let stop_watch = Arc::new(AtomicBool::new(false));
    let watcher = {
        let stop_watch = Arc::clone(&stop_watch);
        let pb = pb.clone();
        std::thread::Builder::new()
            .name("pre-merge-disk-watch".to_string())
            .spawn(move || {
                let mut last_warned = false;
                while !stop_watch.load(AtomicOrdering::Relaxed) {
                    if let Some(avail) = available_space_for(&watch_dir) {
                        const LOW_SPACE_WARN_BYTES: u64 = 2 * 1024 * 1024 * 1024;
                        if avail < LOW_SPACE_WARN_BYTES {
                            if !last_warned {
                                pb.println(format!(
                                    "Warning: temp dir '{}' is low on space ({:.2} GB free) during pre-merge.",
                                    watch_dir.display(),
                                    avail as f64 / (1024.0 * 1024.0 * 1024.0)
                                ));
                                last_warned = true;
                            }
                        } else {
                            last_warned = false;
                        }
                    }
                    std::thread::sleep(Duration::from_secs(3));
                }
            })
            .expect("failed to spawn pre-merge disk-watch thread")
    };

    let merged_result: Result<Vec<tempfile::TempPath>> = groups
        .into_par_iter()
        .map(|g| {
            let result = merge_dedup_group(&g, temp_dir);
            pb.inc(1);
            result
        })
        .collect();

    stop_watch.store(true, AtomicOrdering::Relaxed);
    let _ = watcher.join();
    pb.finish_and_clear();

    merged_result
}

fn stream_merge_and_write(
    runs: &[tempfile::TempPath],
    out: &mut impl Write,
    progress: &ProgressBar,
) -> Result<u64> {
    struct HeapEntry {
        pw: Vec<u8>,
        count: u64,
        idx: usize,
    }
    impl PartialEq for HeapEntry {
        fn eq(&self, other: &Self) -> bool {
            self.pw == other.pw
        }
    }
    impl Eq for HeapEntry {}
    impl Ord for HeapEntry {
        fn cmp(&self, other: &Self) -> Ordering {
            other.pw.cmp(&self.pw)
        }
    }
    impl PartialOrd for HeapEntry {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut readers: Vec<BufReader<File>> = runs
        .iter()
        .map(|p| -> Result<BufReader<File>> {
            Ok(BufReader::with_capacity(4 * 1024 * 1024, File::open(p)?))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut heap = std::collections::BinaryHeap::with_capacity(readers.len());
    for (idx, r) in readers.iter_mut().enumerate() {
        if let Some(rec) = read_record(r)? {
            heap.push(HeapEntry {
                pw: rec.pw,
                count: rec.key as u64,
                idx,
            });
        }
    }

    let mut unique: u64 = 0;
    while let Some(HeapEntry { pw, count, idx }) = heap.pop() {
        let mut _total = count;
        if let Some(next) = read_record(&mut readers[idx])? {
            heap.push(HeapEntry {
                pw: next.pw,
                count: next.key as u64,
                idx,
            });
        }
        while let Some(top) = heap.peek() {
            if top.pw != pw {
                break;
            }
            let HeapEntry {
                count: c2,
                idx: idx2,
                ..
            } = heap.pop().unwrap();
            _total += c2;
            if let Some(next2) = read_record(&mut readers[idx2])? {
                heap.push(HeapEntry {
                    pw: next2.pw,
                    count: next2.key as u64,
                    idx: idx2,
                });
            }
        }
        out.write_all(&pw)?;
        out.write_all(b"\n")?;
        progress.inc(1);
        unique += 1;
    }
    Ok(unique)
}

fn external_sort_spilled_runs(
    runs: &[tempfile::TempPath],
    max_mem_bytes: usize,
    temp_dir: Option<PathBuf>,
    out: &mut impl Write,
    progress: &ProgressBar,
) -> Result<u64> {
    struct CountHeapEntry {
        pw: Vec<u8>,
        count: u64,
        idx: usize,
    }
    impl PartialEq for CountHeapEntry {
        fn eq(&self, other: &Self) -> bool {
            self.pw == other.pw
        }
    }
    impl Eq for CountHeapEntry {}
    impl Ord for CountHeapEntry {
        fn cmp(&self, other: &Self) -> Ordering {
            other.pw.cmp(&self.pw)
        }
    }
    impl PartialOrd for CountHeapEntry {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    let spill_dir = temp_dir.clone().unwrap_or_else(std::env::temp_dir);

    let runs_total_bytes: u64 = runs
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok())
        .map(|m| m.len())
        .sum();
    ensure_disk_space(&spill_dir, runs_total_bytes, "merged counting run")?;

    let merged = if let Some(ref dir) = temp_dir {
        NamedTempFile::new_in(dir)?
    } else {
        NamedTempFile::new()?
    };
    {
        let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, &merged);
        let mut readers: Vec<BufReader<File>> = runs
            .iter()
            .map(|p| -> Result<BufReader<File>> {
                Ok(BufReader::with_capacity(4 * 1024 * 1024, File::open(p)?))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut heap = std::collections::BinaryHeap::with_capacity(readers.len());
        for (idx, r) in readers.iter_mut().enumerate() {
            if let Some(rec) = read_record(r)? {
                heap.push(CountHeapEntry {
                    pw: rec.pw,
                    count: rec.key as u64,
                    idx,
                });
            }
        }

        while let Some(CountHeapEntry { pw, count, idx }) = heap.pop() {
            let mut total = count;
            if let Some(next) = read_record(&mut readers[idx])? {
                heap.push(CountHeapEntry {
                    pw: next.pw,
                    count: next.key as u64,
                    idx,
                });
            }
            while let Some(top) = heap.peek() {
                if top.pw != pw {
                    break;
                }
                let CountHeapEntry {
                    count: c2,
                    idx: idx2,
                    ..
                } = heap.pop().unwrap();
                total += c2;
                if let Some(next2) = read_record(&mut readers[idx2])? {
                    heap.push(CountHeapEntry {
                        pw: next2.pw,
                        count: next2.key as u64,
                        idx: idx2,
                    });
                }
            }
            write_record(
                &mut writer,
                &Record {
                    key: -(total as i64),
                    pw,
                },
            )?;
        }
        writer.flush()?;
    }
    let merged_path = merged.into_temp_path();

    let avg_record_size = {
        let mut r = BufReader::with_capacity(4 * 1024 * 1024, File::open(&merged_path)?);
        if let Some(first) = read_record(&mut r)? {
            (first.pw.len() + 24).max(1)
        } else {
            32
        }
    };

    let chunk_len = std::cmp::max(1, max_mem_bytes / avg_record_size.max(1));

    let mut temp_files: Vec<tempfile::TempPath> = Vec::new();
    let mut reader = BufReader::with_capacity(4 * 1024 * 1024, File::open(&merged_path)?);

    loop {
        let mut chunk: Vec<Record> = Vec::with_capacity(chunk_len);
        let mut chunk_bytes = 0usize;
        while chunk.len() < chunk_len && chunk_bytes < max_mem_bytes {
            match read_record(&mut reader)? {
                Some(rec) => {
                    chunk_bytes += rec.pw.len() + 24;
                    chunk.push(rec);
                }
                None => break,
            }
        }
        if chunk.is_empty() {
            break;
        }
        chunk.par_sort_unstable();

        ensure_disk_space(&spill_dir, chunk_bytes as u64, "sorted chunk run")?;

        let temp_path = write_records_to_temp(&chunk, &temp_dir)?;
        temp_files.push(temp_path);
    }

    struct HeapEntry {
        rec: Record,
        idx: usize,
    }
    impl PartialEq for HeapEntry {
        fn eq(&self, other: &Self) -> bool {
            self.rec == other.rec
        }
    }
    impl Eq for HeapEntry {}
    impl Ord for HeapEntry {
        fn cmp(&self, other: &Self) -> Ordering {
            other.rec.cmp(&self.rec)
        }
    }
    impl PartialOrd for HeapEntry {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut readers: Vec<BufReader<File>> = temp_files
        .iter()
        .map(|p| BufReader::with_capacity(2 * 1024 * 1024, File::open(p).unwrap()))
        .collect();

    let mut heap = std::collections::BinaryHeap::with_capacity(readers.len());
    for (idx, r) in readers.iter_mut().enumerate() {
        if let Some(rec) = read_record(r)? {
            heap.push(HeapEntry { rec, idx });
        }
    }

    let mut out_writer = BufWriter::with_capacity(16 * 1024 * 1024, &mut *out);
    let mut unique: u64 = 0;
    while let Some(HeapEntry { rec, idx }) = heap.pop() {
        out_writer.write_all(&rec.pw)?;
        out_writer.write_all(b"\n")?;
        progress.inc(1);
        unique += 1;
        if let Some(next) = read_record(&mut readers[idx])? {
            heap.push(HeapEntry { rec: next, idx });
        }
    }
    out_writer.flush()?;
    Ok(unique)
}

fn split_into_chunks(data: &[u8], target_chunk_size: usize) -> Vec<&[u8]> {
    let total_len = data.len();
    if target_chunk_size >= total_len {
        return vec![data];
    }
    let num_chunks = (total_len + target_chunk_size - 1) / target_chunk_size;
    let mut chunks = Vec::with_capacity(num_chunks);
    let mut start = 0;
    while start < total_len {
        let end = (start + target_chunk_size).min(total_len);
        let end = if end < total_len {
            match memchr::memchr(b'\n', &data[end..]) {
                Some(offset) => end + offset + 1,
                None => total_len,
            }
        } else {
            total_len
        };
        if start < end {
            chunks.push(&data[start..end]);
        }
        start = end;
    }
    chunks
}

fn read_file(
    path: &PathBuf,
    pb: &ProgressBar,
    index: &CountingIndex,
    keep_trailing_colon: bool,
    use_arena: bool,
    chunk_batch_size: Option<usize>,
) -> Result<u64> {
    let file = File::open(path)?;
    let file_size = file.metadata()?.len() as usize;

    eprintln!("Processing {} ({} bytes)...", path.display(), file_size);
    let mmap = unsafe { Mmap::map(&file)? };
    #[cfg(unix)]
    {
        let _ = mmap.advise(memmap2::Advice::Sequential);
    }
    const IO_CHUNK_SIZE: usize = 16 * 1024 * 1024;
    let chunks = split_into_chunks(&mmap, IO_CHUNK_SIZE);
    eprintln!("Split into {} chunks.", chunks.len());

    let total_chunks = chunks.len();
    let chunks_done = AtomicU64::new(0);
    let lines_done = AtomicU64::new(0);

    let batch_size: usize =
        chunk_batch_size.unwrap_or_else(|| rayon::current_num_threads()).max(1);

    for batch in chunks.chunks(batch_size) {
        if use_arena {
            let pool = arena_pool(rayon::current_num_threads());
            batch
                .par_iter()
                .fold(
                    || FastMap::<&'static [u8], u64>::default(),
                    |mut acc, chunk| {
                        let m = count_chunk_arena(chunk, keep_trailing_colon, pool);
                        let lines: u64 = m.values().sum();
                        lines_done.fetch_add(lines, AtomicOrdering::Relaxed);
                        let done = chunks_done.fetch_add(1, AtomicOrdering::Relaxed) + 1;
                        pb.inc(chunk.len() as u64);
                        if done % 16 == 0 || done == total_chunks as u64 {
                            pb.set_message(format!("{} / {} chunks done", done, total_chunks));
                        }
                        acc.reserve(m.len());
                        for (k, v) in m {
                            *acc.entry(k).or_insert(0) += v;
                        }
                        acc
                    },
                )
                .for_each(|local| fold_into_dashmap_arena(index, local));
        } else {
            batch
                .par_iter()
                .fold(
                    || FastMap::<PwKey, u64>::default(),
                    |mut acc, chunk| {
                        let m = count_chunk(chunk, keep_trailing_colon);
                        let lines: u64 = m.values().sum();
                        lines_done.fetch_add(lines, AtomicOrdering::Relaxed);
                        let done = chunks_done.fetch_add(1, AtomicOrdering::Relaxed) + 1;
                        pb.inc(chunk.len() as u64);
                        if done % 16 == 0 || done == total_chunks as u64 {
                            pb.set_message(format!("{} / {} chunks done", done, total_chunks));
                        }
                        acc.reserve(m.len());
                        for (k, v) in m {
                            *acc.entry(k).or_insert(0) += v;
                        }
                        acc
                    },
                )
                .for_each(|local| fold_into_dashmap(index, local));
        }
        index.maybe_spill()?;
    }

    let total_lines = lines_done.load(AtomicOrdering::Relaxed);
    eprintln!("Finished processing {}.", path.display());
    Ok(total_lines)
}

#[derive(Clone)]
pub struct Record {
    key: i64,
    pw: Vec<u8>,
}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.pw == other.pw
    }
}
impl Eq for Record {}
impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Record {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key).then_with(|| self.pw.cmp(&other.pw))
    }
}

#[inline(always)]
fn write_record(w: &mut impl Write, r: &Record) -> io::Result<()> {
    w.write_all(&r.key.to_le_bytes())?;
    w.write_all(&(r.pw.len() as u32).to_le_bytes())?;
    w.write_all(&r.pw)?;
    Ok(())
}

#[inline(always)]
/// Counts the total number of records across a set of spilled run files
/// without reading the password bytes themselves — only the fixed 12-byte
/// header per record is read, and the payload is skipped via seek. This is
/// an UPPER BOUND on the final unique-password count (since the merge phase
/// still dedups across runs), but it's cheap to compute and gives the merge
/// progress bar a real length instead of 0, so ETA is meaningful instead of
/// always showing "0s".
fn count_records_in_runs(runs: &[tempfile::TempPath]) -> Result<u64> {
    let total: u64 = runs
        .par_iter()
        .map(|p| -> Result<u64> {
            let mut r = BufReader::with_capacity(1024 * 1024, File::open(p)?);
            let mut count = 0u64;
            loop {
                let mut header = [0u8; 12];
                match r.read_exact(&mut header) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e.into()),
                }
                let len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as i64;
                r.seek_relative(len)?;
                count += 1;
            }
            Ok(count)
        })
        .try_reduce(|| 0u64, |a, b| Ok(a + b))?;
    Ok(total)
}

fn read_record(r: &mut impl Read) -> io::Result<Option<Record>> {
    let mut key_buf = [0u8; 8];
    match r.read_exact(&mut key_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let key = i64::from_le_bytes(key_buf);
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut pw = vec![0u8; len];
    r.read_exact(&mut pw)?;
    Ok(Some(Record { key, pw }))
}

struct SyncMmapMut(*mut u8, usize);
unsafe impl Sync for SyncMmapMut {}
unsafe impl Send for SyncMmapMut {}
impl SyncMmapMut {
    #[inline(always)]
    fn ptr(&self) -> *mut u8 {
        self.0
    }
    #[inline(always)]
    fn len(&self) -> usize {
        self.1
    }
}

fn write_output_mmap(records: &[Record], out_path: &PathBuf, progress: &ProgressBar) -> Result<()> {
    let mut offsets: Vec<usize> = Vec::with_capacity(records.len() + 1);
    let mut acc = 0usize;
    offsets.push(0);
    for r in records {
        acc += r.pw.len() + 1;
        offsets.push(acc);
    }
    let total_size = acc as u64;

    if let Some(parent) = out_path.parent() {
        let probe = if parent.as_os_str().is_empty() {
            std::path::Path::new(".")
        } else {
            parent
        };
        ensure_disk_space(probe, total_size, "mmap output file")?;
    }

    let file = File::create(out_path)?;
    file.set_len(total_size)?;
    let mut mmap = unsafe { MmapMut::map_mut(&file)? };

    let base = SyncMmapMut(mmap.as_mut_ptr(), mmap.len());

    records
        .par_iter()
        .enumerate()
        .for_each(|(i, r)| {
            let start = offsets[i];
            let end = offsets[i + 1];
            debug_assert_eq!(end - start, r.pw.len() + 1);
            debug_assert!(end <= base.len());
            unsafe {
                let dst = base.ptr().add(start);
                std::ptr::copy_nonoverlapping(r.pw.as_ptr(), dst, r.pw.len());
                *dst.add(r.pw.len()) = b'\n';
            }
            progress.inc(1);
        });

    mmap.flush()?;
    file.sync_all()?;
    Ok(())
}

const MMAP_TEMP_FILE_THRESHOLD: u64 = 32 * 1024 * 1024;

#[inline]
fn record_encoded_len(pw_len: usize) -> usize {
    8 + 4 + pw_len
}

fn write_entries_to_temp(
    entries: &[(Vec<u8>, u64)],
    temp_dir: &Option<PathBuf>,
    negate_key: bool,
) -> Result<tempfile::TempPath> {
    let total_size: u64 = entries
        .iter()
        .map(|(pw, _)| record_encoded_len(pw.len()) as u64)
        .sum();

    let temp = if let Some(dir) = temp_dir {
        NamedTempFile::new_in(dir)?
    } else {
        NamedTempFile::new()?
    };

    if total_size >= MMAP_TEMP_FILE_THRESHOLD {
        let mut offsets: Vec<usize> = Vec::with_capacity(entries.len() + 1);
        let mut acc = 0usize;
        offsets.push(0);
        for (pw, _) in entries {
            acc += record_encoded_len(pw.len());
            offsets.push(acc);
        }

        let file = temp.as_file();
        file.set_len(acc as u64)?;
        let mut mmap = unsafe { MmapMut::map_mut(file)? };
        let base = SyncMmapMut(mmap.as_mut_ptr(), mmap.len());

        // Safe to use par_iter() here: nothing in this program permanently
        // pins every rayon worker thread anymore (compressed-file support,
        // which used to do that, has been removed), so this parallel copy
        // can freely use all CPU cores for maximum spill-write throughput.
        entries.par_iter().enumerate().for_each(|(i, (pw, count))| {
            let start = offsets[i];
            debug_assert_eq!(offsets[i + 1] - start, record_encoded_len(pw.len()));
            let key: i64 = if negate_key {
                -(*count as i64)
            } else {
                *count as i64
            };
            unsafe {
                let dst = base.ptr().add(start);
                std::ptr::copy_nonoverlapping(key.to_le_bytes().as_ptr(), dst, 8);
                std::ptr::copy_nonoverlapping((pw.len() as u32).to_le_bytes().as_ptr(), dst.add(8), 4);
                std::ptr::copy_nonoverlapping(pw.as_ptr(), dst.add(12), pw.len());
            }
        });

        mmap.flush()?;
        // NOTE: intentionally no file.sync_all() here — these are scratch
        // spill files, not durable output. fsync() forces a blocking wait
        // for the kernel to physically flush dirty pages, which serializes
        // whichever thread calls it behind disk writeback — under I/O
        // pressure (slow/contended temp-dir disk, concurrent swap activity)
        // this can stall the entire spill pipeline behind one write.
        // mmap.flush() makes the data visible to subsequent reads within
        // this process, which is all that's needed: a crash mid-run just
        // means rerunning the tool, no durability guarantee required.
    } else {
        let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, &temp);
        for (pw, count) in entries {
            let key: i64 = if negate_key {
                -(*count as i64)
            } else {
                *count as i64
            };
            write_record(&mut writer, &Record { key, pw: pw.clone() })?;
        }
        writer.flush()?;
    }

    Ok(temp.into_temp_path())
}

fn write_records_to_temp(records: &[Record], temp_dir: &Option<PathBuf>) -> Result<tempfile::TempPath> {
    let total_size: u64 = records
        .iter()
        .map(|r| record_encoded_len(r.pw.len()) as u64)
        .sum();

    let temp = if let Some(dir) = temp_dir {
        NamedTempFile::new_in(dir)?
    } else {
        NamedTempFile::new()?
    };

    if total_size >= MMAP_TEMP_FILE_THRESHOLD {
        let mut offsets: Vec<usize> = Vec::with_capacity(records.len() + 1);
        let mut acc = 0usize;
        offsets.push(0);
        for r in records {
            acc += record_encoded_len(r.pw.len());
            offsets.push(acc);
        }

        let file = temp.as_file();
        file.set_len(acc as u64)?;
        let mut mmap = unsafe { MmapMut::map_mut(file)? };
        let base = SyncMmapMut(mmap.as_mut_ptr(), mmap.len());

        records.par_iter().enumerate().for_each(|(i, r)| {
            let start = offsets[i];
            debug_assert_eq!(offsets[i + 1] - start, record_encoded_len(r.pw.len()));
            unsafe {
                let dst = base.ptr().add(start);
                std::ptr::copy_nonoverlapping(r.key.to_le_bytes().as_ptr(), dst, 8);
                std::ptr::copy_nonoverlapping((r.pw.len() as u32).to_le_bytes().as_ptr(), dst.add(8), 4);
                std::ptr::copy_nonoverlapping(r.pw.as_ptr(), dst.add(12), r.pw.len());
            }
        });

        mmap.flush()?;
        // NOTE: intentionally no file.sync_all() here — see the matching
        // note in write_entries_to_temp. These are scratch sort-spill
        // files; skipping fsync avoids serializing this call behind
        // blocking disk writeback.
    } else {
        let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, &temp);
        for r in records {
            write_record(&mut writer, r)?;
        }
        writer.flush()?;
    }

    Ok(temp.into_temp_path())
}

fn sort_and_write(
    mut records: Vec<Record>,
    max_mem_bytes: usize,
    temp_dir: Option<PathBuf>,
    out: &mut impl Write,
    progress: &ProgressBar,
) -> Result<()> {
    let estimated_mem: usize = records.iter().map(|r| r.pw.len() + 40).sum();

    if estimated_mem <= max_mem_bytes {
        records.par_sort_unstable();
        for r in &records {
            out.write_all(&r.pw)?;
            out.write_all(b"\n")?;
            progress.inc(1);
        }
        return Ok(());
    }

    eprintln!("Dataset exceeds in-memory sort budget; spilling to disk.");

    let spill_dir = temp_dir.clone().unwrap_or_else(std::env::temp_dir);
    ensure_disk_space(&spill_dir, estimated_mem as u64, "sort-phase spill (all chunks)")?;

    let avg_record_size: usize = if records.is_empty() {
        1
    } else {
        (records.iter().map(|r| r.pw.len() + 12).sum::<usize>() / records.len()).max(1)
    };
    const MAX_RUNS: usize = 512;
    let min_chunk_len = (records.len() + MAX_RUNS - 1) / MAX_RUNS;
    let chunk_len = std::cmp::max(
        std::cmp::max(1, max_mem_bytes / avg_record_size.max(1)),
        min_chunk_len.max(1),
    );

    let mut temp_files: Vec<tempfile::TempPath> = Vec::new();

    for chunk in records.chunks_mut(chunk_len) {
        chunk.par_sort_unstable();

        let temp_path = write_records_to_temp(chunk, &temp_dir)?;
        temp_files.push(temp_path);
    }
    drop(records);

    struct HeapEntry {
        rec: Record,
        idx: usize,
    }
    impl PartialEq for HeapEntry {
        fn eq(&self, other: &Self) -> bool {
            self.rec == other.rec
        }
    }
    impl Eq for HeapEntry {}
    impl Ord for HeapEntry {
        fn cmp(&self, other: &Self) -> Ordering {
            other.rec.cmp(&self.rec)
        }
    }
    impl PartialOrd for HeapEntry {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut readers: Vec<BufReader<File>> = temp_files
        .iter()
        .map(|p| BufReader::with_capacity(2 * 1024 * 1024, File::open(p).unwrap()))
        .collect();

    let mut heap = std::collections::BinaryHeap::with_capacity(readers.len());
    for (idx, r) in readers.iter_mut().enumerate() {
        if let Some(rec) = read_record(r)? {
            heap.push(HeapEntry { rec, idx });
        }
    }

    let mut out_writer = BufWriter::with_capacity(16 * 1024 * 1024, &mut *out);
    while let Some(HeapEntry { rec, idx }) = heap.pop() {
        out_writer.write_all(&rec.pw)?;
        out_writer.write_all(b"\n")?;
        progress.inc(1);

        if let Some(next) = read_record(&mut readers[idx])? {
            heap.push(HeapEntry { rec: next, idx });
        }
    }
    out_writer.flush()?;

    Ok(())
}

fn interactive_choice() -> bool {
    print!("Sort by frequency? (y/n): ");
    io::stdout().flush().unwrap();
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
    matches!(input.trim().to_lowercase().as_str(), "y" | "yes" | "true" | "1")
}

#[derive(Parser)]
#[command(
    author,
    version,
    about = "Generates a frequency-sorted or unique password dictionary from pot files and/or plain wordlists."
)]
struct Cli {
    #[arg(required = true)]
    inputs: Vec<PathBuf>,

    #[arg(short = 'o', long)]
    output: Option<PathBuf>,

    #[arg(short = 'p', long)]
    processes: Option<usize>,

    #[arg(long, default_value_t = 0.5)]
    max_mem: f64,

    /// Counting-phase memory budget, as a fraction of total host RAM
    /// (suggested value: 0.20). Unlike --max-mem, this flag is NOT
    /// enforced unless it is explicitly passed on the command line: if
    /// --count-mem is omitted entirely, no counting-phase memory cap is
    /// applied at all (the in-flight hash map is allowed to grow without
    /// spilling to disk based on RAM usage). Pass an explicit value to
    /// opt back into the budgeted/spilling behavior.
    #[arg(long)]
    count_mem: Option<f64>,

    #[arg(long)]
    chunk_batch_size: Option<usize>,

    #[arg(long)]
    temp_dir: Option<PathBuf>,

    #[arg(long)]
    freq: bool,

    #[arg(long)]
    unique: bool,

    #[arg(long)]
    keep_trailing_colon: bool,

    #[arg(long)]
    arena: bool,

    #[arg(long)]
    mmap_output: bool,

    #[arg(long)]
    parallel_files: bool,
}

/// Suggested --count-mem fraction shown in logs/help when the flag is used
/// without a caller-supplied rationale elsewhere. This is NOT applied
/// automatically — see the doc comment on Cli::count_mem.
const SUGGESTED_COUNT_MEM: f64 = 0.20;

fn main() -> Result<()> {
    let cli = Cli::parse();

    if let Some(num) = cli.processes {
        rayon::ThreadPoolBuilder::new()
            .num_threads(num)
            .build_global()
            .unwrap();
    }

    let mut sys = System::new();
    sys.refresh_memory();
    let total_ram = sys.total_memory() as usize;
    let max_mem_bytes = (total_ram as f64 * cli.max_mem) as usize;

    let total_input_bytes: u64 = cli
        .inputs
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok())
        .map(|m| m.len())
        .sum();

    {
        let probe_dir = cli
            .temp_dir
            .clone()
            .unwrap_or_else(std::env::temp_dir);
        if let Some(avail) = available_space_for(&probe_dir) {
            let avail_gb = avail as f64 / (1024.0 * 1024.0 * 1024.0);
            eprintln!(
                "Temp/spill directory: {} ({:.2} GB free)",
                probe_dir.display(),
                avail_gb
            );
            if total_input_bytes > 0 && avail < total_input_bytes {
                eprintln!(
                    "Warning: free space ({:.2} GB) is less than total input size ({:.2} GB). \
                     Large inputs with low duplication may exhaust disk space during spilling.",
                    avail_gb,
                    total_input_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
                );
            }
            // Keep watching for the rest of the run: a one-time check at
            // startup can't catch space draining away later during a long
            // spilling/merging pass (see spawn_disk_space_watchdog).
            spawn_disk_space_watchdog(cli.temp_dir.clone(), total_input_bytes);
        } else {
            eprintln!(
                "Warning: could not determine free space for '{}'; disk-space checks during spilling will be skipped.",
                probe_dir.display()
            );
        }
    }

    let sort_mode = if cli.unique {
        "unique"
    } else if cli.freq {
        "frequency"
    } else if interactive_choice() {
        "frequency"
    } else {
        "unique"
    };
    eprintln!("Sort mode: {}", sort_mode);
    if cli.arena {
        eprintln!("WARNING: Arena allocator is ON. Arenas grow forever and are never freed.");
        eprintln!("         Do not use --arena for large files or you will OOM.");
    }

    let start = Instant::now();

    let pb = ProgressBar::new(0);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {bytes}/{total_bytes} ({eta})  {msg}",
        )
        .unwrap()
        .progress_chars("#>-"),
    );

    // --count-mem is only enforced when explicitly provided. When it is
    // omitted, no counting-phase memory budget is applied: budget_bytes is
    // set to usize::MAX so CountingIndex::maybe_spill() never trips based
    // on RAM usage, and the counting phase always stays fully in memory
    // (disk spilling can still happen at the final sort stage via
    // --max-mem, which keeps its own enforced default).
    let count_budget_bytes: usize = match cli.count_mem {
        Some(frac) => {
            let budget = (total_ram as f64 * frac).max(256.0 * 1024.0 * 1024.0) as usize;
            eprintln!(
                "Counting memory budget: {:.2} GB (spills to disk beyond this).",
                budget as f64 / (1024.0 * 1024.0 * 1024.0)
            );
            budget
        }
        None => {
            eprintln!(
                "Counting memory budget: not set (--count-mem omitted) — no cap enforced; \
                 suggested value if you want one is {:.2}.",
                SUGGESTED_COUNT_MEM
            );
            usize::MAX
        }
    };

    let mut index = CountingIndex::new(count_budget_bytes, cli.temp_dir.clone(), pb.clone());
    let total_lines_acc = AtomicU64::new(0);
    let total_bytes: u64 = cli
        .inputs
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok())
        .map(|m| m.len())
        .sum();
    pb.set_length(total_bytes);

    let auto_parallel_files = cli.parallel_files || cli.inputs.len() > 100;

    let pb = if auto_parallel_files {
        eprintln!(
            "{} input files: processing files in parallel.",
            cli.inputs.len()
        );
        let pb_arc = Arc::new(pb);
        cli.inputs.par_iter().try_for_each(|path| -> Result<()> {
            let lines_read = read_file(path, &pb_arc, &index, cli.keep_trailing_colon, cli.arena, cli.chunk_batch_size)?;
            total_lines_acc.fetch_add(lines_read, AtomicOrdering::Relaxed);
            Ok(())
        })?;
        Arc::try_unwrap(pb_arc).unwrap_or_else(|pb_arc| (*pb_arc).clone())
    } else {
        for path in &cli.inputs {
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            pb.set_message(name.to_string());
            let lines_read = read_file(path, &pb, &index, cli.keep_trailing_colon, cli.arena, cli.chunk_batch_size)?;
            total_lines_acc.fetch_add(lines_read, AtomicOrdering::Relaxed);
        }
        pb
    };
    pb.finish_and_clear();

    let total_lines = total_lines_acc.load(AtomicOrdering::Relaxed);
    if cli.arena {
        eprintln!(
            "Arena allocations (unique-key inserts across all chunks): {}",
            ARENA_ALLOCS.load(AtomicOrdering::Relaxed)
        );
    }
    if index.has_spilled() {
        eprintln!("Counting phase spilled to disk at least once; merging runs now.");
    }

    let write_pb = ProgressBar::new(0);
    write_pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({eta})  writing...",
        )
        .unwrap()
        .progress_chars("#>-"),
    );

    let (unique_passwords, records) = match index.finalize(sort_mode)? {
        FinalizedResult::InMemory(u, r) => (u as usize, Some(r)),
        FinalizedResult::Spilled { runs, sort_mode: sm } => {
            // Upper bound on unique count (merge still dedups across runs),
            // but gives the progress bar a real length so ETA isn't stuck
            // at 0s for the whole merge phase.
            let estimated_total = count_records_in_runs(&runs)?;
            write_pb.set_length(estimated_total);

            if sm == "unique" {
                let mut out: Box<dyn Write> = if let Some(ref out_path) = cli.output {
                    Box::new(BufWriter::with_capacity(16 * 1024 * 1024, File::create(out_path)?))
                } else {
                    Box::new(BufWriter::with_capacity(16 * 1024 * 1024, io::stdout()))
                };
                let unique = stream_merge_and_write(&runs, &mut out, &write_pb)?;
                out.flush()?;
                write_pb.finish_and_clear();

                let elapsed = start.elapsed().as_secs_f32();
                let out_name = cli.output.as_deref().unwrap_or(std::path::Path::new("stdout")).display();
                eprintln!("mode      : {}", sort_mode);
                eprintln!("lines in  : {}", total_lines);
                eprintln!("unique    : {} (streaming merge)", unique);
                eprintln!("output    : {}", out_name);
                eprintln!("time      : {:.1}s", elapsed);
                return Ok(());
            } else {
                let mut out: Box<dyn Write> = if let Some(ref out_path) = cli.output {
                    Box::new(BufWriter::with_capacity(16 * 1024 * 1024, File::create(out_path)?))
                } else {
                    Box::new(BufWriter::with_capacity(16 * 1024 * 1024, io::stdout()))
                };
                let unique = external_sort_spilled_runs(
                    &runs,
                    max_mem_bytes,
                    cli.temp_dir.clone(),
                    &mut out,
                    &write_pb,
                )?;
                out.flush()?;
                write_pb.finish_and_clear();

                let elapsed = start.elapsed().as_secs_f32();
                let out_name = cli.output.as_deref().unwrap_or(std::path::Path::new("stdout")).display();
                eprintln!("mode      : {}", sort_mode);
                eprintln!("lines in  : {}", total_lines);
                eprintln!("unique    : {} (external sort)", unique);
                eprintln!("output    : {}", out_name);
                eprintln!("time      : {:.1}s", elapsed);
                return Ok(());
            }
        }
    };

    eprintln!("Found {} unique passwords.", unique_passwords);

    if unique_passwords == 0 {
        eprintln!("No data.");
        return Ok(());
    }

    let records = records.unwrap();
    write_pb.set_length(records.len() as u64);
    let estimated_output_size: u64 = records.iter().map(|r| (r.pw.len() + 1) as u64).sum();
    const MMAP_OUTPUT_THRESHOLD: u64 = 10 * 1024 * 1024 * 1024;

    let use_mmap = cli.mmap_output
        && estimated_output_size > MMAP_OUTPUT_THRESHOLD
        && records.len() < 100_000_000;

    if cli.output.is_some() && use_mmap {
        let estimated_mem: usize = records.iter().map(|r| r.pw.len() + 40).sum();
        if estimated_mem <= max_mem_bytes {
            eprintln!(
                "Output estimated at {:.2} GB: writing via mmap.",
                estimated_output_size as f64 / (1024.0 * 1024.0 * 1024.0)
            );
            let mut records = records;
            records.par_sort_unstable();
            write_output_mmap(&records, cli.output.as_ref().unwrap(), &write_pb)?;
        } else {
            let mut out: Box<dyn Write> = Box::new(BufWriter::with_capacity(
                16 * 1024 * 1024,
                File::create(cli.output.as_ref().unwrap())?,
            ));
            sort_and_write(records, max_mem_bytes, cli.temp_dir, &mut out, &write_pb)?;
            out.flush()?;
        }
    } else {
        let mut out: Box<dyn Write> = if let Some(ref out_path) = cli.output {
            Box::new(BufWriter::with_capacity(16 * 1024 * 1024, File::create(out_path)?))
        } else {
            Box::new(BufWriter::with_capacity(16 * 1024 * 1024, io::stdout()))
        };
        sort_and_write(records, max_mem_bytes, cli.temp_dir, &mut out, &write_pb)?;
        out.flush()?;
    }
    write_pb.finish_and_clear();

    let elapsed = start.elapsed().as_secs_f32();
    let unique_percent = (unique_passwords as f64 / total_lines as f64) * 100.0;
    let out_name = cli.output.as_deref().unwrap_or(std::path::Path::new("stdout")).display();
    eprintln!("mode      : {}", sort_mode);
    eprintln!("lines in  : {}", total_lines);
    eprintln!("unique    : {} ({:.1}%)", unique_passwords, unique_percent);
    eprintln!("output    : {}", out_name);
    eprintln!("time      : {:.1}s", elapsed);

    Ok(())
}
