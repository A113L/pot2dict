use std::cmp::Ordering;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::Result;
use bumpalo::Bump;
use clap::Parser;
use dashmap::DashMap;
use flate2::read::MultiGzDecoder;
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

fn available_space_for(path: &std::path::Path) -> Option<u64> {
    let probe = if path.exists() {
        path.to_path_buf()
    } else {
        path.ancestors().find(|p| p.exists())?.to_path_buf()
    };
    let probe = probe.canonicalize().ok()?;

    with_refreshed_disks(|disks| {
        disks
            .iter()
            .filter(|d| probe.starts_with(d.mount_point()))
            .max_by_key(|d| d.mount_point().as_os_str().len())
            .map(|d| d.available_space())
    })
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

pub enum FinalizedResult {
    InMemory(u64, Vec<Record>),
    Spilled {
        runs: Vec<tempfile::TempPath>,
        sort_mode: String,
    },
}

struct SpillJob {
    entries: Vec<(Vec<u8>, u64)>,
}

pub struct CountingIndex {
    map: GlobalMap,
    approx_bytes: AtomicUsize,
    budget_bytes: usize,
    temp_dir: Option<PathBuf>,
    spilled_runs: Arc<Mutex<Vec<tempfile::TempPath>>>,
    spill_lock: RwLock<()>,
    spill_tx: Mutex<Option<Sender<SpillJob>>>,
    spill_thread: Mutex<Option<std::thread::JoinHandle<Result<()>>>>,
    progress: ProgressBar,
    spill_count: AtomicUsize,
}

impl CountingIndex {
    fn new(budget_bytes: usize, temp_dir: Option<PathBuf>, progress: ProgressBar) -> Self {
        let spilled_runs = Arc::new(Mutex::new(Vec::new()));

        let (tx, rx) = bounded::<SpillJob>(2);
        let writer_temp_dir = temp_dir.clone();
        let writer_runs = Arc::clone(&spilled_runs);
        let spill_thread = std::thread::Builder::new()
            .name("spill-writer".into())
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
                    entries.par_sort_unstable_by(|a, b| a.0.cmp(&b.0));

                    let temp_path = write_entries_to_temp(&entries, &writer_temp_dir, false)?;
                    writer_runs.lock().push(temp_path);
                }
                Ok(())
            })
            .expect("failed to spawn spill-writer thread");

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
            spill_thread: Mutex::new(Some(spill_thread)),
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
            "spilled {} run{} to disk (~{:.2} GB this run)",
            spill_num,
            if spill_num == 1 { "" } else { "s" },
            approx_gb
        ));

        // Sequential collection since DashMap rayon feature may not be enabled
        let entries: Vec<(Vec<u8>, u64)> = self
            .map
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect();
        self.map.clear();
        self.approx_bytes.store(0, AtomicOrdering::Relaxed);

        // Release write lock BEFORE blocking send
        drop(_guard);

        tx.send(SpillJob { entries })
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

        // Close channel and wait for background writer
        self.spill_tx.lock().take();
        if let Some(handle) = self.spill_thread.lock().take() {
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
                .iter()
                .map(|entry| (entry.key().clone(), *entry.value()))
                .collect();
            entries.par_sort_unstable_by(|a, b| a.0.cmp(&b.0));
            let temp_path = write_entries_to_temp(&entries, &self.temp_dir, false)?;
            spilled_runs.push(temp_path);
        }

        eprintln!(
            "Merging {} spilled counting runs from disk...",
            spilled_runs.len()
        );
        Ok(FinalizedResult::Spilled {
            runs: spilled_runs,
            sort_mode: sort_mode.to_string(),
        })
    }
}

impl Drop for CountingIndex {
    fn drop(&mut self) {
        // If finalize() already ran, spill_tx/spill_thread are already
        // None/joined and this is a no-op. If we're here because an error
        // propagated out before finalize() was called, close the channel
        // (so rx.recv() unblocks) and join the writer thread.
        self.spill_tx.lock().take();
        if let Some(handle) = self.spill_thread.lock().take() {
            let _ = handle.join();
        }
    }
}

fn fold_into_dashmap(index: &CountingIndex, local: FastMap<PwKey, u64>) {
    let _guard = index.spill_lock.read();
    for (k, v) in local {
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

fn fold_into_dashmap_arena(index: &CountingIndex, local: FastMap<&'static [u8], u64>) {
    let _guard = index.spill_lock.read();
    for (k, v) in local {
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

enum CompressedKind {
    Gzip,
    Zstd,
}

fn compressed_kind(path: &PathBuf) -> Option<CompressedKind> {
    match path.extension().and_then(|e| e.to_str()) {
        Some("gz") => Some(CompressedKind::Gzip),
        Some("zst") | Some("zstd") => Some(CompressedKind::Zstd),
        _ => None,
    }
}

fn open_compressed_reader(path: &PathBuf, kind: &CompressedKind) -> Result<Box<dyn Read>> {
    let file = File::open(path)?;
    Ok(match kind {
        CompressedKind::Gzip => {
            Box::new(BufReader::with_capacity(64 * 1024, MultiGzDecoder::new(file)))
        }
        CompressedKind::Zstd => {
            let decoder = zstd::stream::read::Decoder::new(file)?;
            Box::new(BufReader::with_capacity(64 * 1024, decoder))
        }
    })
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

    const SPILL_CHECK_LINES: u64 = 2_000_000;

    if let Some(kind) = compressed_kind(path) {
        drop(file);
        let mut reader = open_compressed_reader(path, &kind)?;
        let mut bytes_read: u64 = 0;
        let mut last_reported: u64 = 0;
        let mut lines_since_check: u64 = 0;
        let mut local: FastMap<PwKey, u64> = FastMap::default();
        let mut line_buf: Vec<u8> = Vec::with_capacity(256);
        let mut total_lines: u64 = 0;
        let mut line_count: u64 = 0;
        let mut reader = BufReader::with_capacity(1 << 20, &mut *reader);

        loop {
            line_buf.clear();
            let n = reader.read_until(b'\n', &mut line_buf)?;
            if n == 0 {
                break;
            }
            bytes_read += n as u64;
            total_lines += 1;
            line_count += 1;
            lines_since_check += 1;
            if line_count >= 16384 {
                pb.inc(bytes_read - last_reported);
                last_reported = bytes_read;
                line_count = 0;
            }
            if let Some(pw) = extract_password(&line_buf, keep_trailing_colon) {
                bump_count(&mut local, pw);
            }
            if lines_since_check >= SPILL_CHECK_LINES {
                fold_into_dashmap(index, std::mem::take(&mut local));
                index.maybe_spill()?;
                lines_since_check = 0;
            }
        }
        pb.inc(bytes_read - last_reported);
        fold_into_dashmap(index, local);
        index.maybe_spill()?;
        Ok(total_lines)
    } else {
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
        file.sync_all()?;
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
        file.sync_all()?;
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

    #[arg(long, default_value_t = 0.3)]
    count_mem: f64,

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
            let total_input_bytes: u64 = cli
                .inputs
                .iter()
                .filter_map(|p| std::fs::metadata(p).ok())
                .map(|m| m.len())
                .sum();
            if total_input_bytes > 0 && avail < total_input_bytes {
                eprintln!(
                    "Warning: free space ({:.2} GB) is less than total input size ({:.2} GB). \
                     Large inputs with low duplication may exhaust disk space during spilling.",
                    avail_gb,
                    total_input_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
                );
            }
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

    let count_budget_bytes = (total_ram as f64 * cli.count_mem).max(256.0 * 1024.0 * 1024.0) as usize;
    eprintln!(
        "Counting memory budget: {:.2} GB (spills to disk beyond this).",
        count_budget_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    );

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
