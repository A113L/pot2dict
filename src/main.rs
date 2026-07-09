use std::cmp::Ordering;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::OnceLock;
use std::time::Instant;

use anyhow::Result;
use bumpalo::Bump;
use clap::Parser;
use dashmap::DashMap;
use flate2::read::MultiGzDecoder;
use hashbrown::HashMap;
use indicatif::{ProgressBar, ProgressStyle};
use memmap2::{Mmap, MmapMut};
use rayon::prelude::*;
use rustc_hash::FxBuildHasher;
use std::hash::BuildHasherDefault;
use rustc_hash::FxHasher;
use sysinfo::System;
use tempfile::NamedTempFile;

// hashbrown with FxHasher — lower memory overhead / faster hashing than std SipHash
pub type FastMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

// Global counter is now a DashMap instead of Mutex<HashMap>. DashMap shards its
// internal storage (16 shards by default, more with `shard-amount` feature), so
// concurrent inserts/updates from many threads/files only contend on the shard
// that owns a given key, not on a single global lock. This is the big win when
// processing many (>100) small input files in parallel: with a single Mutex,
// every file's fold-in serializes; with DashMap, folds from different files can
// proceed concurrently as long as they touch different shards.
pub type GlobalMap = DashMap<Vec<u8>, u64, FxBuildHasher>;

// ---------------------------------------------------------------------------
// Persistent per-thread bump arenas (used only with --arena)
// ---------------------------------------------------------------------------
// When the password space is huge (tens of millions of uniques), the dominant
// cost during counting is not hashing but heap allocation: every new key does
// a malloc via `Vec<u8>`. A bump allocator turns that into a pointer bump.
// We keep one `Bump` alive per rayon worker thread for the *entire* counting
// phase (not per-chunk), and never call `Bump::reset`, so pointers we hand
// out remain valid until the process exits. That lets local per-chunk maps
// store `&'static [u8]` keys with zero copies, and the DashMap key type stays
// `Vec<u8>` compatible via a thin wrapper that only allocates once, at first
// insert into the global map (arena slice -> owned Vec<u8> happens exactly
// once per *unique* password, not once per occurrence).
struct ArenaPool {
    arenas: Vec<Bump>,
}
// Safety: each rayon worker thread only ever touches its own index (obtained
// via `rayon::current_thread_index()`), so there is no concurrent mutable
// access to the same `Bump`. We only hand out `&Bump` and Bump's own alloc
// methods take `&self` (interior mutability), so this is sound.
unsafe impl Sync for ArenaPool {}

static ARENA_POOL: OnceLock<ArenaPool> = OnceLock::new();
static ARENA_ALLOCS: AtomicUsize = AtomicUsize::new(0);

fn arena_pool(num_threads: usize) -> &'static ArenaPool {
    ARENA_POOL.get_or_init(|| ArenaPool {
        arenas: (0..num_threads.max(1)).map(|_| Bump::new()).collect(),
    })
}

/// Allocate `bytes` into this worker's persistent arena and return a
/// `'static` slice. Only used in `--arena` mode.
#[inline(always)]
fn arena_alloc(pool: &'static ArenaPool, bytes: &[u8]) -> &'static [u8] {
    let idx = rayon::current_thread_index().unwrap_or(0) % pool.arenas.len();
    let bump = &pool.arenas[idx];
    let slice = bump.alloc_slice_copy(bytes);
    ARENA_ALLOCS.fetch_add(1, AtomicOrdering::Relaxed);
    // Safety: the arena is never reset and lives for the process lifetime
    // (leaked via `OnceLock`, never dropped), so extending the lifetime to
    // 'static is sound as long as the program doesn't drop ARENA_POOL early
    // (it never does — statics are dropped, if at all, only at process exit,
    // and we don't rely on Drop running).
    unsafe { std::mem::transmute::<&[u8], &'static [u8]>(slice) }
}

// ---------------------------------------------------------------------------
// Trim end — allocation-free
// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// Extract password — works for pot files and plain wordlists
// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// Bump counter using hashbrown's entry_ref — single hash, no double lookup,
// and no allocation on the "key already present" path (entry_ref only needs
// `&[u8]` to probe; `Vec<u8>` is only materialized on first insert).
// ---------------------------------------------------------------------------
#[inline(always)]
fn bump_count(map: &mut FastMap<Vec<u8>, u64>, pw: &[u8]) {
    *map.entry_ref(pw).or_insert(0) += 1;
}

/// Same as `bump_count` but the owned key (on first insert) is allocated from
/// a persistent per-thread bump arena instead of the global heap allocator.
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

// ---------------------------------------------------------------------------
// Count passwords in a chunk — returns local FastMap
// ---------------------------------------------------------------------------
fn count_chunk(chunk: &[u8], keep_trailing_colon: bool) -> FastMap<Vec<u8>, u64> {
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

// ---------------------------------------------------------------------------
// Merge two count maps — fold smaller into larger (owned-key variant)
// ---------------------------------------------------------------------------
fn merge_maps(mut a: FastMap<Vec<u8>, u64>, b: FastMap<Vec<u8>, u64>) -> FastMap<Vec<u8>, u64> {
    if a.len() < b.len() {
        return merge_maps(b, a);
    }
    a.reserve(b.len());
    for (k, v) in b {
        *a.entry(k).or_insert(0) += v;
    }
    a
}

fn merge_maps_arena(
    mut a: FastMap<&'static [u8], u64>,
    b: FastMap<&'static [u8], u64>,
) -> FastMap<&'static [u8], u64> {
    if a.len() < b.len() {
        return merge_maps_arena(b, a);
    }
    a.reserve(b.len());
    for (k, v) in b {
        *a.entry(k).or_insert(0) += v;
    }
    a
}

// ---------------------------------------------------------------------------
// Fold a local map into the global DashMap — no single global lock; only the
// shard(s) touched by these keys are briefly locked.
// ---------------------------------------------------------------------------
fn fold_into_dashmap(global: &GlobalMap, local: FastMap<Vec<u8>, u64>) {
    for (k, v) in local {
        *global.entry(k).or_insert(0) += v;
    }
}

fn fold_into_dashmap_arena(global: &GlobalMap, local: FastMap<&'static [u8], u64>) {
    for (k, v) in local {
        // First time this password is seen globally, pay one heap allocation
        // to give the DashMap an owned `Vec<u8>`. Every other occurrence
        // across every chunk/file was counted using the arena slice, at
        // arena-allocation (not malloc) cost. DashMap has no `entry_ref`,
        // so the copy to `Vec<u8>` happens unconditionally here, but it's
        // still only once per *unique* password rather than once per line.
        *global.entry(k.to_vec()).or_insert(0) += v;
    }
}

// ---------------------------------------------------------------------------
// Split mmap into chunks at newline boundaries
// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// Compressed-stream reading: gzip (.gz) and zstd (.zst)
// ---------------------------------------------------------------------------
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
            Box::new(BufReader::with_capacity(16 * 1024 * 1024, MultiGzDecoder::new(file)))
        }
        CompressedKind::Zstd => {
            // zstd decoder handles its own internal buffering, but we still
            // wrap it so `read_until` doesn't do a syscall-sized read per
            // call. zstd typically decompresses 3-5x faster than gzip at
            // comparable compression ratios, so this path is worth adding
            // whenever you control the input format (re-encode wordlists as
            // .zst instead of .gz).
            let decoder = zstd::stream::read::Decoder::new(file)?;
            Box::new(BufReader::with_capacity(16 * 1024 * 1024, decoder))
        }
    })
}

// ---------------------------------------------------------------------------
// Read a single (possibly compressed) file, folding counts into `global`.
// ---------------------------------------------------------------------------
fn read_file(
    path: &PathBuf,
    pb: &ProgressBar,
    global: &GlobalMap,
    keep_trailing_colon: bool,
    use_arena: bool,
) -> Result<u64> {
    let file = File::open(path)?;
    let file_size = file.metadata()?.len() as usize;

    if let Some(kind) = compressed_kind(path) {
        drop(file); // reopened inside open_compressed_reader
        let mut reader = open_compressed_reader(path, &kind)?;
        let mut bytes_read: u64 = 0;
        let mut last_reported: u64 = 0;
        let mut local: FastMap<Vec<u8>, u64> = FastMap::default();
        let mut line_buf: Vec<u8> = Vec::with_capacity(256);
        let mut total_lines: u64 = 0;
        let mut line_count: u64 = 0;
        // read_until needs BufRead; wrap the trait object.
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
            if line_count >= 16384 {
                // inc() adds a delta on top of the bar's current (cumulative,
                // across all files) position — unlike set_position(), it
                // never resets progress made by files processed earlier.
                pb.inc(bytes_read - last_reported);
                last_reported = bytes_read;
                line_count = 0;
            }
            if let Some(pw) = extract_password(&line_buf, keep_trailing_colon) {
                bump_count(&mut local, pw);
            }
        }
        pb.inc(bytes_read - last_reported);
        fold_into_dashmap(global, local);
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

        if use_arena {
            let pool = arena_pool(rayon::current_num_threads());
            let merged = chunks
                .par_iter()
                .map(|chunk| {
                    let m = count_chunk_arena(chunk, keep_trailing_colon, pool);
                    let lines: u64 = m.values().sum();
                    lines_done.fetch_add(lines, AtomicOrdering::Relaxed);
                    let done = chunks_done.fetch_add(1, AtomicOrdering::Relaxed) + 1;
                    pb.inc(chunk.len() as u64);
                    pb.set_message(format!("{} / {} chunks done", done, total_chunks));
                    m
                })
                .reduce(FastMap::default, merge_maps_arena);
            fold_into_dashmap_arena(global, merged);
        } else {
            let merged = chunks
                .par_iter()
                .map(|chunk| {
                    let m = count_chunk(chunk, keep_trailing_colon);
                    let lines: u64 = m.values().sum();
                    lines_done.fetch_add(lines, AtomicOrdering::Relaxed);
                    let done = chunks_done.fetch_add(1, AtomicOrdering::Relaxed) + 1;
                    pb.inc(chunk.len() as u64);
                    pb.set_message(format!("{} / {} chunks done", done, total_chunks));
                    m
                })
                .reduce(FastMap::default, merge_maps);
            fold_into_dashmap(global, merged);
        }

        let total_lines = lines_done.load(AtomicOrdering::Relaxed);
        eprintln!("Finished processing {}.", path.display());
        Ok(total_lines)
    }
}

// ---------------------------------------------------------------------------
// Sort record — precomputed key for O(1) comparisons
// ---------------------------------------------------------------------------
#[derive(Clone)]
struct Record {
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

// ---------------------------------------------------------------------------
// mmap-based output writer
// ---------------------------------------------------------------------------
// Instead of streaming through a BufWriter (kernel copies user buffer -> page
// cache on every write syscall), we mmap the destination file MAP_SHARED,
// pre-compute each record's byte offset (prefix sum over `len(pw) + 1`), then
// write all records in parallel directly into the mapping. The kernel handles
// dirty-page writeback asynchronously, with a final `flush()` to guarantee
// durability. This mainly helps once output size is large enough (>10 GB)
// that syscall/copy overhead of buffered writes becomes a real cost.
struct SyncMmapMut(*mut u8, usize);
// Safety: each thread writes to a disjoint byte range of the mapping
// (offsets are precomputed and non-overlapping), so concurrent access is
// data-race free even though `MmapMut` isn't `Sync` by default.
unsafe impl Sync for SyncMmapMut {}
unsafe impl Send for SyncMmapMut {}
impl SyncMmapMut {
    // Accessed via a method (not `base.0` directly) so that Rust 2021's
    // disjoint closure captures grab a reference to the whole `SyncMmapMut`
    // struct rather than to the bare `*mut u8` field — the latter would
    // capture `&*mut u8`, which isn't `Sync`, and bypass our unsafe impl.
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
        acc += r.pw.len() + 1; // password + '\n'
        offsets.push(acc);
    }
    let total_size = acc as u64;

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
    Ok(())
}

// ---------------------------------------------------------------------------
// Sort records and write — in-memory or external merge sort
// ---------------------------------------------------------------------------
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

        let temp = if let Some(ref dir) = temp_dir {
            NamedTempFile::new_in(dir)?
        } else {
            NamedTempFile::new()?
        };
        {
            let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, &temp);
            for r in chunk.iter() {
                write_record(&mut writer, r)?;
            }
            writer.flush()?;
        }
        temp_files.push(temp.into_temp_path());
    }
    drop(records);

    use std::collections::BinaryHeap;

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

    let mut heap = BinaryHeap::with_capacity(readers.len());
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

// ---------------------------------------------------------------------------
// Interactive choice
// ---------------------------------------------------------------------------
fn interactive_choice() -> bool {
    print!("Sort by frequency? (y/n): ");
    io::stdout().flush().unwrap();
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
    matches!(input.trim().to_lowercase().as_str(), "y" | "yes" | "true" | "1")
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------
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

    #[arg(long)]
    temp_dir: Option<PathBuf>,

    #[arg(long)]
    freq: bool,

    #[arg(long)]
    unique: bool,

    #[arg(long)]
    keep_trailing_colon: bool,

    /// Use a persistent per-thread bump arena for key allocation during
    /// counting instead of the global heap allocator. Worth enabling once
    /// the unique-password count is expected to exceed ~50M — below that
    /// the allocator overhead this avoids is not the bottleneck.
    /// Note: arena memory is never freed until the process exits.
    #[arg(long)]
    arena: bool,

    /// Write the final sorted output via a memory-mapped file instead of a
    /// buffered writer. Worth enabling once the output is expected to
    /// exceed ~10 GB; below that, buffered I/O is simpler and just as fast.
    #[arg(long)]
    mmap_output: bool,

    /// Process input files in parallel across files (in addition to the
    /// existing intra-file chunk parallelism). Worth enabling with >100
    /// input files, where DashMap's sharding avoids the contention a single
    /// Mutex<HashMap> would create.
    #[arg(long)]
    parallel_files: bool,
}

// ---------------------------------------------------------------------------
// MAIN
// ---------------------------------------------------------------------------
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
        eprintln!("Arena allocator: ON (persistent per-thread bump arenas)");
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

    let global: GlobalMap = DashMap::with_hasher(FxBuildHasher);
    let total_lines_acc = AtomicU64::new(0);
    let total_bytes: u64 = cli
        .inputs
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok())
        .map(|m| m.len())
        .sum();
    pb.set_length(total_bytes);

    let auto_parallel_files = cli.parallel_files || cli.inputs.len() > 100;

    if auto_parallel_files {
        eprintln!(
            "{} input files: processing files in parallel (DashMap sharding avoids single-lock contention).",
            cli.inputs.len()
        );
        cli.inputs.par_iter().try_for_each(|path| -> Result<()> {
            let lines_read = read_file(path, &pb, &global, cli.keep_trailing_colon, cli.arena)?;
            total_lines_acc.fetch_add(lines_read, AtomicOrdering::Relaxed);
            Ok(())
        })?;
    } else {
        for path in &cli.inputs {
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            pb.set_message(name.to_string());
            let lines_read = read_file(path, &pb, &global, cli.keep_trailing_colon, cli.arena)?;
            total_lines_acc.fetch_add(lines_read, AtomicOrdering::Relaxed);
        }
    }
    pb.finish_and_clear();

    let total_lines = total_lines_acc.load(AtomicOrdering::Relaxed);
    let unique_passwords = global.len();
    eprintln!("Found {} unique passwords.", unique_passwords);
    if cli.arena {
        eprintln!(
            "Arena allocations (unique-key inserts across all chunks): {}",
            ARENA_ALLOCS.load(AtomicOrdering::Relaxed)
        );
    }

    if unique_passwords == 0 {
        eprintln!("No data.");
        return Ok(());
    }

    eprintln!(
        "{}...",
        if sort_mode == "frequency" {
            "Sorting by frequency"
        } else {
            "Sorting alphabetically"
        }
    );

    let records: Vec<Record> = if sort_mode == "frequency" {
        global
            .into_iter()
            .map(|(pw, freq)| Record {
                key: -(freq as i64),
                pw,
            })
            .collect()
    } else {
        global
            .into_iter()
            .map(|(pw, _freq)| Record { key: 0, pw })
            .collect()
    };

    let write_pb = ProgressBar::new(unique_passwords as u64);
    write_pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({eta})  writing...",
        )
        .unwrap()
        .progress_chars("#>-"),
    );

    let estimated_output_size: u64 = records.iter().map(|r| (r.pw.len() + 1) as u64).sum();
    const MMAP_OUTPUT_THRESHOLD: u64 = 10 * 1024 * 1024 * 1024; // 10 GB

    if cli.output.is_some()
        && (cli.mmap_output || estimated_output_size > MMAP_OUTPUT_THRESHOLD)
        && sort_mode != "unique_external_spill_placeholder"
    {
        // mmap output path only makes sense once we have the fully sorted,
        // in-memory record set with known final size — so first sort, then
        // write via mmap instead of the generic sort_and_write() streaming
        // writer. If the dataset is so large it needs the external
        // merge-sort spill path, fall back to the normal streaming writer
        // (mixing mmap-output with the disk-spill merge writer is not
        // implemented here).
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
