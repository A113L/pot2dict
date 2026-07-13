# pot2dict

Fast, parallel tool for turning hashcat/john `.pot` files and plain wordlists into a deduplicated, optionally frequency-sorted password dictionary.

## Features

- Multithreaded counting (rayon) over mmap'd input, chunked at newline boundaries
- Gzip (`.gz`) and Zstandard (`.zst`) input support, decompressed and counted in parallel
- `DashMap`-backed global counter — sharded, so many small input files scale without lock contention
- Optional persistent bump-arena key allocation (`--arena`) for very large unique-password sets (~50M+)
- Automatic spill-to-disk during counting when the working set exceeds your counting memory budget, with streaming external merge on read-back
- In-memory parallel sort, with automatic external k-way merge sort spill-to-disk when the dataset exceeds your memory budget
- Optional mmap-based output writer (`--mmap-output`) for very large outputs (~10GB+)
- Frequency-sorted or plain unique output
- Optional parallel processing across input files (`--parallel-files`), auto-enabled above 100 inputs

## Designed for low-RAM machines

pot2dict is built to correctly process input sets much larger than available RAM — it is not just a fast in-memory hash-dedup tool. Every phase (counting, sorting, and merging) has a disk-spill fallback with bounded memory usage, controlled independently via `--count-mem` and `--max-mem`.

This is a deliberate tradeoff. Tools that keep everything in memory (`awk '!seen[$0]++'`, most single-pass dedup utilities) are faster when your input's unique working set fits in RAM, but will swap or get OOM-killed when it doesn't. pot2dict is slower on those same well-fitting datasets, but stays memory-bounded and completes on hardware — e.g. an 8GB machine processing a 20GB+ low-duplication input — where memory-unbounded tools simply can't finish.

**Practical implications:**

- On memory-constrained machines, expect heavier disk spilling and correspondingly longer runtimes on large, low-duplication inputs. This is expected behavior, not a bug — it's the tradeoff that lets the run complete at all.
- Spilling frequency scales with how tight your `--count-mem`/`--max-mem` budgets are relative to input size. If you have RAM to spare, raising these budgets (e.g. to 0.6–0.75 on an otherwise idle machine) produces fewer, larger spill runs and meaningfully less merge overhead — leave enough headroom for the OS and allocator (mimalloc), don't set these to consume all available RAM.
- **Disk placement matters more than CPU on low-RAM runs.** Since spilling and merging are I/O-bound, put `--temp-dir` on a different physical disk than your input file if possible, and prefer SSD over HDD — this typically has a bigger impact on wall-clock time than thread count once you're spilling heavily.
- `--arena` trades memory for speed by never freeing key allocations for the life of the run — avoid it on low-RAM machines with large or low-duplication inputs, since it works against the same memory constraints this tool is otherwise designed to respect.

## Example run (low-RAM, spill-heavy)

Real-world run on a memory-constrained machine (8 GB RAM, 12 threads), merging three large plain-text wordlists (~21 GB combined) with a tight counting budget:

```bash
time ./pot2dict wordlist_a.txt wordlist_b.txt wordlist_c.txt \
  -p 12 --unique --keep-trailing-colon -o merged.txt \
  --temp-dir /mnt/data/tmp \
  --count-mem 0.20 --chunk-batch-size 2 --mmap-output
```

| Stat                     | Value                            |
| ------------------------ | -------------------------------- |
| **System RAM**           | 8 GB                             |
| **Total Input Size**     | 21.16 GiB (2.6× RAM)             |
| **Files Processed**      | 3                                |
| **Total Lines**          | 1,746,922,348 (~1.75B)           |
| **Unique Passwords**     | 1,451,675,930 (~1.45B)           |
| **Duplication Rate**     | 16.9%                            |
| **Threads**              | 12                                |
| **Count Memory Budget**  | 1.53 GB (19% of RAM)             |
| **Max Memory Budget**    | ~4 GB (50% of RAM, default)      |
| **Chunk Batch Size**     | 2                                 |
| **Temp Directory**       | `/mnt/data/tmp` (163.73 GB free) |
| **Spill Runs Created**   | 61                                |
| **Peak Temp Space Used** | ~93 GB (estimated)               |


## Install

```bash
cargo build --release
```

Binary will be at `target/release/pot2dict`.

## Usage

```bash
pot2dict input1.pot input2.txt.gz input3.txt.zst -o dict.txt --freq
```

### Options

| Flag | Description |
|---|---|
| `-o, --output <FILE>` | Output file (default: stdout) |
| `-p, --processes <N>` | Number of threads (default: all cores) |
| `--freq` | Sort by frequency (most common first) |
| `--unique` | Sort alphabetically, dedup only |
| `--max-mem <FRACTION>` | Fraction of system RAM usable for in-memory/output sort (default: 0.5) |
| `--count-mem <FRACTION>` | Fraction of system RAM usable as the counting-phase working-set budget before spilling to disk (default: 0.2, minimum 256 MB) - optional |
| `--chunk-batch-size <N>` | Number of mmap'd input chunks processed per batch before checking whether to spill (default: number of threads) |
| `--temp-dir <DIR>` | Directory for spill files during counting and external sort |
| `--keep-trailing-colon` | Treat a line ending in a bare `:` as the literal password instead of skipping it |
| `--arena` | Use persistent per-thread bump arenas for key allocation (recommended above ~50M unique passwords). Arenas grow for the lifetime of the run and are never freed — don't combine with very large inputs unless you have the RAM to spare. |
| `--mmap-output` | Write output via mmap instead of buffered I/O (recommended above ~10GB output) |
| `--parallel-files` | Process input files in parallel (auto-enabled above 100 input files) |

If neither `--freq` nor `--unique` is passed, you'll be prompted interactively.

## Input format

Each line may be a plain password, or a pot-style `hash:password` line. The password after the last `:` is extracted; lines ending in an empty password (`hash:`) are skipped by default (see `--keep-trailing-colon`).

## Memory behavior

pot2dict uses two separate, independently-tunable memory budgets:

- **Counting budget** (`--count-mem`): caps the size of the in-memory frequency map while reading input. Once exceeded, the current counts are sorted and spilled to a temp file, and counting resumes from an empty map. If any spill occurs, the final result is produced by streaming/external-merging the spilled runs rather than sorting everything in RAM.
- **Sort/output budget** (`--max-mem`): caps how much of the final (already-counted) record set can be sorted in memory before writing. Larger datasets are chunked, sorted per-chunk, spilled, and merged via a k-way heap merge.

Both spill paths write to `--temp-dir` if given, otherwise the system temp directory. On low-RAM machines, these budgets — not CPU or thread count — are the primary lever for run time; see "Designed for low-RAM machines" above.

## License

MIT
