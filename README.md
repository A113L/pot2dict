# pot2dict

Fast, parallel tool for turning hashcat/john `.pot` files and plain wordlists into a deduplicated, optionally frequency-sorted password dictionary.

## Features

- Multithreaded counting (rayon) over mmap'd input, chunked at newline boundaries
- Gzip (`.gz`) and Zstandard (`.zst`) input support
- `DashMap`-backed global counter — sharded, so many small input files scale without lock contention
- Optional persistent bump-arena key allocation (`--arena`) for very large unique-password sets (~50M+)
- Automatic spill-to-disk during counting when the working set exceeds your counting memory budget, with streaming external merge on read-back
- In-memory parallel sort, with automatic external k-way merge sort spill-to-disk when the dataset exceeds your memory budget
- Optional mmap-based output writer (`--mmap-output`) for very large outputs (~10GB+)
- Frequency-sorted or plain unique output
- Optional parallel processing across input files (`--parallel-files`), auto-enabled above 100 inputs

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
| `--count-mem <FRACTION>` | Fraction of system RAM usable as the counting-phase working-set budget before spilling to disk (default: 0.3, minimum 256 MB) |
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

Both spill paths write to `--temp-dir` if given, otherwise the system temp directory.

## License

MIT
