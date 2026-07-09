# pot2dict

Fast, parallel tool for turning hashcat/john `.pot` files and plain wordlists into a deduplicated, optionally frequency-sorted password dictionary.

## Features

- Multithreaded counting (rayon) over mmap'd input, chunked at newline boundaries
- Gzip (`.gz`) and Zstandard (`.zst`) input support
- `DashMap`-backed global counter — sharded, so many small input files scale without lock contention
- Optional persistent bump-arena key allocation (`--arena`) for very large unique-password sets (~50M+)
- In-memory parallel sort, with automatic external k-way merge sort spill-to-disk when the dataset exceeds your memory budget
- Optional mmap-based output writer (`--mmap-output`) for very large outputs (~10GB+)
- Frequency-sorted or plain unique output

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
| `--max-mem <FRACTION>` | Fraction of system RAM usable for in-memory sort (default: 0.5) |
| `--temp-dir <DIR>` | Directory for spill files during external sort |
| `--keep-trailing-colon` | Treat a line ending in a bare `:` as the literal password instead of skipping it |
| `--arena` | Use persistent per-thread bump arenas for key allocation (recommended above ~50M unique passwords) |
| `--mmap-output` | Write output via mmap instead of buffered I/O (recommended above ~10GB output) |
| `--parallel-files` | Process input files in parallel (auto-enabled above 100 input files) |

If neither `--freq` nor `--unique` is passed, you'll be prompted interactively.

## Input format

Each line may be a plain password, or a pot-style `hash:password` line. The password after the last `:` is extracted; lines ending in an empty password (`hash:`) are skipped by default (see `--keep-trailing-colon`).

## License

MIT
