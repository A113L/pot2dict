# pot2dict

Fast, parallel tool for turning hashcat/john `.pot` files and plain wordlists into a deduplicated, optionally frequency-sorted password dictionary.

[![pot2dict.jpg](https://i.postimg.cc/wvnfJkHZ/pot2dict.jpg)](https://postimg.cc/N97kvmKk)

## Features

- Multithreaded counting (rayon) over mmap'd input, chunked at newline boundaries
- `DashMap`-backed global counter — sharded, so many small input files scale without lock contention
- Optional persistent bump-arena key allocation (`--arena`) for very large unique-password sets (~50M+)
- Automatic spill-to-disk during counting when the working set exceeds your counting memory budget, with streaming external merge on read-back
- In-memory parallel sort, with automatic external k-way merge sort spill-to-disk when the dataset exceeds your memory budget
- Optional mmap-based output writer (`--mmap-output`) for very large outputs (~10GB+)
- Frequency-sorted or plain unique output
- Optional parallel processing across input files (`--parallel-files`), auto-enabled above 100 inputs
- Optional desktop GUI front end (see [GUI](#gui) below)

## Install

```bash
cargo build --release
```

Binary will be at `target/release/pot2dict`.

## Usage

```bash
pot2dict input1.pot input2 input3.potfile -o dict.txt --freq
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

## GUI

`pot2dictGUI` is an optional desktop front end (Python + tkinter) for the CLI binary. It builds the
command line from the same flags the Rust binary exposes, runs it as a subprocess, and streams
live progress into a console pane.

It never captures the binary's stdout: if no `--output` path is given, the GUI auto-generates one
before launching, since piping stdout into the log would mean echoing the recovered password list
into a Tk widget line by line. Only stderr (progress bar, spill warnings, final stats) is shown.

### Requirements

- Python 3.8+
- `tkinter` (part of the standard library, but not always installed by default on Linux)
- The compiled `pot2dict` binary (see Install above)

No other dependencies — `pot2dictGUI` uses only the Python standard library.

**Installing tkinter, if missing:**

| OS | Command |
|---|---|
| Windows / macOS (python.org installer) | Included by default — no action needed |
| Ubuntu / Debian | `sudo apt install python3-tk` |
| Fedora | `sudo dnf install python3-tkinter` |
| Arch | `sudo pacman -S tk` |

### Running

```bash
python3 pot2dictGUI
```

On first launch, point **Binary path** at your compiled `pot2dict` / `pot2dict.exe`, add your input
files, and the command preview panel on the right will update live as you adjust options. Hit
**▶ run** to launch it as a subprocess and watch progress in the **LIVE OUTPUT** pane.

## Input format

Each line may be a plain password, or a pot-style `hash:password` line. The password after the last `:` is extracted; lines ending in an empty password (`hash:`) are skipped by default (see `--keep-trailing-colon`).

## Designed for low-RAM machines

pot2dict is built to correctly process input sets much larger than available RAM — it is not just a fast in-memory hash-dedup tool. Every phase (counting, sorting, and merging) has a disk-spill fallback with bounded memory usage, controlled independently via `--count-mem` and `--max-mem`.

This is a deliberate tradeoff. Tools that keep everything in memory (`awk '!seen[$0]++'`, most single-pass dedup utilities) are faster when your input's unique working set fits in RAM, but will swap or get OOM-killed when it doesn't. pot2dict is slower on those same well-fitting datasets, but stays memory-bounded and completes on hardware — e.g. an 8GB machine processing a 20GB+ low-duplication input — where memory-unbounded tools simply can't finish.

**Practical implications:**

- On memory-constrained machines, expect heavier disk spilling and correspondingly longer runtimes on large, low-duplication inputs. This is expected behavior, not a bug — it's the tradeoff that lets the run complete at all.
- Spilling frequency scales with how tight your `--count-mem`/`--max-mem` budgets are relative to input size. If you have RAM to spare, raising these budgets (e.g. to 0.6–0.75 on an otherwise idle machine) produces fewer, larger spill runs and meaningfully less merge overhead — leave enough headroom for the OS and allocator (mimalloc), don't set these to consume all available RAM.
- **Disk placement matters more than CPU on low-RAM runs.** Since spilling and merging are I/O-bound, put `--temp-dir` on a different physical disk than your input file if possible, and prefer SSD over HDD — this typically has a bigger impact on wall-clock time than thread count once you're spilling heavily.
- `--arena` trades memory for speed by never freeing key allocations for the life of the run — avoid it on low-RAM machines with large or low-duplication inputs, since it works against the same memory constraints this tool is otherwise designed to respect.

## Memory behavior

pot2dict uses two separate, independently-tunable memory budgets:

- **Counting budget** (`--count-mem`): caps the size of the in-memory frequency map while reading input. Once exceeded, the current counts are sorted and spilled to a temp file, and counting resumes from an empty map. If any spill occurs, the final result is produced by streaming/external-merging the spilled runs rather than sorting everything in RAM.
- **Sort/output budget** (`--max-mem`): caps how much of the final (already-counted) record set can be sorted in memory before writing. Larger datasets are chunked, sorted per-chunk, spilled, and merged via a k-way heap merge.

Both spill paths write to `--temp-dir` if given, otherwise the system temp directory. On low-RAM machines, these budgets — not CPU or thread count — are the primary lever for run time; see "Designed for low-RAM machines" above.

## Example run (low-RAM, spill-heavy)

Real-world run on a memory-constrained machine (8 GB RAM, 12 threads), merging three runs on large plain-text wordlists with a tight counting budget (--count-mem=0.2):

| Metric                        | Run 1 (44.85 GB) | Run 2 (21.16 GB) | Run 3 (8.93 GB) | Run 4 (3.39 GB) | Trend           |
| ----------------------------- | ---------------- | ---------------- | --------------- | --------------- | --------------- |
| **Input Size**                | 44.85 GiB        | 21.16 GiB        | 8.93 GiB        | 3.39 GiB        | —               |
| **Input : RAM Ratio**         | 5.5×             | 2.6×             | 1.1×            | **0.42×**       | ↓ Less pressure |
| **Count Mem** (`--count-mem`) | 0.15             | 0.20             | 0.20            | 0.20            | —               |
| **Max Mem** (`--max-mem`)     | 0.40             | 0.50             | 0.50            | 0.50            | —               |
| **Total Lines**               | 3.94B            | 1.75B            | 838M            | 329M            | —               |
| **Unique Lines**              | 2.77B            | 1.45B            | 514M            | 253M            | —               |
| **Duplication Rate**          | 29.5%            | 16.9%            | **38.6%**       | 23.0%           | —               |
| **Spill Runs**                | 178              | 61               | 29              | **10**          | ↓ Fewer spills  |
| **Wall Time**                 | 256m 52s         | 106m 15s         | 51m 37s         | **18m 4s**      | ↓ Sub-linear    |
| **Lines/Second**              | 255,300          | 273,700          | 270,700         | **303,100**     | ↑ Faster!       |
| **MB/Second**                 | 3.0              | 3.3              | 2.9             | **3.2**         | Stable          |

## Quick comparison

**System**: Intel Core i5-9300H @ 2.40GHz | 8 Threads | 16 GB RAM

| Tool                     | Command / Settings                                                            | Real Time   | Speedup vs `sort` | Lines In   | Dedup  | Parallel          | Memory           |
| :----------------------- | :---------------------------------------------------------------------------- | :---------- | :---------------- | :--------- | :----- | :---------------- | :--------------- |
| **pot2dict**             | `./pot2dict [7 files] -o ../wlists/wordlist.txt --unique -p 8 --max-mem 0.50` | 38.950s     | 0.36×             | 78,571,277 | 74.62% | 8 threads         | `--max-mem 0.50` |
| **sort -u** (GNU)        | `LC_ALL=C sort -S 50% --parallel=8 -u [7 files]`                              | **14.194s** | **baseline**      | 78,571,277 | 74.62% | 8 threads         | `-S 50%`         |
| **HK Merge Tool** (Mono) | `mono App.Merge.exe o="../wlists/wordlist.txt" t=8 c=7 [7 files]`             | 58.681s     | 0.24×             | 78,571,277 | 74.62% | 8 threads (`t=8`) | `c=7`            |



## License

MIT
