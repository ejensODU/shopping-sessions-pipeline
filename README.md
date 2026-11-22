# Shopping Sessions — Zero-Dependency C++20 Engine

High-throughput, zero-dependency C++20 implementation of the Databricks Shopping Sessions pipeline, packaged as a static binary in a minimal `FROM scratch` (`~3.5 MB`) container image.

## Architecture

All stages are general-purpose with exact fallbacks (no dataset assumptions, no third-party runtime libraries, no Apache Arrow/Parquet SDK):

1. **Parallel `pread` I/O & Persistent Thread Pool (`src/io.hpp`)**
   - Byte-range parallel `pread` scanner with 512 KiB 64-byte-aligned buffers and exact line-boundary ownership (avoids `mmap` page-fault lock contention and `exit_group` PTE teardown storms).
   - Persistent `ThreadPool` reused across all stages; `Catalog` loading (`products.csv`, `campaigns.csv`) overlaps concurrently with `People` link resolution.

2. **Raw 64-Bit ASCII Keys & Lock-Free Union-Find (`src/keys.hpp`, `src/catalog_people.hpp`)**
   - Zero-conversion 64-bit raw-ASCII encoding for 2..9-byte prefixed IDs (`e...`, `u...`, `k...`) with 3-instruction decode (`__builtin_clzll`), 1-instruction `bswap64` bytewise ordering, and mutex-protected exact string-interning fallback for arbitrary lengths/UTF-8/casing.
   - Single-pass parallel lock-free intern + path-compressed DSU with prefetching, producing a scaled Blocked Bloom Filter (`wyhash64`, <1% FP rate) + compact non-canonical alias hash table.

3. **Fused SIMD Event & Click Parsing + Pipelined Resolution (`src/main.cpp`, `src/parse.hpp`)**
   - Window-aware `FastTime` parser with SWAR 8-digit validation, unified ISO F1/F2 parsing with same-day `ingest_ts` shortcut, SWAR 13-digit F3 parsing, and exact proleptic Gregorian general fallback.
   - 6-instruction SSE ASCII validation + 32-byte AVX2 tail validation, with exact RFC-4180 fallback (`split_fields_slow`).
   - 64-row 4-pass prefetch-pipelined Bloom + alias resolution hides both Bloom L3/DRAM latency and alias-table DRAM latency.

4. **Single-Pass L2-Resident AoS Deduplication & Arena Reuse (`src/arena.hpp`)**
   - Per-worker private 16 KiB `WorkerArena` slab allocators (safe runtime `/proc/buddyinfo` THP probe avoids kernel compaction stalls while enabling 2 MiB hugepages on clean VMs).
   - Adaptive 512/1024-bucket AoS deduplication (`wyhash64 >> 32` + `fastrange32`, 1 cache line per slot).
   - `dedup_arenas` are reset in O(1) after deduplication and reused for clicks (zero extra page faults).

5. **Two-Level Radix Grouping, Branchless Sorting & Custom Parquet v1 Writer (`src/parquet_writer.hpp`)**
   - LPT shard scheduling -> single-pass 64 coarse buckets -> L1d-resident per-bucket hash table -> 8-byte packed `(ts_off << 32) | price_cat` sort (branchless 2..8 sorting networks + depth-limited branchless Lomuto quicksort for `n < 512`, 8-bit/11-bit LSD radix for large whales).
   - Transposed SoA campaign accumulators (`[campaign][day]`) and single-click fast-path.
   - Zero-dependency Apache Parquet v1 writer (Thrift Compact Protocol, `UNCOMPRESSED`, `RLE_DICTIONARY` for `user_id`/`day`/`top_category`, `PLAIN` numeric columns) verified with PyArrow, Polars, and DuckDB.
   - Automatic `run_wide` general fallback for arbitrary windows (>49 days), >256 categories, >65,535 campaigns, or out-of-range `ingest_ts`.

## Building & Running

```sh
# Build static linux/amd64 container image (~3.5 MB)
docker build --platform linux/amd64 -t shopping-sessions:latest .

# Run with scorer lockdown flags
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m \
  --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 \
  --cpus=8 --memory 24g --memory-swap 24g --user 1000:1000 \
  -v /path/to/data:/data:ro \
  -v /path/to/output:/output:rw \
  -v /path/to/scratch:/scratch:rw \
  shopping-sessions:latest
```

## Local Verification & Benchmarks

Verified bit-exact across:
- All **36/36 official fixtures** (native & inside Docker with scorer flags, verified with both PyArrow and Polars `compare.py` + DuckDB)
- **1M `dev`**, **20M `p20`**, **60M `f60`**, **3 fresh random seeds**, and **12 adversarial edge-case suites** (wide mode >50d leap-year 2028, >256 categories, ingest-overflow fallback, 0-row empty tables, all-null `top_category`, CRLF/no-EOF-newline, UTF-8/emoji/quoted IDs, 500-hop link chain + 1000-alias kiosk, 600 equal timestamps, 5,000-event whale, multi-RowGroup >68K users / 140K rows, 5,000-byte CSV lines).

Local benchmarks on AMD Ryzen 5 3600 (AVX2, under host background load):
- **Single-core `p20` (21.5M events, 1.85 GB):** `2.85s` user + `0.77s` sys = **`3.65s`** (`3.56x` faster than the `12.99s` C++/Arrow baseline)
- **8-thread Docker scorer-flag `p20` (median of 3):** **`2.01s`** end-to-end Docker wall (vs `4.05s` baseline under identical load)
- **8-thread Docker scorer-flag `f60` (60M events, 5.25 GB, warm):** **`4.65s`** end-to-end Docker wall, peak RSS `2.5 GiB`
- **Container image size:** **`3.5 MB`** (`1.06 MB` compressed, `FROM scratch`)

## License

MIT License. See [LICENSE](LICENSE). Zero third-party runtime dependencies.
