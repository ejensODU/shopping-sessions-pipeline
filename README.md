# Shopping Sessions

A C++20 submission for the Databricks Shopping Sessions track of the 2026 Builders Buildathon. The executable reads the mounted CSV dataset and manifest, then writes `user_daily/` and `campaign_daily/` as Parquet. The root Dockerfile builds the scorer's Linux amd64 image.

## Build and run

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions .
```

For a local scorer-style run, put the generated dataset under `dev/`. Create fresh `output/` and `scratch/` directories writable by UID 1000. Run this command from the repository root.

```sh
mkdir -p output scratch
docker run --rm --platform linux/amd64 --cpus=8 --memory=24g --memory-swap=24g \
  --read-only --network=none --tmpfs /tmp:rw,size=64m \
  -v "$PWD/dev:/data:ro" -v "$PWD/output:/output:rw" -v "$PWD/scratch:/scratch:rw" \
  shopping-sessions
```

The runtime starts the compiled binary directly, uses up to eight workers and needs no network. Set `NCPU` to use fewer workers. Set `PROFILE=1` to print phase timings to standard error. The build targets x86-64-v3, including AVX2 and BMI2, supported by the Intel Cascade Lake or newer machines described in the starter kit.

## Reproduce the checks

On x86-64 Linux with a C++20 compiler and Python 3.12, install the pinned wheel and build the same executable locally. Set `FIXTURES`, `GENERATOR` and `DEV_ANSWER` to the unpacked organizer directories. `GENERATOR` contains Cargo.toml, and `DEV_ANSWER` contains the two answer CSVs.

```sh
python3 -m pip install pyarrow==26.0.1
make local
python3 tools/verify.py fixtures "$FIXTURES" -- "$PWD/build/shopping-sessions"
cargo build --release --manifest-path "$GENERATOR/Cargo.toml"
"$GENERATOR/target/release/generate" --seed 1 --events 1000000 --clicks 250000 --output dev
out=$(mktemp -d)
NCPU=8 ./build/shopping-sessions dev "$out"
python3 tools/verify.py compare "$out" "$DEV_ANSWER"
```

The verifier checks the Parquet schema, column names, unique keys, row counts and every value in both tables. It accepts shuffled rows and multiple part files. Output directories should be dedicated to one run.

## Measured results

Each timing below is the wall time of `docker run` under the scorer's flags on an AMD Ryzen 5 3600, taken as the median of 7 runs with a warm page cache. A container that does nothing takes about 0.39 s on this host. The organizer times the container on its own Intel machines.

| Dataset | Events / clicks | Median (s) | Largest process RSS |
| --- | ---: | ---: | ---: |
| Official dev, seed 1 | 1M / 250k | 0.57 | not measured |
| Practice scale, seed 37 | 20M / 5M | 3.4 to 3.6 | 0.72 GiB |
| Final scale, seed 83 | 60M / 15M | 8.4 to 8.8 | 2.09 GiB |

All 36 organizer fixtures passed. The dev run matched the published answer across all 117,038 user-day rows and 27,400 campaign-day rows. An independent single-threaded reference implementation recomputed both complete tables for seed 37. All 2,248,281 user-day rows and 28,014 campaign-day rows matched, including duplicate-key checks.

At final scale, the two tables reconcile sessions and revenue. The organizers supply an official answer for dev only.

## Practice and final-size benchmarks

Generate other seeds with the organizer's Rust executable. The benchmark helper pins up to eight available logical CPUs, starts a fresh native process per run and writes each result into a separate directory. It reports the start-to-exit wall time and peak process RSS on Linux.

```sh
generate --seed 37 --events 20000000 --clicks 5000000 --output practice
python3 tools/benchmark.py practice timing/practice --runs 3
generate --seed 83 --events 60000000 --clicks 15000000 --output final
python3 tools/benchmark.py final timing/final --runs 3
```

## Implementation

- CSV splitting uses SIMD delimiter and quote masks. The parser preserves spaces and carriage returns, validates the entire row, and converts the three timestamp forms to UTC milliseconds.
- The link graph includes aliases that appear only in link files. Every valid edge participates in the union, and each component carries its bytewise smallest ID. Up to nine nonzero ASCII bytes fit losslessly in a 64-bit key. Longer, Unicode and NUL-containing IDs use a full-byte interner.
- Events are partitioned by the hash of their exact ID. Each partition selects the earliest ingest across all files. An invalid earliest copy suppresses later copies. The compact event representation is selected from the actual window length and category count.
- Surviving events and valid clicks are partitioned by canonical person. Each worker orders one person's timeline, splits at gaps strictly over 30 minutes, and assigns a session's purchases and events to its UTC start date.
- For assists, a click covers an interval of sorted session starts. Two cursors track the interval, and a per-campaign endpoint counts only newly covered starts. Additions are grouped by day. This avoids materializing a potentially huge session–click join for a shared device or heavily active shopper.
- The native Apache Parquet writer emits 65,536-row groups. Campaign totals are combined across all person partitions. Output order has no effect on the score.

## Dependencies and attribution

The pipeline and verification script were written for this event with AI assistance. It uses GCC and the C++ standard library. Apache Arrow and Apache Parquet C++ are supplied by the PyArrow 26.0.1 wheel, distributed under Apache License 2.0. The event organizers supply the rules, fixtures, Rust generator and answer CSVs separately. The entry code is released under the MIT License.

[Official track, starter files, and scoring](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions)
