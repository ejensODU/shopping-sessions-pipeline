#!/usr/bin/env python3
"""Measure complete native runs, including input reads and Parquet writes, on Linux."""
import argparse
import os
from pathlib import Path
import statistics
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data", type=Path)
    parser.add_argument("output", type=Path, help="Parent directory for new per-run output folders")
    parser.add_argument("--binary", type=Path, default=Path("build/shopping-sessions"))
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--cpus", type=int, default=8)
    args = parser.parse_args()
    if args.runs < 1 or not 1 <= args.cpus <= 8:
        parser.error("Choose at least one run and one to eight CPUs")

    cpu_ids = sorted(os.sched_getaffinity(0))[:args.cpus]
    os.sched_setaffinity(0, cpu_ids)
    args.output.mkdir(parents=True, exist_ok=True)
    durations = []
    peaks = []
    for number in range(1, args.runs + 1):
        out = tempfile.mkdtemp(prefix=f"run-{number}-", dir=args.output.resolve())
        env = dict(os.environ, NCPU=str(len(cpu_ids)), DATA_DIR=str(args.data.resolve()), OUTPUT_DIR=out)
        start = time.perf_counter()
        child = subprocess.Popen([str(args.binary.resolve())], env=env)
        _, status, usage = os.wait4(child.pid, 0)
        elapsed = time.perf_counter() - start
        child.returncode = os.waitstatus_to_exitcode(status)
        if child.returncode:
            raise SystemExit(f"Run {number} exited with status {child.returncode}")
        durations.append(elapsed)
        peaks.append(usage.ru_maxrss / 1024**2)
        print(f"Run {number}: {elapsed:.3f} s, peak RSS {peaks[-1]:.2f} GiB, {out}", flush=True)
    print(f"Median {statistics.median(durations):.3f} s on {len(cpu_ids)} logical CPUs; "
          f"maximum process RSS {max(peaks):.2f} GiB")


if __name__ == "__main__":
    main()
