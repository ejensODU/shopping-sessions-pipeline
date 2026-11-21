#!/usr/bin/env python3
"""Check scored Parquet output using PyArrow and the organizer's expected CSVs."""

import argparse
import csv
import os
from datetime import date
from pathlib import Path
import subprocess
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq


TABLES = {
    "user_daily": ("user_id", "day", "sessions", "events", "revenue_cents", "top_category"),
    "campaign_daily": ("campaign_id", "day", "channel", "sessions", "purchasing_sessions",
                       "revenue_cents", "assisted_sessions"),
}
STRINGS = {"user_id", "campaign_id", "channel", "top_category"}


def expected_value(name, value):
    if name == "day":
        return date.fromisoformat(value)
    if name in STRINGS:
        return None if name == "top_category" and value == "" else value
    return int(value)


def valid_type(name, dtype):
    if name == "day":
        return pa.types.is_date32(dtype)
    if name in STRINGS:
        if pa.types.is_dictionary(dtype):
            dtype = dtype.value_type
        return pa.types.is_string(dtype) or pa.types.is_large_string(dtype)
    return pa.types.is_integer(dtype)


def check_table(folder, answer, table):
    cols = TABLES[table]
    directory = folder / table
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError(f"invalid output directory {directory}")
    paths = sorted(p for p in directory.glob("*.parquet")
                   if p.is_file() and not p.is_symlink() and not p.name.startswith((".", "_")))
    if not paths:
        raise ValueError(f"no scored Parquet files in {directory}")

    expected = {}
    with (answer / f"{table}.csv").open(encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source)
        if set(reader.fieldnames or ()) != set(cols):
            raise ValueError(f"unexpected columns in {source.name}")
        for record in reader:
            row = tuple(expected_value(c, record[c]) for c in cols)
            key = row[:2]
            if key in expected:
                raise ValueError(f"{table}: duplicate expected key {key!r}")
            expected[key] = row

    count = len(expected)
    for path in paths:
        parquet = pq.ParquetFile(path)
        schema = parquet.schema_arrow
        if len(schema) != len(cols) or set(schema.names) != set(cols):
            raise ValueError(f"{path.name}: expected {cols}, got {schema.names}")
        for name in cols:
            dtype = schema.field(name).type
            if not valid_type(name, dtype):
                raise ValueError(f"{path.name}: wrong type {dtype} for {name}")
        for batch in parquet.iter_batches(columns=list(cols), use_threads=False):
            values = batch.to_pydict()
            for row in zip(*(values[c] for c in cols)):
                key = row[:2]
                correct = expected.pop(key, None)
                if correct is None:
                    raise ValueError(f"{table}: extra or duplicate key {key!r}")
                if row != correct:
                    raise ValueError(f"{table}: key {key!r}, expected {correct!r}, got {row!r}")
    if expected:
        raise ValueError(f"{table}: missing {len(expected)} rows; example {next(iter(expected))!r}")
    return count


def compare(output, answer):
    return " / ".join(f"{check_table(output, answer, table):,} {table}" for table in TABLES)


def run_fixtures(directory, command):
    cases = sorted(p for p in directory.iterdir() if (p / "expected").is_dir())
    if not cases:
        raise ValueError(f"no fixtures in {directory}")
    failures = 0
    for case in cases:
        data = case / "input" if (case / "input").is_dir() else case / "data"
        with tempfile.TemporaryDirectory() as temp:
            out = Path(temp) / "output"
            scratch = Path(temp) / "scratch"
            out.mkdir()
            scratch.mkdir()
            env = dict(os.environ, DATA_DIR=str(data.resolve()), OUTPUT_DIR=str(out),
                       SCRATCH_DIR=str(scratch))
            try:
                subprocess.run(command, env=env, check=True)
                print(f"{case.name}: {compare(out, case / 'expected')}")
            except (ValueError, OSError, subprocess.CalledProcessError) as error:
                print(f"{case.name}: FAIL: {error}")
                failures += 1
    print(f"{len(cases) - failures}/{len(cases)} fixtures passed")
    return int(failures != 0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    checks = modes.add_parser("compare", help="compare generated output with the answer CSV folder")
    checks.add_argument("output", type=Path)
    checks.add_argument("expected", type=Path)
    fixtures = modes.add_parser("fixtures", help="run a pipeline using DATA_DIR, OUTPUT_DIR and SCRATCH_DIR")
    fixtures.add_argument("directory", type=Path)
    fixtures.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.mode == "compare":
        print(compare(args.output, args.expected))
        return 0
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("provide a pipeline command after the fixture directory")
    return run_fixtures(args.directory, command)


if __name__ == "__main__":
    raise SystemExit(main())
