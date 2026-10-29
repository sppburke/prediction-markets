#!/usr/bin/env python3
"""Sample the ranking cache's drive while one rank-and-push run is alive (metadata only).

`sample`: every --interval seconds (once and exit with --once), while --parent (the wrapper's PID) is this process's
parent, append one JSON line to
--out: the time, the free bytes of the cache folder's filesystem (statvfs; the quantity `df --output=avail` shows), the
size of every regular file directly in that folder (the installed cache, candidates, their -wal/-shm files, spools) and
the byte total of each extra folder (--dir NAME=PATH; the wrapper passes the export folder). Free bytes measure
everything the run consumes, SQLite's sort scratch included: SQLite unlinks those files as it opens them, so no folder
total sees them. A resumed run of the same cycle appends to the same file, after dropping a last line a crash left
unfinished. It never opens a database or reads any file's contents.

`summarize SAMPLES`: the samples file folded into one JSON object: the first sample's free bytes and file sizes (before
the run's candidate copy), the smallest free bytes and when, each name's largest size, the sample count, the first and last sample times and the largest gap between consecutive samples (a gap much
longer than the interval means nothing was sampled then). A peak is caught when it lasts at least one interval. The
samples file is the record; the summary is computed from it whenever it is needed (docs/26, capacity).
"""

import argparse
import json
import os
import time


def folder_bytes(path):
    total = 0
    for parent, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.lstat(os.path.join(parent, name)).st_size
            except FileNotFoundError:
                pass  # removed while walking
    return total


def sample(cache_dir, folders):
    stats = os.statvfs(cache_dir)
    files = {}
    with os.scandir(cache_dir) as entries:
        for entry in entries:
            try:
                if entry.is_file(follow_symlinks=False):
                    files[entry.name] = entry.stat(follow_symlinks=False).st_size
            except FileNotFoundError:
                pass
    return {
        "t": round(time.time(), 1),
        "free_bytes": stats.f_bavail * stats.f_frsize,
        "files": files,
        "dirs": {name: folder_bytes(path) for name, path in folders.items() if os.path.isdir(path)},
    }


def drop_unfinished_line(path):
    try:
        with open(path, "rb+") as handle:
            data = handle.read()
            if data and not data.endswith(b"\n"):
                handle.truncate(data.rfind(b"\n") + 1)
    except FileNotFoundError:
        pass


def summarize(path):
    with open(path, "rb") as handle:
        complete = handle.read().split(b"\n")[:-1]  # a last line without its newline was never finished
    records = [json.loads(line) for line in complete]
    if not records:
        raise SystemExit(f"{path}: no samples")
    lowest = min(records, key=lambda record: record["free_bytes"])
    peaks = {}
    for record in records:
        for name, size in [*record["files"].items(), *record["dirs"].items()]:
            peaks[name] = max(size, peaks.get(name, 0))
    times = [record["t"] for record in records]
    return {
        "first_free_bytes": records[0]["free_bytes"],
        "first_files": records[0]["files"],
        "min_free_bytes": lowest["free_bytes"],
        "min_free_at": lowest["t"],
        "peak_bytes": peaks,
        "samples": len(records),
        "first_t": times[0],
        "last_t": times[-1],
        "largest_gap_s": round(max((b - a for a, b in zip(times, times[1:])), default=0.0), 1),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("sample")
    run.add_argument("--parent", type=int, required=True)
    run.add_argument("--cache-dir", required=True)
    run.add_argument("--out", required=True)
    run.add_argument("--interval", type=float, required=True)
    run.add_argument("--dir", action="append", default=[], metavar="NAME=PATH")
    run.add_argument("--once", action="store_true")
    fold = commands.add_parser("summarize")
    fold.add_argument("samples")
    args = parser.parse_args()
    if args.command == "summarize":
        print(json.dumps(summarize(args.samples), sort_keys=True))
        return 0
    folders = dict(item.split("=", 1) for item in args.dir)
    drop_unfinished_line(args.out)
    while os.getppid() == args.parent:
        record = sample(args.cache_dir, folders)
        with open(args.out, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
        if args.once:
            break
        time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
