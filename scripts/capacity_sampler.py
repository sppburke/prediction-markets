#!/usr/bin/env python3
"""Sample the ranking cache's drive while one rank-and-push run is alive (metadata only).

`sample`: every --interval seconds (once and exit with --once), while --parent (the wrapper's PID) is this process's
parent, append one JSON line to --out: the time, the configured interval, the free bytes of the cache folder's
filesystem (statvfs; the quantity `df --output=avail` shows), the size of every regular file directly in that folder
(the installed cache, candidates, their -wal/-shm files, spools) and the byte total of each extra folder (--dir
NAME=PATH; the wrapper passes the export folder). Free bytes measure everything the run consumes, SQLite's sort scratch
included: SQLite unlinks those files as it opens them, so no folder total sees them. A resumed run of the same cycle
appends to the same file, after dropping a last line a crash left unfinished. The wrapper marks each run's first sample
`start` (taken before anything is staged) and the sample its exit trap takes after the run's children ended `end`
(`--once --mark`). It never opens a database or reads any file's contents.

`summarize SAMPLES`: the samples file folded into one JSON object: the run attempts (each from a `start` sample) and how
many lack their `end` sample, the first sample's free bytes and file sizes (before the cycle's candidate copy), the
smallest free bytes and when, each name's largest size, the sample count, the first and last sample times, the
largest gap between consecutive samples of one attempt (between attempts no wrapper ran), and `qualified`: true only
when every attempt ended and no gap within an attempt exceeds three times that attempt's recorded interval. Values are
sampled: a peak between two samples can be missed, and one lasting longer than the largest gap is caught. The samples
file is the record; the summary is computed from it whenever it is needed (docs/26, capacity).
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
    attempts = []
    for record in records:
        if record.get("mark") == "start" or not attempts:
            attempts.append([])
        attempts[-1].append(record)
    gaps = [later["t"] - earlier["t"] for attempt in attempts for earlier, later in zip(attempt, attempt[1:])]
    unterminated = sum(attempt[-1].get("mark") != "end" for attempt in attempts)

    def covered(attempt):
        intervals = [record.get("interval") for record in attempt]
        if not all(isinstance(interval, (int, float)) and interval > 0 for interval in intervals):
            return False  # a sample without its interval cannot show its coverage
        limit = 3 * max(intervals)
        return all(later["t"] - earlier["t"] <= limit for earlier, later in zip(attempt, attempt[1:]))

    lowest = min(records, key=lambda record: record["free_bytes"])
    peaks = {}
    for record in records:
        for name, size in [*record["files"].items(), *record["dirs"].items()]:
            peaks[name] = max(size, peaks.get(name, 0))
    return {
        "attempts": len(attempts),
        "unterminated_attempts": unterminated,
        "first_free_bytes": records[0]["free_bytes"],
        "first_files": records[0]["files"],
        "min_free_bytes": lowest["free_bytes"],
        "min_free_at": lowest["t"],
        "peak_bytes": peaks,
        "samples": len(records),
        "first_t": records[0]["t"],
        "last_t": records[-1]["t"],
        "largest_gap_s": round(max(gaps, default=0.0), 1),
        "qualified": unterminated == 0 and all(covered(attempt) for attempt in attempts),
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
    run.add_argument("--mark", choices=("start", "end"))
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
        record["interval"] = args.interval
        if args.mark:
            record["mark"] = args.mark
        with open(args.out, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
        if args.once:
            break
        time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
