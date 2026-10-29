#!/usr/bin/env python3
"""Sample the ranking cache's drive while one rank-and-push run is alive (metadata only).

Every --interval seconds, until its parent (the wrapper) ends, append one JSON line to --out: the time, the free bytes of
the cache folder's filesystem (statvfs; the quantity `df --output=avail` shows), the size of every regular file directly
in that folder (the installed cache, candidates, their -wal/-shm files, spools) and the byte total of each extra folder
(--dir NAME=PATH, for example SQLite's temporary folder and the export folder). --summary holds the smallest free bytes
seen (with its time) and each name's largest size, replaced atomically after every sample and carried over by a resumed
run of the same cycle, so a peak that is gone before the cycle ends stays recorded. A peak is caught when it lasts at least
one interval. It never opens a database or reads any file's contents (docs/26, capacity).
"""

import argparse
import json
import os
import tempfile
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


def merge(summary, record):
    if summary["min_free_bytes"] is None or record["free_bytes"] < summary["min_free_bytes"]:
        summary["min_free_bytes"] = record["free_bytes"]
        summary["min_free_at"] = record["t"]
    peaks = summary["peak_bytes"]
    for name, size in [*record["files"].items(), *record["dirs"].items()]:
        peaks[name] = max(size, peaks.get(name, 0))
    summary["samples"] += 1


def write_atomically(path, value):
    fd, tmp = tempfile.mkstemp(prefix=".capacity_summary.", dir=os.path.dirname(path) or ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, sort_keys=True)
            handle.write("\n")
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--summary", required=True)
    parser.add_argument("--interval", type=float, required=True)
    parser.add_argument("--dir", action="append", default=[], metavar="NAME=PATH")
    args = parser.parse_args()
    folders = dict(item.split("=", 1) for item in args.dir)
    summary = {"min_free_bytes": None, "min_free_at": None, "peak_bytes": {}, "samples": 0}
    if os.path.exists(args.summary):
        with open(args.summary, encoding="utf-8") as handle:
            summary = json.load(handle)
    parent = os.getppid()
    while os.getppid() == parent:
        record = sample(args.cache_dir, folders)
        with open(args.out, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
        merge(summary, record)
        write_atomically(args.summary, summary)
        time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
