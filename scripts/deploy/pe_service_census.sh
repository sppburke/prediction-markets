#!/bin/bash
# Process census for the release-2 (#744) guarded stop/start (docs/35 Procedure step 4). Read-only.
# Matches every process, other than the allowed PIDs, whose executable is one of the known pe-service binaries
# (installed, .bak-* or staged, given by --binary/--binary-glob) by path, including a deleted or replaced executable
# or an argv[0] resolved against the process cwd, or byte-identical to one of them (size + sha256), or whose argv
# names this generation's config (resolved against the process cwd). Other programs are never matched by name.
# Records PID, /proc start time, executable path and sha256 (read through /proc/<pid>/exe, so deleted or replaced
# executables still hash), cwd and argv. One JSON line per match, then a summary line.
# Exit: 0 = clear, 3 = matches found, 2 = a candidate could not be inspected (treat as not clear).
# Usage: pe_service_census.sh --config <path> [--cwd-root <dir>] [--allow <pid>]... [--binary <path>]... [--binary-glob <glob>]...
set -euo pipefail
exec python3 - "$@" <<'PY'
import argparse, glob, hashlib, json, os, sys, time
p = argparse.ArgumentParser()
p.add_argument("--config", required=True)
p.add_argument("--cwd-root", default=None, help="directory a relative --config is resolved against")
p.add_argument("--allow", action="append", default=[], type=int)
p.add_argument("--binary", action="append", default=[])
p.add_argument("--binary-glob", action="append", default=[])
a = p.parse_args()
config = a.config if os.path.isabs(a.config) else os.path.join(a.cwd_root or os.getcwd(), a.config)
config = os.path.realpath(config)
def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""): h.update(chunk)
    return h.hexdigest()
known, known_paths = {}, set()
paths = list(a.binary)
for g in a.binary_glob: paths += glob.glob(g)
for path in paths:
    if os.path.isfile(path):
        known.setdefault(os.path.getsize(path), {})[sha256(path)] = path
        known_paths.add(os.path.realpath(path))
with open("/proc/stat") as f:
    btime = next(int(l.split()[1]) for l in f if l.startswith("btime "))
hz = os.sysconf("SC_CLK_TCK")
me = {os.getpid(), os.getppid()}
matches, unreadable = [], []
for name in os.listdir("/proc"):
    if not name.isdigit(): continue
    pid = int(name)
    if pid in me: continue
    base = f"/proc/{pid}"
    try:
        with open(f"{base}/cmdline", "rb") as f: raw = f.read()
    except (FileNotFoundError, ProcessLookupError): continue
    except PermissionError: raw = b""
    argv = [x.decode(errors="replace") for x in raw.rstrip(b"\0").split(b"\0")] if raw else []
    try:
        exe = os.readlink(f"{base}/exe")
    except FileNotFoundError:
        continue  # exited or kernel thread
    except PermissionError:
        exe = None
    try:
        cwd = os.readlink(f"{base}/cwd")
    except (FileNotFoundError, PermissionError):
        cwd = None
    reasons = []
    exe_path = exe[:-len(" (deleted)")] if exe and exe.endswith(" (deleted)") else exe
    argv0 = argv[0] if argv else None
    if argv0 and not os.path.isabs(argv0): argv0 = os.path.join(cwd, argv0) if cwd else None
    if exe_path and os.path.realpath(exe_path) in known_paths: reasons.append("binary-path")
    elif argv0 and os.path.realpath(argv0) in known_paths: reasons.append("argv0-path")
    for arg in argv[1:]:
        cand = arg if os.path.isabs(arg) else (os.path.join(cwd, arg) if cwd else None)
        if cand and os.path.realpath(cand) == config: reasons.append("config-argument"); break
    digest = None
    if exe is not None:
        try:
            size = os.stat(f"{base}/exe").st_size
            if size in known or reasons:
                digest = sha256(f"{base}/exe")
                if digest in known.get(size, {}): reasons.append("known-binary-hash")
        except (FileNotFoundError, ProcessLookupError):
            continue
        except PermissionError:
            if reasons: unreadable.append(pid)
    if not reasons: continue
    if exe is None: unreadable.append(pid)
    try:
        with open(f"{base}/stat") as f: fields = f.read().rsplit(")", 1)[1].split()
        start = btime + int(fields[19]) / hz
    except (FileNotFoundError, ProcessLookupError):
        continue
    rec = {"pid": pid, "allowed": pid in a.allow, "start_unix": round(start, 2),
           "start_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start)), "exe": exe, "exe_sha256": digest,
           "cwd": cwd, "argv": argv, "reasons": reasons}
    print(json.dumps(rec, sort_keys=True))
    if pid not in a.allow: matches.append(pid)
summary = {"census": "unreadable" if unreadable else ("matches" if matches else "clear"),
           "matches": sorted(matches), "unreadable": sorted(set(unreadable)), "allowed": a.allow, "config": config,
           "known_binaries": sorted(v for d in known.values() for v in d.values()),
           "taken_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
print(json.dumps(summary, sort_keys=True))
sys.exit(2 if unreadable else (3 if matches else 0))
PY
