#!/usr/bin/env python3
"""Adversarial verification of analysis_capacity.py's paired-triple TTR analysis.

Independent recompute: grouping done by STRICT structural parse (not the sub-regex),
collision counting via lists (the original dict-of-dicts would hide collisions),
NW t-stat cross-checked against statsmodels OLS HAC (Bartlett, maxlags=2).
"""
import re
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm

HERE = Path(__file__).resolve().parent
rm = pd.read_csv(HERE / "return_matrix.csv", index_col=0)
cols = list(rm.columns)
n_weeks = rm.shape[0]
print(f"matrix shape: {rm.shape[0]} weeks x {rm.shape[1]} configs")
print(f"NaNs in matrix: {int(rm.isna().sum().sum())}")
print(f"duplicate column names: {len(cols) - len(set(cols))}")

# ---------------- (a) grammar + grouping ----------------
TTR_RE = re.compile(r"ttr(\d+(?:\.\d+)?)")

# 1. every config: how many times does the sub-regex match? how many 'ttr' substrings at all?
multi_match = [c for c in cols if len(TTR_RE.findall(c)) != 1]
multi_ttr_substr = [c for c in cols if c.count("ttr") != 1]
print(f"\nconfigs where r'ttr(\\d+(?:\\.\\d+)?)' matches != 1 time: {len(multi_match)}")
print(f"configs where literal substring 'ttr' occurs != 1 time: {len(multi_ttr_substr)}")
for c in (multi_match + multi_ttr_substr)[:5]:
    print("  offender:", c)

# 2. strict structural grammar parse: 5 pipe-fields, 4th = ttr<t>_pb<lo>-<hi>_act<a>_trl<r>_hl<h>, 5th = churn<x>
FIELD4 = re.compile(r"^ttr(\d+(?:\.\d+)?)_pb(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)_act(\d+)_trl(\d+)_hl(\d+(?:\.\d+)?)$")
CHURN = re.compile(r"^churn(\d+(?:\.\d+)?)$")
bad_grammar = []
struct_groups = defaultdict(list)   # structural key (ttr removed) -> [(ttr, config), ...]
sub_groups = defaultdict(list)      # sub-regex key -> [config, ...]  (LISTS, so collisions are visible)
for c in cols:
    parts = c.split("|")
    ok = len(parts) == 5 and FIELD4.match(parts[3]) and CHURN.match(parts[4])
    if not ok:
        bad_grammar.append(c)
        continue
    m = FIELD4.match(parts[3])
    ttr = float(m.group(1))
    skey = (parts[0], parts[1], parts[2], m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), parts[4])
    struct_groups[skey].append((ttr, c))
    sub_groups[TTR_RE.sub("ttrX", c)].append(c)

print(f"\nconfigs failing strict grammar parse: {len(bad_grammar)}")
for c in bad_grammar[:5]:
    print("  bad:", c)

# 3. any OTHER numeric in the key that the sub-regex could have matched?
#    i.e. does 'ttr'+digits appear anywhere outside the field-4 leading position?
stray = []
for c in cols:
    m = TTR_RE.search(c)
    parts = c.split("|")
    if len(parts) == 5 and not parts[3].startswith(f"ttr{m.group(1)}"):
        stray.append(c)
print(f"configs where the sub-regex match is NOT the field-4 leading ttr: {len(stray)}")

# 4. group counts, member counts, ttr sets — via lists so collisions can't hide
print(f"\nstructural groups: {len(struct_groups)}; sub-regex groups: {len(sub_groups)}")
sizes = sorted({len(v) for v in sub_groups.values()})
print(f"sub-regex group sizes seen: {sizes}; total configs across groups: {sum(len(v) for v in sub_groups.values())}")
ssizes = sorted({len(v) for v in struct_groups.values()})
print(f"structural group sizes seen: {ssizes}; total: {sum(len(v) for v in struct_groups.values())}")
bad_triples = {k: v for k, v in struct_groups.items() if sorted(t for t, _ in v) != [24.0, 48.0, 72.0]}
print(f"structural groups whose ttr set != {{24.0,48.0,72.0}}: {len(bad_triples)}")

# 5. the two grouping methods must induce the SAME partition of the 768 configs
part_struct = {frozenset(c for _, c in v) for v in struct_groups.values()}
part_sub = {frozenset(v) for v in sub_groups.values()}
print(f"partitions identical (struct parse vs sub-regex): {part_struct == part_sub}")

# ---------------- NW t-stat: two independent implementations ----------------
def nw_t_manual(d, lag=2):
    d = np.asarray(d, float); n = len(d); m = d.mean(); e = d - m
    s = e @ e / n
    for k in range(1, lag + 1):
        s += 2.0 * (1.0 - k / (lag + 1.0)) * (e[k:] @ e[:-k]) / n
    return m / np.sqrt(s / n)

def nw_t_sm(d, lag=2):
    d = np.asarray(d, float)
    res = sm.OLS(d, np.ones((len(d), 1))).fit(cov_type="HAC",
        cov_kwds={"maxlags": lag, "use_correction": False})
    return float(res.tvalues[0])

# ---------------- (b)/(c) recompute comparisons ----------------
by_ttr = {}  # ttr -> config list aligned by structural group order
gkeys = sorted(struct_groups)  # deterministic order
for t in (24.0, 48.0, 72.0):
    by_ttr[t] = [dict(struct_groups[k])[t] for k in gkeys]

claims = {
    (48.0, 72.0): dict(mean=+3.992, nwt=+0.11, weeks=11, frac=0.45, med=-19.8),
    (24.0, 72.0): dict(mean=-62.208, nwt=-1.94, weeks=7, frac=0.30, med=None),
}
for (a, b) in [(48.0, 72.0), (24.0, 72.0), (24.0, 48.0)]:
    A = rm[by_ttr[a]].to_numpy()   # 24 x 256
    B = rm[by_ttr[b]].to_numpy()
    D = A - B
    weekly = D.mean(axis=1)        # 24 per-week means across triples
    cum = D.sum(axis=0)            # 256 per-triple cumulative diffs
    tman = nw_t_manual(weekly)
    tsm = nw_t_sm(weekly)
    print(f"\n=== ttr{a:.0f} - ttr{b:.0f} (n_triples={D.shape[1]}) ===")
    print(f"mean weekly diff : {weekly.mean():+.6f}")
    print(f"NW-t manual      : {tman:+.4f}   statsmodels HAC: {tsm:+.4f}")
    print(f"weeks won        : {int((weekly > 0).sum())}/{n_weeks}")
    print(f"triples cum>0    : {(cum > 0).mean():.4f} ({int((cum > 0).sum())}/{len(cum)})")
    print(f"median cum diff  : {np.median(cum):+.4f}")
    cl = claims.get((a, b))
    if cl:
        print(f"claimed          : mean {cl['mean']:+.3f}, NW-t {cl['nwt']:+.2f}, "
              f"weeks {cl['weeks']}/24, frac {cl['frac']:.0%}, median {cl['med']}")
