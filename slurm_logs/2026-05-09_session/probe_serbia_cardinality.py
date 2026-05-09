"""
Serbia/2007 cardinality probe -- design input for #246 (D-2).

Question: can we recover the missing `naselje` disambiguator in
`individual.dta` (roster) by joining on a tuple that *is* present in
both individual.dta and domacinstva.dta (sample)?

Three candidate keys to test:
  K1 = (popkrug, dom)              -- known ambiguous (3868 unique vs 5557 HHs)
  K2 = (opstina, popkrug, dom)     -- adds municipality
  K3 = (opstina, popkrug, naselje, dom) -- full sample compound (control)

For each key, on each file, report the unique-tuple count and whether it
matches the file's row-/HH-cardinality.

Also test whether (opstina, popkrug) determines `naselje` in
`enumeration_district.dta` -- if yes, we could add `naselje` to roster
via a 2-column join.
"""
import warnings
warnings.simplefilter("ignore")

import os
import sys
import time

from lsms_library.local_tools import get_dataframe

T0 = time.time()
os.chdir("lsms_library/countries/Serbia/2007/_")

print(f"=== Serbia/2007 cardinality probe ===", flush=True)
print(f"python: {sys.executable}", flush=True)
print(f"cwd:    {os.getcwd()}", flush=True)
print()


def report(label, path, keys):
    print(f"--- {label}: {path} ---", flush=True)
    df = get_dataframe(path)
    print(f"  rows: {df.shape[0]}", flush=True)
    cols = set(df.columns)
    for k in keys:
        present = [c for c in k if c in cols]
        if len(present) != len(k):
            missing = [c for c in k if c not in cols]
            print(f"  {k}: MISSING {missing}", flush=True)
            continue
        n = df[list(k)].drop_duplicates().shape[0]
        print(f"  {k}: unique={n}", flush=True)
    return df


roster = report(
    "individual.dta (roster)",
    "../Data/individual.dta",
    [
        ("popkrug", "dom"),
        ("opstina", "popkrug", "dom"),
        ("opstina", "popkrug", "naselje", "dom"),
        ("opstina", "popkrug"),
    ],
)
print()

sample = report(
    "domacinstva.dta (sample)",
    "../Data/domacinstva.dta",
    [
        ("popkrug", "dom"),
        ("opstina", "popkrug", "dom"),
        ("opstina", "popkrug", "naselje", "dom"),
        ("opstina", "popkrug"),
        ("opstina", "popkrug", "naselje"),
    ],
)
print()

ed = report(
    "enumeration_district.dta (cluster)",
    "../Data/enumeration_district.dta",
    [
        ("popkrug",),
        ("opstina", "popkrug"),
        ("opstina", "popkrug", "naselje"),
    ],
)
print()

# Key question 1: does (opstina, popkrug) -> naselje uniquely?
print("=== Q1: does (opstina, popkrug) determine naselje in ED? ===", flush=True)
groups = ed.groupby(["opstina", "popkrug"])["naselje"].nunique()
n_one = (groups == 1).sum()
n_many = (groups > 1).sum()
print(f"  (opstina, popkrug) tuples with 1 naselje:   {n_one}", flush=True)
print(f"  (opstina, popkrug) tuples with >1 naselje:  {n_many}", flush=True)
if n_many == 0:
    print("  YES -- (opstina, popkrug) -> naselje is functional.", flush=True)
else:
    print("  NO  -- multiple naseljes per (opstina, popkrug).", flush=True)
    print(f"  Example: {groups[groups > 1].head(3).to_dict()}", flush=True)
print()

# Key question 2: in sample, does (opstina, popkrug, dom) match the HH row count?
print("=== Q2: is (opstina, popkrug, dom) unique per sample HH? ===", flush=True)
hh_rows = sample.shape[0]
opd = sample[["opstina", "popkrug", "dom"]].drop_duplicates().shape[0]
print(f"  sample rows:                  {hh_rows}", flush=True)
print(f"  unique (opstina, popkrug, dom): {opd}", flush=True)
if opd == hh_rows:
    print("  YES -- (opstina, popkrug, dom) uniquely identifies sample HHs.", flush=True)
else:
    dup = hh_rows - opd
    print(f"  NO  -- {dup} HHs share (opstina, popkrug, dom) with another HH.", flush=True)
print()

# Key question 3: in roster, does (opstina, popkrug, dom) collapse the same
# 17375 person-rows down to a count consistent with 5557 sample HHs?
print("=== Q3: roster grouped by (opstina, popkrug, dom) -- HH count ===", flush=True)
roster_hh = roster[["opstina", "popkrug", "dom"]].drop_duplicates().shape[0]
roster_rows = roster.shape[0]
print(f"  roster rows (people):            {roster_rows}", flush=True)
print(f"  unique (opstina, popkrug, dom):  {roster_hh}", flush=True)
print(f"  avg HH size if {roster_hh} HHs: {roster_rows / max(roster_hh, 1):.2f}", flush=True)

# Optional: per-HH person count distribution
hh_sizes = roster.groupby(["opstina", "popkrug", "dom"]).size()
print(f"  HH size distribution: min={hh_sizes.min()} median={hh_sizes.median():.1f} max={hh_sizes.max()}", flush=True)
print()

# Key question 4: does roster's (opstina, popkrug, dom) intersect sample's?
print("=== Q4: roster keys vs sample keys overlap ===", flush=True)
roster_keys = set(map(tuple, roster[["opstina", "popkrug", "dom"]].drop_duplicates().values.tolist()))
sample_keys = set(map(tuple, sample[["opstina", "popkrug", "dom"]].drop_duplicates().values.tolist()))
overlap = roster_keys & sample_keys
only_roster = roster_keys - sample_keys
only_sample = sample_keys - roster_keys
print(f"  roster keys: {len(roster_keys)}", flush=True)
print(f"  sample keys: {len(sample_keys)}", flush=True)
print(f"  overlap:     {len(overlap)}", flush=True)
print(f"  roster-only: {len(only_roster)}", flush=True)
print(f"  sample-only: {len(only_sample)}", flush=True)
print()

print(f"=== elapsed: {time.time()-T0:.1f}s ===", flush=True)
