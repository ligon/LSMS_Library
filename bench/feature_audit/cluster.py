#!/usr/bin/env python
"""Collapse the scanner's raw findings into root-cause CLUSTERS.

The deterministic wide-net (`scan.py`) emits one record per (country, feature,
kwargs, check) — ~565 candidate findings on the full sweep.  Most share a root
cause (one schema mismatch surfacing in 30 countries; one warning message
repeated) or are by-design.  Fanning an agent per raw record would just
re-derive the same handful of causes 30× over.

This script clusters the warn/fail/error records by (check, severity,
normalized-detail) so the agentic phase triages ~20-40 PATTERNS, each carrying
its occurrence count and the affected features/countries.  Output: clusters.json
(sorted hardest-first), which is fed to `audit.workflow.js` as its `args`.

    python bench/feature_audit/cluster.py \
        --in  bench/feature_audit/results/2026-06-18/results.jsonl \
        --out bench/feature_audit/results/2026-06-18/clusters.json
"""
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict

# Severity order for "hardest first" sorting.
SEV_RANK = {"B": 0, "A": 1, "C": 2, None: 3}
STATUS_RANK = {"fail": 0, "error": 1, "warn": 2}


def _normalize(detail: str, countries: set[str]) -> str:
    """Strip the per-occurrence specifics from a detail string so the same root
    cause clusters regardless of which country / number / path it mentions."""
    s = detail or ""
    # quoted strings and filesystem paths -> <X>
    s = re.sub(r"'[^']*'|\"[^\"]*\"", "<X>", s)
    s = re.sub(r"/\S+", "<PATH>", s)
    # country names -> <C> (longest first so "Serbia and Montenegro" wins)
    for c in sorted(countries, key=len, reverse=True):
        if c:
            s = s.replace(c, "<C>")
    # numbers (ints, floats, percents, signed) -> <N>
    s = re.sub(r"[-+]?\d[\d,]*\.?\d*%?", "<N>", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:240]


def cluster(records: list[dict]) -> list[dict]:
    cand = [r for r in records if r.get("status") in ("warn", "fail", "error")]
    countries = {r.get("country") for r in cand if r.get("country")}

    groups: dict[tuple, dict] = {}
    for r in cand:
        pat = _normalize(r.get("detail", ""), countries)
        key = (r.get("check"), r.get("severity"), r.get("status"), pat)
        g = groups.get(key)
        if g is None:
            g = groups[key] = {
                "cluster_id": None,
                "check": r.get("check"),
                "severity": r.get("severity"),
                "status": r.get("status"),
                "pattern": pat,
                "count": 0,
                "features": set(),
                "countries": set(),
                "example_detail": r.get("detail", ""),
                "example_fingerprint": r.get("fingerprint", ""),
                "example_kwargs": r.get("kwargs", {}),
            }
        g["count"] += 1
        if r.get("feature"):
            g["features"].add(r["feature"])
        if r.get("country"):
            g["countries"].add(r["country"])

    clusters = []
    for g in groups.values():
        g["features"] = sorted(g["features"])
        g["countries"] = sorted(g["countries"])
        clusters.append(g)

    clusters.sort(key=lambda g: (
        STATUS_RANK.get(g["status"], 9),
        SEV_RANK.get(g["severity"], 9),
        -g["count"],
    ))
    for i, g in enumerate(clusters, 1):
        g["cluster_id"] = f"C{i:03d}"
    return clusters


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--in", dest="inp", required=True, help="results.jsonl")
    ap.add_argument("--out", default=None, help="clusters.json (default: stdout summary only)")
    args = ap.parse_args(argv)

    records = [json.loads(l) for l in open(args.inp) if l.strip()]
    clusters = cluster(records)

    raw = sum(1 for r in records if r.get("status") in ("warn", "fail", "error"))
    print(f"{raw} candidate findings -> {len(clusters)} clusters")
    print(f"{'id':6} {'stat':5} {'sev':3} {'n':>4}  {'check':26} feats/countries")
    for g in clusters:
        print(f"{g['cluster_id']:6} {g['status']:5} {str(g['severity']):3} "
              f"{g['count']:>4}  {g['check']:26} "
              f"{len(g['features'])}f/{len(g['countries'])}c  {g['pattern'][:48]}")

    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(clusters, fh, indent=2)
        print(f"-> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
