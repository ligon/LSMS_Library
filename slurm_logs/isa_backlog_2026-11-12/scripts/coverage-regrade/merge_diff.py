#!/usr/bin/env python
"""Merge per-country coverage shards into the committed snapshot, then diff.

Runs INSIDE a burst job with the clone's lsms_library importable.  The merge is
the library's own ``save_snapshot(shard, work, authoritative_countries=[C])``,
applied once per accepted shard -- exactly what ``make matrix C=<C>`` does to
the committed file, country by country.  Never a wholesale replace.

Usage:
    python merge_diff.py --old OLD.csv --status-dir status/ --out merge/ \
        --expected countries_light.txt countries_heavy.txt [--pin C=path ...] \
        [--skip C ...]
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

from lsms_library.coverage_matrix import save_snapshot

KEY = ["country", "feature", "wave"]
READINESS_BUILT = {"blessed", "sane", "builds"}
NOT_BUILT = {"absent", "undeclared", "n/a", "declared", "blocked", "unconfigured"}


def read(p) -> pd.DataFrame:
    return pd.read_csv(p, dtype=str, keep_default_na=False)


def parse_status(p: Path) -> dict:
    d = {}
    for line in p.read_text().splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            d[k] = v
    d["_file"] = str(p)
    return d


def pick_shards(status_dir: Path, pins: dict) -> dict:
    """country -> status dict of its most recent successful attempt."""
    best: dict[str, dict] = {}
    for p in sorted(status_dir.glob("*.status")):
        s = parse_status(p)
        if s.get("rc") != "0" or not s.get("shard") or not Path(s["shard"]).exists():
            continue
        c = s["country"]
        if c not in best or s.get("end", "") > best[c].get("end", ""):
            best[c] = s
    for c, path in pins.items():
        best[c] = {"country": c, "shard": path, "end": "pinned", "_file": "pin"}
    return best


def worse_rule(o: str, n: str) -> str | None:
    """The explicit 'got worse' ordering used in the report."""
    if n == o:
        return None
    if n == "broken":
        return "R1 ->broken"
    if n == "dropped" and o != "broken":
        return "R2 ->dropped"
    if n == "builds" and o in {"sane", "blessed"}:
        return "R3 sane->builds"
    if o in READINESS_BUILT and n in NOT_BUILT:
        return "R4 built->not built/graded"
    if o == "blessed":
        return "R5 blessing lost"
    return None


def classify_vanished(r, new: pd.DataFrame) -> str:
    c, f, w = r["country"], r["feature"], r["wave"]
    nc = new[new["country"] == c]
    if nc.empty:
        return "COUNTRY GONE"
    nf = nc[nc["feature"] == f]
    if r["tier"] == "undeclared" and w == "" and not nf.empty:
        return "undeclared row replaced by per-wave rows (feature now declared)"
    if r["tier"] == "unconfigured":
        return "unconfigured country row replaced"
    if nf.empty:
        return "feature no longer emitted for this country"
    if w not in set(nc["wave"]):
        return "wave no longer emitted for this country"
    if w == "" and (nf["wave"] != "").any():
        return "country-level row replaced by per-wave rows"
    if w != "" and (nf["wave"] == "").all():
        return "per-wave rows replaced by a country-level row"
    return "cell no longer emitted (unclassified)"


def classify_appeared(r, old: pd.DataFrame) -> str:
    c, f, w = r["country"], r["feature"], r["wave"]
    oc = old[old["country"] == c]
    if oc.empty:
        return "new country"
    if r["tier"] == "undeclared" and w == "":
        return "new undeclared row (vocabulary grew, or feature undeclared)"
    if f not in set(oc["feature"]):
        return "new feature for this country"
    if w not in set(oc["wave"]):
        return "new wave for this country"
    of = oc[oc["feature"] == f]
    if (of["tier"] == "undeclared").any() and w != "":
        return "feature newly declared (was undeclared)"
    return "new cell for existing feature/wave"


def md_table(df: pd.DataFrame, max_rows: int | None = None) -> str:
    if df.empty:
        return "_(none)_\n"
    d = df if max_rows is None else df.head(max_rows)
    cols = list(d.columns)
    out = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for _, r in d.iterrows():
        out.append("| " + " | ".join(str(r[c]).replace("|", "\\|").replace("\n", " ")
                                     for c in cols) + " |")
    if max_rows is not None and len(df) > max_rows:
        out.append(f"\n_... {len(df) - max_rows} more rows in the CSV_")
    return "\n".join(out) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", type=Path, required=True)
    ap.add_argument("--status-dir", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--expected", type=Path, nargs="+", required=True)
    ap.add_argument("--pin", nargs="*", default=[], help="C=path overrides")
    ap.add_argument("--skip", nargs="*", default=[], help="countries NOT to merge")
    ap.add_argument("--min-row-ratio", type=float, default=0.8)
    args = ap.parse_args(argv)

    out = args.out
    out.mkdir(parents=True, exist_ok=True)
    expected = []
    for f in args.expected:
        expected += [l for l in f.read_text().splitlines() if l.strip()]
    pins = dict(p.split("=", 1) for p in args.pin)

    old = read(args.old)
    assert not old.duplicated(KEY).any(), "old snapshot has duplicate keys"
    work = out / "new_latest.csv"
    shutil.copyfile(args.old, work)

    best = pick_shards(args.status_dir, pins)
    gate_rows = []
    merged = []
    for c in expected:
        n_old = int((old["country"] == c).sum())
        row = {"country": c, "n_old": n_old}
        if c in args.skip:
            row.update(accepted=False, reason="skipped by operator")
            gate_rows.append(row)
            continue
        s = best.get(c)
        if s is None:
            row.update(accepted=False, reason="no successful shard")
            gate_rows.append(row)
            continue
        sh = read(s["shard"])
        row.update(shard=s["shard"], node=s.get("node", ""), job=s.get("jobid", ""),
                   elapsed_s=s.get("elapsed_s", ""), maxrss_kb=s.get("maxrss_kb", ""),
                   intree_parquet=s.get("intree_parquet", ""),
                   n_new=len(sh),
                   n_broken_old=int(((old["country"] == c) & (old["tier"] == "broken")).sum()),
                   n_broken_new=int((sh["tier"] == "broken").sum()))
        reasons = []
        if set(sh["country"]) != {c}:
            reasons.append(f"shard countries {sorted(set(sh['country']))}")
        if ((sh["feature"] == "<country>") & (sh["tier"] == "broken")).any():
            reasons.append("country load failed")
        decl = sh[sh["coverage"] == "declared"]
        old_decl = old[(old["country"] == c) & (old["coverage"] == "declared")]
        old_all_broken = len(old_decl) > 0 and (old_decl["tier"] == "broken").all()
        if len(decl) and (decl["tier"] == "broken").all() and not old_all_broken:
            reasons.append("every declared cell broken (was not before)")
        if n_old and len(sh) < args.min_row_ratio * n_old:
            reasons.append(f"shard has {len(sh)} rows vs {n_old} before")
        if sh.duplicated(KEY).any():
            reasons.append("duplicate keys in shard")
        if reasons:
            row.update(accepted=False, reason="; ".join(reasons))
            gate_rows.append(row)
            continue
        save_snapshot(sh, work, authoritative_countries=[c])
        merged.append(c)
        row.update(accepted=True, reason="")
        gate_rows.append(row)
    gate = pd.DataFrame(gate_rows)
    gate.to_csv(out / "gate.csv", index=False)

    new = read(work)
    dups = new[new.duplicated(KEY, keep=False)]
    dups.to_csv(out / "duplicate_keys.csv", index=False)

    m = old.merge(new, on=KEY, how="outer", suffixes=("_old", "_new"), indicator=True)
    both = m[m["_merge"] == "both"].copy()
    vanished = m[m["_merge"] == "left_only"][KEY + ["tier_old", "n_rows_old", "detail_old"]].copy()
    appeared = m[m["_merge"] == "right_only"][KEY + ["tier_new", "n_rows_new", "detail_new"]].copy()

    vanished["why"] = [classify_vanished({"country": r.country, "feature": r.feature,
                                          "wave": r.wave, "tier": r.tier_old}, new)
                       for r in vanished.itertuples()]
    appeared["why"] = [classify_appeared({"country": r.country, "feature": r.feature,
                                          "wave": r.wave, "tier": r.tier_new}, old)
                       for r in appeared.itertuples()]
    vanished.to_csv(out / "vanished.csv", index=False)
    appeared.to_csv(out / "appeared.csv", index=False)

    # Transitions over common keys; vanished/appeared get a pseudo-tier.
    trans = pd.concat([
        both[KEY + ["tier_old", "tier_new"]],
        vanished.assign(tier_new="(gone)")[KEY + ["tier_old", "tier_new"]],
        appeared.assign(tier_old="(new)")[KEY + ["tier_old", "tier_new"]],
    ], ignore_index=True)
    overall = (trans.groupby(["tier_old", "tier_new"]).size()
               .rename("cells").reset_index().sort_values("cells", ascending=False))
    overall.to_csv(out / "transitions_overall.csv", index=False)
    changed = trans[trans["tier_old"] != trans["tier_new"]]
    by_cf = (changed.groupby(["country", "feature", "tier_old", "tier_new"]).size()
             .rename("cells").reset_index())
    by_cf.to_csv(out / "transitions_by_country_feature.csv", index=False)

    both["rule"] = [worse_rule(o, n) for o, n in zip(both["tier_old"], both["tier_new"])]
    worse = both[both["rule"].notna()][KEY + ["tier_old", "tier_new", "rule", "n_rows_old",
                                             "n_rows_new", "detail_old", "detail_new"]]
    vanished_built = vanished[vanished["tier_old"].isin(READINESS_BUILT)]
    worse = pd.concat([worse, vanished_built.rename(columns={"why": "rule"}).assign(
        tier_new="(gone)", rule=lambda d: "R6 built cell vanished: " + d["rule"])],
        ignore_index=True)
    worse.to_csv(out / "worse.csv", index=False)

    # Same tier but the row count fell by more than 10%: not a tier regression,
    # but worth seeing before a release.
    num = both[(both["tier_old"] == both["tier_new"])
               & both["tier_old"].isin(READINESS_BUILT)].copy()
    num["o"] = pd.to_numeric(num["n_rows_old"], errors="coerce")
    num["n"] = pd.to_numeric(num["n_rows_new"], errors="coerce")
    shrunk = num[(num["o"] > 0) & (num["n"] < 0.9 * num["o"])]
    shrunk = shrunk.assign(ratio=(shrunk["n"] / shrunk["o"]).round(3))[
        KEY + ["tier_new", "n_rows_old", "n_rows_new", "ratio"]]
    shrunk.to_csv(out / "rows_shrunk.csv", index=False)

    # Detail-only changes among unchanged tiers (counted, not listed).
    detail_changed = int(((both["tier_old"] == both["tier_new"])
                          & (both["detail_old"] != both["detail_new"])).sum())
    nrows_changed = int(((both["tier_old"] == both["tier_new"])
                         & (both["n_rows_old"] != both["n_rows_new"])).sum())

    per_country = []
    for c in sorted(set(old["country"]) | set(new["country"])):
        oc, nc = old[old["country"] == c], new[new["country"] == c]
        tc = trans[trans["country"] == c]
        per_country.append({
            "country": c, "merged": c in merged, "cells_old": len(oc), "cells_new": len(nc),
            "tier_changes": int((tc["tier_old"] != tc["tier_new"]).sum()),
            "worse": int((worse["country"] == c).sum()),
            "sane+blessed old": int(oc["tier"].isin({"sane", "blessed"}).sum()),
            "sane+blessed new": int(nc["tier"].isin({"sane", "blessed"}).sum()),
            "broken old": int((oc["tier"] == "broken").sum()),
            "broken new": int((nc["tier"] == "broken").sum()),
            "dropped old": int((oc["tier"] == "dropped").sum()),
            "dropped new": int((nc["tier"] == "dropped").sum()),
        })
    pc = pd.DataFrame(per_country)
    pc.to_csv(out / "per_country.csv", index=False)

    tiers = sorted(set(old["tier"]) | set(new["tier"]))
    tier_counts = pd.DataFrame({
        "tier": tiers,
        "before": [int((old["tier"] == t).sum()) for t in tiers],
        "after": [int((new["tier"] == t).sum()) for t in tiers],
    })
    tier_counts["delta"] = tier_counts["after"] - tier_counts["before"]
    tier_counts.to_csv(out / "tier_counts.csv", index=False)

    summary = {
        "cells_before": len(old), "cells_after": len(new),
        "countries_before": sorted(set(old["country"])),
        "countries_after": sorted(set(new["country"])),
        "countries_missing_after": sorted(set(old["country"]) - set(new["country"])),
        "merged": merged,
        "not_merged": [r["country"] for r in gate_rows if not r.get("accepted")],
        "duplicate_keys_after": len(dups),
        "common": len(both), "vanished": len(vanished), "appeared": len(appeared),
        "tier_changed_common": int((both["tier_old"] != both["tier_new"]).sum()),
        "worse": len(worse), "rows_shrunk": len(shrunk),
        "detail_only_changes": detail_changed, "n_rows_only_changes": nrows_changed,
        "vanished_why": dict(Counter(vanished["why"])),
        "appeared_why": dict(Counter(appeared["why"])),
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2))

    md = ["# merge/diff summary\n",
          f"cells before {len(old)}, after {len(new)}; common {len(both)}, "
          f"vanished {len(vanished)}, appeared {len(appeared)}; duplicate keys after: {len(dups)}\n",
          "## tier counts\n", md_table(tier_counts),
          "## transitions (all, incl. (new)/(gone))\n", md_table(overall[overall.tier_old != overall.tier_new]),
          "## gate\n", md_table(gate.drop(columns=[c for c in ["shard"] if c in gate.columns])),
          "## per country\n", md_table(pc),
          "## worse\n", md_table(worse[KEY + ["tier_old", "tier_new", "rule", "detail_new"]]),
          "## vanished why\n", md_table(pd.Series(summary["vanished_why"]).rename("cells").reset_index()),
          "## appeared why\n", md_table(pd.Series(summary["appeared_why"]).rename("cells").reset_index()),
          "## rows shrunk >10% (same built tier)\n", md_table(shrunk),
          ]
    (out / "summary.md").write_text("\n".join(md))
    print(json.dumps({k: v for k, v in summary.items()
                      if not k.startswith("countries_")}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
