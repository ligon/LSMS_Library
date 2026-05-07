#!/usr/bin/env python3
"""Aggregate sweep_*/summary.tsv into a per-config table.

Usage: python analyze.py <sweep_dir>
"""
import json
import statistics as stats
import sys
from collections import defaultdict
from pathlib import Path


def main(sweep_dir):
    sweep_dir = Path(sweep_dir).resolve()
    summary_tsv = sweep_dir / "summary.tsv"
    if not summary_tsv.exists():
        print(f"missing {summary_tsv}", file=sys.stderr)
        sys.exit(2)

    rows = []
    for line in summary_tsv.read_text().splitlines()[1:]:  # skip header
        parts = line.split("\t")
        if len(parts) < 8:
            continue
        cfg, trial, n, elapsed, succ, fail, frate, exc = parts[:8]
        rows.append({
            "config": cfg,
            "trial": int(trial),
            "n": int(n),
            "elapsed": float(elapsed) if elapsed != "NA" else float("nan"),
            "successes": int(succ) if succ != "NA" else 0,
            "failures": int(fail) if fail != "NA" else 0,
            "fail_rate": float(frate) if frate != "NA" else float("nan"),
            "exc": exc,
        })

    by_cfg = defaultdict(list)
    for r in rows:
        by_cfg[r["config"]].append(r)

    print(f"Sweep: {sweep_dir.name}")
    print(f"  {len(rows)} trials across {len(by_cfg)} configs")
    print()
    print(f"{'config':<22} {'trials':>6} {'mean_elapsed':>12} {'mean_fail':>10} {'fail_rates':<24}")
    print("-" * 80)
    for cfg, trs in by_cfg.items():
        elapsed = [t["elapsed"] for t in trs if t["elapsed"] == t["elapsed"]]
        frates = [t["fail_rate"] for t in trs if t["fail_rate"] == t["fail_rate"]]
        me = stats.mean(elapsed) if elapsed else float("nan")
        mf = stats.mean(frates) if frates else float("nan")
        rate_list = ", ".join(f"{f:.2f}" for f in frates)
        print(f"{cfg:<22} {len(trs):>6} {me:>12.2f} {mf:>10.3f} {rate_list:<24}")

    # Also show raw exception counts per config (across trials)
    print()
    print("Exception summary by config:")
    for cfg, trs in by_cfg.items():
        agg = defaultdict(int)
        for t in trs:
            try:
                d = json.loads(t["exc"]) if t["exc"] not in ("", "NO_RESULT") else {}
            except json.JSONDecodeError:
                d = {}
            for k, v in d.items():
                agg[k] += v
        print(f"  {cfg:<22} {dict(agg) if agg else '-'}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else ".")
