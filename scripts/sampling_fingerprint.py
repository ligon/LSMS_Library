#!/usr/bin/env python3
"""Derive a per-(country, wave) sampling-design FINGERPRINT from ``sample()``.

This is a SCREEN, not documentation.  It reports what the realized sample
looks like -- how many PSUs, how many strata, how dispersed the weights are
-- from which clustering, stratification and panel structure are legible.
It cannot tell you the number of sampling STAGES, whether selection was
PPS, or about any oversample the published weights already deflate, which
is most of them.

Use it to (a) get cross-country coverage where no prose exists, and (b)
find where the prose and the data disagree.  Do not promote a column of
this table into a `sampling.yml` claim without a document behind it -- see
`docs/guide/population.md` and `lsms_library/capability.py` for why this
library keeps a `source_type` / `confidence` beside every such assertion.

Writes CSV to stdout, or to --out.
"""
from __future__ import annotations

import argparse
import sys
import traceback
import warnings

import numpy as np
import pandas as pd


def _nunique(g: pd.DataFrame, col: str):
    """Distinct non-null values, or None when the column is absent.

    Never returns ``pd.NA``: comparing that against anything yields NA, and
    ``bool(NA)`` raises ``TypeError: boolean value of NA is ambiguous``.
    ``None`` is falsy and compares cleanly, so guards below stay simple.
    """
    if col not in g.columns:
        return None
    try:
        return int(g[col].nunique(dropna=True))
    except (TypeError, ValueError):
        # unhashable / exotic dtype
        return None


def _fingerprint_wave(g: pd.DataFrame) -> dict:
    out = {"n_hh": len(g)}
    out["n_psu"] = _nunique(g, "v")
    out["hh_per_psu"] = (round(len(g) / out["n_psu"], 2)
                         if out["n_psu"] else None)
    out["n_strata"] = _nunique(g, "strata")

    if "weight" in g.columns:
        w = pd.to_numeric(g["weight"], errors="coerce").dropna()
        w = w[np.isfinite(w)]
        out["wt_n"] = len(w)
        if len(w):
            mean, med = float(w.mean()), float(w.median())
            if mean:
                out["wt_cv"] = round(float(w.std()) / mean, 3)
            if med:
                out["wt_max_over_med"] = round(float(w.max()) / med, 2)
            # self-weighting shows up as a single distinct value
            out["wt_distinct"] = int(w.nunique())
    if "panel_weight" in g.columns:
        pw = pd.to_numeric(g["panel_weight"], errors="coerce")
        out["panel_wt_pct"] = round(100 * float(pw.notna().mean()))
    if "Rural" in g.columns:
        vals = g["Rural"].astype("string")
        if bool(vals.notna().any()):
            out["rural_pct"] = round(100 * float((vals == "Rural").mean()))
    return out


def fingerprint_country(name: str) -> list[dict]:
    import lsms_library as ll
    c = ll.Country(name)
    if "sample" not in c.data_scheme:
        return [{"country": name, "wave": pd.NA, "status": "no sample table"}]
    df = c.sample().reset_index()
    if "t" not in df:
        return [{"country": name, "wave": pd.NA, "status": "no t level"}]
    rows = []
    for wave, g in df.groupby("t", dropna=False):
        rows.append({"country": name, "wave": wave, "status": "ok",
                     **_fingerprint_wave(g)})
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("countries", nargs="*", help="default: every country with a sample table")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    warnings.simplefilter("ignore")
    from lsms_library.paths import countries_root

    names = args.countries or sorted(
        d.name for d in countries_root().iterdir()
        if (d / "_" / "data_scheme.yml").exists()
    )

    rows: list[dict] = []
    for n in names:
        try:
            got = fingerprint_country(n)
            rows.extend(got)
            ok = sum(r.get("status") == "ok" for r in got)
            print(f"{n:22s} {ok} wave(s)", file=sys.stderr, flush=True)
        except Exception as exc:
            rows.append({"country": n, "wave": pd.NA,
                         "status": f"FAILED: {type(exc).__name__}: {str(exc)[:120]}"})
            print(f"{n:22s} FAILED {type(exc).__name__}", file=sys.stderr, flush=True)
            traceback.print_exc(limit=1, file=sys.stderr)

    cols = ["country", "wave", "status", "n_hh", "n_psu", "hh_per_psu", "n_strata",
            "wt_n", "wt_cv", "wt_max_over_med", "wt_distinct", "panel_wt_pct", "rural_pct"]
    out = pd.DataFrame(rows).reindex(columns=cols)
    if args.out:
        out.to_csv(args.out, index=False)
        print(f"\nwrote {args.out} ({len(out)} rows)", file=sys.stderr)
    else:
        out.to_csv(sys.stdout, index=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
