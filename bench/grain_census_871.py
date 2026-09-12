#!/usr/bin/env python
"""GH #871 pre-landing census: what would row-coherent selection change?

READ-ONLY.  Measures, per ``(country, table, wave)`` cell, the difference
between today's per-column ``groupby(...).first()`` composite and a row-coherent
SELECTION at the two core *build*-path collapse sites.  ``--reducer`` picks the
candidate: ``nth0`` (row 0 of the group -- the original proposal, and what the
2026-09-12 REPORT.org measured) or ``most-complete`` (the most complete observed
row, which is what GH #871 landed).  Sites:

  Site 1  ``country._normalize_dataframe_index``   (declared-index collapse)
  Site 2  ``country._collapse_to_cluster_grain``   (household -> cluster projection)

Inputs are the warm caches ONLY -- nothing is rebuilt, no DVC, no network:

  * ``{data_root}/{C}/var/{table}.parquet``      -> the stamped ``lsms_grain_audit``
    (``local_tools.read_parquet_grain_audit``), which gives the CELL LIST but no
    per-column or per-row detail.
  * ``{data_root}/{C}/{wave}/_/{table}.parquet`` -> the PRE-collapse wave frame
    (written by ``Wave.grab_data`` for the YAML route, by ``local_tools.to_parquet``
    from ``_/{table}.py`` for the script route).  Both are pre-Site-1 and
    pre-Site-2.

A cell with no pre-collapse wave parquet is reported ``uncensused`` -- never
rebuilt.

Usage::

    cd <worktree> && PYTHONPATH=<worktree> LSMS_BUILD_WORKERS=1 \
        python bench/grain_census_871.py --out slurm_logs/gh871_grain_census
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from pathlib import Path

import pandas as pd

ALWAYS_TABLES = ("household_roster", "cluster_features")


# --------------------------------------------------------------------------
# Site emulation
# --------------------------------------------------------------------------
def _declared(scheme_entry):
    from lsms_library.country import _declared_index_levels
    return _declared_index_levels(scheme_entry)


def _cat_to_str(df):
    """Site 1's unordered-categorical pre-pass (country.py:5963-5966)."""
    for col in df.columns:
        if hasattr(df[col], "cat") and not df[col].cat.ordered:
            df[col] = df[col].astype(str).replace(
                {"nan": pd.NA, "None": pd.NA, "<NA>": pd.NA})
    return df


def _site1_frame(df, declared, wave):
    """Reproduce _normalize_dataframe_index up to (not including) the collapse."""
    current = list(df.index.names)
    if ("j" in declared and "i" not in declared
            and "i" in current and "j" not in current):
        df = df.rename_axis(index={"i": "j"})
        current = list(df.index.names)
    missing = [l for l in declared if l not in current]
    if missing:
        df = df.reset_index()
        for level in missing:
            if level == "t" and wave is not None:
                df[level] = wave
        available = [l for l in declared if l in df.columns]
        if available:
            df = df.set_index(available)
        current = list(df.index.names)
    present_declared = [l for l in declared if l in current]
    if present_declared:
        remaining = [l for l in current if l not in present_declared]
        try:
            df = df.reorder_levels(present_declared + remaining)
        except (ValueError, TypeError):
            pass
    extra = [l for l in df.index.names if l not in declared]
    if (extra and isinstance(df.index, pd.MultiIndex)
            and len(df.index.names) > len(extra)):
        try:
            df = df.droplevel(extra)
        except ValueError:
            pass
    levels = [l for l in declared if l in df.index.names]
    return df, levels


def _cluster_cleanups(df):
    """The rest of Wave.cluster_features after the Site 2 projection."""
    if "i" in df.columns:
        df = df.drop(columns="i")
    if "m" in (df.index.names or []):
        df = df.reset_index(level="m").rename(columns={"m": "Region"})
    if "m" in df.columns:
        df = df.rename(columns={"m": "Region"})
    return df


# --------------------------------------------------------------------------
# The diff itself
# --------------------------------------------------------------------------
def _derivation_conflicts(df, levels):
    """Groups carrying >1 distinct Derivation key (split on MULTI_SEP)."""
    try:
        import importlib
        derivations = importlib.import_module("lsms_library.derivations")
    except Exception:                                         # noqa: BLE001
        return None
    col = derivations.COLUMN
    if col not in df.columns:
        return None
    sep = derivations.MULTI_SEP
    flat = df.reset_index()[list(levels) + [col]]
    flat[col] = flat[col].astype("string")
    flat = flat.dropna(subset=[col])
    if flat.empty:
        return 0
    flat[col] = flat[col].str.split(sep, regex=False)
    flat = flat.explode(col)
    flat[col] = flat[col].str.strip()
    n = flat.groupby(list(levels), dropna=False, observed=True)[col].nunique()
    return int((n > 1).sum())


#: Which candidate reducer the diff measures against today's ``first()``.
#: ``nth0`` is the original proposal (row 0 of the group); ``most-complete`` is
#: what landed for GH #871 (the most complete observed row).  Set from
#: ``--reducer`` in ``main()`` BEFORE the pool forks, so workers inherit it.
REDUCER = "nth0"


def _diff_at(df, levels, site, country, table, wave):
    """Compare first() against the candidate reducer on *df* grouped by *levels*.

    Returns (record, collapsed_first_frame).  ``collapsed_first_frame`` is what
    today's code serves onward (so the Site-1 pass after a Site-2 projection
    sees the same frame the library would).
    """
    from lsms_library.country import _audit_index_collapse

    rec = dict(country=country, table=table, wave=wave, site=site,
               levels=",".join(levels), rows=int(len(df)),
               unique_index=bool(df.index.is_unique))
    if not levels:
        rec.update(status="no_levels")
        return rec, df
    if df.index.is_unique:
        rec.update(status="unique", dropped=0, destroyed=0,
                   conflicting_groups=0, nan_key_rows=0, rows_changed=0,
                   served_na_new_total=0, cols_involved="",
                   served_na_new_by_col="{}", derivation_conflicts=0)
        return rec, df

    audit = _audit_index_collapse(df, levels) or {}
    rec.update(dropped=int(audit.get("dropped", 0)),
               destroyed=int(audit.get("destroyed", 0)),
               conflicting_groups=int(audit.get("conflicting_groups", 0)),
               nan_key_rows=int(audit.get("nan_key_rows", 0)),
               audit_unauditable=audit.get("unauditable") or "")

    # Columns today's code does NOT select row-wise and the proposal leaves
    # alone: the additive measures (summed, ``_ADDITIVE_MEASURE_COLUMNS``) and a
    # ``Price`` re-derived from the summed totals.  Identical under both
    # reducers, so they are excluded from the diff rather than counted as
    # changes.
    try:
        from lsms_library.feature import _ADDITIVE_MEASURE_COLUMNS
        additive = [c for c in (_ADDITIVE_MEASURE_COLUMNS.get(table) or ())
                    if c in df.columns]
    except Exception:                                         # noqa: BLE001
        additive = []
    reserved = list(additive)
    if additive and "Price" in df.columns and {"Expenditure", "Quantity"} <= set(df.columns):
        reserved.append("Price")
    rec["reducer_columns"] = "|".join(reserved)
    # Mirror the build's residual re-audit (country.py:5992-6006): for an
    # additive table the stamp reports what the SUM does not reconcile, so the
    # census must re-audit on the same residual or it will disagree with the
    # stamp (measured: Nigeria/assets 13,447 raw vs 11,936 stamped).
    if reserved and not audit.get("unauditable"):
        residual = _audit_index_collapse(df.drop(columns=reserved), levels)
        if residual is None:
            rec.update(destroyed=0, conflicting_groups=0)
        elif not residual.get("unauditable"):
            rec.update(destroyed=int(residual["destroyed"]),
                       conflicting_groups=int(residual["conflicting_groups"]),
                       destroyed_unreconciled=int(audit.get("destroyed", 0)))

    g = df.groupby(level=levels, observed=True)
    f = g.first()
    if REDUCER == "most-complete":
        # GH #871 as LANDED (@ligon, 2026-09-12): select the MOST COMPLETE row of
        # the group -- fewest NA cells over the columns core does not reduce,
        # ties on original order.  The exclude set is the library's: the additive
        # measures, a RE-DERIVED ``Price``, and ``Derivation`` on both branches
        # (see country.most_complete_row for why each one has no vote).
        from lsms_library.country import most_complete_row
        from lsms_library.derivations import COLUMN as _DERIV
        exclude = set(additive)
        if _DERIV in df.columns:
            exclude.add(_DERIV)
        if additive and "Price" in df.columns and {"Expenditure", "Quantity"} <= set(df.columns):
            exclude.add("Price")
        r = most_complete_row(df, levels, exclude=exclude)
    else:
        r = g.nth(0)
        # nth(0) keeps the ORIGINAL index (all levels, original row order).
        # Reduce it to the group key so it aligns with first(), then sort as the
        # proposal does.
        extra = [n for n in (r.index.names or []) if n not in levels]
        if extra and len(r.index.names) > len(extra):
            r = r.droplevel(extra)
        if isinstance(r.index, pd.MultiIndex) and list(r.index.names) != list(levels):
            r = r.reorder_levels(list(levels))
        r = r.sort_index()
    r = r.reindex(f.index)
    r = r[f.columns]

    # Stringify like the audit does (so every dtype is comparable), but
    # NORMALISE the missing-value spelling first: ``first()`` and ``nth(0)`` can
    # return the SAME missing value in different clothes (``None`` vs ``pd.NA``
    # vs ``float('nan')``) on an object column, and a naive ``astype(str)``
    # diff then flags every such row as "changed" (measured: 193,115 of 194,246
    # rows in Burkina_Faso/shocks/2014, a cell whose audited destruction is ONE
    # row).  ``isna()`` is the ground truth for missingness; use it.
    if reserved:
        keep = [c for c in f.columns if c not in reserved]
        f, r = f[keep], r[keep]
    raw_mask = f.astype(str).ne(r.astype(str))
    fs = f.astype(str).mask(f.isna(), "<NA>")
    rs = r.astype(str).mask(r.isna(), "<NA>")
    mask = fs.ne(rs)
    cols = mask.any()
    cols_involved = [c for c in cols.index if bool(cols[c])]
    na_new = ((f.notna() & r.isna()).sum()).astype(int)
    na_new = {c: int(v) for c, v in na_new.items() if v}
    rec.update(status="diffed",
               rows_changed=int(mask.any(axis=1).sum()),
               rows_changed_raw=int(raw_mask.any(axis=1).sum()),
               cols_involved="|".join(map(str, cols_involved)),
               served_na_new_total=int(sum(na_new.values())),
               served_na_new_by_col=json.dumps(na_new, sort_keys=True),
               derivation_conflicts=_derivation_conflicts(df, levels))
    return rec, f


def census_cell(task):
    """One (country, table, wave) cell.  Returns a list of per-site records."""
    country, table, wave = task["country"], task["table"], task["wave"]
    base = dict(country=country, table=table, wave=wave,
                stamped_destroyed=task.get("stamped_destroyed"),
                stamped_unauditable=task.get("stamped_unauditable") or "",
                stamped_sites=task.get("stamped_sites") or "",
                always=bool(task.get("always")))
    path = task.get("path")
    if not path or not Path(path).exists():
        return [dict(base, site="", status="uncensused",
                     note="no pre-collapse wave parquet")]
    try:
        df = pd.read_parquet(path)
    except Exception as exc:                                  # noqa: BLE001
        return [dict(base, site="", status="unreadable", note=repr(exc))]
    if df.empty:
        return [dict(base, site="", status="empty")]
    # Replay the build's own per-wave restriction (grab_data country.py:1646 and
    # build_wave :3743-3748): a script-path parquet shared by several waves of
    # one folder (Nigeria's PP/PH quarters) otherwise gets counted whole, once
    # per wave.  This is the build's step, not an approximation.
    if "t" in (df.index.names or []) and wave is not None:
        sub = df[df.index.get_level_values("t").astype(str) == str(wave)]
        if len(sub):
            df = sub
            base["t_filtered"] = True

    out = []
    try:
        declared = task["declared"]
        if table == "cluster_features":
            if "i" in (df.index.names or []):
                keep = [l for l in df.index.names if l != "i"]
                audit_frame = df.droplevel("i")
                audit_frame = _cat_to_str(audit_frame.copy())
                rec, collapsed = _diff_at(audit_frame, keep, "Site2",
                                          country, table, wave)
                out.append(dict(base, **rec))
                df = collapsed
            df = _cluster_cleanups(df)
        frame, levels = _site1_frame(df, declared, wave)
        frame = _cat_to_str(frame.copy())
        rec, _ = _diff_at(frame, levels, "Site1", country, table, wave)
        out.append(dict(base, **rec))
    except Exception:                                         # noqa: BLE001
        out.append(dict(base, site="", status="error",
                        note=traceback.format_exc(limit=3)))
    return out


# --------------------------------------------------------------------------
# Task construction
# --------------------------------------------------------------------------
def build_tasks():
    from lsms_library.country import Country
    from lsms_library.local_tools import read_parquet_grain_audit
    from lsms_library.paths import data_root

    root = Path(data_root())
    stamps = []                      # every stamped report, verbatim
    cells = {}                       # (country, table, wave) -> task

    countries = sorted(p.name for p in root.iterdir()
                       if p.is_dir() and p.name != "dvc-cache")

    scheme_cache = {}

    def scheme_entry(country, table):
        if country not in scheme_cache:
            try:
                res = Country(country).resources or {}
            except Exception:                                 # noqa: BLE001
                res = {}
            scheme_cache[country] = res.get("Data Scheme") or {}
        return scheme_cache[country].get(table)

    def wave_parquet(country, table, wave):
        if wave is None:
            return None
        p = root / country / str(wave) / "_" / f"{table}.parquet"
        return str(p) if p.exists() else None

    def add(country, table, wave, always=False):
        key = (country, table, wave)
        if key in cells:
            cells[key]["always"] = cells[key]["always"] or always
            return cells[key]
        entry = scheme_entry(country, table)
        cells[key] = dict(country=country, table=table, wave=wave,
                          declared=_declared(entry),
                          path=wave_parquet(country, table, wave),
                          always=always, stamped_destroyed=None,
                          stamped_unauditable="", stamped_sites="")
        return cells[key]

    # 1. the stamped cell list
    for country in countries:
        var = root / country / "var"
        if not var.is_dir():
            continue
        for pq in sorted(var.glob("*.parquet")):
            table = pq.stem
            reports = read_parquet_grain_audit(pq) or []
            for rep in reports:
                rep = dict(rep)
                rep.setdefault("country", country)
                rep.setdefault("table", table)
                stamps.append(rep)
                if not (rep.get("destroyed") or rep.get("unauditable")):
                    continue
                t = add(rep.get("country") or country,
                        rep.get("table") or table, rep.get("wave"))
                t["stamped_destroyed"] = (t["stamped_destroyed"] or 0) + int(
                    rep.get("destroyed") or 0)
                if rep.get("unauditable"):
                    t["stamped_unauditable"] = str(rep["unauditable"])[:200]
                sites = set(filter(None, t["stamped_sites"].split("|")))
                sites.add(str(rep.get("site") or "Site1"))
                t["stamped_sites"] = "|".join(sorted(sites))

    # 1b. every cached pre-collapse wave parquet, so the census does not depend
    #     on the L2-country stamp layer being warm (only 388 var/ parquets exist
    #     against ~504 declared (country, table) pairs, and a cell with no var/
    #     parquet would otherwise be invisible rather than uncensused).
    for country in countries:
        for pq in sorted((root / country).glob("*/_/*.parquet")):
            add(country, pq.stem, pq.parent.parent.name)

    # 2. every household_roster / cluster_features cell the CONFIG declares,
    #    stamped or not -- that is where the served_na_new cost lives.  Enumerated
    #    from config (not from what happens to be cached) so a cell with no
    #    pre-collapse parquet is reported ``uncensused`` rather than omitted.
    for country in countries:
        try:
            c = Country(country)
            declared_tables = set(c.resources.get("Data Scheme") or {})
            waves = list(c.waves)
        except Exception:                                     # noqa: BLE001
            declared_tables, waves = set(), []
        for table in ALWAYS_TABLES:
            seen = set()
            for pq in sorted((root / country).glob(f"*/_/{table}.parquet")):
                w = pq.parent.parent.name
                seen.add(w)
                add(country, table, w, always=True)
            if table in declared_tables:
                for w in waves:
                    if str(w) not in seen:
                        add(country, table, str(w), always=True)

    return list(cells.values()), stamps



def _reconcile(df):
    """Does the census's pre-collapse frame match the frame the build collapsed?

    The stamped ``destroyed`` is the only available check.  Any cell where the
    census total differs from the stamp means the frame differs from the one
    Site 1/2 actually saw (``id_walk`` re-keying, a script-path parquet the
    build filters differently, a wave-folder mismatch) -- so its numbers are
    discounted, not trusted.
    """
    if "stamped_destroyed" not in df.columns:
        return {}
    cens = (df[df["status"] == "diffed"]
            .groupby(["country", "table", "wave"], dropna=False)["destroyed"]
            .sum())
    stamped = (df[df["stamped_destroyed"].notna()]
               .drop_duplicates(["country", "table", "wave"])
               .set_index(["country", "table", "wave"])["stamped_destroyed"])
    out = {"cells_compared": 0, "cells_agreeing": 0, "mismatches": []}
    for key, want in stamped.items():
        got = cens.get(key)
        if got is None:
            continue
        out["cells_compared"] += 1
        if int(got) == int(want):
            out["cells_agreeing"] += 1
        else:
            out["mismatches"].append(
                {"country": key[0], "table": key[1], "wave": key[2],
                 "stamped_destroyed": int(want), "census_destroyed": int(got)})
    return out


# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="slurm_logs/gh871_grain_census")
    ap.add_argument("--procs", type=int, default=40)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--only", default="", help="substring filter on country")
    ap.add_argument("--reducer", default="nth0",
                    choices=("nth0", "most-complete"),
                    help="candidate reducer to diff against first() "
                         "(most-complete is what GH #871 landed)")
    args = ap.parse_args()

    global REDUCER
    REDUCER = args.reducer
    print(f"reducer under test: {REDUCER}", file=sys.stderr)

    os.environ.setdefault("LSMS_BUILD_WORKERS", "1")
    import lsms_library
    print(f"lsms_library from {lsms_library.__file__}", file=sys.stderr)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    tasks, stamps = build_tasks()
    if args.only:
        tasks = [t for t in tasks if args.only in t["country"]]
    if args.limit:
        tasks = tasks[: args.limit]
    print(f"{len(tasks)} cells, {len(stamps)} stamped reports "
          f"({time.time() - t0:.1f}s to index)", file=sys.stderr)

    # Biggest files first so the tail of the pool is short.
    def size(t):
        p = t.get("path")
        try:
            return -Path(p).stat().st_size
        except Exception:                                     # noqa: BLE001
            return 0
    tasks.sort(key=size)

    records = []
    if args.procs > 1:
        import multiprocessing as mp
        with mp.Pool(args.procs) as pool:
            for i, recs in enumerate(pool.imap_unordered(census_cell, tasks), 1):
                records.extend(recs)
                if i % 25 == 0:
                    print(f"  {i}/{len(tasks)} ({time.time() - t0:.0f}s)",
                          file=sys.stderr)
    else:
        for t in tasks:
            records.extend(census_cell(t))

    df = pd.DataFrame(records)
    csv = out / "census_871.csv"
    df.to_csv(csv, index=False)
    (out / "stamps_871.json").write_text(json.dumps(stamps, indent=1))

    d = df[df["status"].eq("diffed")] if "status" in df.columns else df.iloc[:0]
    summary = {
        "generated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "elapsed_s": round(time.time() - t0, 1),
        "cells_total": int(len(set(zip(df["country"], df["table"], df["wave"].astype(str))))),
        "records": int(len(df)),
        "status_counts": df["status"].value_counts().to_dict(),
        "cells_uncensused": int((df["status"] == "uncensused").sum()),
        "rows_changed_total": int(d["rows_changed"].sum()) if len(d) else 0,
        "served_na_new_total": int(d["served_na_new_total"].sum()) if len(d) else 0,
        "destroyed_total": int(d["destroyed"].sum()) if len(d) else 0,
        "derivation_conflicts_total": (
            int(d["derivation_conflicts"].fillna(0).sum()) if len(d) else 0),
        "by_site": (d.groupby("site")[["rows_changed", "served_na_new_total"]]
                    .sum().to_dict() if len(d) else {}),
        "by_table": (d.groupby("table")[["rows_changed", "served_na_new_total"]]
                     .sum().sort_values("served_na_new_total", ascending=False)
                     .head(25).to_dict() if len(d) else {}),
        "rows_changed_raw_total": int(d["rows_changed_raw"].sum()) if len(d) else 0,
        "conflicting_groups_total": int(d["conflicting_groups"].sum()) if len(d) else 0,
        "unauditable_records": int((df.get("audit_unauditable", pd.Series(dtype=str))
                                    .fillna("") != "").sum()),
        "site2_cols_involved": sorted(
            {c for v in d[d["site"] == "Site2"]["cols_involved"].fillna("")
             for c in str(v).split("|") if c}) if len(d) else [],
        "stamp_reconciliation": _reconcile(df),
        "top15_served_na_new": (
            d.nlargest(15, "served_na_new_total")[
                ["country", "table", "wave", "site", "rows", "destroyed",
                 "rows_changed", "served_na_new_total", "served_na_new_by_col"]
            ].to_dict("records") if len(d) else []),
        "top15_rows_changed": (
            d.nlargest(15, "rows_changed")[
                ["country", "table", "wave", "site", "rows", "destroyed",
                 "rows_changed", "served_na_new_total", "cols_involved"]
            ].to_dict("records") if len(d) else []),
    }
    (out / "summary_871.json").write_text(json.dumps(summary, indent=1,
                                                     default=str))
    print(json.dumps({k: v for k, v in summary.items()
                      if not k.startswith("top15")}, indent=1, default=str))
    print(f"wrote {csv}", file=sys.stderr)


if __name__ == "__main__":
    main()
