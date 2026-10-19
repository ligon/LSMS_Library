#!/usr/bin/env python
"""GH #928 census: which column each key-resolving caller keys on, per table.

Config-only (no microdata, no cache): reads every ``categorical_mapping`` level
the library reads -- the package root, every country ``_/``, every wave ``_/``
-- and every SERVED table (``Country.categorical_mapping``, the merged
global+country view, and ``Wave.categorical_mapping``, which overlays the wave
level).  For each served table it records the key column of each caller and a
hash of what that caller serves, so a before/after diff shows exactly which
served mapping moved.

The callers (``country.py``):

  merge    ``_row_union_categorical`` via ``_categorical_key_column``
           (also ``_unit_kg_factors``)
  auto     ``_apply_categorical_mappings._build_replace_dict`` -- driven through
           the REAL method on a synthetic frame (column and index level named
           after the table, holding every cell value of the table), once for
           the default target and once per other column as a ``labels=`` target
  market   ``_market_lookup``'s cluster_features normalisation
  hook     the ``harmonize_<method>`` hook in ``_finalize_result``

``market`` and ``hook`` were inline before GH #928, so on the base they are
emulated by a verbatim copy of their inline expression; after, the census calls
the resolver they now route through (``_categorical_key_column``).  The script
detects which by the resolver's signature.

Run:  PYTHONSAFEPATH=1 PYTHONPATH=$PWD .venv/bin/python \
          slurm_logs/gh928_key_resolver/census.py OUT.json
"""
from __future__ import annotations

import hashlib
import inspect
import json
import sys
import warnings
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

warnings.filterwarnings("ignore")
import lsms_library  # noqa: E402
from lsms_library import country as cm  # noqa: E402
from lsms_library.local_tools import (categorical_mapping_files,  # noqa: E402
                                      global_categorical_level,
                                      load_categorical_level)
from lsms_library.paths import countries_root  # noqa: E402

AFTER = "idxvars" in inspect.signature(cm._categorical_key_column).parameters
PL = "Preferred Label"


def h(obj) -> str:
    return hashlib.sha256(repr(obj).encode()).hexdigest()[:16]


def table_hash(t: pd.DataFrame) -> str:
    return h((list(map(str, t.columns)), [str(d) for d in t.dtypes],
              t.to_csv(index=False)))


def inline_key(table):
    """Verbatim copy of the pre-#928 inline pick at the market/hook sites."""
    if PL not in table.columns:
        return None
    src = [c for c in table.columns if c != PL]
    return src[0] if src else None


def market_key(table):
    if not AFTER:
        return inline_key(table)
    if PL not in table.columns:
        return None
    return cm._categorical_key_column(table)


hook_key = market_key   # same expression at both sites, before and after


def fake_country(name, table):
    fake = SimpleNamespace(name="Census", categorical_mapping={name: table})
    fake._apply_categorical_mappings = (
        cm.Country._apply_categorical_mappings.__get__(fake))
    return fake


def values_of(table):
    vals = []
    for c in table.columns:
        col = table[c]
        if isinstance(col, pd.DataFrame):      # duplicate header
            col = col.iloc[:, 0]
        vals.extend(col.tolist())
    seen, out = set(), []
    for v in vals:
        k = repr(v)
        if k not in seen:
            seen.add(k)
            out.append(v)
    return out


def auto_served(name, table, variant=None):
    """What _apply_categorical_mappings serves for every cell value of table."""
    vals = values_of(table)
    idx = pd.MultiIndex.from_arrays(
        [pd.Index(vals, dtype=object), pd.RangeIndex(len(vals))],
        names=[name, "_n"])
    df = pd.DataFrame({name: pd.Series(vals, dtype=object, index=idx)})
    df = df.copy()
    labels = {name: variant} if variant is not None else None
    try:
        out = fake_country(name, table)._apply_categorical_mappings(
            df, labels=labels)
    except Exception as exc:              # the error class is the served result
        return f"ERR {type(exc).__name__}"
    return h((out[name].tolist(), list(out.index.get_level_values(name))))


def served_row(scope, name, t):
    if not hasattr(t, "columns"):
        return None
    cols = [str(c) for c in t.columns]
    row = {"scope": scope, "table": name, "cols": cols,
           "table_hash": table_hash(t),
           "declared_key": t.attrs.get("categorical_key"),
           "k_merge": cm._categorical_key_column(t),
           "k_market": market_key(t), "k_hook": hook_key(t)}
    if PL in t.columns:
        row["auto_default"] = auto_served(name, t)
        # every non-Preferred column as a labels= target; strip ' Label'
        row["auto_variants"] = {}
        for c in cols:
            if c == PL:
                continue
            variant = c[:-len(" Label")] if c.endswith(" Label") else c
            row["auto_variants"][c] = auto_served(name, t, variant)
        # the key _build_replace_dict uses for a variant target (excludes it)
        row["k_auto_variant"] = {
            c: next((x for x in cols if x not in (PL, c)), None)
            for c in cols if c != PL}
        if AFTER:
            row["k_auto_variant"] = {
                c: cm._categorical_key_column(t, exclude=(c,))
                for c in cols if c != PL}
    keys = {k for k in (row["k_merge"], row["k_market"], row["k_hook"]) if k}
    row["callers_disagree"] = len(keys) > 1
    return row


def main(out_path):
    root = countries_root()
    levels = [("GLOBAL", global_categorical_level())]
    for cdir in sorted(p for p in root.iterdir() if p.is_dir()):
        if (cdir / "_").is_dir():
            levels.append((cdir.name, cdir / "_"))
        for w in sorted(p for p in cdir.iterdir() if (p / "_").is_dir() and p.name != "_"):
            levels.append((f"{cdir.name}/{w.name}", w / "_"))

    raw = []
    for scope, lvl in levels:
        if not categorical_mapping_files(lvl):
            continue
        try:
            tabs = load_categorical_level(lvl)
        except Exception as exc:
            raw.append({"scope": scope, "error": f"{type(exc).__name__}: {exc}"})
            continue
        for name, t in tabs.items():
            r = served_row(scope, name, t)
            if r:
                r["files"] = [str(p.relative_to(lvl)) for p in categorical_mapping_files(lvl)]
                raw.append(r)

    served = []
    kgf = {}
    for cdir in sorted(p for p in root.iterdir() if p.is_dir() and (p / "_").is_dir()):
        c = cdir.name
        try:
            C = cm.Country(c, preload_panel_ids=False, verbose=False)
        except TypeError:
            C = cm.Country(c)
        except Exception as exc:
            served.append({"scope": c, "error": f"{type(exc).__name__}: {exc}"})
            continue
        try:
            maps = C.categorical_mapping
        except Exception as exc:
            served.append({"scope": c, "error": f"{type(exc).__name__}: {exc}"})
            continue
        for name, t in maps.items():
            r = served_row(c, name, t)
            if r:
                served.append(r)
        try:
            kgf[c] = h(sorted(C._unit_kg_factors().items()))
        except Exception as exc:
            kgf[c] = f"ERR {type(exc).__name__}"
        for w in sorted(p for p in cdir.iterdir() if (p / "_").is_dir() and p.name != "_"):
            if not categorical_mapping_files(w / "_"):
                continue          # wave view == country view; already covered
            try:
                W = cm.Wave(w.name, w.name, C)
                wmaps = W.categorical_mapping
            except Exception as exc:
                served.append({"scope": f"{c}/{w.name}", "error": f"{type(exc).__name__}: {exc}"})
                continue
            for name, t in wmaps.items():
                r = served_row(f"{c}/{w.name}", name, t)
                if r:
                    served.append(r)

    out = {"after": AFTER, "lsms_library": lsms_library.__file__,
           "raw_levels": raw, "served": served, "unit_kg_factors": kgf}
    Path(out_path).write_text(json.dumps(out, indent=0, default=str))
    n_dis = sum(1 for r in raw if r.get("callers_disagree"))
    print(f"after={AFTER} raw tables={len([r for r in raw if 'table' in r])} "
          f"served tables={len([r for r in served if 'table' in r])} "
          f"raw callers_disagree={n_dis} errors="
          f"{sum(1 for r in raw + served if 'error' in r)} -> {out_path}")


if __name__ == "__main__":
    main(sys.argv[1])
