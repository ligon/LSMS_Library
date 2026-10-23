"""GH #1259 -- which values should count: the three candidate rules, measured.

Read-only; globs ``<data_root>/*/var/<table>.parquet`` for every screened table
(``quantity_audit._SCREENED_COLUMNS``) and never calls ``Country()``.

Variants of ``quantity_audit._cell_reference``, all on the SAME ladder, K,
quantile and floor (only the two "which values count" choices move):

  A  development   quantile over every finite value, MIN_CELL counts every
                   finite value (zeros included)
  B  quantile-only quantile over strictly positive values, MIN_CELL still
                   counts every finite value -- the reading GH #1259 proposed
                   first ("probably the floor too")
  C  shipped       quantile AND MIN_CELL over strictly positive values
  C0 shipped, QUANTITY_REFERENCE_FLOOR = 0 -- is the one-unit floor still
                   binding anywhere once zeros cannot define a scale?

Variant C is the imported module's own ``_cell_reference`` (asserted: the run
refuses a checkout whose module is not the positive-only rule), so it cannot
drift from what ships; A and B are re-implemented here around the module's own
``_group_stats``.

Usage: python variants.py <checkout> <data_root> [<data_root> ...]
Prints COUNTS only (per table, per country, per variant).
"""
from __future__ import annotations

import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

checkout = Path(sys.argv[1])
roots = [Path(p) for p in sys.argv[2:]]

import lsms_library  # noqa: E402
from lsms_library import quantity_audit as qa  # noqa: E402

assert os.path.realpath(lsms_library.__file__).startswith(
    os.path.realpath(checkout) + os.sep), lsms_library.__file__
print("lsms_library:", lsms_library.__file__)


def ladder_reference(values, keys, *, quantile_values, count_values, floor):
    """The module's ladder with the two value choices made explicit."""
    n = len(values)
    axis_codes = {a: keys[a][0] + 1 for a in keys}
    axis_width = {a: int(len(keys[a][1])) + 1 for a in keys}
    base = [k for k in ("t",) if k in keys]
    rungs = []
    for cols in (base + [k for k in ("u", "j") if k in keys],
                 base + [k for k in ("u",) if k in keys], base):
        name = ",".join(cols) if cols else "whole-frame"
        if name not in [seen for seen, _ in rungs]:
            rungs.append((name, cols))
    reference = np.full(n, np.nan)
    for _name, cols in rungs:
        todo = np.isnan(reference)
        if not todo.any():
            break
        codes = np.zeros(n, dtype=np.int64)
        for col in cols:
            codes = codes * axis_width[col] + axis_codes[col]
        stat, _ = qa._group_stats(quantile_values, codes,
                                  qa.QUANTITY_REFERENCE_QUANTILE)
        _, size = qa._group_stats(count_values, codes,
                                  qa.QUANTITY_REFERENCE_QUANTILE)
        take = todo & (size >= qa.QUANTITY_MIN_CELL) & (stat > 0)
        reference[take] = np.maximum(stat[take], floor)
    return reference


def fired(values, reference):
    with np.errstate(invalid="ignore", divide="ignore"):
        return values / reference > qa.QUANTITY_OUTLIER_K


# Refuse to run against a checkout whose shipped rule is not variant C.
_probe_v = np.array([0.0] * 309 + [600.0] * 27 + [3500., 3600., 10200.,
                    15000., 16200., 18480., 30000., 38500., 2e5, 4e6])
_probe_k = {"t": (np.zeros(len(_probe_v), dtype=np.int64),
                  np.array(["w"], dtype=object))}
_probe_ref, _ = qa._cell_reference(_probe_v, _probe_k)
assert abs(_probe_ref[0] - 23_088.0) < 1e-6, (
    "this checkout's _cell_reference is not the positive-only rule")

totals: dict = {}
for root in roots:
    print(f"\n#### data_root {root}")
    for table, columns in qa._SCREENED_COLUMNS.items():
        for column in columns:
            for path in sorted(root.glob(f"*/var/{table}.parquet")):
                country = path.parent.parent.name
                df = pd.read_parquet(path)
                if column not in df.columns:
                    continue
                v = qa._numeric(df[column])
                axes = {a: qa._axis(df, a) for a in ("t", "u", "j")}
                keys = {a: axes[a] for a in ("t", "u", "j") if axes[a] is not None}
                pos = np.where(v > 0, v, np.nan)
                ref = {
                    "A": ladder_reference(v, keys, quantile_values=v,
                                          count_values=v,
                                          floor=qa.QUANTITY_REFERENCE_FLOOR),
                    "B": ladder_reference(v, keys, quantile_values=pos,
                                          count_values=v,
                                          floor=qa.QUANTITY_REFERENCE_FLOOR),
                    "C": qa._cell_reference(v, keys)[0],
                    "C0": ladder_reference(v, keys, quantile_values=pos,
                                           count_values=pos, floor=0.0),
                }
                f = {k: fired(v, r) for k, r in ref.items()}
                counts = {k: int(x.sum()) for k, x in f.items()}
                key = (str(root), f"{table}.{column}")
                tt = totals.setdefault(key, {k: 0 for k in counts})
                for k, n in counts.items():
                    tt[k] += n
                b_not_c = int((f["B"] & ~f["C"]).sum())
                c_not_b = int((f["C"] & ~f["B"]).sum())
                if len(set(counts.values())) > 1 or b_not_c or c_not_b:
                    print(f"{table}.{column} {country}: {counts} "
                          f"B-only={b_not_c} C-only={c_not_b}")
            print(f"  {table}.{column} TOTAL {totals.get((str(root), f'{table}.{column}'))}")
