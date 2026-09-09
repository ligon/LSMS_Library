"""GH #857 -- the sweep QUANTITY_OUTLIER_K and the reference quantile were chosen from.

Read-only, and it GLOBS ``data_root()``: no hardcoded country list, because the
first version had one and five countries got published as though they were the
corpus (red team, 2026-09-09).  It never calls ``Country()`` and never builds.

It calls the library's OWN ``_cell_reference`` rather than reimplementing the
ladder, so the sweep cannot drift from the rule it is meant to justify; the
quantile and the floor are swept by monkeypatching the module constants.

What to look at: the full ratio ranking, and the fact that it has NO
discontinuity.  That absence is why ``QUANTITY_OUTLIER_K`` is documented as a
stated tolerance rather than a measured boundary.
"""
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
pd.set_option("display.width", 250)

import lsms_library
print("lsms_library:", lsms_library.__file__)
from lsms_library import quantity_audit as qa
from lsms_library.paths import data_root

root = data_root()
print("data_root:", root)
frames = []
for path in sorted(root.glob("*/var/crop_production.parquet")):
    country = path.parent.parent.name
    df = pd.read_parquet(path)
    values = qa._numeric(df["Quantity"])
    axes = {a: qa._axis(df, a) for a in ("t", "i", "plot", "j", "u")}
    frames.append((country, df, values, axes))
print(f"{len(frames)} warm countries: {[c for c, *_ in frames]}\n")


def sweep(quantile, floor, min_cell):
    old = (qa.QUANTITY_REFERENCE_QUANTILE, qa.QUANTITY_REFERENCE_FLOOR,
           qa.QUANTITY_MIN_CELL)
    qa.QUANTITY_REFERENCE_QUANTILE, qa.QUANTITY_REFERENCE_FLOOR, \
        qa.QUANTITY_MIN_CELL = quantile, floor, min_cell
    try:
        rows = []
        for country, df, values, axes in frames:
            keys = {a: axes[a] for a in ("t", "u", "j") if axes[a] is not None}
            reference, rung = qa._cell_reference(values, keys)
            with np.errstate(invalid="ignore", divide="ignore"):
                ratio = values / reference
            rows.append(pd.DataFrame({
                "country": country,
                "t": [qa._label(axes["t"], k) for k in range(len(df))],
                "i": [qa._label(axes["i"], k) for k in range(len(df))],
                "j": [qa._label(axes["j"], k) for k in range(len(df))],
                "u": [qa._label(axes["u"], k) for k in range(len(df))],
                "Quantity": values, "ref": reference, "rung": rung,
                "ratio": ratio}))
        return pd.concat(rows, ignore_index=True)
    finally:
        (qa.QUANTITY_REFERENCE_QUANTILE, qa.QUANTITY_REFERENCE_FLOOR,
         qa.QUANTITY_MIN_CELL) = old


SHIPPED = (qa.QUANTITY_REFERENCE_QUANTILE, qa.QUANTITY_REFERENCE_FLOOR,
           qa.QUANTITY_MIN_CELL)
print(f"shipped rule: quantile={SHIPPED[0]} floor={SHIPPED[1]} "
      f"min_cell={SHIPPED[2]} K={qa.QUANTITY_OUTLIER_K}\n")

for quantile in (0.90, 0.95, 0.99):
    for floor in (0.0, 1.0):
        d = sweep(quantile, floor, 30)
        counts = " ".join(
            f"K={K}:{int((d['ratio'] > K).sum())}"
            for K in (10, 30, 50, 100, 150, 200, 300, 500))
        print(f"quantile={quantile} floor={floor} min_cell=30 -> {counts}")
    print()

d = sweep(*SHIPPED)
fired = d[d["ratio"] > qa.QUANTITY_OUTLIER_K]
print(f"\nSHIPPED rule fires on {len(fired)} of {len(d):,} rows "
      f"({100 * len(fired) / len(d):.4f}%)")
print(fired.sort_values("ratio", ascending=False).head(40).to_string(index=False))
judged = d[d["ratio"].notna()]
print(f"\n{len(judged):,} rows have a reference; "
      f"{len(d) - len(judged):,} do not (thin cell, or a NaN quantity)")
print("ratio quantiles -- look for a gap; there is none:")
print(judged["ratio"].describe(percentiles=[.9, .99, .999, .9999]))
