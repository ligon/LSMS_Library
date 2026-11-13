"""Compare per-row food_kg_factors, development vs fix (aggregates only).

Usage: python compare_downstream.py <private_dir> <out.json> Country [...]
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

private, out = Path(sys.argv[1]), sys.argv[2]
res = {}
for country in sys.argv[3:]:
    b = pd.read_parquet(private / f"development__{country}__food_kg_factors.parquet")
    f = pd.read_parquet(private / f"fix__{country}__food_kg_factors.parquet")
    rec = {"rows": [len(b), len(f)], "columns": list(f.columns)}
    others = [n for n in b.index.names if n != "u"]
    ub = pd.Series(b.index.get_level_values("u"), index=b.index)
    uf = pd.Series(f.index.get_level_values("u"), index=f.index)
    # Relabelled rows: base 'unit' keys absent on the fix side, matched on the other levels.
    bo = b.loc[b.index.difference(f.index)].reset_index()
    fo = f.loc[f.index.difference(b.index)].reset_index()
    m = bo.merge(fo, on=others, suffixes=("_dev", "_fix"))
    rec["relabelled"] = {
        "n": int(len(m)),
        "u_pairs": {f"{a}->{c}": int(n) for (a, c), n in m.groupby(["u_dev", "u_fix"]).size().items()},
        "source_pairs": {f"{a}->{c}": int(n) for (a, c), n in
                         m.groupby(["KgFactorSource_dev", "KgFactorSource_fix"]).size().items()},
    }
    # Genuine 'unit' rows present on both sides.
    common = b.index.intersection(f.index)
    g = common[pd.Series(common.get_level_values("u")).eq("unit").values]
    kb, kf = b.loc[g, "kg_per_unit"], f.loc[g, "kg_per_unit"]
    changed = ~np.isclose(kb, kf, equal_nan=True, rtol=1e-12)
    ratio = (kf[changed] / kb[changed])
    rec["genuine_unit"] = {
        "rows": int(len(g)), "factor_changed": int(changed.sum()),
        "source_pairs_changed": {f"{a}->{c}": int(n) for (a, c), n in pd.DataFrame(
            {"a": b.loc[g, "KgFactorSource"][changed].values,
             "c": f.loc[g, "KgFactorSource"][changed].values}).groupby(["a", "c"]).size().items()},
        "ratio_min": float(ratio.min()) if len(ratio) else None,
        "ratio_median": float(ratio.median()) if len(ratio) else None,
        "ratio_max": float(ratio.max()) if len(ratio) else None,
    }
    # Every other common row: any factor change?
    rest = common.difference(g)
    rc = ~np.isclose(b.loc[rest, "kg_per_unit"], f.loc[rest, "kg_per_unit"], equal_nan=True, rtol=1e-12)
    rec["other_rows_factor_changed"] = int(rc.sum())
    if rc.sum():
        rec["other_rows_changed_by_u"] = {str(k): int(v) for k, v in
                                          pd.Series(rest[rc].get_level_values("u")).value_counts().items()}
    res[country] = rec
    print(country, json.dumps(rec, default=str))
json.dump(res, open(out, "w"), indent=1, sort_keys=True, default=str)
