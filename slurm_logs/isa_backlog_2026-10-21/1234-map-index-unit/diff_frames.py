"""Diff the base and branch cold-built frames (aggregate output only).

Usage: python diff_frames.py <frames_base> <frames_branch> <out.json> Country:table [...]

Outer-joins the two frames on the FULL index and classifies every key:

  common          key in both: values equal, or values differ (`common_diff`)
  base_only       key only on development
  branch_only     key only on the branch
  matched         a base_only key and a branch_only key that agree on every
                  index level except `u` (the relabelled row), with values
                  compared

The fix is clean for a table when: every base_only key has u='unit', every
branch_only key has u='Unknown', every base_only key is matched to a
branch_only key with equal values, and common_diff == 0.  A common_diff on a
u='unit' key is a minted row the base had SUMMED into a genuine 'unit' row
(Panama); a common_diff on u='Unknown' would be a branch-side collision with a
pre-existing 'Unknown'.
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

fb, fr, out = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
specs = [s.split(":") for s in sys.argv[4:]]


def load(p):
    df = pd.read_parquet(p)
    if "u" not in df.index.names:
        df = df.set_index("u", append=True)
    return df


def row_equal(a: pd.DataFrame, b: pd.DataFrame) -> pd.Series:
    eq = pd.Series(True, index=a.index)
    for c in a.columns:
        x, y = a[c], b[c]
        same_na = x.isna() & y.isna()
        try:
            if pd.api.types.is_float_dtype(x) and pd.api.types.is_float_dtype(y):
                v = np.isclose(x.astype(float), y.astype(float), rtol=1e-12, atol=0, equal_nan=True)
                e = pd.Series(v, index=a.index)
            else:
                e = (x.astype(object) == y.astype(object)).fillna(False).astype(bool)
        except TypeError:
            e = (x.astype(str) == y.astype(str))
        eq &= (e | same_na)
    return eq


res = json.load(open(out)) if Path(out).exists() else {}
for country, table in specs:
    B = load(fb / f"{country}__{table}.parquet")
    R = load(fr / f"{country}__{table}.parquet")
    rec = {"rows_base": len(B), "rows_branch": len(R),
           "columns_equal": list(B.columns) == list(R.columns),
           "index_equal": list(B.index.names) == list(R.index.names),
           "dup_index_base": int(B.index.duplicated().sum()),
           "dup_index_branch": int(R.index.duplicated().sum())}
    ub = B.index.get_level_values("u").astype(object)
    ur = R.index.get_level_values("u").astype(object)
    rec["u_counts"] = {
        "base_unit": int((ub == "unit").sum()), "base_Unknown": int((ub == "Unknown").sum()),
        "base_null": int(pd.isna(ub).sum()),
        "branch_unit": int((ur == "unit").sum()), "branch_Unknown": int((ur == "Unknown").sum()),
        "branch_null": int(pd.isna(ur).sum())}
    # Work on unique keys only (duplicates, if any, are reported above).
    Bu = B[~B.index.duplicated(keep=False)]
    Ru = R[~R.index.duplicated(keep=False)]
    cols = [c for c in B.columns if c in R.columns]
    common = Bu.index.intersection(Ru.index)
    eq = row_equal(Bu.loc[common, cols], Ru.loc[common, cols])
    cu = pd.Series(common.get_level_values("u").astype(object), index=common)
    diff_keys = common[~eq.values]
    rec["common"] = int(len(common))
    rec["common_diff"] = int((~eq).sum())
    rec["common_diff_by_u"] = (pd.Series(diff_keys.get_level_values("u").astype(object))
                               .value_counts(dropna=False).astype(int).to_dict())
    rec["common_diff_by_u"] = {str(k): int(v) for k, v in rec["common_diff_by_u"].items()}
    bo = Bu.index.difference(Ru.index)
    ro = Ru.index.difference(Bu.index)
    rec["base_only"] = int(len(bo))
    rec["branch_only"] = int(len(ro))
    rec["base_only_u"] = {str(k): int(v) for k, v in
                          pd.Series(bo.get_level_values("u").astype(object)).value_counts(dropna=False).items()}
    rec["branch_only_u"] = {str(k): int(v) for k, v in
                            pd.Series(ro.get_level_values("u").astype(object)).value_counts(dropna=False).items()}
    # Match base_only to branch_only on every level but u.
    others = [n for n in B.index.names if n != "u"]
    bdf = Bu.loc[bo, cols].reset_index()
    rdf = Ru.loc[ro, cols].reset_index()
    m = bdf.merge(rdf, on=others, how="outer", suffixes=("_b", "_r"), indicator=True)
    matched = m[m["_merge"] == "both"]
    rec["matched"] = int(len(matched))
    rec["matched_u_pairs"] = {f"{a}->{b}": int(n) for (a, b), n in
                              matched.groupby(["u_b", "u_r"], dropna=False).size().items()}
    if len(matched):
        a = matched[[f"{c}_b" for c in cols]].set_axis(cols, axis=1)
        b = matched[[f"{c}_r" for c in cols]].set_axis(cols, axis=1)
        rec["matched_value_diff"] = int((~row_equal(a, b)).sum())
    else:
        rec["matched_value_diff"] = 0
    rec["unmatched_base_only"] = int((m["_merge"] == "left_only").sum())
    rec["unmatched_branch_only"] = int((m["_merge"] == "right_only").sum())
    # By-wave relabel counts.
    rec["relabelled_by_t"] = {str(k): int(v) for k, v in
                              matched.groupby("t").size().items()} if "t" in matched else {}
    clean = (set(rec["base_only_u"]) <= {"unit"} and set(rec["branch_only_u"]) <= {"Unknown"}
             and rec["unmatched_base_only"] == 0 and rec["unmatched_branch_only"] == 0
             and rec["matched_value_diff"] == 0 and rec["common_diff"] == 0)
    rec["clean_relabel_only"] = bool(clean)
    res[f"{country}/{table}"] = rec
    print(f"{country}/{table}:", json.dumps(rec, default=str))
json.dump(res, open(out, "w"), indent=1, sort_keys=True, default=str)
