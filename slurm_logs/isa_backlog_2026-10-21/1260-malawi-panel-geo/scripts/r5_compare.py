"""Compare two build_all.py outputs table by table.  Counts only."""
import os, sys, glob
import numpy as np, pandas as pd
a_dir, b_dir = sys.argv[1], sys.argv[2]
names = sorted({os.path.basename(p)[:-4] for p in glob.glob(f"{a_dir}/*.pkl") + glob.glob(f"{b_dir}/*.pkl")})
for n in names:
    pa, pb = f"{a_dir}/{n}.pkl", f"{b_dir}/{n}.pkl"
    if not (os.path.exists(pa) and os.path.exists(pb)):
        print(f"{n:42s} MISSING base={os.path.exists(pa)} branch={os.path.exists(pb)}"); continue
    a, b = pd.read_pickle(pa), pd.read_pickle(pb)
    if not isinstance(a, pd.DataFrame):
        print(f"{n:42s} {'IDENTICAL' if a == b else 'DIFFERENT'} (non-frame, len {len(a)} / {len(b)})"); continue
    a2, b2 = a.sort_index(), b.sort_index()
    if a2.shape == b2.shape and list(a2.columns) == list(b2.columns) and a2.index.equals(b2.index) and a2.equals(b2) and (a2.dtypes == b2.dtypes).all():
        print(f"{n:42s} IDENTICAL {a.shape}"); continue
    msg = [f"{n:42s} DIFFERENT base {a.shape} branch {b.shape}"]
    cols = sorted(set(a.columns) | set(b.columns))
    msg.append(f"  cols only base {sorted(set(a.columns)-set(b.columns))} only branch {sorted(set(b.columns)-set(a.columns))}")
    ia, ib = set(a.index), set(b.index)
    msg.append(f"  index rows only base {len(ia-ib)} only branch {len(ib-ia)} common {len(ia & ib)}; dup idx base {int(a.index.duplicated().sum())} branch {int(b.index.duplicated().sum())}")
    if not a.index.duplicated().any() and not b.index.duplicated().any():
        common = sorted(ia & ib, key=str)
        ac, bc = a.loc[common], b.loc[common]
        shared = [c for c in a.columns if c in b.columns]
        diff_rows = np.zeros(len(common), dtype=bool)
        for c in shared:
            x, y = ac[c], bc[c]
            neq = ~((x.astype(object) == y.astype(object)).fillna(False).to_numpy(dtype=bool) | (x.isna().to_numpy() & y.isna().to_numpy()))
            if neq.any():
                msg.append(f"  col {c}: {int(neq.sum())} rows differ")
            diff_rows |= neq
        msg.append(f"  common rows differing in any column: {int(diff_rows.sum())}")
        if "t" in a.index.names:
            tvals = pd.Index(common).get_level_values("t") if isinstance(pd.Index(common), pd.MultiIndex) else None
            if tvals is not None and diff_rows.any():
                msg.append(f"  differing rows by t: {pd.Series(tvals[diff_rows]).value_counts().sort_index().to_dict()}")
    for c in [c for c in a.columns if c in b.columns]:
        if a[c].dtype != b[c].dtype:
            msg.append(f"  dtype {c}: {a[c].dtype} -> {b[c].dtype}")
    print("\n".join(msg))
