"""Part 2 base-vs-branch details from the build_all pickles.  Counts / sums only."""
import sys
import numpy as np, pandas as pd
B, R = sys.argv[1], sys.argv[2]
def ld(d, t):
    return pd.read_pickle(f"{d}/{t}.pkl").reset_index()
KEYS = ['Malawi::crop_production::sale-basis-unknown-wildcard', 'Malawi::crop_production::sale-basis-from-price',
        'Malawi::crop_production::sale-basis-from-price-overrule', 'Malawi::crop_production::sale-basis-not-applicable',
        'Malawi::crop_production::sale-basis-no-reference', 'Malawi::crop_production::contradiction-price-sale',
        'Malawi::crop_production::contradiction-price-harvest']
short = {k: k.split('::')[-1] for k in KEYS}
for side, d in (("base", B), ("branch", R)):
    cp = ld(d, "crop_production")
    print(f"== {side} crop_production")
    for w, x in cp.groupby("t"):
        q = x["Quantity"]
        dv = x["Derivation"].value_counts().to_dict()
        print(f"  {w}: rows {len(x)}; Quantity NA {int(q.isna().sum())}, zero {int((q == 0).sum())}, sum {float(q.sum()):,.1f}; "
              f"Value_sold sum {float(x['Value_sold'].sum()):,.0f}, non-NA {int(x['Value_sold'].notna().sum())}; "
              f"Quantity_sold sum {float(x['Quantity_sold'].sum()):,.2f}; keys {[dv.get(k, 0) for k in KEYS]}")
    print("  key order:", [short[k] for k in KEYS])
cpB, cpR = ld(B, "crop_production"), ld(R, "crop_production")
# rows of branch that share (t,i,plot_id,crop,u,condition) with another (the formerly collapsed groups)
g = cpR.groupby(["t", "i", "plot_id", "crop", "u", "condition"], dropna=False).size()
print("branch groups at the old grain with >1 native record, by wave:", g[g > 1].groupby(level="t").size().to_dict(),
      "rows in them:", g[g > 1].groupby(level="t").sum().to_dict())
print("branch crop_variety distinct within those groups (groups with >1 variety):",
      int((cpR.groupby(["t", "i", "plot_id", "crop", "u", "condition"], dropna=False)["crop_variety"].nunique() > 1).sum()))
# plot_labor / plot_inputs: same rows, only the sentinel / marker
for t, col, val in (("plot_labor", "plot_id", "unknown_plot"), ("plot_inputs", "crop", "(not crop-specific)")):
    b, r = ld(B, t), ld(R, t)
    rb = r.copy(); rb[col] = rb[col].astype(object).where(rb[col] != val, np.nan)
    if t == "plot_inputs" and "plot_id" in rb:
        pass
    key = [c for c in b.columns if c not in ("PersonDays", "Wage", "Quantity", "Purchased", "Quantity_purchased", "Improved", "Applied")]
    b2 = b.sort_values(key).reset_index(drop=True); r2 = rb.sort_values(key).reset_index(drop=True)
    same = b2.shape == r2.shape and b2.astype(str).equals(r2.astype(str))
    print(f"{t}: base rows {len(b)}, branch rows {len(r)}; branch '{val}' on {col}: {int((r[col] == val).sum())}; "
          f"base NaN {col}: {int(b[col].isna().sum())}; branch NaN {col}: {int(r[col].isna().sum())}; "
          f"branch == base once '{val}' is read back as NaN: {same}")
    if t == "plot_labor":
        print("  unknown_plot rows by wave:", r[r[col] == val].groupby("t").size().to_dict(), "households:", int(r.loc[r[col] == val, "i"].nunique()),
              "by source:", r[r[col] == val]["source"].value_counts().to_dict(), "PersonDays on them:", round(float(r.loc[r[col] == val, "PersonDays"].sum()), 1))
    else:
        print("  marker rows by wave:", r[r[col] == val].groupby("t").size().to_dict(), "on Seed:", int(((r[col] == val) & (r["input"] == "Seed")).sum()))
u = cpR[cpR["plot_id"] == "unknown_plot"]
print("crop_production unknown_plot rows:", len(u), "by wave", u.groupby("t").size().to_dict(), "with Quantity", int(u["Quantity"].notna().sum()),
      "Quantity sum", round(float(u["Quantity"].sum()), 1), "with a sale", int(u["Value_sold"].notna().sum()), "perennial", int(u["perennial"].fillna(False).sum()))
