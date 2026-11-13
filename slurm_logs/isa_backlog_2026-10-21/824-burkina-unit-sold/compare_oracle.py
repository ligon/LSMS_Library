"""GH #824 (Burkina Faso 2018-19): development vs branch, plus a raw oracle of
s16cq16b against the served ``Unit_sold``.  Emits AGGREGATES ONLY.

Usage (branch checkout imported; see REPRODUCE in README.org):
    python compare_oracle.py <branch_checkout> <base_served.parquet> \
        <branch_served.parquet> <branch_wave.parquet> <out.json>

The served frames are written by a cold ``Country('Burkina_Faso').crop_production()``
in a fresh ``LSMS_DATA_DIR`` per checkout; the wave parquet is the branch's
``<LSMS_DATA_DIR>/Burkina_Faso/2018-19/_/crop_production.parquet`` (pre-id_walk,
one row per source line).  The oracle reads the raw file through
``get_dataframe`` and canonicalises with the ``u`` table read by
``df_from_orgfile`` -- a different reader from the build's
``get_categorical_mapping``.
"""
import json
import os
import sys
import warnings

warnings.simplefilter("ignore")
checkout, base_p, branch_p, wave_p, out = sys.argv[1:6]
import pandas as pd  # noqa: E402
import lsms_library  # noqa: E402

assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep)
from lsms_library.local_tools import df_from_orgfile, format_id, get_dataframe  # noqa: E402

ROOT = os.path.join(checkout, "lsms_library/countries/Burkina_Faso")
res = {}

# ---------------------------------------------------------------- dev vs branch
base = pd.read_parquet(base_p)
br = pd.read_parquet(branch_p)
res["rows"] = {"development": len(base), "branch": len(br)}
res["index_names_equal"] = list(base.index.names) == list(br.index.names)
res["index_equal"] = bool(base.index.equals(br.index))
res["new_columns"] = [c for c in br.columns if c not in base.columns]
res["dropped_columns"] = [c for c in base.columns if c not in br.columns]
try:
    pd.testing.assert_frame_equal(base, br[list(base.columns)], check_exact=True)
    res["pre_existing_columns_identical"] = True
except AssertionError as e:  # pragma: no cover
    res["pre_existing_columns_identical"] = False
    res["frame_diff"] = str(e)[:300]

# ---------------------------------------------------------------- coverage
f = br.reset_index()
f["u"] = f["u"].astype("string")
priced = ((f["Quantity_sold"].fillna(0) > 0) & (f["Value_sold"].fillna(0) > 0)).astype(bool)
us = f["Unit_sold"].astype("string")
both = (priced & us.notna() & f["u"].notna() & (f["u"] != "unit").fillna(False)).astype(bool)
same = (us == f["u"]).fillna(False).astype(bool)
res["coverage"] = {
    "Unit_sold_dtype": str(us.dtype),
    "Unit_sold_non_null": int(us.notna().sum()),
    "priced": int(priced.sum()),
    "priced_with_Unit_sold": int((priced & us.notna()).sum()),
    "priced_without_Unit_sold": int((priced & us.isna()).sum()),
    "Unit_sold_on_unpriced_rows": int((~priced & us.notna()).sum()),
    "Unit_sold_on_rows_with_Quantity_sold_gt0": int(((f["Quantity_sold"].fillna(0) > 0) & us.notna()).sum()),
    "priced_knowing_both": int(both.sum()),
    "priced_agree": int((both & same).sum()),
    "priced_differ": int((both & ~same).sum()),
    "Unit_sold_labels": us.value_counts(dropna=False).rename(index=lambda x: str(x)).to_dict(),
    "u_is_unit_sentinel_on_priced": int((priced & (f["u"] == "unit").fillna(False)).sum()),
}
dis = f[both & ~same]
res["disagreeing_priced_cells_u_to_Unit_sold"] = (
    dis.groupby(["u", "Unit_sold"]).size().sort_values(ascending=False)
    .rename(index=lambda x: x).reset_index(name="n").to_dict(orient="records"))

# ---------------------------------------------------------------- raw oracle
wave_dir = os.path.join(ROOT, "2018-19", "_")
sys.path.insert(0, os.path.join(ROOT, "_"))
from burkina_faso import ehcvm_i  # noqa: E402

cwd = os.getcwd()
os.chdir(wave_dir)
src = get_dataframe("../Data/s16c_me_bfa2018.dta", convert_categoricals=True)
codes = get_dataframe("../Data/s16c_me_bfa2018.dta", convert_categoricals=False)
os.chdir(cwd)
utab = df_from_orgfile(os.path.join(ROOT, "_", "categorical_mapping.org"), name="u")
umap = dict(zip(utab["Original Label"].astype(str).str.strip(), utab["Preferred Label"].astype(str).str.strip()))
ftab = df_from_orgfile(os.path.join(ROOT, "_", "categorical_mapping.org"), name="harmonize_food")
fmap = dict(zip(ftab["Original Label"].astype(str).str.strip(), ftab["Preferred Label"].astype(str).str.strip()))


def canon(s, mp):
    s = s.astype("string")
    return s.map(lambda x: mp.get(x, x) if pd.notna(x) else pd.NA).astype("string")


raw = pd.DataFrame({
    "i": codes.apply(lambda r: ehcvm_i(r["grappe"], r["menage"]), axis=1).values,
    "plot_id": (codes["s16cq02"].apply(format_id).astype(str) + "_"
                + codes["s16cq03"].apply(format_id).astype(str)).values,
    "crop": canon(src["s16cq04"], fmap).values,
    "u": canon(src["s16cq12b"], umap).values,
    "raw_sold_unit": src["s16cq16b"].astype("string").values,
    "oracle_Unit_sold": canon(src["s16cq16b"], umap).values,
})
key = ["i", "plot_id", "crop", "u"]
wave = pd.read_parquet(wave_p).reset_index()
res["oracle"] = {
    "raw_rows": len(raw), "raw_s16cq16b_non_null": int(raw["raw_sold_unit"].notna().sum()),
    "raw_s16cq16b_labels": sorted(raw["raw_sold_unit"].dropna().unique().tolist()),
    "raw_s16cq12b_labels": sorted(src["s16cq12b"].astype("string").dropna().unique().tolist()),
    "wave_rows": len(wave), "raw_key_unique": bool(not raw.duplicated(key).any()),
}
j = raw.merge(wave[key + ["Unit_sold"]], on=key, how="outer", indicator=True)
eq = (j["oracle_Unit_sold"].fillna("<NA>") == j["Unit_sold"].astype("string").fillna("<NA>"))
res["oracle"].update({
    "joined_both": int((j["_merge"] == "both").sum()),
    "raw_only": int((j["_merge"] == "left_only").sum()),
    "wave_only": int((j["_merge"] == "right_only").sum()),
    "Unit_sold_mismatch_rows": int((~eq & (j["_merge"] == "both")).sum()),
    "Unit_sold_match_rows": int((eq & (j["_merge"] == "both")).sum()),
})
# served vs wave: the served table drops the six all-payload-NA lines
sv = f["Unit_sold"].value_counts(dropna=False).rename(index=str).to_dict()
wv = wave["Unit_sold"].value_counts(dropna=False).rename(index=str).to_dict()
res["oracle"]["served_minus_wave_Unit_sold_counts"] = {k: sv.get(k, 0) - wv.get(k, 0)
                                                     for k in sorted(set(sv) | set(wv))}
# raw label agreement (before any mapping) on priced rows -- the audit's 4,801 / 546
rq = pd.to_numeric(src["s16cq16a"], errors="coerce")
rv = pd.to_numeric(src["s16cq17"], errors="coerce")
rp = ((rq > 0) & (rv > 0)).astype(bool)
hu = src["s16cq12b"].astype("string")
su = src["s16cq16b"].astype("string")
rb = (rp & hu.notna() & su.notna()).astype(bool)
rsame = (hu == su).fillna(False).astype(bool)
res["oracle"]["raw_priced"] = int(rp.sum())
res["oracle"]["raw_priced_agree_unmapped"] = int((rb & rsame).sum())
res["oracle"]["raw_priced_differ_unmapped"] = int((rb & ~rsame).sum())

# ---------------------------------------------------------------- price test
# The audit's test (data-private/824/scripts/pricetest.py::price_test), re-run
# on the SERVED branch table: on a disagreeing priced row, is V/Q closer (in
# |log|) to the agreeing-row median price for (t, crop, Unit_sold) or for
# (t, crop, u)?  Benchmarks need >= min_n agreeing rows.
import numpy as np  # noqa: E402


def price_test(d, min_n):
    d = d[(d.q > 0) & (d.v > 0) & d.su.notna() & d.hu.notna()].copy()
    d["p"] = d.v / d.q
    bench = d[d.su == d.hu].groupby(["t", "crop", "su"])["p"].agg(["median", "size"])
    bench = bench[bench["size"] >= min_n]["median"]
    dis = d[d.su != d.hu].copy()
    dis["b_sold"] = [bench.get((t, c, u), np.nan) for t, c, u in zip(dis.t, dis.crop, dis.su)]
    dis["b_harv"] = [bench.get((t, c, u), np.nan) for t, c, u in zip(dis.t, dis.crop, dis.hu)]
    ok = dis.b_sold.notna() & dis.b_harv.notna() & (dis.b_sold != dis.b_harv)
    dd = dis[ok]
    closer = np.abs(np.log(dd.p / dd.b_sold)) < np.abs(np.log(dd.p / dd.b_harv))
    return {"disagreeing": len(dis), "benchmarkable": int(ok.sum()), "Unit_sold_closer": int(closer.sum())}


pt = pd.DataFrame({"t": f["t"].astype(str), "crop": f["crop"].astype(str),
                   "hu": f["u"].where(f["u"] != "unit"), "su": us,
                   "q": f["Quantity_sold"].astype(float), "v": f["Value_sold"].astype(float)})
pt = pt.astype({"hu": object, "su": object})
res["price_test_served"] = {f"min_n={n}": price_test(pt, n) for n in (5, 10)}

with open(out, "w") as fh:
    json.dump(res, fh, indent=1, ensure_ascii=False, default=str)
print(json.dumps(res, indent=1, ensure_ascii=False, default=str))
