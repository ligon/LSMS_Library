"""Raw oracle for crop_production's native records: per wave, the number of
distinct (household, plot key, module, raw crop code) harvest lines in the raw
Module G / P files whose code has a crop label, against the distinct
(i, plot_id, production_obs) of the built wave parquet.  Counts only."""
import os as _os, lsms_library as _ll
_top = _os.path.realpath(_os.environ.get("CLONE") or _os.getcwd())
assert _os.path.realpath(_ll.__file__).startswith(_top + _os.sep), _ll.__file__
import sys, warnings
import pandas as pd
warnings.simplefilter("ignore")
from lsms_library.paths import countries_root, data_root
from lsms_library.local_tools import get_dataframe, format_id
ROOT = countries_root() / "Malawi"
sys.path.insert(0, str(ROOT / "_"))
import malawi as M
labels = M._malawi_code_map("harmonize_crop")
RECODE = {1100: 1001, 1021: 1018}   # the GH #869 IHS4/IHS5 Module P recode (others identity)
for wave, halves in M._CROP_SALE_SOURCES.items():
    tuples = set(); blank_plot = 0
    for h in halves:
        d = ROOT / wave / h["dir"]
        for mod, f, plotcol, cropcol in (("G", h["g"], h["gplot"], h["gcrop"]), ("P", h["p"], h["pplot"], h["pcrop"])):
            df = get_dataframe(str(d / f), convert_categoricals=False)
            if cropcol is None:
                cropcol = "ag_p0c" if "ag_p0c" in df.columns else "crop_code"
            hh = h["prefix"] + df[h["hhid"]].apply(format_id)
            if h["plot"] == "garden":
                pk = df["gardenid"].apply(format_id) + "_" + df["plotid"].apply(format_id)
            else:
                pk = df[plotcol].apply(format_id)
            raw = pd.to_numeric(df[cropcol], errors="coerce").astype("Int64")
            lib = (raw + 1000).replace(RECODE) if mod == "P" else raw
            ok = lib.map(labels).notna() & raw.notna()
            blank_plot += int((ok & pk.isna()).sum())
            for a, b, c in zip(hh[ok], pk[ok].fillna("<none>"), raw[ok]):
                tuples.add((a, b, mod, int(c)))
    built = pd.read_parquet(data_root() / "Malawi" / wave / "_" / "crop_production.parquet").reset_index()
    served = set(zip(built["i"], built["plot_id"], built["production_obs"]))
    n_raw, n_served = len(tuples), len(served)
    print(f"{wave}: raw distinct (hh, plot, module, code) with a label {n_raw}; served distinct (i, plot_id, production_obs) {n_served}; "
          f"served rows {len(built)}; raw lines with a label and no plot key {blank_plot}; served unknown_plot rows {int((built['plot_id'] == 'unknown_plot').sum())}")
