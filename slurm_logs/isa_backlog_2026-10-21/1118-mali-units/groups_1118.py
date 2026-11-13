"""GH1118 Mali: would dropping zero-quantity slots erase a plot's 'used' answer?

Per plot (one row of the plot input module) and input GROUP whose use flag is
'Oui': does any type in the group carry a positive quantity?  A group whose
types are all zero/missing is one where the zero rows are the ONLY served
trace of the 'yes'.  Reads only.

Usage: python groups_1118.py <checkout> <out.json>   (PYTHONPATH=<checkout>)
"""
import json, os, sys, warnings
from pathlib import Path
checkout, out_path = Path(sys.argv[1]).resolve(), sys.argv[2]
import pandas as pd
import lsms_library
from lsms_library.local_tools import get_dataframe
assert os.path.realpath(lsms_library.__file__).startswith(str(checkout) + os.sep)
warnings.simplefilter("ignore")
SPEC = {
    "2014-15": ("../Data/EACIS2C_p2.dta", {
        "inorganic": ("s2cq21", ["s2cq25a", "s2cq25c", "s2cq25e", "s2cq25g"]),
        "pesticide": ("s2cq26", ["s2cq29a", "s2cq29c", "s2cq29e", "s2cq29g"])}),
    "2017-18": ("../Data/eaci17_s07dp2.dta", {
        "inorganic": ("s7dq22", ["s7dq26a1", "s7dq26b1", "s7dq26c1", "s7dq26d1"]),
        "pesticide": ("s7dq27", ["s7dq30a1", "s7dq30b1", "s7dq30c1", "s7dq30d1"])}),
}
res = {}
for wave, (fn, groups) in SPEC.items():
    cwd = os.getcwd(); os.chdir(checkout / "lsms_library/countries/Mali" / wave / "_")
    try:
        df = get_dataframe(fn)
    finally:
        os.chdir(cwd)
    res[wave] = {}
    for g, (flag, qcols) in groups.items():
        yes = (df[flag].astype("string").str.strip() == "Oui").fillna(False).astype(bool)
        y2 = yes.to_numpy()[:, None]
        q = df[qcols].apply(pd.to_numeric, errors="coerce").mask(lambda x: x.isin([99, 999, 9999, 99999]))
        anypos = (q > 0).any(axis=1)
        allzero = (q.fillna(0) == 0).all(axis=1)
        zero_slots = int(((q == 0).to_numpy() & y2).sum())
        res[wave][g] = {
            "plots_flag_oui": int(yes.sum()),
            "oui_with_a_positive_type": int((yes & anypos).sum()),
            "oui_all_types_zero_or_missing": int((yes & allzero).sum()),
            "zero_slots_on_oui_plots": int(zero_slots),
            "zero_slots_on_all_zero_oui_plots": int(((q == 0).to_numpy() & (yes & allzero).to_numpy()[:, None]).sum()),
        }
        print(wave, g, res[wave][g])
json.dump(res, open(out_path, "w"), indent=1)
