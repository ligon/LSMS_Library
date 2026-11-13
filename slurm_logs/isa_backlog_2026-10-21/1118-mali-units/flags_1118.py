"""GH1118 Mali: what the survey's own use flag says on zero-quantity, no-unit slots.

For each fertilizer / pesticide slot in the EAC-I plot input module, tabulates
the slot's use-flag answer on raw rows whose quantity is exactly 0 and whose
unit is blank, versus rows with a positive quantity.  Reads only.

Usage (from the checkout, PYTHONPATH=<checkout>, private LSMS_DATA_DIR):
    python flags_1118.py <checkout> <out.json>
"""
import json
import os
import sys
import warnings
from pathlib import Path

checkout, out_path = Path(sys.argv[1]).resolve(), sys.argv[2]
import pandas as pd  # noqa: E402
import lsms_library  # noqa: E402
from lsms_library.local_tools import get_dataframe  # noqa: E402

assert os.path.realpath(lsms_library.__file__).startswith(str(checkout) + os.sep), lsms_library.__file__
warnings.simplefilter("ignore")

# (file, flag column, [(slot, qty, unit), ...]) per wave, from the wave scripts' docstrings.
SPEC = {
    "2014-15": ("../Data/EACIS2C_p2.dta", [
        ("s2cq21", [("urea", "s2cq25a", "s2cq25b"), ("dap", "s2cq25c", "s2cq25d"),
                    ("npk", "s2cq25e", "s2cq25f"), ("other_inorganic", "s2cq25g", "s2cq25h")]),
        ("s2cq04", [("manure", "s2cq08a", "s2cq08b")]),
        ("s2cq09", [("compost", "s2cq14a", "s2cq14b")]),
        ("s2cq15", [("other_organic", "s2cq20a", "s2cq20b")]),
        ("s2cq26", [("pesticide", "s2cq29a", "s2cq29b"), ("fungicide", "s2cq29c", "s2cq29d"),
                    ("herbicide", "s2cq29e", "s2cq29f"), ("other_phytosanitary", "s2cq29g", "s2cq29h")]),
    ]),
    "2017-18": ("../Data/eaci17_s07dp2.dta", [
        ("s7dq22", [("urea", "s7dq26a1", "s7dq26a2"), ("dap", "s7dq26b1", "s7dq26b2"),
                    ("npk", "s7dq26c1", "s7dq26c2"), ("other_inorganic", "s7dq26d1", "s7dq26d2")]),
        ("s7dq05", [("manure", "s7dq09a", "s7dq09b")]),
        ("s7dq10", [("compost", "s7dq15a", "s7dq15b")]),
        ("s7dq16", [("other_organic", "s7dq21a", "s7dq21b")]),
        ("s7dq27", [("pesticide", "s7dq30a1", "s7dq30a2"), ("fungicide", "s7dq30b1", "s7dq30b2"),
                    ("herbicide", "s7dq30c1", "s7dq30c2"), ("other_phytosanitary", "s7dq30d1", "s7dq30d2")]),
    ]),
}

mali_dir = checkout / "lsms_library/countries/Mali"
results = {}
for wave, (fn, groups) in SPEC.items():
    cwd = os.getcwd()
    os.chdir(mali_dir / wave / "_")
    try:
        df = get_dataframe(fn)
    finally:
        os.chdir(cwd)
    wave_res = {}
    for flag, slots in groups:
        fl = df[flag].astype("string").str.strip().fillna("<NA>")
        for slot, qc, uc in slots:
            q = pd.to_numeric(df[qc], errors="coerce")
            u = df[uc].astype("string").str.strip()
            blank = u.isna() | (u == "")
            zero_blank = (q == 0) & blank
            pos = q > 0
            wave_res[slot] = {
                "flag": flag,
                "zero_qty_blank_unit": int(zero_blank.sum()),
                "flag_on_zero_blank": fl[zero_blank].value_counts().to_dict(),
                "positive_qty": int(pos.sum()),
                "flag_on_positive": fl[pos].value_counts().to_dict(),
            }
    results[wave] = wave_res
    tot = {}
    for r in wave_res.values():
        for k, v in r["flag_on_zero_blank"].items():
            tot[k] = tot.get(k, 0) + v
    print(wave, "zero-qty/blank-unit rows by use-flag answer:", tot)

with open(out_path, "w") as f:
    json.dump(results, f, indent=1, ensure_ascii=False)
