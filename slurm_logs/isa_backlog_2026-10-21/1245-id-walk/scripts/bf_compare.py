"""Burkina served-id comparison base vs cand (aggregates only printed).
For each table: rows per wave, whether non-2021-22 rows are identical, and for
2021-22 how the served household ids moved, checked against the cover truth."""
import json, os, pickle, sys, warnings
warnings.simplefilter("ignore")
import pandas as pd
D = "/local/job40390255/data-private/1245"
b = pickle.load(open(f"{D}/bf_ti_base.pkl", "rb"))
c = pickle.load(open(f"{D}/bf_ti_cand.pkl", "rb"))
new = json.load(open("/local/job40390255/worktrees/1245/lsms_library/countries/Burkina_Faso/_/updated_ids.json"))["2021-22"]
import subprocess
old = json.loads(subprocess.check_output(["git", "-C", "/local/job40390255/worktrees/1245", "show",
      "d353b7ea7:lsms_library/countries/Burkina_Faso/_/updated_ids.json"]))["2021-22"]
added = set(new) - set(old)            # 80 raw 2021-22 ids newly linked
# served id under base = old.get(x, x) (closure == raw: base map is chain-free)
exp_base = {x: old.get(x, x) for x in new}
exp_cand = {x: new[x] for x in new}
moved_expected = {x for x in new if exp_base[x] != exp_cand[x]}
out = {"expected_moved_households": len(moved_expected)}
for t in b:
    fb, fc = b[t], c[t]
    r = {"rows": [len(fb), len(fc)]}
    other_b = fb[fb["t"] != "2021-22"]; other_c = fc[fc["t"] != "2021-22"]
    r["non_2021_22_identical"] = bool(other_b.reset_index(drop=True).equals(other_c.reset_index(drop=True)))
    ib = set(fb.loc[fb["t"] == "2021-22", "i"]); ic = set(fc.loc[fc["t"] == "2021-22", "i"])
    r["hh_2021_22"] = [len(ib), len(ic)]
    # map base served ids back to raw, then forward with the new map
    inv_b = {v: k for k, v in exp_base.items()}
    pred_c = {exp_cand.get(inv_b.get(i, i), inv_b.get(i, i)) for i in ib}
    r["cand_ids_equal_prediction"] = pred_c == ic
    r["ids_only_in_base"] = len(ib - ic); r["ids_only_in_cand"] = len(ic - ib)
    # duplicates on the full index in cand
    r["dup_index_cand"] = int(fc.duplicated().sum()); r["dup_index_base"] = int(fb.duplicated().sum())
    # row-level: within 2021-22, rows whose i changed
    if "pid" in fb.columns or True:
        keyb = fb[fb["t"] == "2021-22"].copy(); keyc = fc[fc["t"] == "2021-22"].copy()
        keyb["i"] = keyb["i"].map(lambda i: exp_cand.get(inv_b.get(i, i), inv_b.get(i, i)))
        cols = list(keyb.columns)
        r["rows_2021_22_equal_after_remap"] = bool(
            keyb.sort_values(cols).reset_index(drop=True).equals(keyc.sort_values(cols).reset_index(drop=True)))
    out[t] = r
print(json.dumps(out, indent=1))
json.dump(out, open(f"{D}/bf_compare.json", "w"), indent=1)
