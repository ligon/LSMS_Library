"""Feature() assembly with Malawi's native production_obs / marker / sentinel.
Counts only."""
import os as _os, lsms_library as _ll
_top = _os.path.realpath(_os.environ.get("CLONE") or _os.getcwd())
assert _os.path.realpath(_ll.__file__).startswith(_top + _os.sep), _ll.__file__
import warnings
import lsms_library as ll
for feat in ("crop_production", "plot_inputs", "plot_labor"):
    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter("always")
        df = ll.Feature(feat)(["Malawi", "Tanzania"])
    r = df.reset_index()
    kept = sorted(set(r["country"]))
    print(f"{feat}: rows {len(df)}; kept {kept}; index {list(df.index.names)}")
    excl = [str(w.message)[:160] for w in ws if "exclud" in str(w.message).lower()]
    print("   exclusion warnings:", len(excl), excl[:2])
    m = r[r["country"] == "Malawi"]
    print(f"   Malawi rows {len(m)}")
    if "production_obs" in r:
        po = m["production_obs"].astype(str)
        print("   Malawi production_obs: reported:<G|P>:<code>", int(po.str.fullmatch(r"reported:[GP]:\d+").sum()),
              "| unresolved_observation", int((po == "unresolved_observation").sum()), "| NaN", int(m["production_obs"].isna().sum()))
    if "plot_id" in r:
        print("   Malawi plot_id unknown_plot", int((m["plot_id"].astype(str) == "unknown_plot").sum()), "| NaN", int(m["plot_id"].isna().sum()))
    if "j" in r and feat == "plot_inputs":
        print("   Malawi j '(not crop-specific)'", int((m["j"].astype(str) == "(not crop-specific)").sum()), "| NaN j", int(m["j"].isna().sum()),
              "| Tanzania NaN j", int(r[r["country"] == "Tanzania"]["j"].isna().sum()))
