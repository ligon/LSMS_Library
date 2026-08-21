#!/usr/bin/env python
"""GH #461 verification gate (normal backend, cleared cache).

Caches were cleared first, so the normal backend re-runs each wave script via
run_make_target -> exercises get_categorical_mapping / df_from_orgfile.  A clean
build confirms the #461 fix (missing named table -> KeyError) does NOT break a
legitimate harmonize_* read.
"""
import lsms_library as ll

TARGETS = {
    "Senegal": ["crop_production", "plot_inputs", "plot_labor", "livestock"],
    "Niger":   ["crop_production", "plot_inputs", "plot_labor", "livestock"],
}

fails = []
for country, feats in TARGETS.items():
    c = ll.Country(country, preload_panel_ids=False)
    for f in feats:
        try:
            df = getattr(c, f)()
            n = len(df)
            ikey = df.index.get_level_values("i").notna().mean() if "i" in df.index.names else float("nan")
            print(f"PASS {country}/{f}: rows={n} i-key={ikey:.3f}")
        except Exception as e:  # noqa: BLE001
            print(f"FAIL {country}/{f}: {type(e).__name__}: {e}")
            fails.append((country, f, type(e).__name__))

print("---")
if fails:
    print("FAILURES:", fails)
    raise SystemExit(1)
print("ALL PASS")
