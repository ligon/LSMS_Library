"""Adversarial cold-cache build verification for EthiopiaRHS.

For every feature in the data_scheme, cold-build via diagnostics.load_feature
(LSMS_NO_CACHE=1 set in the environment), run is_this_feature_sane, and report:
  - build OK / exception (with traceback)
  - report.ok and full check list (status + message)
  - any non-acceptable WARN (only framework-joined-v index_levels_match_scheme is allowed)
  - per-wave coverage (which of the 8 waves appear in the t index level)
  - row count and shape
"""
import os
import sys
import traceback
import warnings

# Hard-assert no-cache is active.
assert os.environ.get("LSMS_NO_CACHE") == "1", "LSMS_NO_CACHE must be 1"

import pandas as pd
import lsms_library as ll
from lsms_library import diagnostics

COUNTRY = "EthiopiaRHS"
ACCEPTABLE_WARN_NAMES = {"index_levels_match_scheme"}  # framework-joined-v only

c = ll.Country(COUNTRY)
waves = list(c.waves)
features = sorted(c.data_scheme)

print(f"COUNTRY={COUNTRY}")
print(f"WAVES={waves}")
print(f"FEATURES={features}")
print("=" * 80)

results = {}

for feat in features:
    print(f"\n### FEATURE: {feat}")
    rec = {"feature": feat, "build_ok": False, "report_ok": None,
           "exception": None, "warns": [], "fails": [],
           "waves_covered": None, "rows": None, "index_names": None,
           "bad_warns": []}
    # Capture warnings emitted during build/sanity.
    with warnings.catch_warnings(record=True) as wlist:
        warnings.simplefilter("always")
        try:
            df = diagnostics.load_feature(c, feat)
            rec["build_ok"] = True
        except Exception as e:
            rec["exception"] = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            print(f"  BUILD FAILED: {type(e).__name__}: {e}")
            print(rec["exception"])
            results[feat] = rec
            continue

    # Property features (panel_ids) return dict, not DataFrame.
    if feat in diagnostics._PROPERTY_FEATURES:
        rec["rows"] = len(df) if df is not None else 0
        rec["index_names"] = "property(dict)"
        print(f"  PROPERTY feature -> type={type(df).__name__}, len={rec['rows']}")
        # Still run sanity (it handles property features).
        try:
            report = diagnostics.is_this_feature_sane(df, COUNTRY, feat)
        except Exception as e:
            rec["exception"] = "sanity raised: " + "".join(
                traceback.format_exception(type(e), e, e.__traceback__))
            print(f"  SANITY RAISED: {e}")
            results[feat] = rec
            continue
    else:
        if not isinstance(df, pd.DataFrame):
            rec["exception"] = f"expected DataFrame, got {type(df)}"
            print(f"  NON-DATAFRAME RESULT: {type(df)}")
            results[feat] = rec
            continue
        rec["rows"] = len(df)
        rec["index_names"] = list(df.index.names)
        # Wave coverage if a t level exists.
        if "t" in df.index.names:
            covered = sorted(str(x) for x in df.index.get_level_values("t").unique())
            rec["waves_covered"] = covered
        elif "t" in df.columns:
            covered = sorted(str(x) for x in df["t"].unique())
            rec["waves_covered"] = covered
        print(f"  shape={df.shape} index={rec['index_names']} waves={rec['waves_covered']}")
        try:
            report = diagnostics.is_this_feature_sane(df, COUNTRY, feat)
        except Exception as e:
            rec["exception"] = "sanity raised: " + "".join(
                traceback.format_exception(type(e), e, e.__traceback__))
            print(f"  SANITY RAISED: {e}")
            results[feat] = rec
            continue

    rec["report_ok"] = report.ok
    for chk in report.checks:
        if chk.status == "fail":
            rec["fails"].append((chk.name, chk.message))
        elif chk.status == "warn":
            rec["warns"].append((chk.name, chk.message))
            if chk.name not in ACCEPTABLE_WARN_NAMES:
                rec["bad_warns"].append((chk.name, chk.message))

    print(f"  report.ok={report.ok}")
    for name, msg in rec["fails"]:
        print(f"  FAIL[{name}]: {msg}")
    for name, msg in rec["warns"]:
        tag = "WARN-OK" if name in ACCEPTABLE_WARN_NAMES else "WARN-BAD"
        print(f"  {tag}[{name}]: {msg}")

    # Python warnings captured during this feature.
    pywarns = [str(w.message) for w in wlist]
    if pywarns:
        print(f"  PYWARNINGS ({len(pywarns)}):")
        for pw in pywarns[:20]:
            print(f"    - {pw}")
    rec["pywarns"] = pywarns

    results[feat] = rec

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
overall_fail = False
for feat in features:
    r = results[feat]
    status = "OK"
    if not r["build_ok"]:
        status = "BUILD-FAIL"
        overall_fail = True
    elif r["exception"]:
        status = "SANITY-RAISED"
        overall_fail = True
    elif r["fails"]:
        status = "SANE-FAIL"
        overall_fail = True
    elif r["bad_warns"]:
        status = "BAD-WARN"
        # bad warns are reported but per brief only the v-join warn is "acceptable";
        # mark for review but it does not by itself fail report.ok
    print(f"  {feat:28s} {status:14s} rows={r['rows']} report_ok={r['report_ok']} "
          f"waves={r['waves_covered']} bad_warns={[n for n,_ in r['bad_warns']]}")

print("\nOVERALL_FAIL=" + str(overall_fail))
