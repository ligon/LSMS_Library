#!/usr/bin/env python
"""Post-merge verification (#333 + #334 + #335 all in development).

Confirms the two features touched by #335's Feature.__call__ rewrite assemble
cleanly cross-country, and that #335's "drop undeclared extra index levels"
safety net didn't break the individual_education cells added in #333.
"""
import os
os.environ["LSMS_NO_CACHE"] = "1"
import traceback
import lsms_library as ll

def check(name, expect_new=()):
    print(f"\n===== Feature('{name}') =====")
    try:
        df = ll.Feature(name)()
        idx = list(df.index.names)
        print(f"  shape={df.shape}  index.names={idx}  cols={list(df.columns)}")
        # interview_date regression guard: must NOT be the old collapsed
        # single unnamed object index of tuple-strings.
        collapsed = (idx == [None]) or (len(idx) == 1 and idx[0] is None)
        print(f"  collapsed-index (BAD if True): {collapsed}")
        if "country" in idx:
            countries = sorted(df.index.get_level_values("country").unique())
            print(f"  countries ({len(countries)}): {', '.join(countries)}")
            for c in expect_new:
                print(f"    new-cell present? {c}: {c in countries}")
        # all-NaN column guard
        for col in df.columns:
            f = float(df[col].isna().mean())
            flag = "  <-- ALL-NaN" if f == 1.0 else ""
            print(f"    {col}: NaN={f:.1%}{flag}")
    except Exception as e:
        print(f"  ERROR: {type(e).__name__}: {e}")
        traceback.print_exc()

check("interview_date")
check("individual_education", expect_new=("Azerbaijan", "Cambodia", "China", "Guatemala"))
