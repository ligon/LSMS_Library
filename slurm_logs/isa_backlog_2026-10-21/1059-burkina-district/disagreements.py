"""GH #1059: the three shared grappes whose District names a different
province in 2018-19 and 2021-22 -- household counts and panel linkage.

Reads the two EHCVM cover pages through get_dataframe (aggregates only).
Usage: python disagreements.py <checkout> <out.json>
"""
import json
import os
import subprocess
import sys
from collections import Counter

checkout, out = sys.argv[1:3]
import lsms_library as ll  # noqa: E402
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

top = subprocess.check_output(["git", "-C", checkout, "rev-parse", "--show-toplevel"], text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__

root = countries_root() / "Burkina_Faso"
d18 = get_dataframe(str(root / "2018-19" / "Data" / "s00_me_bfa2018.dta"))
d21 = get_dataframe(str(root / "2021-22" / "Data" / "s00_me_bfa2021.dta"))
GRAPPES = {19: ("Séno", "Yagha"), 365: ("Poni", "Noumbiel"), 549: ("Kompienga", "Gourma")}

res = {"s00_me_bfa2018_rows": len(d18), "s00_me_bfa2021_rows": len(d21), "grappes": []}
for g, (p18, p21) in GRAPPES.items():
    a = d18[d18["grappe"].astype(float) == g]
    b = d21[d21["grappe"].astype(float) == g]
    prev = b["grappe_EHCVM1"].astype(float)
    res["grappes"].append({
        "grappe": g,
        "expected": {"2018-19": p18, "2021-22": p21},
        "rows_2018_19": len(a),
        "rows_2021_22": len(b),
        "s00q02_2018_19": dict(Counter(a["s00q02"].astype(str))),
        "s00q02_2021_22": dict(Counter(b["s00q02"].astype(str))),
        "s00q01_2018_19": dict(Counter(a["s00q01"].astype(str))),
        "s00q01_2021_22": dict(Counter(b["s00q01"].astype(str))),
        "rows_2021_22_with_grappe_EHCVM1_same": int((prev == g).sum()),
        "rows_2021_22_with_grappe_EHCVM1_other": int(((prev != g) & prev.notna()).sum()),
        "rows_2021_22_with_no_EHCVM1_link": int(prev.isna().sum()),
    })
with open(out, "w") as f:
    json.dump(res, f, indent=1, ensure_ascii=False, default=str)
print(json.dumps(res, indent=1, ensure_ascii=False, default=str))
