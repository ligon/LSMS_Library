"""Compare the pickled frames from two eq_build.py runs (aggregates only)."""
import json
import pickle
import sys

import pandas as pd

a, b, tables = sys.argv[1], sys.argv[2], sys.argv[3:]
for t in tables:
    x = pickle.load(open(f"{a}/{t}.pkl", "rb"))
    y = pickle.load(open(f"{b}/{t}.pkl", "rb"))
    res = {"table": t, "rows": [len(x), len(y)]}
    try:
        pd.testing.assert_frame_equal(x, y, check_exact=True)
        res["assert_frame_equal"] = "equal"
    except AssertionError as e:
        res["assert_frame_equal"] = "DIFFERENT: " + str(e)[:300]
    res["attrs_keys_equal"] = sorted(x.attrs) == sorted(y.attrs)
    res["attrs_equal"] = repr(x.attrs) == repr(y.attrs)
    print(json.dumps(res))
