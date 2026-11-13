"""GH1118 Mali: split null plot_inputs units into raw absence vs mapping loss.

Runs each EAC-I wave's real ``plot_inputs.py`` in-process with
``mali.plot_inputs_finalize`` wrapped to capture its input frame and
``to_parquet`` disabled, so nothing is written.  For the captured frame it
reports, per wave:

* rows retained by the existing content predicate whose mapped ``u`` is null,
  split into RAW-ABSENT (raw value null/blank, or the slot has no unit
  question) and UNMAPPED (a non-blank raw label ``_unit_labels`` maps to null),
  with the unmapped labels and counts;
* null ``u`` in the finalized output, split by whether a quantity is present
  and by input.

Usage (from the checkout, PYTHONPATH=<checkout>, private LSMS_DATA_DIR):
    python diagnose_1118.py <checkout> <out.json>
"""
import json
import os
import runpy
import sys
import warnings
from collections import Counter
from pathlib import Path

checkout, out_path = Path(sys.argv[1]).resolve(), sys.argv[2]
import pandas as pd  # noqa: E402
import lsms_library  # noqa: E402
import lsms_library.local_tools as lt  # noqa: E402

assert os.path.realpath(lsms_library.__file__).startswith(str(checkout) + os.sep), lsms_library.__file__
warnings.simplefilter("ignore")

mali_dir = checkout / "lsms_library/countries/Mali"
sys.path.insert(0, str(mali_dir / "_"))
import mali  # noqa: E402

assert Path(mali.__file__).resolve() == mali_dir / "_" / "mali.py", mali.__file__

captured = {}
orig_finalize = mali.plot_inputs_finalize
lt.to_parquet = lambda *a, **k: None  # build nothing, write nothing


def report(wave, raw):
    raw = raw.copy()
    raw_u = raw["u"]
    mapped = mali._unit_labels(raw_u)
    # Mirror the finalizer's content predicate on the raw frame.
    q = pd.to_numeric(raw["Quantity"], errors="coerce")
    qp = pd.to_numeric(raw["Quantity_purchased"], errors="coerce")
    q = q.mask(q.isin([99, 999, 9999, 99999]))
    qp = qp.mask(qp.isin([99, 999, 9999, 99999]))
    purchased = raw["Purchased"].astype("boolean")
    improved = raw["Improved"].astype("boolean")
    content = (q.notna() | qp.notna() | purchased.fillna(False).astype(bool) | improved.notna())
    input_ok = mali._input_labels(raw["input"]).notna()
    kept = content & input_ok

    raw_str = raw_u.astype("string").str.strip()
    blank = raw_str.isna() | (raw_str == "")
    null_mapped = mapped.isna() & kept
    unmapped = null_mapped & ~blank
    absent = null_mapped & blank

    qa = q[absent]
    absent_quantity_profile = {
        "rows": int(absent.sum()),
        "quantity_na": int(qa.isna().sum()),
        "quantity_zero": int((qa == 0).sum()),
        "quantity_positive": int((qa > 0).sum()),
        "quantity_negative": int((qa < 0).sum()),
        "quantity_positive_quantiles": {str(p): float(qa[qa > 0].quantile(p))
                                        for p in (0.1, 0.25, 0.5, 0.75, 0.9)} if (qa > 0).any() else {},
        "purchased_qty_present": int(qp[absent].notna().sum()),
    }
    fin = orig_finalize(raw)
    fin_null = fin["u"].isna()
    has_q = fin["Quantity"].notna() | fin["Quantity_purchased"].notna()
    return {
        "raw_rows": int(len(raw)),
        "kept_rows_pre_collapse": int(kept.sum()),
        "kept_null_u_pre_collapse": int(null_mapped.sum()),
        "raw_absent": int(absent.sum()),
        "unmapped_nonblank": int(unmapped.sum()),
        "unmapped_labels": Counter(raw_str[unmapped].tolist()).most_common(),
        "raw_absent_quantity_profile": absent_quantity_profile,
        "raw_absent_by_input": Counter(mali._input_labels(raw["input"])[absent].astype(str).tolist()).most_common(),
        "final_rows": int(len(fin)),
        "final_null_u": int(fin_null.sum()),
        "final_null_u_with_quantity": int((fin_null & has_q).sum()),
        "final_null_u_without_quantity": int((fin_null & ~has_q).sum()),
        "final_null_u_by_input": Counter(fin.index.get_level_values("input")[fin_null].astype(str).tolist()).most_common(),
        "final_u_values": Counter(fin["u"].astype(str).tolist()).most_common(),
    }


results = {}
for wave in ("2014-15", "2017-18"):
    def capture(df, _w=wave):
        captured[_w] = df.copy()
        return orig_finalize(df)

    mali.plot_inputs_finalize = capture
    wdir = mali_dir / wave / "_"
    cwd = os.getcwd()
    os.chdir(wdir)
    try:
        runpy.run_path(str(wdir / "plot_inputs.py"), run_name="__main__")
        # The categorical helpers resolve tables relative to the script cwd.
        results[wave] = report(wave, captured[wave])
    finally:
        os.chdir(cwd)
    print(wave, {k: v for k, v in results[wave].items() if not isinstance(v, list)})
    print("   unmapped labels:", results[wave]["unmapped_labels"][:20])

with open(out_path, "w") as f:
    json.dump(results, f, indent=1)
