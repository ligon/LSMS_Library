"""Red team #1716: rebuild the windows_build_check.py fingerprint from a WARM read
and save per-row hashes (job-local only; never to shared scratch).

usage: python dump_uganda.py <job-local prefix>
Prints only shapes, digests and counts -- no survey values.
"""
import os
import subprocess
import sys
import warnings

warnings.simplefilter("ignore")
import pandas as pd  # noqa: E402
import lsms_library as ll  # noqa: E402

top = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
prefix = sys.argv[1]
assert prefix.startswith("/local/job"), prefix
MASK = 0xFFFFFFFFFFFF
for table in ("shocks", "assets"):
    df = getattr(ll.Country("Uganda"), table)()
    # Exactly windows_build_check.py:81-83.
    flat = df.reset_index()
    flat = flat[sorted(flat.columns)].sort_values(sorted(flat.columns), kind="mergesort").reset_index(drop=True)
    h = pd.util.hash_pandas_object(flat.astype(str), index=False)
    digest = int(h.sum()) & MASK
    out = pd.DataFrame({"t": flat["t"].astype(str), "h": h.to_numpy()})
    out.to_parquet(f"{prefix}-{table}.parquet")
    print(f"WARM {table} rows={len(df)} cols={df.shape[1]} digest={digest:012x} index={list(df.index.names)}")
