"""GH #1259 -- cold-build ``crop_production`` for every declaring country.

The shared warm cache graded ``stale`` against development ``9a0ed5f3c`` for
every screened table (all 16 ``crop_production`` parquets), so the corpus sweep
is re-run on a COLD build of development into a private ``LSMS_DATA_DIR``.
Site Q's rule is not a build input (0 hashes move; see README), so the frames
built here are the frames either checkout serves.

Usage: LSMS_DATA_DIR=<private root> python build_cold.py <checkout> [table]
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
import traceback
import warnings

checkout = sys.argv[1]
table = sys.argv[2] if len(sys.argv) > 2 else "crop_production"

import lsms_library as ll  # noqa: E402
from lsms_library.paths import countries_root, data_root  # noqa: E402

assert os.path.realpath(ll.__file__).startswith(
    os.path.realpath(checkout) + os.sep), ll.__file__
print("lsms_library:", ll.__file__,
      subprocess.check_output(["git", "-C", checkout, "rev-parse", "--short",
                               "HEAD"], text=True).strip(), flush=True)
print("data_root:", data_root(), flush=True)
assert ".local/share/lsms_library" not in str(data_root()), "never the shared cache"
warnings.simplefilter("ignore")

declaring = []
for cdir in sorted(countries_root().iterdir()):
    scheme = cdir / "_" / "data_scheme.yml"
    if scheme.exists() and f"{table}:" in scheme.read_text():
        declaring.append(cdir.name)
print(f"{len(declaring)} countries declare {table}: {declaring}", flush=True)

for name in declaring:
    t0 = time.time()
    try:
        df = getattr(ll.Country(name), table)()
        print(f"{name}: {df.shape} in {time.time() - t0:.0f} s", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"{name}: FAILED after {time.time() - t0:.0f} s: "
              f"{type(exc).__name__}: {exc}", flush=True)
        traceback.print_exc()
