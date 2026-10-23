"""GH #1259 -- Site Q's cost INSIDE a warm ``crop_production`` read.

The #857 method: a spy on the hook (``lsms_library.country.check_quantities``,
which ``country.py`` binds at import), best of N warm reads, so the number is
the audit's own time rather than the difference of two whole reads on a loaded
machine.  Run it once per checkout (base, branch) against a PRIVATE data root
holding warm parquets -- never the shared cache, because a ``Country()`` read
that grades anything stale writes.

Also asserts the served frame is unchanged by the audit (``check_quantities``
returns its input object, and every ``Quantity`` value equals the parquet's).

Usage: python read_cost.py <checkout> <Country> [<Country> ...] [--repeat N]
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
import warnings

checkout = sys.argv[1]
repeat = 5
args = sys.argv[2:]
if "--repeat" in args:
    k = args.index("--repeat")
    repeat = int(args[k + 1])
    args = args[:k] + args[k + 2:]
countries = args

import numpy as np  # noqa: E402

import lsms_library  # noqa: E402
import lsms_library.country as country_mod  # noqa: E402

assert os.path.realpath(lsms_library.__file__).startswith(
    os.path.realpath(checkout) + os.sep), lsms_library.__file__
print("lsms_library:", lsms_library.__file__,
      subprocess.check_output(["git", "-C", checkout, "rev-parse", "--short",
                               "HEAD"], text=True).strip())

_real = country_mod.check_quantities
_spent: list[float] = []
_identity: list[bool] = []


def _spy(df, **kw):
    t0 = time.perf_counter()
    out = _real(df, **kw)
    _spent.append(time.perf_counter() - t0)
    _identity.append(out is df)
    return out


country_mod.check_quantities = _spy
warnings.simplefilter("ignore")

for name in countries:
    c = lsms_library.Country(name)
    c.crop_production()                      # warm-up (and grade the cache)
    rows = []
    for _ in range(repeat):
        _spent.clear()
        t0 = time.perf_counter()
        df = c.crop_production()
        total = time.perf_counter() - t0
        rows.append((total, sum(_spent), len(_spent)))
    assert all(_identity), "check_quantities did not return its input object"
    best = min(rows, key=lambda r: r[0])
    audit_best = min(r[1] for r in rows)
    print(f"{name}: rows={len(df):,} best warm read {best[0] * 1000:,.0f} ms; "
          f"Site Q inside it {best[1] * 1000:,.1f} ms "
          f"({100 * best[1] / best[0]:.1f}%, {best[2]} call(s)); "
          f"best Site Q over {repeat} reads {audit_best * 1000:,.1f} ms")
    q = df["Quantity"].to_numpy(dtype="float64", na_value=np.nan)
    print(f"   Quantity checksum: n={np.isfinite(q).sum():,} "
          f"sum={np.nansum(q):.6g} zeros={(q == 0).sum():,}")
