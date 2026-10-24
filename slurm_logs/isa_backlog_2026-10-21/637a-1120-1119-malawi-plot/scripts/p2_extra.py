"""After build_all.py in the same LSMS_DATA_DIR: harvest_kg (with and without
the shipped IHS5 factors), Site I null-index reports, and grain counts.
Counts / sums only."""
import os as _os, lsms_library as _ll
_top = _os.path.realpath(_os.environ.get("CLONE") or _os.getcwd())
assert _os.path.realpath(_ll.__file__).startswith(_top + _os.sep), _ll.__file__
import sys, warnings
import pandas as pd
warnings.simplefilter("ignore")
import lsms_library as ll
from lsms_library.transformations import harvest_kg, harvest_kg_factors
from lsms_library.null_index_audit import null_index_reports
from lsms_library.paths import countries_root
sys.path.insert(0, str(countries_root() / "Malawi" / "_"))
import malawi as M
c = ll.Country("Malawi")
cp = c.crop_production()
base = harvest_kg(cp)
shipped = M.crop_conversion_factors()
ship = harvest_kg(M.with_region(cp), shipped_factors=shipped)
fac = harvest_kg_factors(M.with_region(cp), shipped_factors=shipped)
b = base.reset_index().groupby("t")["Harvest_kg"].sum()
s = ship.reset_index().groupby("t")["Harvest_kg"].sum()
print("harvest_kg (no shipped) by wave:", {k: round(float(v), 1) for k, v in b.items()})
print("harvest_kg (shipped)    by wave:", {k: round(float(v), 1) for k, v in s.items()})
print("harvest_kg rows: no shipped", len(base), "shipped", len(ship))
print("kg_factor_sources (shipped):", fac.attrs.get("kg_factor_sources"))
r = cp.reset_index()
print("crop_production index:", list(cp.index.names))
print("crop_production rows by wave:", r.groupby("t").size().to_dict())
for col in ("plot_id", "u", "condition", "production_obs"):
    if col in r.columns:
        print(f"  NaN {col}:", int(r[col].isna().sum()))
for t in ("crop_production", "plot_labor", "plot_inputs"):
    getattr(c, t)()
    reps = null_index_reports(country="Malawi", table=t)
    print(f"Site I reports {t}: {len(reps)}", [(x.get('level'), x.get('wave') or x.get('t'), x.get('n_null') or x.get('count')) for x in reps][:12])
