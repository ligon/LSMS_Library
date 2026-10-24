"""harvest_kg decomposition on a built crop_production: all rows vs rows off
unknown_plot.  Sums / counts only."""
import os as _os, lsms_library as _ll
_top = _os.path.realpath(_os.environ.get("CLONE") or _os.getcwd())
assert _os.path.realpath(_ll.__file__).startswith(_top + _os.sep), _ll.__file__
import sys, warnings
warnings.simplefilter("ignore")
import lsms_library as ll
from lsms_library.transformations import harvest_kg
from lsms_library.paths import countries_root
sys.path.insert(0, str(countries_root() / "Malawi" / "_"))
import malawi as M
cp = ll.Country("Malawi").crop_production()
shipped = M.crop_conversion_factors()
known = cp[cp.index.get_level_values("plot_id") != "unknown_plot"] if "plot_id" in cp.index.names else cp
for name, frame in (("all rows", cp), ("off unknown_plot", known)):
    b = harvest_kg(frame).reset_index().groupby("t")["Harvest_kg"].agg(["sum", "size"])
    s = harvest_kg(M.with_region(frame), shipped_factors=shipped).reset_index().groupby("t")["Harvest_kg"].agg(["sum", "size"])
    print(name, "no shipped:", {k: (round(float(v["sum"]), 1), int(v["size"])) for k, v in b.iterrows()})
    print(name, "shipped   :", {k: (round(float(v["sum"]), 1), int(v["size"])) for k, v in s.iterrows()})
