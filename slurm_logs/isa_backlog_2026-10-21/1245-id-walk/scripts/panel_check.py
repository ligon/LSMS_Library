"""Run diagnostics.check_panel_consistency for one country in the current
LSMS_DATA_DIR (warm); print one JSON line of check statuses.
Usage: python panel_check.py <checkout> <country>"""
import json, os, sys, warnings
warnings.simplefilter("ignore")
checkout, country = sys.argv[1:3]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep)
from lsms_library.diagnostics import check_panel_consistency
rep = check_panel_consistency(ll.Country(country))
print(json.dumps({"country": country,
                  "checks": {c.name: [c.status, c.message[:240]] for c in rep.checks}}))
