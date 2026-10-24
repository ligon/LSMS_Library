"""Variable and value labels of the Nigeria cover movement fields (metadata only)."""
import json, os, sys
import pyreadstat
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(os.environ["CLONE"]) + os.sep)
from lsms_library import data_access
out = {}
for rel in ("Nigeria/2018-19/Data/secta_plantingw4.dta",
            "Nigeria/2023-24/Data/Post Planting Wave 5/Household/secta_plantingw5.dta"):
    p = data_access.get_data_file(rel)
    _, meta = pyreadstat.read_dta(str(p), metadataonly=True)
    labs = dict(zip(meta.column_names, meta.column_labels))
    o = out[rel] = {}
    for v in ("hhsmlc", "hh_moved", "tracked_obs", "trackedw4", "trackedw5pp", "hhidyn"):
        if v in labs:
            vl = meta.variable_to_label.get(v)
            o[v] = {"label": labs[v],
                    "values": {str(k): s for k, s in (meta.value_labels.get(vl, {}) if vl else {}).items()}}
json.dump(out, open(sys.argv[1], "w"), indent=1)
print("ok")
