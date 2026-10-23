"""GH #1234: what the function-level `from .transformations import U_UNKNOWN`
contributes to the build closure.  Usage: python closure_probe.py <out.json>"""
import json, sys
from lsms_library import _build_registry as R
seen, parts = set(), []
for _qn, (fn, _t) in sorted(R._BUILD_TRANSFORMS.items()):
    parts += R._closure_parts(fn, seen)
out = {
    "u_unknown_parts": [p for p in parts if p.startswith("lsms_library.local_tools.U_UNKNOWN")],
    "map_index_in_closure": any(p.startswith("lsms_library.local_tools.map_index=") for p in parts),
    "transformations_callables_in_closure": sorted(q for q in seen if q.startswith("lsms_library.transformations.")),
}
json.dump(out, open(sys.argv[1], "w"), indent=1)
print(out)
