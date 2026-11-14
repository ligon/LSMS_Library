"""Dump every country's sampling records per Country.waves (config only)."""
import json
import subprocess
import sys
import warnings

warnings.simplefilter("ignore")
import lsms_library as ll  # noqa: E402
from lsms_library.sampling import sampling_records  # noqa: E402

out = {"head": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
       "countries": {}, "errors": {}}
for c in ll.countries():
    try:
        recs = sampling_records(c)
        waves = list(ll.Country(c).waves)
        cs = ll.Country(c).sampling
        assert cs is recs or cs == recs
    except Exception as e:
        out["errors"][c] = f"{type(e).__name__}: {e}"
        continue
    if not recs:
        continue
    out["countries"][c] = {
        "waves": waves,
        "records": {w: {"keys": sorted(r.keys()),
                        "design_tag": r.design_tag,
                        "strata_holds": getattr(r, "strata_holds", None),
                        "cluster_id_continuity": r.cluster_id_continuity,
                        "json": json.dumps(r, sort_keys=True, default=str)}
                    for w, r in recs.items()},
    }
with open(sys.argv[1], "w") as fh:
    json.dump(out, fh, indent=1, sort_keys=True)
print("records dumped:", {c: len(v["records"]) for c, v in out["countries"].items()},
      "errors:", out["errors"])
