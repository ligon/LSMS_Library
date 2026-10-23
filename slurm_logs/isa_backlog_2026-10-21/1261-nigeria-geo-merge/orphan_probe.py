"""GH #1261: who are the geo-only households?  AGGREGATES ONLY.

    python orphan_probe.py <checkout> <out.json>

For each Nigeria wave whose household geovariables file is wired into
cluster_features, compares the hhid sets of the post-planting cover (the
cluster_features primary sub-df), the post-harvest cover, and the geo file,
and characterises the geo-only households: do they appear in the PH cover,
does the geo file give them a cluster code (ea) or coordinates?
"""
import importlib.util
import json
import os
import subprocess
import sys
import warnings

checkout, out = sys.argv[1], sys.argv[2]
import pandas as pd  # noqa: E402
import lsms_library as ll  # noqa: E402
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

top = subprocess.check_output(['git', '-C', checkout, 'rev-parse', '--show-toplevel'],
                              text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
warnings.simplefilter('ignore')

NG = countries_root() / 'Nigeria'
_spec = importlib.util.spec_from_file_location('_nigeria_1261', NG / '_' / 'nigeria.py')
nigeria = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(nigeria)


def clusters(df):
    """The composite `v` (nigeria.cluster_id) of each row, from raw codes."""
    return df.apply(lambda r: nigeria.cluster_id(r.get('state'), r.get('lga'),
                                                 r.get('ea'), r.get('hhid')), axis=1)


SPEC = {
    '2010-11': ('Post Planting Wave 1/Household/secta_plantingw1.dta',
                'Post Harvest Wave 1/Household/secta_harvestw1.dta',
                'nga_householdgeovariables_y1.csv'),
    '2012-13': ('Post Planting Wave 2/Household/secta_plantingw2.dta',
                'Post Harvest Wave 2/Household/secta_harvestw2.dta',
                'nga_householdgeovars_y2.csv'),
    '2015-16': ('secta_plantingw3.dta', 'secta_harvestw3.dta',
                'NGA_HouseholdGeovars_Y3.dta'),
    '2018-19': ('secta_plantingw4.dta', 'secta_harvestw4.dta',
                'nga_householdgeovars_y4.dta'),
}


def hh(df):
    col = 'hhid' if 'hhid' in df.columns else [c for c in df.columns if c.lower() == 'hhid'][0]
    return df[col].map(lambda x: str(int(float(x))) if pd.notna(x) else None)


res = {}
for w, (pp, ph, geo) in SPEC.items():
    rec = {}
    try:
        d_pp = get_dataframe(str(NG / w / 'Data' / pp), convert_categoricals=False)
        d_geo = get_dataframe(str(NG / w / 'Data' / geo))
        try:
            d_ph = get_dataframe(str(NG / w / 'Data' / ph), convert_categoricals=False)
        except Exception as e:  # noqa: BLE001
            d_ph = None
            rec['ph_error'] = repr(e)[:200]
        s_pp, s_geo = set(hh(d_pp).dropna()), set(hh(d_geo).dropna())
        orphans = s_geo - s_pp
        rec.update(pp_households=len(s_pp), geo_households=len(s_geo),
                   geo_only=len(orphans), cover_only=len(s_pp - s_geo))
        if d_ph is not None:
            s_ph = set(hh(d_ph).dropna())
            rec.update(ph_households=len(s_ph),
                       geo_only_in_ph_cover=len(orphans & s_ph),
                       ph_only_vs_pp=len(s_ph - s_pp))
        g = d_geo.assign(_hh=hh(d_geo))
        go = g[g['_hh'].isin(orphans)]
        lat = [c for c in g.columns if c.lower() == 'lat_dd_mod']
        ea = [c for c in g.columns if c.lower() == 'ea']
        rec['geo_columns_cluster'] = [c for c in g.columns if c.lower() in ('ea', 'lga', 'state', 'zone')]
        if lat:
            rec['geo_only_with_latitude'] = int(go[lat[0]].notna().sum())
        if ea:
            rec['geo_only_with_ea'] = int(go[ea[0]].notna().sum())
            if len(go) and go[ea[0]].notna().any():
                pp_v = set(clusters(d_pp).dropna())
                go_v = set(clusters(go).dropna())
                rec['geo_only_clusters'] = len(go_v)
                rec['geo_only_clusters_on_pp_cover'] = len(go_v & pp_v)
                rec['geo_only_clusters_absent_from_pp_cover'] = len(go_v - pp_v)
    except Exception as e:  # noqa: BLE001
        rec['error'] = repr(e)[:300]
    res[w] = rec
    print(w, rec, flush=True)

with open(out, 'w') as f:
    json.dump(res, f, indent=1, sort_keys=True)
