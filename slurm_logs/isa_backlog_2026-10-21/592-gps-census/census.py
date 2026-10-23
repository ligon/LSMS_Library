"""GH #592 GPS census: per-wave cluster / GPS counts for cluster_features.

Usage (from the worktree, fresh LSMS_DATA_DIR):
    PYTHONPATH=<worktree> python census.py Country [Country ...] > out.json

Emits AGGREGATES ONLY (counts) -- never a coordinate or an identifier.

For each country:
  * cold-builds Country(C).cluster_features() and counts, per wave t,
    clusters (distinct v), clusters with non-null Latitude / Longitude / both;
  * records the GrainCollapseWarning reports filed for cluster_features
    (Site 1 and Site 2) -- counts only;
  * re-reads each wave's PRE-COLLAPSE frame via Wave.grab_data (warm L2-wave
    after the build) and, where it is household grain (`i` in index or
    columns), counts households, households with GPS, clusters whose
    households carry >1 distinct non-null coordinate pair, and clusters where
    some but not all households have a fix (Site 2 hazard measurement).
"""
import json
import os
import subprocess
import sys
import warnings

import lsms_library
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__

import pandas as pd
import lsms_library as ll
from lsms_library.country import grain_reports


def wave_census(cf: pd.DataFrame) -> dict:
    df = cf.reset_index()
    out = {}
    for t, g in df.groupby('t', sort=True):
        lat = g['Latitude'] if 'Latitude' in g else pd.Series(dtype=float)
        lon = g['Longitude'] if 'Longitude' in g else pd.Series(dtype=float)
        out[str(t)] = {
            'rows': int(len(g)),
            'clusters': int(g['v'].nunique()),
            'clusters_with_lat': int(g.loc[lat.notna(), 'v'].nunique()) if len(lat) else 0,
            'clusters_with_lon': int(g.loc[lon.notna(), 'v'].nunique()) if len(lon) else 0,
            'clusters_with_both': int(g.loc[lat.notna() & lon.notna(), 'v'].nunique()) if len(lat) and len(lon) else 0,
            'has_lat_col': 'Latitude' in g,
            'has_lon_col': 'Longitude' in g,
        }
    return out


def precollapse(c, wave: str) -> dict:
    w = c[wave]
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        df = w.grab_data('cluster_features')
    hh_grain = ('i' in df.index.names) or ('i' in df.columns)
    flat = df.reset_index()
    res = {'hh_grain': bool(hh_grain), 'rows': int(len(flat)),
           'columns': sorted(map(str, flat.columns))}
    if 'Latitude' not in flat or 'Longitude' not in flat:
        res['gps_columns_present'] = False
        return res
    res['gps_columns_present'] = True
    has = flat['Latitude'].notna() & flat['Longitude'].notna()
    res['rows_with_gps'] = int(has.sum())
    if not hh_grain:
        # cluster-grain source: count duplicated (t, v) and disagreement
        g = flat.groupby(['t', 'v'], dropna=False)
    else:
        res['households'] = int(flat['i'].nunique())
        g = flat.groupby(['t', 'v'], dropna=False)
    pair = flat['Latitude'].round(6).astype(str) + ',' + flat['Longitude'].round(6).astype(str)
    flat = flat.assign(_pair=pair.where(has), _has=has)
    g = flat.groupby(['t', 'v'], dropna=False)
    n_distinct = g['_pair'].nunique(dropna=True)
    any_has = g['_has'].any()
    all_has = g['_has'].all()
    size = g.size()
    res['groups'] = int(len(size))
    res['multi_row_groups'] = int((size > 1).sum())
    res['groups_with_gps'] = int(any_has.sum())
    res['groups_gps_disagree'] = int((n_distinct > 1).sum())
    res['groups_gps_partial'] = int((any_has & ~all_has).sum())
    res['nan_v_rows'] = int(flat['v'].isna().sum())
    # Per-column: which columns make the (t, v) projection lossy?  A group
    # "disagrees" on a column when it holds >1 distinct non-null value; it is
    # "partial" when some rows are null and some are not (a whole-row audit
    # counts that as a conflict too).  Rounded to 6 dp for the coordinates.
    per_col = {}
    for col in ('Region', 'District', 'Rural', 'Latitude', 'Longitude'):
        if col not in flat:
            continue
        s = flat[col]
        key = s.round(6).astype(str) if col in ('Latitude', 'Longitude') else s.astype(str)
        key = key.where(s.notna())
        tmp = flat[['t', 'v']].assign(_k=key, _nn=s.notna())
        gg = tmp.groupby(['t', 'v'], dropna=False)
        nd = gg['_k'].nunique(dropna=True)
        anyn = gg['_nn'].any()
        alln = gg['_nn'].all()
        per_col[col] = {'groups_disagree': int((nd > 1).sum()),
                        'groups_partial': int((anyn & ~alln).sum())}
    res['per_column'] = per_col
    return res


def main(countries):
    os.environ.setdefault('LSMS_BUILD_WORKERS', '1')
    result = {}
    for C in countries:
        entry = {}
        from lsms_library.paths import data_root
        entry['l2_country_cache_existed_before'] = (data_root() / C / 'var' / 'cluster_features.parquet').exists()
        c = ll.Country(C)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            try:
                cf = c.cluster_features()
                entry['error'] = None
            except Exception as exc:  # report, don't hide
                cf = None
                entry['error'] = f'{type(exc).__name__}: {exc}'
        entry['waves_declared'] = list(c.waves)
        entry['warnings'] = {}
        for w in caught:
            k = w.category.__name__
            entry['warnings'][k] = entry['warnings'].get(k, 0) + 1
        # Warning TEXT is not kept: messages can quote cluster identifiers
        # (the Site C warning names a composite `v`).  Counts only.
        if cf is not None:
            entry['columns'] = list(map(str, cf.columns))
            entry['index'] = list(map(str, cf.index.names))
            entry['census'] = wave_census(cf)
        reps = grain_reports(country=C, table='cluster_features')
        entry['grain_reports'] = [
            {k: r.get(k) for k in ('wave', 'site', 'kind', 'levels', 'rows',
                                   'dropped', 'destroyed', 'conflicting_groups',
                                   'nan_key_rows', 'unauditable', 'columns')}
            for r in reps]
        entry['precollapse'] = {}
        for wave in c.waves:
            try:
                entry['precollapse'][wave] = precollapse(c, wave)
            except Exception as exc:
                entry['precollapse'][wave] = {'error': f'{type(exc).__name__}: {exc}'[:400]}
        result[C] = entry
        print(f'done {C}', file=sys.stderr, flush=True)
    json.dump(result, sys.stdout, indent=1, default=str)


if __name__ == '__main__':
    main(sys.argv[1:])
