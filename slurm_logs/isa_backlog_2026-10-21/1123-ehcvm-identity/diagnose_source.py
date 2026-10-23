"""GH #1123 Phase 1: native input identity vs the declared plot_inputs key.

Usage: python diagnose_source.py <checkout> <aggregate.json> <private_dir>

Runs each wave's ACTUAL plot_inputs.py (cwd = its _/ dir, to_parquet replaced
by a capture), then rebuilds the declared (t, i, input, crop, u) tuple for
every applied native source line with the script's own helpers and maps, and
checks that reconstruction against the captured frame.  Collision groups on
the declared key are classified by their served payload
(Quantity, Purchased, Quantity_purchased):

  exact          every line carries the same payload
  complementary  lines differ only where one side is NA
  conflicting    some column has two different non-null values

Only aggregate counts go to <aggregate.json>; row-level frames (household
ids with values) go to <private_dir> and are never committed.
"""
import collections
import json
import os
import runpy
import sys
from pathlib import Path

import pandas as pd

checkout, out_json, private = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
import lsms_library  # noqa: E402
from lsms_library import local_tools  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

assert os.path.realpath(lsms_library.__file__).startswith(
    os.path.realpath(checkout) + os.sep), lsms_library.__file__
assert os.path.realpath(countries_root()).startswith(os.path.realpath(checkout) + os.sep)
private.mkdir(parents=True, exist_ok=True)

PAYLOAD = ['Quantity', 'Purchased', 'Quantity_purchased']
KEY = ['t', 'i', 'input', 'crop', 'u']

SPECS = {
    ('Niger', '2011-12'): dict(hh=['hid'], slot='as02cq02', crop='as02cq04', unit='as02cq05b',
                               qty='as02cq05a', flag='as02cq07', qflag='as02cq08a', ecvma=True),
    ('Niger', '2014-15'): dict(hh=['GRAPPE', 'MENAGE', 'EXTENSION'], slot='AS02CQ02', crop='AS02CQ04',
                               unit='AS02CQ05B', qty='AS02CQ05A', flag='AS02CQ07', qflag='AS02CQ08A',
                               ecvma=True),
    ('Niger', '2018-19'): dict(hh=['grappe', 'menage'], slot='s16bq01', unit='s16bq03b',
                               qty='s16bq03a', flag='s16bq05', qflag='s16bq07a'),
    ('Niger', '2021-22'): dict(hh=['grappe', 'menage'], slot='s16bq01', unit='s16bq03b',
                               qty='s16bq03a', flag='s16bq05', qflag='s16bq07a'),
    ('Senegal', '2018-19'): dict(hh=['grappe', 'menage'], slot='s16bq01', unit='s16bq03b',
                                 qty='s16bq03a', flag='s16bq05', qflag='s16bq07a'),
    ('CotedIvoire', '2018-19'): dict(hh=['grappe', 'menage'], slot='s16bq01', unit='s16bq03b',
                                     qty='s16bq03a', flag='s16bq05', qflag='s16bq07a'),
}


def run_script(country, wave):
    wdir = countries_root() / country / wave / '_'
    captured = []
    orig = local_tools.to_parquet
    local_tools.to_parquet = lambda frame, *a, **k: captured.append(frame.copy())
    cwd = os.getcwd()
    saved_path = list(sys.path)
    try:
        os.chdir(wdir)
        sys.modules.pop(country.lower(), None)
        g = runpy.run_path(str(wdir / 'plot_inputs.py'), run_name='__gh1123_diag__')
    finally:
        os.chdir(cwd)
        sys.path[:] = saved_path
        local_tools.to_parquet = orig
    assert len(captured) == 1, (country, wave, len(captured))
    return g, captured[0]


def declared_key(country, wave, g, spec):
    """Rebuild each applied native line's declared tuple from the script's
    own helpers (mirrors the script's DataFrame construction exactly)."""
    src, srcn = g['src'], g['srcn']
    out = pd.DataFrame(index=range(len(srcn)))
    out['t'] = wave
    out['i'] = g['hh'].values
    out['input'] = g['_input_labels'](src[spec['slot']], g['input_map']).values
    if spec.get('ecvma'):
        out['crop'] = g['_crop_labels'](srcn[spec['crop']], src[spec['crop']], g['crop_map']).values
    else:
        out['crop'] = g['_seed_crop_labels'](src[spec['slot']], g['seed_crop_map']).values
    unit_fn = g.get('_unit_labels') or g['_input_labels']
    out['u'] = unit_fn(src[spec['unit']], g['unit_map']).values
    # The served payload, read back from the script's own written frame below.
    out['crop'] = out['crop'].astype('string').fillna('(not crop-specific)')
    u_na = 'Unknown' if country == 'Niger' else 'Manquant'
    out['u'] = out['u'].astype('string').fillna(u_na)
    for c in spec['hh']:
        out['_n_' + c] = srcn[c].values
    out['_n_slot'] = srcn[spec['slot']].values
    if spec.get('ecvma'):
        out['_n_crop'] = srcn[spec['crop']].values
    out['_n_unit'] = srcn[spec['unit']].values
    keep = out['input'].notna()
    if country == 'CotedIvoire':
        keep &= out['i'].notna()
    return out[keep].reset_index(drop=True)


def classify(group):
    pay = group[PAYLOAD]
    if len(pay.drop_duplicates()) == 1:
        return 'exact'
    for c in PAYLOAD:
        vals = pay[c].dropna().unique()
        if len(vals) > 1:
            return 'conflicting'
    return 'complementary'


agg = {}
for (country, wave), spec in SPECS.items():
    g, written = run_script(country, wave)
    srcn = g['srcn']
    native_cols = spec['hh'] + [spec['slot']] + ([spec['crop']] if spec.get('ecvma') else [])
    rec = {
        'applied_native_rows': int(len(srcn)),
        'native_key': native_cols,
        'native_key_null_rows': int(srcn[native_cols].isna().any(axis=1).sum()),
        'native_key_dup_rows': int(srcn.duplicated(native_cols).sum()),
        'hh_slot_dup_rows': int(srcn.duplicated(spec['hh'] + [spec['slot']]).sum()),
        'households': int(srcn[spec['hh']].drop_duplicates().shape[0]),
        'distinct_i': int(g['hh'].nunique()),
        'written_rows': int(len(written)),
    }
    dk = declared_key(country, wave, g, spec)
    # Payload in the written frame row order equals dk's order (finish keeps
    # row order); verify the tuple multiset and then attach the payload.
    wflat = written.reset_index()
    assert len(wflat) == len(dk), (len(wflat), len(dk))
    for c in KEY:
        a = wflat[c].astype('string').reset_index(drop=True)
        b = dk[c].astype('string').reset_index(drop=True)
        assert a.equals(b), (country, wave, c, int((a != b).sum()))
    for c in PAYLOAD:
        dk[c] = wflat[c].values
    rec['reconstruction_matches_written_frame'] = True
    dup = dk.duplicated(KEY, keep=False)
    groups = dk[dup].groupby(KEY, dropna=False, sort=False)
    kinds = collections.Counter()
    pairs = collections.Counter()
    sizes = collections.Counter()
    kind_extra = collections.Counter()
    for _, grp in groups:
        k = classify(grp)
        kinds[k] += 1
        kind_extra[k] += len(grp) - 1
        sizes[len(grp)] += 1
        sig = tuple(sorted(int(x) for x in grp['_n_slot']))
        if spec.get('ecvma'):
            sig = ('slots', sig, 'same crop code' if grp['_n_crop'].nunique() == 1 else 'crop codes differ')
        pairs[str(sig)] += 1
    rec.update({
        'declared_key_rows': int(len(dk)),
        'declared_key_unique': int(len(dk) - dk.duplicated(KEY).sum()),
        'extra_rows_lost_if_collapsed': int(dk.duplicated(KEY).sum()),
        'collision_groups': int(sum(kinds.values())),
        'collision_groups_by_kind': dict(kinds),
        'extra_rows_by_kind': dict(kind_extra),
        'collision_group_sizes': {str(k): v for k, v in sorted(sizes.items())},
        'collision_native_slot_signatures': dict(pairs.most_common()),
        'declared_rows_by_crop_on_seed': {str(k): int(v) for k, v in
                                          dk.loc[dk['input'] == 'Seed', 'crop'].value_counts().items()},
    })
    agg[f'{country}/{wave}'] = rec
    dk.to_parquet(private / f'native_declared_{country}_{wave}.parquet')
    print(country, wave, json.dumps({k: rec[k] for k in [
        'applied_native_rows', 'native_key_dup_rows', 'hh_slot_dup_rows', 'households', 'distinct_i',
        'written_rows', 'extra_rows_lost_if_collapsed', 'collision_groups',
        'collision_groups_by_kind', 'collision_native_slot_signatures']}))

out_json.write_text(json.dumps(agg, indent=1, sort_keys=True))
