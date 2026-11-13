"""GH #1123: compare base vs candidate served plot_inputs, and the candidate
against every native source line.

Usage: python compare.py <private_base_dir> <private_cand_dir> <out.json> <country> [...]

Inputs are the private parquets written by build_country.py
(``{label}_{country}_plot_inputs.parquet`` and the transform frames) and by
diagnose_source.py (``native_declared_{country}_{wave}.parquet``).  Only
aggregate counts are written to <out.json>.

Checks, per country:
  1. candidate index unique; served rows == applied native lines minus the
     all-NA hollow lines that ``dropna(how='all')`` removes;
  2. projecting the candidate's new crop labels back onto the old catch-all
     (``Autre céréale`` / ``Autre tubercule`` -> ``Autre crop``) reproduces the
     base key set exactly; every base row whose projected key is unique is
     byte-equal (all columns, dtypes) to its candidate row; every base row in a
     formerly colliding group equals one of that group's candidate rows;
  3. every non-hollow native line is served by the candidate with its exact
     payload (joined on the declared key, with the written->served ``u``
     relabelling learned from uniquely-joined lines and checked to be a
     function);
  4. nitrogen_kg is frame-identical; seed_kg changes only for households that
     carried a collision group.
"""
import json
import os
import sys
from pathlib import Path

import pandas as pd

pbase, pcand, out_json = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
countries = sys.argv[4:]
KEY = ['t', 'i', 'input', 'crop', 'u']
PAYLOAD = ['Quantity', 'Purchased', 'Quantity_purchased']
NEW = {'Autre céréale': 'Autre crop', 'Autre tubercule': 'Autre crop'}
# The served i has been through id_walk; apply the same committed crosswalk
# to the wave-script i (only Niger 2014-15 is non-identity for these waves).
ROOT = Path(os.environ['LSMS_COUNTRIES_ROOT'])
UPDATED = {c: json.loads((ROOT / c / '_' / 'updated_ids.json').read_text())
           for c in ('Niger', 'Senegal')}
WAVES = {'Niger': ['2011-12', '2014-15', '2018-19', '2021-22'],
         'Senegal': ['2018-19'], 'CotedIvoire': ['2018-19']}


def load(d, label, country, what='plot_inputs'):
    return pd.read_parquet(d / f'{label}_{country}_{what}.parquet')


res = {}
for country in countries:
    B = load(pbase, 'base-cold', country)
    C = load(pcand, 'cand-cold', country)
    r = {'rows_base': len(B), 'rows_cand': len(C), 'cand_index_unique': bool(C.index.is_unique),
         'columns_equal': list(B.columns) == list(C.columns),
         'index_names_equal': list(B.index.names) == list(C.index.names),
         'dtypes_equal': bool((B.dtypes == C.dtypes).all())}
    names = list(C.index.names)
    Bf, Cf = B.reset_index(), C.reset_index()
    assert not Bf['crop'].isin(list(NEW)).any(), 'new labels already in base'
    # -- 2. projection back onto the base vocabulary
    Cp = Cf.copy()
    Cp['crop'] = Cp['crop'].replace(NEW)
    r['cand_rows_relabelled'] = {str(k): int(v) for k, v in
                                 Cf.loc[Cf['crop'].isin(list(NEW)), 't'].value_counts().sort_index().items()}
    kb = Bf.set_index(names).index
    kc = Cp.set_index(names).index
    r['projected_key_sets_equal'] = bool(set(kb) == set(kc))
    dup = kc.duplicated(keep=False)
    r['projected_collision_groups'] = int(pd.Index(kc[dup]).nunique())
    r['projected_extra_rows'] = int(kc.duplicated().sum())
    single = Cp.loc[~dup].set_index(names).sort_index()
    Bsingle = Bf.set_index(names).loc[single.index]
    pd.testing.assert_frame_equal(Bsingle, single, check_exact=True)
    r['base_rows_byte_equal_on_unique_keys'] = int(len(single))
    grp = Cp.loc[dup]
    matched = 0
    for key, g in grp.groupby(names, sort=False):
        brow = Bf.set_index(names).loc[[key]]
        assert len(brow) == 1
        gv = g.drop(columns=names).reset_index(drop=True)
        bv = brow.reset_index(drop=True)
        hit = any(gv.iloc[[j]].reset_index(drop=True).equals(bv) for j in range(len(gv)))
        assert hit, key[:1]
        matched += 1
    r['base_rows_in_collision_groups_matching_a_cand_row'] = matched
    # -- 1/3. native oracle
    native_total = hollow_total = 0
    per_wave = {}
    for w in WAVES[country]:
        n = pd.read_parquet(pcand / f'native_declared_{country}_{w}.parquet')
        walk = UPDATED.get(country, {}).get(w, {})
        n['i'] = n['i'].map(lambda x: walk.get(x, x))
        hollow = n[PAYLOAD].isna().all(axis=1)
        native_total += len(n)
        hollow_total += int(hollow.sum())
        served = Cf[Cf['t'] == w]
        nn = n.loc[~hollow].copy()
        # written u -> served u, learned on (t,i,input,crop)-unique lines
        k4 = ['t', 'i', 'input', 'crop']
        nu = nn[~nn.duplicated(k4, keep=False)]
        su = served[~served.duplicated(k4, keep=False)]
        m = nu.merge(su[k4 + ['u']], on=k4, suffixes=('_w', '_s'))
        umap = m.groupby('u_w')['u_s'].agg(lambda s: sorted(set(s)))
        assert all(len(v) == 1 for v in umap), 'u relabel is not a function'
        umap = {k: v[0] for k, v in umap.items()}
        nn['u'] = nn['u'].map(lambda x: umap.get(x, x))
        j = nn.merge(served, on=KEY, how='left', suffixes=('_n', ''), indicator=True)
        missing = int((j['_merge'] != 'both').sum())
        assert len(j) == len(nn), 'native line joined more than one served row'
        same = pd.Series(True, index=j.index)
        for c in PAYLOAD:
            a = pd.Series(j[c + '_n'].array, dtype=served[c].dtype, index=j.index)
            b = j[c]
            same &= (a == b).fillna(False).astype(bool) | (a.isna() & b.isna())
        eq = int((~same).sum())
        per_wave[w] = {'native': int(len(n)), 'hollow': int(hollow.sum()),
                       'served': int(len(served)), 'native_lines_not_served': missing,
                       'native_lines_payload_differs': eq, 'u_relabels': len([k for k, v in umap.items() if k != v])}
    r['native_by_wave'] = per_wave
    r['native_total'] = native_total
    r['hollow_total'] = hollow_total
    r['served_equals_native_minus_hollow'] = bool(len(C) == native_total - hollow_total)
    # -- 4. transforms
    for fn in ('nitrogen_kg', 'seed_kg'):
        tb = load(pbase, 'base-cold', country, fn)
        tc = load(pcand, 'cand-cold', country, fn)
        col = tb.columns[0]
        both = tb.join(tc, how='outer', lsuffix='_b', rsuffix='_c')
        diff = both[(both[col + '_b'] != both[col + '_c'])
                    & ~(both[col + '_b'].isna() & both[col + '_c'].isna())]
        # A household's rows are summed in index order; a relabelled crop can
        # re-sort its rows, which moves a float sum by an ULP.  Separate those
        # from real changes.
        delta = (diff[col + '_c'] - diff[col + '_b']).abs()
        scale = diff[[col + '_b', col + '_c']].abs().max(axis=1)
        ulp = delta <= 1e-12 * scale
        real = diff[~ulp]
        r[fn] = {'frame_identical': bool(tb.equals(tc)), 'rows_base': len(tb), 'rows_cand': len(tc),
                 'same_index': bool(tb.index.equals(tc.index)),
                 'total_base': float(tb[col].sum()), 'total_cand': float(tc[col].sum()),
                 'households_changed': int(len(real)),
                 'households_float_reorder_only': int(ulp.sum()),
                 'max_abs_float_reorder_delta': float(delta[ulp].max()) if ulp.any() else 0.0}
        coll_hh = set(map(tuple, Cp.loc[dup, ['t', 'i']].drop_duplicates().to_numpy()))
        idx = real.reset_index()[['t', 'i']]
        r[fn]['changed_households_all_in_collision_groups'] = bool(
            all(tuple(x) in coll_hh for x in idx.to_numpy()))
        r[fn]['collision_households'] = len(coll_hh)
    res[country] = r
    print(country, json.dumps(r, default=str))

out_json.write_text(json.dumps(res, indent=1, sort_keys=True, default=str))
