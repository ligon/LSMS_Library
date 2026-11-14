"""Compare cold `sample` frames: base (e5fdd2fa8), prev (df1df60ed), cand (this head).

usage: python -I compare_sample.py FRAMES_DIR OUT_JSON
FRAMES_DIR/{base,prev,cand}/<C>.parquet; cand may also hold <C>_cluster_features.parquet.
"""
import json, sys
from pathlib import Path
import pandas as pd

D = Path(sys.argv[1])
NEW = '2021-22'
KEY = '{}::sample::region-milieu-stratum'
out = {}


def load(side, c):
    f = D / side / f'{c}.parquet'
    if not f.exists():
        return None
    df = pd.read_parquet(f)
    df['t'] = df['t'].astype(str)
    return df.sort_values(['t', 'i']).reset_index(drop=True)


def frame_equal(a, b, cols):
    a = a[['i', 't'] + cols].reset_index(drop=True)
    b = b[['i', 't'] + cols].reset_index(drop=True)
    try:
        pd.testing.assert_frame_equal(a, b, check_dtype=False)
        return True, None
    except AssertionError as exc:
        return False, str(exc)[:600]


for c in ('Togo', 'Benin', 'CotedIvoire'):
    base, prev, cand = load('base', c), load('prev', c), load('cand', c)
    rec = {'columns': {s: (list(f.columns) if f is not None else None)
                       for s, f in (('base', base), ('prev', prev), ('cand', cand))}}
    rec['rows_by_wave'] = {s: (f.groupby('t').size().to_dict() if f is not None else None)
                           for s, f in (('base', base), ('prev', prev), ('cand', cand))}
    waves = sorted(set(cand['t']))
    rec['waves'] = {}
    for w in waves:
        cw = cand[cand['t'] == w].reset_index(drop=True)
        pw = prev[prev['t'] == w].reset_index(drop=True)
        bw = base[base['t'] == w].reset_index(drop=True)
        r = {'rows': {'base': len(bw), 'prev': len(pw), 'cand': len(cw)}}
        r['i_equal_prev'] = set(cw['i']) == set(pw['i'])
        r['i_equal_base'] = set(cw['i']) == set(bw['i']) if len(bw) else None
        s = cw['strata']
        r['strata'] = {'n_labels': int(s.nunique()), 'na': int(s.isna().sum()),
                       'labels': {str(k): int(v) for k, v in s.value_counts().items()}}
        r['strata_prev'] = {'n_labels': int(pw['strata'].nunique()), 'na': int(pw['strata'].isna().sum())}
        d = cw['Derivation'].astype('string') if 'Derivation' in cw else pd.Series(pd.NA, index=cw.index, dtype='string')
        r['derivation'] = {'stamped': int((d == KEY.format(c)).sum()),
                           'na': int(d.isna().sum()),
                           'stamped_equals_strata_notna': bool(((d == KEY.format(c)).fillna(False) == s.notna()).all())}
        if w == NEW:
            other = [x for x in cw.columns if x not in ('i', 't', 'strata', 'Derivation')]
            r['cand_vs_prev_excluding_strata_derivation'] = frame_equal(cw, pw, other)
            r['columns_other'] = other
            r['prev_has_derivation'] = 'Derivation' in pw.columns
        else:
            cols = [x for x in bw.columns if x not in ('i', 't')]
            r['cand_vs_base_all_base_columns'] = frame_equal(cw, bw, cols)
            r['cand_vs_prev_all_prev_columns'] = frame_equal(cw, pw, [x for x in pw.columns if x not in ('i', 't')])
            r['extra_columns_vs_base'] = [x for x in cw.columns if x not in bw.columns]
        # clusters holding two strata
        per = cw.groupby('v')['strata'].nunique()
        r['grappes_two_strata'] = {str(k): int(v) for k, v in per[per > 1].items()}
        rec['waves'][w] = r
    # Labels shared across waves (cand)
    sets = {w: set(cand.loc[cand['t'] == w, 'strata'].dropna()) for w in waves}
    ehcvm = [w for w in waves if w in ('2018', '2018-19', NEW)]
    if len(ehcvm) == 2:
        a, b = ehcvm
        rec['shared_labels'] = sorted(sets[a] & sets[b])
        rec['only_' + a] = sorted(sets[a] - sets[b])
        rec['only_' + b] = sorted(sets[b] - sets[a])
    # Region part vs cluster_features.Region (cand)
    cf_path = D / 'cand' / f'{c}_cluster_features.parquet'
    if cf_path.exists():
        cf = pd.read_parquet(cf_path)
        cf['t'] = cf['t'].astype(str)
        j = cand[cand['t'].isin(ehcvm)].drop_duplicates(['t', 'v']).merge(
            cf[['t', 'v', 'Region', 'Rural']], on=['t', 'v'], how='left', suffixes=('', '_cf'))
        region = j['strata'].str.rsplit(' ', n=1).str[0]
        milieu = j['strata'].str.rsplit(' ', n=1).str[1]
        rec['region_part_is_cluster_features_region'] = {
            w: bool((region[j['t'] == w] == j.loc[j['t'] == w, 'Region'].astype('string')).all()) for w in ehcvm}
        rec['milieu_part_matches_Rural'] = {
            w: bool((milieu[j['t'] == w].map({'Urbain': 'Urban', 'Rural': 'Rural'})
                     == j.loc[j['t'] == w, 'Rural'].astype('string')).all()) for w in ehcvm}
    out[c] = rec
json.dump(out, open(sys.argv[2], 'w'), indent=1, ensure_ascii=False, default=str)
for c, rec in out.items():
    print(c, {w: (r['rows'], r['strata']['n_labels'], r.get('cand_vs_prev_excluding_strata_derivation', r.get('cand_vs_base_all_base_columns'))[0])
              for w, r in rec['waves'].items()})
