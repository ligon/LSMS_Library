#!/usr/bin/env python
"""STEP 3 verification for GhanaSPS ``livestock`` (GH #729, #736, #140).

Cold build of ``Country('GhanaSPS').livestock()`` under
``LSMS_GRAIN_STRICT=1`` on a private ``LSMS_DATA_DIR``, reporting per wave
the brief's seven checks: rows and the full distinct ``animal`` list;
``(t, i, animal)`` uniqueness, GrainCollapseWarning count, and the sample
join (``v`` is NOT attached to livestock by canonical design -- see below --
so the equivalent check is the share of livestock households present in
``sample()``); HeadCount sum per species and the zero-head share (pre- and
post-hook); HerdValue > 0 share given HeadCount > 0 and given HeadCount == 0
(pre-hook, since the hook removes the zero rows); median HerdValue /
HeadCount for cattle, goats/sheep and chicken; W3 HeadSold non-null share
and sum.  Then the hook's bounded reducer is audited -- the exact duplicate
groups it summed per wave -- plus ``is_this_feature_sane``,
``null_read_reports`` for this table, a parquet read-back, and
``Feature('livestock')`` assembly.

``LSMS_READ_STRICT=1`` is deliberately NOT exported: on this branch it is
fatal inside ``GhanaSPS/sample`` (``Rural`` / ``weight`` / ``panel_weight``
100% null in waves 2-3, a documented producer-side gap), and every
household table re-enters ``sample()``.  The read-strict condition for THIS
table is asserted directly: ``null_read_reports(country='GhanaSPS',
table='livestock') == []``.

Run from the main checkout root with LSMS_COUNTRIES_ROOT and LSMS_DATA_DIR
set; ``__main__``-guarded so it is never imported by a test collector.
"""
import os
import sys
import warnings


def main():
    import numpy as np
    import pandas as pd

    import lsms_library as ll
    from lsms_library.paths import countries_root, data_root
    from lsms_library.local_tools import get_dataframe, all_dfs_from_orgfile
    from lsms_library.diagnostics import is_this_feature_sane
    from lsms_library.null_read_audit import null_read_reports

    assert 'wt-gsps-livestock' in str(countries_root()), countries_root()
    assert os.environ.get('LSMS_GRAIN_STRICT') == '1', 'export LSMS_GRAIN_STRICT=1'
    assert os.environ.get('LSMS_READ_STRICT') in (None, ''), 'unset LSMS_READ_STRICT (see docstring)'
    pd.set_option('display.width', 200)
    print('countries_root:', countries_root())
    print('data_root:', data_root())
    pq_country = data_root('GhanaSPS') / 'var' / 'livestock.parquet'
    print('cold? country parquet exists before build:', pq_country.exists())

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        c = ll.Country('GhanaSPS')
        df = c.livestock()
    grain = [w for w in caught if w.category.__name__ == 'GrainCollapseWarning']
    nullread = [w for w in caught if 'NullRead' in w.category.__name__]
    other = sorted({(w.category.__name__, str(w.message)[:160]) for w in caught
                    if w.category.__name__ != 'GrainCollapseWarning'
                    and 'NullRead' not in w.category.__name__
                    and 'DeprecationWarning' not in w.category.__name__})

    print('\n=== delivered frame ===')
    print('index:', df.index.names, ' columns:', list(df.columns), ' shape:', df.shape)
    print('dtypes:', df.dtypes.to_dict())
    print('GrainCollapseWarning count (whole build):', len(grain))
    for w in grain:
        print('   GRAIN:', str(w.message)[:300])
    print('NullRead warning count (whole build, incl. the sample re-entry):', len(nullread))
    for w in nullread:
        print('   NULLREAD:', str(w.message)[:120])
    reps_ls = null_read_reports(country='GhanaSPS', table='livestock')
    print('null_read_reports(GhanaSPS, livestock):', len(reps_ls),
          '(read-strict condition asserted directly: must be 0)')
    print('other warnings (deduped):')
    for cat, msg in other:
        print('   ', cat, '|', msg)

    # ---- declared vocabulary: the table's Preferred Labels + the 3 pass-throughs
    R = countries_root() / 'GhanaSPS'
    tables = all_dfs_from_orgfile(R / '_' / 'categorical_mapping.org')
    hs = tables['harmonize_species']
    assert not hs['Alternate Spelling'].duplicated().any(), \
        hs.loc[hs['Alternate Spelling'].duplicated(keep=False)]
    vocab = set(hs['Preferred Label'].dropna())
    passthrough = {'50', 'cut', 'dake'}
    print('\nPreferred Labels (%d):' % len(vocab), sorted(vocab))
    print('documented pass-throughs:', sorted(passthrough))

    # ---- raw sources (pre-hook), for the pre-filter checks and the dedup audit
    def raw_w1():
        r = get_dataframe(str(R / '2009-10/Data/S3AI.dta'))
        out = pd.DataFrame({
            'i': r['hhno'].astype(str),
            'animal_raw': r['animal_id'].astype(object),
            'HeadCount': pd.to_numeric(r['s3ai_1'], errors='coerce'),
        })
        cedis = pd.to_numeric(r['s3ai_3i'], errors='coerce')
        pes = pd.to_numeric(r['s3ai_3ii'], errors='coerce')
        out['HerdValue'] = (cedis.fillna(0) + pes.fillna(0) / 100).where(cedis.notna() | (pes.fillna(0) > 0))
        out['HeadSold'] = np.nan
        return out

    def raw_w2():
        r = get_dataframe(str(R / '2013-14/Data/03ai_animalquestions.dta'))
        return pd.DataFrame({'i': r['FPrimary'].astype(str), 'animal_raw': r['animal'].astype(object),
                             'HeadCount': pd.to_numeric(r['quantity'], errors='coerce'),
                             'HerdValue': pd.to_numeric(r['currentvalue'], errors='coerce'),
                             'HeadSold': np.nan})

    def raw_w3():
        a = get_dataframe(str(R / '2017-18/Data/03ai_animalquestions.dta'))
        b = get_dataframe(str(R / '2017-18/Data/03ai_animalquestions_osp.dta'))
        r = pd.concat([a, b], axis=0, sort=False)
        return pd.DataFrame({'i': r['FPrimary'].astype(str), 'animal_raw': r['animal'].astype(object),
                             'HeadCount': pd.to_numeric(r['quantity'], errors='coerce'),
                             'HerdValue': pd.to_numeric(r['currentvalue'], errors='coerce'),
                             'HeadSold': pd.to_numeric(r['quantitysold'], errors='coerce')})

    raw = {'2009-10': raw_w1(), '2013-14': raw_w2(), '2017-18': raw_w3()}
    label = hs.set_index('Alternate Spelling')['Preferred Label'].to_dict()
    for t, r in raw.items():
        r['animal'] = r['animal_raw'].map(lambda x: label.get(x, x))

    # sample membership per wave
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        s = c.sample().reset_index()
    sample_ids = {str(t): set(g['i'].astype(str)) for t, g in s.groupby('t')}

    flat = df.reset_index()
    flat['t'] = flat['t'].astype(str)
    all_ok = True
    dedup_groups = {}
    for t in ['2009-10', '2013-14', '2017-18']:
        w = flat[flat['t'] == t]
        r = raw[t]
        print(f'\n=== {t} ===')
        # 1. rows; distinct animal values (full list)
        animals = sorted(w['animal'].dropna().unique())
        print(f'1. rows: {len(w):,}  households: {w["i"].nunique():,}  distinct animal ({len(animals)}): {animals}')
        vc = w['animal'].value_counts()
        print('   animal counts:', vc.to_dict())
        bad = set(animals) - vocab - passthrough
        print(f'   animal SUBSET of Preferred Labels + pass-throughs: {not bad}  offenders: {sorted(bad)}')
        # 2. uniqueness, grain warnings, sample join (v not attached by design)
        uniq = not w.duplicated(subset=['t', 'i', 'animal']).any()
        null_animal = w['animal'].isna().sum()
        hh = set(w['i'].astype(str))
        in_sample = len(hh & sample_ids.get(t, set()))
        print(f'2. (t, i, animal) unique: {uniq}  null animal in delivered: {null_animal}  '
              f'GrainCollapseWarning (this build): {len(grain)}')
        print(f'   v attached: {"v" in w.columns}  (livestock has NO v by canonical design: index_info omits v; skip_extra)  '
              f'-> households in sample(): {in_sample:,}/{len(hh):,} = {in_sample/len(hh):.1%}')
        # 3. HeadCount sum per species; zero-head share (delivered and pre-hook)
        hc = w.groupby('animal')['HeadCount'].sum(min_count=1).sort_values(ascending=False)
        print('3. HeadCount sum per species:', {k: (None if pd.isna(v) else int(v)) for k, v in hc.items()})
        z_post = (w['HeadCount'] == 0).mean()
        z_pre = (r['HeadCount'] == 0).mean()
        print(f'   HeadCount == 0 share: delivered {z_post:.2%} ({(w["HeadCount"]==0).sum()} rows); '
              f'pre-hook source {z_pre:.2%} ({(r["HeadCount"]==0).sum():,} of {len(r):,} rows)  '
              f'HeadCount null delivered: {w["HeadCount"].isna().sum()}')
        # 4. HerdValue > 0 share given HeadCount > 0 / == 0 (pre-hook, and delivered)
        pos = r['HeadCount'] > 0
        zero = r['HeadCount'] == 0
        v_pos = (r.loc[pos, 'HerdValue'] > 0).mean() if pos.any() else float('nan')
        v_zero = (r.loc[zero, 'HerdValue'] > 0).mean() if zero.any() else float('nan')
        n_zero_v = int((r.loc[zero, 'HerdValue'] > 0).sum())
        print(f'4. PRE-HOOK  HerdValue > 0 | HeadCount > 0: {v_pos:.2%} ({pos.sum():,} rows);  '
              f'| HeadCount == 0: {v_zero if zero.any() else float("nan"):.2%} ({n_zero_v} of {zero.sum():,} zero-head rows carry a value)')
        dpos = w['HeadCount'] > 0
        print(f'   DELIVERED HerdValue > 0 | HeadCount > 0: {(w.loc[dpos, "HerdValue"] > 0).mean():.2%};  '
              f'HerdValue non-null: {w["HerdValue"].notna().mean():.2%};  HerdValue == 0 rows: {(w["HerdValue"]==0).sum()}')
        # 5. median HerdValue / HeadCount
        per = (w['HerdValue'] / w['HeadCount']).replace([np.inf, -np.inf], np.nan)
        def med(labels):
            m = w['animal'].isin(labels) & (w['HeadCount'] > 0)
            return per[m].median()
        mc, mg, mch = med(['Cattle']), med(['Goats', 'Sheep']), med(['Chicken'])
        print(f'5. median HerdValue/HeadCount (GHS): cattle {mc:.1f}  goats/sheep {mg:.1f}  chicken {mch:.1f}  '
              f'-> cattle >> goats >> chicken: {mc > mg > mch}')
        # 6. HeadSold
        if t == '2017-18':
            print(f'6. HeadSold non-null: {w["HeadSold"].notna().sum():,}/{len(w):,} = {w["HeadSold"].notna().mean():.2%}  '
                  f'sum {w["HeadSold"].sum():,.0f}  > 0 rows {(w["HeadSold"] > 0).sum():,}  '
                  f'HeadSold > HeadCount rows {(w["HeadSold"] > w["HeadCount"]).sum()}  max {w["HeadSold"].max():,.0f}')
        else:
            print(f'6. HeadSold: not asked in this wave -> all null ({w["HeadSold"].isna().all()})')
        # dedup audit: the exact groups the hook summed
        rk = r[r['animal'].notna()].copy()
        holds = (rk[['HeadCount', 'HeadSold', 'HerdValue']].fillna(0) > 0).any(axis=1)
        rk = rk[holds]
        dup = rk.duplicated(subset=['i', 'animal'], keep=False)
        groups = rk[dup].groupby(['i', 'animal']).agg(lines=('HeadCount', 'size'),
                                                       HeadCount=('HeadCount', 'sum'),
                                                       HerdValue=('HerdValue', 'sum'))
        dedup_groups[t] = groups
        print(f'   dedup audit: source rows after null-animal drop + keep-rule {len(rk):,}; '
              f'duplicate (i, animal) groups summed by the hook: {len(groups)} ({dup.sum()} lines) -> delivered {len(w):,} '
              f'(expected {len(rk) - dup.sum() + len(groups):,}: {len(rk) - dup.sum() + len(groups) == len(w)})')
        if len(groups):
            print(groups.to_string())
        print(f'   null-animal source rows dropped: {r["animal"].isna().sum()}; '
              f'keep-rule dropped: {(~holds).sum():,}')
        # household accounting: who vanished, and why
        src_hh = set(r['i'])
        lost = sorted(src_hh - hh)
        only_null = [h for h in lost if r.loc[r['i'] == h, 'animal'].isna().all()]
        only_filler = [h for h in lost if h not in only_null]
        print(f'   households: source {len(src_hh):,} -> delivered {len(hh):,}; lost {len(lost)}: '
              f'{len(only_null)} had ONLY null-animal rows {only_null}, '
              f'{len(only_filler)} had nothing positive (all filler) {only_filler if len(only_filler) <= 20 else str(len(only_filler)) + " households"}')
        all_ok &= (not bad) and uniq and (null_animal == 0) and (in_sample == len(hh)) and (mc > mg > mch)
        all_ok &= (len(rk) - dup.sum() + len(groups) == len(w))

    print('\n=== is_this_feature_sane ===')
    report = is_this_feature_sane(df, country='GhanaSPS', feature='livestock')
    report.summarize()
    print('report.ok:', report.ok)
    all_ok &= bool(report.ok)
    all_ok &= (len(grain) == 0) and (len(reps_ls) == 0)

    print('\n=== parquet read-back (pre-finalize) ===')
    for t in ['2009-10', '2013-14', '2017-18']:
        p = data_root('GhanaSPS') / t / '_' / 'livestock.parquet'
        if p.exists():
            wp = pd.read_parquet(p)
            print(f'{t} wave parquet: shape {wp.shape} cols {list(wp.columns)} index {wp.index.names} '
                  f'null animal {pd.isna(wp.index.get_level_values("animal")).sum()} unique {wp.index.is_unique} '
                  f'animal subset {set(wp.index.get_level_values("animal").dropna()) <= (vocab | passthrough)}')
    if pq_country.exists():
        cp = pd.read_parquet(pq_country)
        print(f'country parquet: shape {cp.shape} index {cp.index.names} cols {list(cp.columns)}')

    print('\n=== Feature() assembly (GhanaSPS only) ===')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        f = ll.Feature('livestock')(['GhanaSPS'])
    print('Feature index:', f.index.names, ' shape:', f.shape, ' columns:', list(f.columns))

    print('\n=== currency ===')
    from lsms_library.currency import currency_for, _monetary_columns
    print('currency_for(GhanaSPS, wave):', {t: currency_for('GhanaSPS', t) for t in ['2009-10', '2013-14', '2017-18']})
    print("_monetary_columns('livestock', 'GhanaSPS'):", sorted(_monetary_columns('livestock', 'GhanaSPS')))
    all_ok &= 'HerdValue' in _monetary_columns('livestock', 'GhanaSPS')

    print('\nALL STEP-3 GATES PASS:', all_ok)
    return 0 if all_ok else 1


if __name__ == '__main__':
    sys.exit(main())
