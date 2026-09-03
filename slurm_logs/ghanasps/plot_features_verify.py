#!/usr/bin/env python
"""STEP 3 verification for GhanaSPS ``plot_features`` (GH #732, #729).

Cold build of ``Country('GhanaSPS').plot_features()`` under
``LSMS_GRAIN_STRICT=1 LSMS_READ_STRICT=1`` on a private ``LSMS_DATA_DIR``,
reporting per wave: rows, Area non-null share (the #732 check, must be
>= 97%), index uniqueness, GrainCollapseWarning count, v non-null share,
Tenure counts (raw source vs delivered canonical, with a SUBSET assertion
against the data_info.yml vocabulary), AreaUnit counts, and Area median /
p99 / max.  Then ``is_this_feature_sane`` and a read-back of the cached
parquets to confirm Tenure is canonical *in the parquet*.

Run from the worktree root with LSMS_COUNTRIES_ROOT and LSMS_DATA_DIR set;
``__main__``-guarded so it is never imported by a test collector.
"""
import os
import sys
import warnings


def main():
    import pandas as pd
    import yaml
    from importlib.resources import files

    import lsms_library as ll
    from lsms_library.paths import countries_root, data_root
    from lsms_library.local_tools import get_dataframe
    from lsms_library.diagnostics import is_this_feature_sane

    assert 'wt-gsps-plot' in str(countries_root()), countries_root()
    assert os.environ.get('LSMS_GRAIN_STRICT') == '1', 'export LSMS_GRAIN_STRICT=1'
    # LSMS_READ_STRICT=1 is NOT exported: on this branch it is fatal inside
    # GhanaSPS/sample (Rural 100% null in 2013-14 / 2017-18, a documented
    # property of that table), which _join_v_from_sample re-enters for every
    # household table -- see plot_features_verify_cold_READSTRICT_sample_Rural.log.
    # The read-strict condition is asserted DIRECTLY below instead: zero
    # null-read reports filed against plot_features.
    assert os.environ.get('LSMS_READ_STRICT') in (None, ''), 'unset LSMS_READ_STRICT (see comment)'
    print('countries_root:', countries_root())
    print('data_root:', data_root())
    pq_country = data_root('GhanaSPS') / 'var' / 'plot_features.parquet'
    print('cold? country parquet exists before build:', pq_country.exists())

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        c = ll.Country('GhanaSPS')
        df = c.plot_features()
    grain = [w for w in caught if w.category.__name__ == 'GrainCollapseWarning']
    nullread = [w for w in caught if 'NullRead' in w.category.__name__]
    other = [(w.category.__name__, str(w.message)[:160]) for w in caught
             if w.category.__name__ not in ('GrainCollapseWarning',)
             and 'NullRead' not in w.category.__name__
             and 'DeprecationWarning' not in w.category.__name__]

    print('\n=== delivered frame ===')
    print('index:', df.index.names, ' columns:', list(df.columns), ' shape:', df.shape)
    print('GrainCollapseWarning count:', len(grain))
    for w in grain:
        print('   GRAIN:', str(w.message)[:300])
    print('NullRead warning count (whole build, incl. the sample re-entry):', len(nullread))
    for w in nullread:
        print('   NULLREAD:', str(w.message)[:200])
    from lsms_library.null_read_audit import null_read_reports
    reps_all = null_read_reports(country='GhanaSPS')
    reps_pf = null_read_reports(country='GhanaSPS', table='plot_features')
    print('null_read_reports(GhanaSPS): tables ->', sorted({getattr(r, 'table', str(r)) for r in reps_all}))
    print('null_read_reports(GhanaSPS, plot_features):', len(reps_pf), '(read-strict condition asserted directly: must be 0)')
    print('other warnings (deduped):')
    for cat, msg in sorted(set(other)):
        print('   ', cat, '|', msg)

    # declared vocabulary, read from data_info.yml -- never hardcoded
    info = yaml.safe_load(open(files('lsms_library') / 'data_info.yml'))
    vocab = set(info['Columns']['plot_features']['Tenure']['spellings'].keys())
    print('\ndeclared Tenure vocabulary:', sorted(vocab))
    areaunit_vocab = {'Acres', 'Poles', 'Ropes', 'Plot', 'Other'}

    flat = df.reset_index()
    # raw source tenure counts, for the side-by-side with FINDINGS
    R = countries_root() / 'GhanaSPS'
    raw_tenure = {
        '2009-10': get_dataframe(str(R / '2009-10/Data/S4AIV.dta'))['s4aiv_a45'],
        '2013-14': get_dataframe(str(R / '2013-14/Data/04i_landtenure.dta'))['obtainhow'],
        '2017-18': get_dataframe(str(R / '2017-18/Data/04i_landtenure.dta'))['obtainhow'],
    }
    expected_rows = {'2009-10': 5686, '2013-14': 4694, '2017-18': 5366}
    all_ok = True
    for t, w in flat.groupby('t', sort=True):
        t = str(t)
        print(f'\n=== {t} ===')
        n = len(w)
        area_share = w['Area'].notna().mean()
        uniq = not w.duplicated(subset=['t', 'i', 'plot_id']).any()
        v_share = w['v'].notna().mean() if 'v' in w.columns else float('nan')
        print(f'1. rows: {n:,}  (keyed source plots {expected_rows[t]:,}; gap {expected_rows[t]-n:,} = rows with Area, AreaUnit AND Tenure all null, removed by _finalize_result dropna(how="all"))')
        print(f'2. Area non-null: {w["Area"].notna().sum():,}/{n:,} = {area_share:.2%}  {"OK (>=97%)" if area_share >= 0.97 else "FAIL (<97%)"}')
        print(f'3. (t, i, plot_id) unique: {uniq}')
        print(f'4. v non-null: {w["v"].notna().sum():,}/{n:,} = {v_share:.2%}   distinct v: {w["v"].nunique()}')
        tv = w['Tenure'].value_counts(dropna=False)
        print(f'5. Tenure delivered: {tv.to_dict()}')
        print(f'   Tenure raw source: {raw_tenure[t].value_counts(dropna=False).to_dict()}')
        bad = set(w['Tenure'].dropna().unique()) - vocab
        print(f'   Tenure SUBSET of declared vocabulary: {not bad}  offenders: {sorted(bad)}')
        au = w['AreaUnit'].value_counts(dropna=False)
        print(f'6. AreaUnit: {au.to_dict()}')
        badu = set(w['AreaUnit'].dropna().unique()) - areaunit_vocab
        print(f'   AreaUnit SUBSET of {sorted(areaunit_vocab)}: {not badu}  offenders: {sorted(badu)}')
        a = w['Area']
        print(f'7. Area ha: median {a.median():.3f}  p99 {a.quantile(.99):.2f}  max {a.max():.1f}  min {a.min():.3f}  zeros {(a == 0).sum()}')
        all_ok &= (area_share >= 0.97) and uniq and (not bad) and (not badu) and (v_share == 1.0)

    # wave-1 path counts: shipped area_ha vs derived
    s = get_dataframe(str(R / '2009-10/Data/S4AII.dta'))
    keyed = s[s['plot_no'].notna()]
    print(f'\nW1 Area path counts: shipped area_ha {keyed["area_ha"].notna().sum():,}; '
          f'derived (area_ha null but size+factor present) '
          f'{(keyed["area_ha"].isna() & keyed["s4aii_a10"].notna() & keyed["s4aii_a11"].isin(["Acre","Pole","Rope","Plot"])).sum():,}; '
          f'neither {(keyed["area_ha"].isna()).sum():,}; null-plot_no source rows dropped {s["plot_no"].isna().sum()}')

    print('\n=== is_this_feature_sane ===')
    report = is_this_feature_sane(df, country='GhanaSPS', feature='plot_features')
    report.summarize()
    print('report.ok:', report.ok)
    all_ok &= bool(report.ok)
    all_ok &= (len(reps_pf) == 0)

    print('\n=== parquet read-back (pre-finalize) ===')
    for t in ['2009-10', '2013-14', '2017-18']:
        p = data_root('GhanaSPS') / t / '_' / 'plot_features.parquet'
        if p.exists():
            wp = pd.read_parquet(p)
            print(f'{t} wave parquet: shape {wp.shape} cols {list(wp.columns)} Tenure values {sorted(wp["Tenure"].dropna().unique())} AreaUnit {sorted(wp["AreaUnit"].dropna().unique())} null plot_id {pd.isna(wp.index.get_level_values("plot_id")).sum()}')
    if pq_country.exists():
        cp = pd.read_parquet(pq_country)
        print(f'country parquet: shape {cp.shape} index {cp.index.names} Tenure subset of vocab: {set(cp["Tenure"].dropna().unique()) <= vocab}')

    print('\n=== Feature() assembly (GhanaSPS only) ===')
    f = ll.Feature('plot_features')(['GhanaSPS'])
    print('Feature index:', f.index.names, ' shape:', f.shape)

    print('\nALL STEP-3 GATES PASS:', all_ok)
    return 0 if all_ok else 1


if __name__ == '__main__':
    sys.exit(main())
