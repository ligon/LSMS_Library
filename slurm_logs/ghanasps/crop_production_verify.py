#!/usr/bin/env python
"""STEP 3 verification for GhanaSPS ``crop_production`` (GH #729, #140).

Cold build of ``Country('GhanaSPS').crop_production()`` under
``LSMS_GRAIN_STRICT=1`` on a private ``LSMS_DATA_DIR``, reporting per wave:
rows / distinct j / distinct u / distinct season; index uniqueness, the
GrainCollapseWarning count and the v non-null share; the join rate of
(t, i, plot_id) into plot_features(); the 2009-10 per-season row counts
against the raw S4AV1 / S4AV2 fill (the discriminating check for the S4AV2
label error: MINOR rows must be the SMALLER set and come from S4AV2);
Quantity / Quantity_sold / Value_sold non-null shares (2013-14 must be 0%
for the sales columns); the top-10 crops; j and u SUBSET assertions against
the harmonize_crop / harmonizedunit vocabularies; the 2017-18 dfs site-4
check recomputed from the source files; ``null_read_reports`` for the table
(the read-strict condition asserted directly, because LSMS_READ_STRICT=1 is
fatal inside GhanaSPS/sample -- CONTENTS.org); ``is_this_feature_sane``; a
read-back of the wave parquets; and a Feature() assembly.

Run from the worktree root with LSMS_COUNTRIES_ROOT and LSMS_DATA_DIR set;
``__main__``-guarded so it is never imported by a test collector.
"""
import os
import sys
import warnings


def main():
    import pandas as pd

    import lsms_library as ll
    from lsms_library.paths import countries_root, data_root
    from lsms_library.local_tools import get_dataframe, all_dfs_from_orgfile
    from lsms_library.diagnostics import is_this_feature_sane
    from lsms_library.null_read_audit import null_read_reports

    assert 'wt-gsps-crop' in str(countries_root()), countries_root()
    assert os.environ.get('LSMS_GRAIN_STRICT') == '1', 'export LSMS_GRAIN_STRICT=1'
    assert os.environ.get('LSMS_READ_STRICT') in (None, ''), 'unset LSMS_READ_STRICT (fatal inside GhanaSPS/sample; asserted directly below)'
    print('countries_root:', countries_root())
    print('data_root:', data_root())
    R = countries_root() / 'GhanaSPS'
    pq_country = data_root('GhanaSPS') / 'var' / 'crop_production.parquet'
    pq_w1 = data_root('GhanaSPS') / '2009-10' / '_' / 'crop_production.parquet'
    print('cold? country parquet exists before build:', pq_country.exists(), '; W1 wave parquet exists before build:', pq_w1.exists())

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        c = ll.Country('GhanaSPS')
        df = c.crop_production()
    grain = [w for w in caught if w.category.__name__ == 'GrainCollapseWarning']
    other = sorted({(w.category.__name__, str(w.message)[:160]) for w in caught
                    if w.category.__name__ not in ('GrainCollapseWarning', 'DeprecationWarning')
                    and 'NullRead' not in w.category.__name__})

    print('\n=== delivered frame ===')
    print('index:', df.index.names, ' columns:', list(df.columns), ' shape:', df.shape)
    print('GrainCollapseWarning count:', len(grain))
    for w in grain:
        print('   GRAIN:', str(w.message)[:300])
    reps = null_read_reports(country='GhanaSPS', table='crop_production')
    print('null_read_reports(GhanaSPS, crop_production):', len(reps), '(read-strict condition asserted directly: must be 0)')
    for r in reps:
        print('   NULLREAD:', r)
    print('other warnings (deduped):')
    for cat, msg in other:
        print('   ', cat, '|', msg)

    tables = all_dfs_from_orgfile(R / '_' / 'categorical_mapping.org')
    crop_vocab = set(tables['harmonize_crop']['Preferred Label'].dropna().astype(str).str.strip())
    sys.path.insert(0, str(R / '_'))
    import ghanasps
    # 'Kg': the framework's global categorical_mapping/u.org folds every kg
    # variant onto 'Kg' at API time for ANY table with a u level (the parquet
    # carries 'Kilogram', food_acquired is served the same way)
    unit_vocab = set(ghanasps.harvest_unit_vocabulary()) | {ghanasps._CROP_UNIT_UNKNOWN, 'Kg'}
    residual_codes = {str(k) for k in (1, 46, 50, 51, 53, 57, 58, 61, 80, 94)}

    flat = df.reset_index()
    flat['t'] = flat['t'].astype(str)
    pf = c.plot_features().reset_index()
    pf['t'] = pf['t'].astype(str)
    pf_keys = set(zip(pf['t'], pf['i'].astype(str), pf['plot_id'].astype(str)))

    # raw 2009-10 fill, for check 4
    v1 = get_dataframe(str(R / '2009-10/Data/S4AV1.dta'), convert_categoricals=False)
    v2 = get_dataframe(str(R / '2009-10/Data/S4AV2.dta'), convert_categoricals=False)
    raw_major_crop1 = int(v1['s4v_a81i'].notna().sum())
    raw_minor_crop1 = int(v2['s4v_a122i'].notna().sum())

    all_ok = True
    for t, w in flat.groupby('t', sort=True):
        print(f'\n=== {t} ===')
        n = len(w)
        seasons = sorted(w['season'].astype(str).unique())
        print(f'1. rows {n:,}; distinct j {w["j"].nunique()}; distinct u {w["u"].nunique()}; distinct season {seasons}')
        uniq = not w.duplicated(subset=['t', 'i', 'plot_id', 'j', 'u', 'season']).any()
        v_share = w['v'].notna().mean() if 'v' in w.columns else float('nan')
        print(f'2. (t, i, plot_id, j, u, season) unique: {uniq}; v non-null {w["v"].notna().sum():,}/{n:,} = {v_share:.2%}; distinct v {w["v"].nunique()}')
        keys = set(zip(w['t'], w['i'].astype(str), w['plot_id'].astype(str)))
        joined = len(keys & pf_keys)
        print(f'3. (t, i, plot_id) in plot_features(): {joined:,}/{len(keys):,} plots = {joined/len(keys):.2%}')
        if t == '2009-10':
            per = w.groupby('season').size().to_dict()
            print(f'4. rows per season {per}; raw crop-1 quantity fill: S4AV1 (major) {raw_major_crop1:,} vs S4AV2 (minor) {raw_minor_crop1:,}; '
                  f'minor is the smaller set: {per.get("minor", 0) < per.get("major", 0)}')
            all_ok &= seasons == ['major', 'minor'] and per.get('minor', 0) < per.get('major', 0)
        else:
            print(f'4. (single-recall wave) season constant: {seasons}')
            all_ok &= seasons == ['annual']
        qs = w['Quantity'].notna().mean(); qss = w['Quantity_sold'].notna().mean(); vss = w['Value_sold'].notna().mean()
        print(f'5. Quantity non-null {qs:.2%}; Quantity_sold non-null {qss:.2%}; Value_sold non-null {vss:.2%}'
              + ('  (2013-14 must be 0% / 0% by design)' if t == '2013-14' else ''))
        if t == '2013-14':
            all_ok &= (qss == 0.0) and (vss == 0.0)
        if t == '2009-10':
            all_ok &= (qss == 0.0)
        top = w['j'].value_counts().head(10)
        print('6. top-10 crops:', top.to_dict())
        bad_j = set(w['j'].astype(str).unique()) - crop_vocab
        # 2009-10 part-qualified products are 'Crop (part)' or 'Cocoyam Leaves'
        bad_j = {j for j in bad_j if not (t == '2009-10' and (j == 'Cocoyam Leaves' or (j.endswith(')') and j.rsplit(' (', 1)[0] in crop_vocab)))}
        bad_u = set(w['u'].astype(str).unique()) - unit_vocab - (residual_codes if t == '2009-10' else set())
        print(f'   j SUBSET of harmonize_crop Preferred Labels (+ 2009-10 part-qualified products): {not bad_j}  offenders: {sorted(bad_j)}')
        print(f'   u SUBSET of harmonizedunit vocabulary (+ Unknown, + 2009-10 residual codes): {not bad_u}  offenders: {sorted(bad_u)}')
        print(f'   u values: {sorted(w["u"].astype(str).unique())}')
        if t == '2009-10':
            qual = w[~w['j'].astype(str).isin(crop_vocab)]['j'].value_counts()
            print(f'   part-qualified products (2009-10 only): {qual.to_dict()}')
        hm = w['harvest_month'].dropna().astype(str)
        print(f'   harvest_month non-null {w["harvest_month"].notna().mean():.2%}; sample {hm.head(5).tolist()}; max tokens {hm.str.split().map(len).max() if len(hm) else 0}')
        all_ok &= uniq and (not bad_j) and (not bad_u) and (v_share == 1.0) and (joined == len(keys))

    # 2017-18 dfs site-4 check, recomputed from the source files
    h3 = get_dataframe(str(R / '2017-18/Data/04n_harvestquestions.dta'))
    s3 = get_dataframe(str(R / '2017-18/Data/04o_cropsalesstoresquestions.dta'))
    for nm, d in (('04n_harvestquestions', h3), ('04o_cropsalesstoresquestions', s3)):
        k = d[['FPrimary', 'plotid', 'cropname']]
        print(f'\n2017-18 {nm}: rows {len(d):,}; duplicated (FPrimary, plotid, cropname) INCLUDING blank-crop rows: {int(k.duplicated().sum())} (must be 0)')
        all_ok &= int(k.duplicated().sum()) == 0

    print('\nmeasured drops (2013-14 / 2017-18 no-harvest rows):')
    h2 = get_dataframe(str(R / '2013-14/Data/04n_harvestquestions.dta'))
    print(f'   2013-14 04n rows {len(h2):,}; quantity AND unit null: {int((h2["harvestquantity"].isna() & h2["harvestunit"].isna()).sum())}')
    print(f'   2017-18 04n rows {len(h3):,}; blank crop: {int((h3["cropname"].astype(str).str.strip() == "").sum())}; '
          f'quantity AND unit null among crop rows: {int((h3["harvestquantity"].isna() & h3["harvestunit"].isna() & (h3["cropname"].astype(str).str.strip() != "")).sum())}')

    print('\n=== is_this_feature_sane ===')
    report = is_this_feature_sane(df, country='GhanaSPS', feature='crop_production')
    report.summarize()
    print('report.ok:', report.ok)
    all_ok &= bool(report.ok)
    all_ok &= (len(reps) == 0) and (len(grain) == 0)

    print('\n=== parquet read-back (pre-finalize) ===')
    for t in ['2009-10', '2013-14', '2017-18']:
        p = data_root('GhanaSPS') / t / '_' / 'crop_production.parquet'
        if p.exists():
            wp = pd.read_parquet(p)
            print(f'{t} wave parquet: shape {wp.shape} index {wp.index.names} cols {list(wp.columns)} seasons {sorted(set(wp.index.get_level_values("season")))}')
        else:
            print(f'{t} wave parquet: MISSING at {p}')
    print('W1 wave parquet under LSMS_DATA_DIR (not in-tree):', pq_w1.exists(), '; in-tree copy absent:', not (R / '2009-10' / '_' / 'crop_production.parquet').exists())
    all_ok &= pq_w1.exists() and not (R / '2009-10' / '_' / 'crop_production.parquet').exists()
    if pq_country.exists():
        cp = pd.read_parquet(pq_country)
        print(f'country parquet: shape {cp.shape} index {cp.index.names}')

    print('\n=== Feature() assembly (GhanaSPS only) ===')
    f = ll.Feature('crop_production')(['GhanaSPS'])
    print('Feature index:', f.index.names, ' shape:', f.shape)

    print('\nALL STEP-3 GATES PASS:', all_ok)
    return 0 if all_ok else 1


if __name__ == '__main__':
    sys.exit(main())
