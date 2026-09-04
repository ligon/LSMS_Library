#!/usr/bin/env python
"""STEP 3 verification for GhanaSPS plot_labor.  __main__-guarded.

Run COLD (empty LSMS_DATA_DIR bar the dvc-cache symlink) with
LSMS_GRAIN_STRICT=1.  LSMS_READ_STRICT=1 cannot be used: it aborts inside
GhanaSPS `sample` on Rural / weight / panel_weight before this table is
reached (CONTENTS.org, plot features), so the read-strict condition is
asserted directly via null_read_reports().
"""
import os
import sys
import warnings

import numpy as np
import pandas as pd

import lsms_library as ll
from lsms_library.country import grain_reports
from lsms_library.local_tools import get_dataframe
from lsms_library.null_read_audit import null_read_reports
from lsms_library.paths import countries_root

assert 'wt-gsps-labor' in str(countries_root()), countries_root()
pd.set_option('display.width', 200)
pd.set_option('display.max_rows', 120)

C = ll.Country('GhanaSPS')
WAVES = ['2009-10', '2013-14', '2017-18']


def hdr(s):
    print('\n' + '=' * 78 + f'\n{s}\n' + '=' * 78, flush=True)


def main():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        df = C.plot_labor()
    print(f'plot_labor: {df.shape}, index {list(df.index.names)}')
    print('columns:', list(df.columns), [str(t) for t in df.dtypes])
    grain = [w for w in caught if 'GrainCollapse' in type(w.message).__name__]
    print(f'GrainCollapseWarnings during the build: {len(grain)}')
    for w in grain:
        print('  ', str(w.message)[:400])
    other = [w for w in caught if 'GrainCollapse' not in type(w.message).__name__]
    print(f'other warnings: {len(other)}')
    for w in other[:20]:
        print(f'   [{type(w.message).__name__}] {str(w.message)[:300]}')

    hdr('CHECK 1 -- rows; distinct stage / source / season, per wave')
    for t in WAVES:
        d = df.xs(t, level='t', drop_level=False)
        print(f'{t}: {len(d):,} rows | '
              f'season={sorted(set(d.index.get_level_values("season")))} | '
              f'stage={sorted(set(d.index.get_level_values("stage")))} | '
              f'source={sorted(set(d.index.get_level_values("source")))}')
    print('plots per wave:',
          {t: df.xs(t, level='t').index.droplevel(['season', 'stage', 'source'])
              .drop_duplicates().shape[0] for t in WAVES})

    hdr('CHECK 2 -- index unique; grain reports; v coverage')
    print('index unique:', df.index.is_unique)
    print('duplicated index tuples:', int(df.index.duplicated().sum()))
    reps = [r for r in grain_reports() if getattr(r, 'table', None) == 'plot_labor']
    print(f'grain_reports() for plot_labor: {len(reps)}')
    for r in reps:
        print('  ', r)
    print("'v' in index names:", 'v' in (df.index.names or []))
    if 'v' in (df.index.names or []):
        v = df.index.get_level_values('v')
        print(f'v non-null: {int(pd.notna(v).sum()):,} / {len(df):,} '
              f'= {100 * pd.notna(v).mean():.2f}%')
        for t in WAVES:
            d = df.xs(t, level='t')
            vv = d.index.get_level_values('v')
            print(f'  {t}: v non-null {100 * pd.notna(vv).mean():.2f}% '
                  f'({d.index.get_level_values("v").nunique()} clusters)')
    print('null_read_reports(GhanaSPS, plot_labor):',
          null_read_reports(country='GhanaSPS', table='plot_labor'))

    hdr('CHECK 3 -- join rate of (t, i, plot_id) into plot_features()')
    pf = C.plot_features()
    pf_keys = set(map(tuple, pf.reset_index()[['t', 'i', 'plot_id']].to_numpy()))
    lab = df.reset_index()[['t', 'i', 'plot_id']].drop_duplicates()
    for t in WAVES:
        k = lab[lab['t'] == t]
        hit = sum(tuple(r) in pf_keys for r in k.to_numpy())
        print(f'{t}: {hit:,} / {len(k):,} labour plots in plot_features() '
              f'= {100 * hit / len(k):.2f}%')

    hdr('CHECK 4 -- 2009-10 rows per (season, stage): 8 cells, minor << major')
    w1 = df.xs('2009-10', level='t')
    tab = w1.groupby(level=['season', 'stage']).size().unstack('season')
    print(tab)
    print('all 8 cells populated:', bool((tab.notna() & (tab > 0)).all().all()))
    print('minor/major ratio per stage:', (tab['minor'] / tab['major']).round(3).to_dict())
    print('PersonDays by (season, stage):')
    print(w1.groupby(level=['season', 'stage'])['PersonDays'].sum().unstack('season').round(0))

    hdr('CHECK 5 -- PersonDays sum by source per wave; zero-row rule')
    print(df.groupby([df.index.get_level_values('t'),
                      df.index.get_level_values('source')])['PersonDays']
          .agg(['size', 'sum', 'mean', 'median', 'max']).round(2))
    print('\nrows with PersonDays == 0 delivered:', int((df['PersonDays'] == 0).sum()),
          '(the keep rule is PersonDays > 0)')
    print('rows with PersonDays null delivered:', int(df['PersonDays'].isna().sum()))
    print('\nCandidate / kept / dropped per wave (from the wave scripts):')
    print('  2009-10  136,464 candidates -> 30,864 kept; 515 zero, 105,085 never reported')
    print('  2013-14  164,255 candidates -> 27,874 kept;  85 zero, 136,296 never reported')
    print('  2017-18  100,115 candidates -> 31,149 kept;  43 zero,  68,923 never reported '
          '(+3,200 blank-stage cells dropped first)')

    hdr('CHECK 6 -- WageRate* and WageUnit on hired rows')
    for t in WAVES:
        d = df.xs(t, level='t', drop_level=False)
        srcs = set(d.index.get_level_values('source'))
        hired_like = [s for s in srcs if s in ('hired', 'casual', 'permanent')]
        h = d[d.index.get_level_values('source').isin(hired_like)]
        print(f'\n--- {t}: {len(h):,} rows on {sorted(hired_like)} ---')
        for c in ('WageRateMen', 'WageRateWomen', 'WageRateChildren'):
            nn = int(h[c].notna().sum())
            print(f'  {c}: non-null {nn:,} / {len(h):,} = {100 * nn / max(len(h),1):.2f}%')
        print('  WageUnit value_counts:', dict(h['WageUnit'].value_counts(dropna=False)))
        if h['WageRateMen'].notna().any():
            med = h.groupby(h['WageUnit'].astype(str))[
                ['WageRateMen', 'WageRateWomen', 'WageRateChildren']].median().round(2)
            print('  median rate by WageUnit:')
            print(med.to_string().replace('\n', '\n    '))
        # non-hired rows must carry no rate
        nh = d[~d.index.get_level_values('source').isin(hired_like)]
        bad = int(nh[['WageRateMen', 'WageRateWomen', 'WageRateChildren',
                      'WageUnit']].notna().any(axis=1).sum())
        print(f'  non-hired rows carrying any wage datum: {bad}')

    hdr('CHECK 6b -- currency attaches GHS')
    cur = C.plot_labor(currency='index')
    print('index names with currency=index:', list(cur.index.names))
    print('currency values:', sorted(set(cur.index.get_level_values('currency'))))

    hdr('CHECK 7 -- the indirect stage checks (2009-10 assignment)')
    indirect(df)

    hdr('CHECK 8 -- Hours')
    for t in WAVES:
        d = df.xs(t, level='t')
        print(f'{t}: Hours non-null {int(d["Hours"].notna().sum()):,} / {len(d):,} '
              f'| median {d["Hours"].median()} | mean {round(float(d["Hours"].mean()), 2) if d["Hours"].notna().any() else None} '
              f'| max {d["Hours"].max()}')

    hdr('SANITY')
    from lsms_library.diagnostics import is_this_feature_sane
    try:
        r = is_this_feature_sane(df, 'GhanaSPS', 'plot_labor')
        print('is_this_feature_sane:', r)
    except Exception as e:
        print('is_this_feature_sane raised:', type(e).__name__, e)


W1_TO_LATER = {
    'land_preparation': ['clearing_and_land_preparation', 'ploughing', 'planting'],
    'field_management': ['chemical_application', 'weeding'],
    'harvesting': ['harvesting'],
    'post_harvest': ['post_harvest'],
}


def indirect(df):
    root = countries_root() / 'GhanaSPS' / '2009-10' / 'Data'
    print('--- (i) cross-wave coarse-stage PersonDays profile ---')
    print('    A permuted 2009-10 file -> stage map would show a permuted profile.')
    fold = {v: k for k, vs in W1_TO_LATER.items() for v in vs}
    rows = {}
    for t in WAVES:
        d = df.xs(t, level='t')
        coarse = pd.Series(d.index.get_level_values('stage'), index=d.index).map(
            lambda s: fold.get(s, s))
        share = d.groupby(coarse.to_numpy())['PersonDays'].sum()
        rows[t] = (100 * share / share.sum()).round(1)
    prof = pd.DataFrame(rows).reindex(list(W1_TO_LATER))
    print(prof.to_string())
    w1 = prof['2009-10']
    later = prof[['2013-14', '2017-18']].mean(axis=1)
    print('  rank of 2009-10 :', list(w1.sort_values(ascending=False).index))
    print('  rank of 2013/17 :', list(later.sort_values(ascending=False).index))
    print('  max |share diff| vs later mean:', float((w1 - later).abs().max()).__round__(1), 'pp')
    # what a wrong assignment would look like
    import itertools
    best = None
    for perm in itertools.permutations(list(W1_TO_LATER)):
        cand = pd.Series(w1.to_numpy(), index=list(perm)).reindex(later.index)
        dev = float((cand - later).abs().max())
        tag = 'TRUE ' if list(perm) == list(W1_TO_LATER) else '     '
        if best is None or dev < best[0]:
            best = (dev, perm, tag)
        if list(perm) == list(W1_TO_LATER):
            true_dev = dev
    print(f'  the TRUE assignment deviates {true_dev:.1f} pp; the best of all 24 '
          f'permutations deviates {best[0]:.1f} pp ({best[2].strip() or "a permutation"}: {best[1]})')
    print('  BETWEEN-WAVE NOISE FLOOR -- the two later waves both carry the stage')
    print('  in their own data, so their disagreement bounds what this test can')
    print(f'  resolve: max |2013-14 - 2017-18| = '
          f'{float((prof["2013-14"] - prof["2017-18"]).abs().max()):.1f} pp.')

    print('\n--- (ia) plot COVERAGE by coarse stage -- the ORDERING is the test ---')
    print('    No directional prior: what is tested is whether 2009-10\'s four')
    print('    blocks reproduce the ORDER the two later waves show, since those')
    print('    two carry the stage in their own data.  The 1<->2 swap that check')
    print('    (i) prefers would INVERT 2009-10\'s first two entries.')
    for t in WAVES:
        d = df.xs(t, level='t')
        allplots = d.index.droplevel(['season', 'stage', 'source']).drop_duplicates().shape[0]
        coarse = pd.Series(d.index.get_level_values('stage'), index=d.index).map(
            lambda s: fold.get(s, s))
        line = {}
        for cs in W1_TO_LATER:
            sub = d[(coarse == cs).to_numpy()]
            n = sub.index.droplevel(['season', 'stage', 'source']).drop_duplicates().shape[0]
            line[cs] = round(100 * n / allplots, 1)
        print(f'  {t}: of {allplots:,} labour plots, ' +
              ', '.join(f'{k} {v}%' for k, v in line.items()))

    print('\n--- (ib) hired / non-household share of person-days by coarse stage ---')
    print('    Same ordering test on a different quantity.')
    for t in WAVES:
        d = df.xs(t, level='t')
        src = pd.Series(d.index.get_level_values('source'), index=d.index)
        hiredish = src.isin(['hired', 'casual', 'permanent']).to_numpy()
        coarse = pd.Series(d.index.get_level_values('stage'), index=d.index).map(
            lambda s: fold.get(s, s)).to_numpy()
        tot = d.groupby(coarse)['PersonDays'].sum()
        hi = d[hiredish].groupby(coarse[hiredish])['PersonDays'].sum()
        sh = (100 * hi / tot).round(1).reindex(list(W1_TO_LATER))
        print(f'  {t}: ' + ', '.join(f'{k} {v}%' for k, v in sh.items()))

    print('\n--- (ii) harvest-stage labour vs a crop_production harvest record ---')
    print('    2009-10 is (plot, season) grain in BOTH tables, so the two seasons')
    print('    are checked separately: S4AIX3 -> major, S4AIX7 -> minor.')
    cp = C.crop_production()
    for t, seasons in [('2009-10', ['major', 'minor']), ('2013-14', ['last']), ('2017-18', ['last'])]:
        d = df.xs(t, level='t')
        c = cp.xs(t, level='t')
        for s in seasons:
            if t == '2009-10':
                cps = set(map(tuple, c.xs(s, level='season').reset_index()[['i', 'plot_id']]
                              .drop_duplicates().to_numpy()))
            else:
                cps = set(map(tuple, c.reset_index()[['i', 'plot_id']]
                              .drop_duplicates().to_numpy()))
            for stage in ('harvesting', 'post_harvest', 'land_preparation',
                          'clearing_and_land_preparation'):
                sub = d[(d.index.get_level_values('season') == s)
                        & (d.index.get_level_values('stage') == stage)]
                if not len(sub):
                    continue
                plots = set(map(tuple, sub.reset_index()[['i', 'plot_id']]
                                .drop_duplicates().to_numpy()))
                hit = len(plots & cps)
                print(f'  {t}/{s}/{stage:31s}: {hit:5,} / {len(plots):5,} '
                      f'= {100 * hit / len(plots):5.1f}% of labour plots have a harvest record')

    print('\n--- (iii) the season anchor, in the 2009-10 data itself ---')
    d1 = get_dataframe(str(root / 'S4AIX1.dta'))
    d5 = get_dataframe(str(root / 'S4AIX5.dta'))
    order = ['January', 'February', 'March', 'April', 'May', 'June', 'July',
             'August', 'September', 'October', 'November', 'December']
    a289 = d1['s4aix_289i'].value_counts().reindex(order).fillna(0).astype(int)
    a326 = d5['s4aix_326i'].value_counts().reindex(order).fillna(0).astype(int)
    print('  S4AIX1 A289.1 "Starting month for the MAJOR season of dominant crop":')
    print('   ', a289.to_dict())
    print('  S4AIX5 A326.1 "Starting month for the MINOR season of dominant crop":')
    print('   ', a326.to_dict())
    print(f'  Feb-Jun share: major {100 * a289["February":"June"].sum() / a289.sum():.1f}%, '
          f'minor {100 * a326["February":"June"].sum() / a326.sum():.1f}%')
    print(f'  Jul-Oct share: major {100 * a289["July":"October"].sum() / a289.sum():.1f}%, '
          f'minor {100 * a326["July":"October"].sum() / a326.sum():.1f}%')

    print('\n--- (iiia) which S4AIX file OPENS each season ---')
    print('    A289 / A326 ("months for the MAJOR / MINOR season of dominant')
    print('    crop") are asked ONCE per season, at the head of the season\'s')
    print('    labour sequence.  Whichever file carries them is the season\'s')
    print('    FIRST block, and the questionnaire heads the first block')
    print('    "LAND PREPARATION".  A swap of blocks 1 and 2 would put field')
    print('    management before the season had been dated.')
    for k in range(1, 9):
        d = get_dataframe(str(root / f'S4AIX{k}.dta'), convert_categoricals=False)
        months = [c for c in d.columns if c in ('s4aix_289i', 's4aix_289ii',
                                                's4aix_326i', 's4aix_326ii')]
        print(f'    S4AIX{k}: season-month columns {months or "none"}')

    print('\n--- (iv) the DIRECT in-data anchor (asserted by the wave script) ---')
    print('    2009-10/_/plot_labor.py::_assert_block reads each S4AIX file\'s own')
    print('    variable labels and fails the build unless its labour cells carry')
    print('    the A-number range the questionnaire heads with that stage.')
    print('    A290-A298 land preparation / A299-A307 field management /')
    print('    A308-A316 harvesting / A317-A325 post-harvest, + A327-A362 minor.')


if __name__ == '__main__':
    main()
