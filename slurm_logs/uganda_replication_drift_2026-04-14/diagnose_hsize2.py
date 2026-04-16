"""
Diagnose the 1315-row `log HSize` drift in Uganda household_characteristics.

Neither the API nor the replication parquet has a raw `HSize` column —
both only carry `log HSize` plus 14 age-bracket count columns.  So we
reconstruct HSize on each side by summing the 14 brackets, then compare.

Hypotheses to distinguish:
  H1  age_handler() is recovering members the replication dropped, so
      HSize_api > HSize_repl.  We'd see a signed skew (api > repl), and
      for the top-drifted HHs, household_roster on the API side would
      have more rows than on the replication side.
  H2  Some specific bracket (e.g. F 51+ vs Females 51-99) has a different
      cutoff on one side.  We'd see the drift concentrated in that
      bracket and not the others.
  H3  The pivot on `Sex` is including/excluding NaN-sex members
      differently, or the age-bracket binning is counting different-typed
      Age values (Int64 vs float64).
"""
from __future__ import annotations

import warnings
from pathlib import Path

import numpy as np
import pandas as pd

pd.set_option("display.max_columns", 40)
pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 40)

REPL_DIR = Path.home() / (
    "Projects/RiskSharing_Replication/external_data/"
    "LSMS_Library/lsms_library/countries/Uganda/var"
)

API_BRACKETS = [f'{s} {b}' for s in ('F', 'M')
                for b in ('00-03', '04-08', '09-13', '14-18', '19-30', '31-50', '51+')]
REPL_BRACKETS = [f'{s} {b}' for s in ('Females', 'Males')
                 for b in ('00-03', '04-08', '09-13', '14-18', '19-30', '31-50', '51-99')]
BRACKET_PAIRS = list(zip(API_BRACKETS, REPL_BRACKETS))


def _call_api(name, **kw):
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return getattr(ll.Country('Uganda'), name)(**kw)


def _load_repl(name):
    return pd.read_parquet(REPL_DIR / f'{name}.parquet')


def banner(s):
    print('\n' + '=' * 78)
    print('  ' + s)
    print('=' * 78)


def main():
    banner('household_characteristics: reconstruct HSize from age brackets')

    api = _call_api('household_characteristics', market='Region')
    repl = _load_repl('household_characteristics')
    print(f'API  shape={api.shape}  cols[:5]={list(api.columns)[:5]}')
    print(f'REPL shape={repl.shape}  cols[:5]={list(repl.columns)[:5]}')

    # Reconstruct HSize on each side.  Age-bracket columns are counts.
    api = api.copy()
    repl = repl.copy()
    api['HSize'] = api[API_BRACKETS].sum(axis=1)
    repl['HSize'] = repl[REPL_BRACKETS].sum(axis=1)

    # Merge on common index.
    common = [lev for lev in repl.index.names if lev in api.index.names]
    a = api.reset_index()[common + ['HSize', 'log HSize'] + API_BRACKETS]
    r = repl.reset_index()[common + ['HSize', 'log HSize'] + REPL_BRACKETS]
    m = a.merge(r, on=common, suffixes=('_api', '_repl'))
    print(f'Merged rows: {len(m)}')

    # Sanity check: does HSize reconstruction equal exp(log HSize) on each side?
    for side in ('api', 'repl'):
        recon = m[f'HSize_{side}']
        logged = m[f'log HSize_{side}']
        expected_from_log = np.exp(logged)
        # Where both are finite and non-null
        ok = recon.notna() & logged.notna()
        if ok.any():
            diff = (recon[ok] - expected_from_log[ok]).abs()
            print(f'  {side} sanity: max |HSize - exp(log HSize)| = {diff.max():.6g} '
                  f'(over {int(ok.sum())} rows)')

    # Compare log HSize
    both = m['log HSize_api'].notna() & m['log HSize_repl'].notna()
    log_diff = (m['log HSize_api'] - m['log HSize_repl'])
    log_diff_abs = log_diff.abs()
    over = both & (log_diff_abs > 0.02)
    print(f'\nlog HSize |Δ| > 0.02: {int(over.sum())} of {int(both.sum())}')
    for thr in (0.02, 0.1, 0.5, 1.0):
        print(f'  |Δ| > {thr:>4}: {int((both & (log_diff_abs > thr)).sum()):>5}')

    # Sign: API heavier or lighter than REPL?
    print(f'\nSigned drift over outlier rows:')
    print(f'  api log > repl log (api has MORE members):  {int(((log_diff > 0) & over).sum())}')
    print(f'  api log < repl log (api has FEWER members): {int(((log_diff < 0) & over).sum())}')

    # HSize-level drift: how many members are gained/lost on average?
    hs_diff = (m['HSize_api'] - m['HSize_repl'])
    banner('HSize raw Δ (api - repl) on outlier rows')
    print(hs_diff[over].describe().to_string())
    vc = hs_diff[over].astype('Int64').value_counts().sort_index().head(20)
    print('\nΔ HSize value counts:')
    print(vc.to_string())

    # Per-bracket drift: which brackets differ most?
    banner('Per-bracket drift over outlier rows (mean Δ api - repl)')
    rows = []
    for api_col, repl_col in BRACKET_PAIRS:
        d = m.loc[over, api_col] - m.loc[over, repl_col]
        rows.append({'bracket (api/repl)': f'{api_col} / {repl_col}',
                     'mean Δ': d.mean(),
                     'max |Δ|': d.abs().max(),
                     'nonzero Δ': int((d != 0).sum())})
    brkt_df = pd.DataFrame(rows)
    print(brkt_df.to_string(index=False))

    # Top-15 by log HSize |Δ|
    banner('Top 15 rows by |Δ log HSize|')
    top = m[over].assign(abs_diff=log_diff_abs[over]).nlargest(15, 'abs_diff')
    cols = common + ['HSize_api', 'HSize_repl', 'log HSize_api', 'log HSize_repl', 'abs_diff']
    print(top[cols].to_string(index=False))

    # For top-5 drifted HHs, probe household_roster on both sides.
    banner('household_roster member counts for top 5 drifted HHs')
    api_ros = _call_api('household_roster').reset_index()
    try:
        repl_ros = _load_repl('household_roster').reset_index()
    except Exception as exc:
        print(f'(replication household_roster not loadable: {exc})')
        repl_ros = None

    top5 = m[over].assign(abs_diff=log_diff_abs[over]).nlargest(5, 'abs_diff')
    for _, row in top5.iterrows():
        key = {c: row[c] for c in common}
        print(f'\n--- {key}')
        print(f'    HSize api={row["HSize_api"]}  repl={row["HSize_repl"]}  '
              f'Δ={row["HSize_api"] - row["HSize_repl"]:+g}  '
              f'log Δ={row["log HSize_api"] - row["log HSize_repl"]:+.4f}')
        mask = (api_ros['i'] == key['i']) & (api_ros['t'] == key['t'])
        a_ros = api_ros[mask]
        age_null = int(a_ros['Age'].isna().sum()) if 'Age' in a_ros.columns else 'n/a'
        print(f'    API roster rows: {len(a_ros)}  age-null: {age_null}')
        if repl_ros is not None:
            mask_r = (repl_ros['i'] == key['i']) & (repl_ros['t'] == key['t'])
            r_ros = repl_ros[mask_r]
            age_null_r = int(r_ros['Age'].isna().sum()) if 'Age' in r_ros.columns else 'n/a'
            print(f'    REPL roster rows: {len(r_ros)}  age-null: {age_null_r}')

    # Per-wave breakdown
    banner('Outlier counts by wave (t)')
    print(m[over].groupby('t').size().to_string())


if __name__ == '__main__':
    main()
