"""
Side-by-side comparison of household_characteristics and household_roster
for the most-drifted households in log HSize (API vs replication).

Outputs a readable dump for each HH: the 14 age-bracket counts from
household_characteristics on both sides, and every member from
household_roster on both sides (with Sex, Age, Relationship/kinship).

Run from the repo root:

    .venv/bin/python slurm_logs/uganda_replication_drift_2026-04-14/eyeball_hsize.py

Set N_ROWS=... to control how many drifted HHs to dump (default 10).
"""
from __future__ import annotations

import warnings
from pathlib import Path

import numpy as np
import pandas as pd

pd.set_option("display.max_columns", 40)
pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 50)
pd.set_option("display.max_rows", 80)

N_ROWS = 10
ATOL = 0.02

REPL_DIR = Path.home() / (
    "Projects/RiskSharing_Replication/external_data/"
    "LSMS_Library/lsms_library/countries/Uganda/var"
)

API_BRACKETS = [f'{s} {b}' for s in ('F', 'M')
                for b in ('00-03', '04-08', '09-13', '14-18', '19-30', '31-50', '51+')]
REPL_BRACKETS = [f'{s} {b}' for s in ('Females', 'Males')
                 for b in ('00-03', '04-08', '09-13', '14-18', '19-30', '31-50', '51-99')]


def _call_api(name, **kw):
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return getattr(ll.Country('Uganda'), name)(**kw)


def _load_repl(name):
    return pd.read_parquet(REPL_DIR / f'{name}.parquet')


def hr(char='=', width=78):
    print(char * width)


def main():
    print('Loading data...')
    api_hc = _call_api('household_characteristics', market='Region')
    repl_hc = _load_repl('household_characteristics')
    api_ros = _call_api('household_roster')
    repl_ros = _load_repl('household_roster')

    # Reconstruct HSize from age brackets
    api_hc = api_hc.copy()
    repl_hc = repl_hc.copy()
    api_hc['HSize'] = api_hc[API_BRACKETS].sum(axis=1)
    repl_hc['HSize'] = repl_hc[REPL_BRACKETS].sum(axis=1)

    # Merge on common index
    common = [lev for lev in repl_hc.index.names if lev in api_hc.index.names]
    a = api_hc.reset_index()
    r = repl_hc.reset_index()
    m = a.merge(r, on=common, suffixes=('_api', '_repl'))

    both = m['log HSize_api'].notna() & m['log HSize_repl'].notna()
    log_diff = (m['log HSize_api'] - m['log HSize_repl'])
    log_diff_abs = log_diff.abs()
    over = both & (log_diff_abs > ATOL)

    print(f'\nMerged rows: {len(m)}  |Δ log HSize| > {ATOL}: {int(over.sum())}')
    top = m[over].assign(abs_diff=log_diff_abs[over]).nlargest(N_ROWS, 'abs_diff')

    # Prepare household_roster data for lookups
    api_ros_flat = api_ros.reset_index()
    repl_ros_flat = repl_ros.reset_index()

    for rank, (idx, row) in enumerate(top.iterrows(), 1):
        i_val = row['i']
        t_val = row['t']

        hr('=')
        print(f'  #{rank}  i={i_val!r}  t={t_val!r}  '
              f'log HSize: api={row["log HSize_api"]:.4f}  '
              f'repl={row["log HSize_repl"]:.4f}  '
              f'Δ={row["log HSize_api"] - row["log HSize_repl"]:+.4f}')
        print(f'  HSize (sum of brackets): api={int(row["HSize_api"])}  '
              f'repl={int(row["HSize_repl"])}  '
              f'Δ={int(row["HSize_api"] - row["HSize_repl"]):+d}')
        hr('=')

        # Age bracket side-by-side
        print('\n  Age-bracket counts:')
        print(f'  {"Bracket":<16}  {"API":>5}  {"REPL":>5}  {"Δ":>5}')
        print(f'  {"-"*16}  {"-----":>5}  {"-----":>5}  {"-----":>5}')
        for api_col, repl_col in zip(API_BRACKETS, REPL_BRACKETS):
            va = row.get(f'{api_col}_api', row.get(api_col, float('nan')))
            vr = row.get(f'{repl_col}_repl', row.get(repl_col, float('nan')))
            va = int(va) if pd.notna(va) else '?'
            vr = int(vr) if pd.notna(vr) else '?'
            delta = ''
            if isinstance(va, int) and isinstance(vr, int) and va != vr:
                delta = f'{va - vr:+d}  <---'
            short = api_col  # e.g. "F 00-03"
            print(f'  {short:<16}  {va:>5}  {vr:>5}  {delta}')

        # Household roster members — API side
        mask_a = (api_ros_flat['i'] == i_val) & (api_ros_flat['t'] == t_val)
        hh_api = api_ros_flat[mask_a]
        ros_cols_api = [c for c in ['pid', 'Sex', 'Age', 'Generation', 'Distance', 'Affinity']
                        if c in hh_api.columns]
        print(f'\n  API household_roster ({len(hh_api)} members):')
        if not hh_api.empty:
            print(hh_api[ros_cols_api].to_string(index=False))
        else:
            print('    (no rows)')

        # Household roster members — REPL side
        mask_r = (repl_ros_flat['i'] == i_val) & (repl_ros_flat['t'] == t_val)
        hh_repl = repl_ros_flat[mask_r]
        # Replication has Relationship instead of kinship decomposition
        ros_cols_repl = [c for c in ['pid', 'Sex', 'Age', 'Relationship', 'Relation']
                         if c in hh_repl.columns]
        print(f'\n  REPL household_roster ({len(hh_repl)} members):')
        if not hh_repl.empty:
            print(hh_repl[ros_cols_repl].to_string(index=False))
        else:
            print('    (no rows)')
        print()


if __name__ == '__main__':
    main()
