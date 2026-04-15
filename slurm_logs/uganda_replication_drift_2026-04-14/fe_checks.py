#!/usr/bin/env python3
"""
Functional equivalence checks for Uganda schema-drift features.
Writes fe_results.json and prints a summary.
"""
import os, sys, warnings, hashlib, json
from pathlib import Path
import pandas as pd
import numpy as np

os.environ['LSMS_NO_CACHE'] = '0'

REPL_DIR = Path(os.path.expanduser(
    '~/Projects/RiskSharing_Replication/external_data/LSMS_Library/lsms_library/countries/Uganda/var/'
))

def content_hash(df):
    try:
        h = pd.util.hash_pandas_object(df, index=True)
        return hashlib.sha256(h.values.tobytes()).hexdigest()[:16]
    except Exception:
        return 'hash_error'

def row_compare(rep_t, api_t, common_idx):
    """Compare two DataFrames on common index, return stats dict."""
    rep_r = rep_t.reset_index().set_index([c for c in common_idx if c in rep_t.reset_index().columns])
    api_r = api_t.reset_index().set_index([c for c in common_idx if c in api_t.reset_index().columns])
    shared = rep_r.index.intersection(api_r.index)
    out = {
        'rows_common': len(shared),
        'rows_only_rep': len(rep_r.index.difference(api_r.index)),
        'rows_only_api': len(api_r.index.difference(rep_r.index)),
    }
    # MAD on common numeric columns
    common_cols = sorted(set(rep_t.columns) & set(api_t.columns))
    num_cols = [c for c in common_cols
                if pd.api.types.is_numeric_dtype(rep_t[c]) and
                   pd.api.types.is_numeric_dtype(api_t[c])][:2]
    mad = {}
    for c in num_cols:
        try:
            rv = rep_r.loc[shared, c].astype(float)
            av = api_r.loc[shared, c].astype(float)
            d = (rv - av).abs().mean()
            mad[c] = float(d) if pd.notna(d) else None
        except Exception as e:
            mad[c] = f'error: {e}'
    out['sample_mad'] = mad
    total = out['rows_common'] + out['rows_only_rep'] + out['rows_only_api']
    if total > 0 and out['rows_only_rep'] == 0 and out['rows_only_api'] == 0:
        out['success'] = True
    elif out['rows_common'] > 0:
        out['success'] = 'partial'
    else:
        out['success'] = False
    return out

def verdict_from(out):
    if out.get('success') is True:
        return 'YES'
    elif out.get('success') == 'partial':
        n = out.get('rows_common', 0)
        m = n + out.get('rows_only_rep', 0) + out.get('rows_only_api', 0)
        return f'PARTIAL({n}/{m})'
    return 'NO'

def main():
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        uganda = ll.Country('Uganda')

    fe_results = {}

    # =========================================================
    # 1. food_expenditures
    # =========================================================
    print('\n=== food_expenditures ===', flush=True)
    rep_fe = pd.read_parquet(REPL_DIR / 'food_expenditures.parquet')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        api_fe = uganda.food_expenditures(market='Region')
    print(f'  rep shape={rep_fe.shape}, api shape={api_fe.shape}')
    print(f'  rep cols={rep_fe.columns.tolist()}, api cols={api_fe.columns.tolist()}')

    # Transform: rename Expenditure -> x in API
    api_fe_t = api_fe.rename(columns={'Expenditure': 'x'})
    stats = row_compare(rep_fe, api_fe_t, ['i', 't', 'm', 'j'])
    print(f'  After rename Expenditure->x: {stats}')
    fe_results['food_expenditures'] = {
        'transforms': [
            {
                'description': "rename API col 'Expenditure' -> 'x' to match replication",
                'post_shape_rep': list(rep_fe.shape),
                'post_shape_api': list(api_fe_t.shape),
                'post_cols_rep': rep_fe.columns.tolist(),
                'post_cols_api': api_fe_t.columns.tolist(),
                **stats,
                'hash_match': content_hash(rep_fe) == content_hash(api_fe_t),
            }
        ],
        'verdict': verdict_from(stats),
    }

    # =========================================================
    # 2. household_characteristics
    # =========================================================
    print('\n=== household_characteristics ===', flush=True)
    rep_hc = pd.read_parquet(REPL_DIR / 'household_characteristics.parquet')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        api_hc = uganda.household_characteristics(market='Region')
    print(f'  rep shape={rep_hc.shape}, idx={rep_hc.index.names}, cols={rep_hc.columns.tolist()[:3]}...')
    print(f'  api shape={api_hc.shape}, idx={api_hc.index.names}, cols={api_hc.columns.tolist()[:3]}...')

    # Column rename map: Females->F, Males->M, 51-99->51+
    col_map = {}
    for c in api_hc.columns:
        if c == 'log HSize':
            col_map[c] = c
        else:
            # e.g. "F 00-03" -> "Females 00-03", "M 51+" -> "Males 51-99"
            sex_part, age_part = c.split(' ', 1)
            full_sex = 'Females' if sex_part == 'F' else 'Males'
            if age_part.endswith('+'):
                age_old = age_part.replace('+', '-99')
            else:
                age_old = age_part
            col_map[c] = f'{full_sex} {age_old}'
    api_hc_renamed = api_hc.rename(columns=col_map)
    print(f'  Renamed api cols: {api_hc_renamed.columns.tolist()[:3]}...')

    # Reorder index (t,m,i) -> sort same as rep (i,t,m)
    # Both have same levels i,t,m. Just reorder api to (i,t,m)
    api_hc_reordered = api_hc_renamed.reorder_levels(['i', 't', 'm'])
    print(f'  api after reorder: idx={api_hc_reordered.index.names}')

    stats_hc = row_compare(rep_hc, api_hc_reordered, ['i', 't', 'm'])
    print(f'  After col-rename + reorder: {stats_hc}')
    fe_results['household_characteristics'] = {
        'transforms': [
            {
                'description': "rename API col abbreviations (F->Females, M->Males, 51+->51-99), reorder index (t,m,i)->(i,t,m)",
                'post_shape_rep': list(rep_hc.shape),
                'post_shape_api': list(api_hc_reordered.shape),
                'post_cols_rep': rep_hc.columns.tolist(),
                'post_cols_api': api_hc_reordered.columns.tolist(),
                'post_index_api': api_hc_reordered.index.names,
                **stats_hc,
                'hash_match': content_hash(rep_hc) == content_hash(api_hc_reordered),
            }
        ],
        'verdict': verdict_from(stats_hc),
    }

    # =========================================================
    # 3. household_roster
    # =========================================================
    print('\n=== household_roster ===', flush=True)
    rep_hr = pd.read_parquet(REPL_DIR / 'household_roster.parquet')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        api_hr = uganda.household_roster()
    print(f'  rep shape={rep_hr.shape}, idx={rep_hr.index.names}, cols={rep_hr.columns.tolist()}')
    print(f'  api shape={api_hr.shape}, idx={api_hr.index.names}, cols={api_hr.columns.tolist()}')

    # Transform 1: drop v from API index, drop kinship cols, keep Age+Sex+Relationship/Relation
    api_hr_t1 = api_hr.reset_index('v', drop=True)
    # Drop kinship decomp cols, rename Relationship->Relation
    api_hr_t1 = api_hr_t1.drop(columns=['Generation', 'Distance', 'Affinity'], errors='ignore')
    if 'Relationship' in api_hr_t1.columns:
        api_hr_t1 = api_hr_t1.rename(columns={'Relationship': 'Relation'})
    # Keep only common cols
    common_cols = sorted(set(rep_hr.columns) & set(api_hr_t1.columns))
    rep_hr_t = rep_hr[common_cols]
    api_hr_t = api_hr_t1[common_cols]
    print(f'  After drop-v + kinship collapse: rep={rep_hr_t.shape}, api={api_hr_t.shape}, common_cols={common_cols}')

    # Compare on (i,t,pid) — duplicates possible after dropping v
    if api_hr_t.index.duplicated().any():
        print(f'  API has {api_hr_t.index.duplicated().sum()} duplicate (i,t,pid) after dropping v - taking first')
        api_hr_t = api_hr_t[~api_hr_t.index.duplicated(keep='first')]

    stats_hr = row_compare(rep_hr_t, api_hr_t, ['i', 't', 'pid'])
    print(f'  Row comparison: {stats_hr}')

    # Additional check: do Age values agree?
    rep_r = rep_hr_t.reset_index().set_index(['i', 't', 'pid'])
    api_r = api_hr_t.reset_index().set_index(['i', 't', 'pid'])
    shared = rep_r.index.intersection(api_r.index)
    age_mad = (rep_r.loc[shared, 'Age'].astype(float) - api_r.loc[shared, 'Age'].astype(float)).abs().mean()
    print(f'  Age MAD on {len(shared)} common rows: {age_mad}')

    fe_results['household_roster'] = {
        'transforms': [
            {
                'description': "drop 'v' from API index, drop kinship decomp cols, rename Relationship->Relation, compare on (i,t,pid) with Age+Sex",
                'post_shape_rep': list(rep_hr_t.shape),
                'post_shape_api': list(api_hr_t.shape),
                'post_cols': common_cols,
                **stats_hr,
                'age_mad': float(age_mad) if pd.notna(age_mad) else None,
                'hash_match': content_hash(rep_hr_t) == content_hash(api_hr_t),
            }
        ],
        'verdict': verdict_from(stats_hr),
    }

    # =========================================================
    # 4. locality
    # =========================================================
    print('\n=== locality ===', flush=True)
    rep_loc = pd.read_parquet(REPL_DIR / 'locality.parquet')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        api_loc = uganda.locality(market='Region')
    print(f'  rep shape={rep_loc.shape}, cols={rep_loc.columns.tolist()}')
    print(f'  api shape={api_loc.shape}, cols={api_loc.columns.tolist()}')
    print(f'  rep v sample: {rep_loc["v"].head(3).tolist()}')
    print(f'  api Parish sample: {api_loc["Parish"].head(3).tolist()}')

    # The replication has v (cluster id), api has Parish (place name)
    # These are semantically different - no column-rename can equate them
    # Check: does sample() have v for the same households?
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        samp = uganda.sample()
    print(f'  sample shape={samp.shape}, cols={samp.columns.tolist()}, idx={samp.index.names}')

    # Join v from sample onto locality (i,t) -> v
    samp_v = samp.reset_index()[['i', 't', 'v']].drop_duplicates().set_index(['i', 't'])
    loc_r = rep_loc.reset_index().set_index(['i', 't'])
    # Check if v from sample matches v in replication
    common_it = loc_r.index.intersection(samp_v.index)
    rep_v_vals = loc_r.loc[common_it, 'v']
    samp_v_vals = samp_v.loc[common_it, 'v']
    try:
        match_rate = (rep_v_vals.values == samp_v_vals.values).mean()
        print(f'  v from sample vs. v in replication: match_rate={match_rate:.4f} on {len(common_it)} rows')
    except Exception as e:
        print(f'  v match check error: {e}')
        match_rate = None

    fe_results['locality'] = {
        'transforms': [
            {
                'description': "locality: replication 'v' is cluster EA id, API 'Parish' is place name — semantically different columns; no column-rename equivalence possible. Cross-check: v from sample() vs replication v.",
                'v_from_sample_match_rate': float(match_rate) if match_rate is not None else None,
                'verdict_detail': "API returns Parish (human-readable label), replication stored raw cluster EA code 'v'. The shim changed semantics — not a rename, a replacement.",
                'success': False,
            }
        ],
        'verdict': 'NO',
    }

    return fe_results


if __name__ == '__main__':
    fe = main()
    out = Path('/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/slurm_logs/uganda_replication_drift_2026-04-14/fe_results.json')
    with open(out, 'w') as f:
        json.dump(fe, f, indent=2, default=str)
    print(f'\nFE results saved to {out}')
    print('\n=== VERDICTS ===')
    for feat, res in fe.items():
        print(f'  {feat}: {res["verdict"]}')
