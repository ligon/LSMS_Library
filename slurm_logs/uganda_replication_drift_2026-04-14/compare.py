#!/usr/bin/env python3
"""
Uganda replication-package vs. current API drift comparison.
READ-ONLY: no file modifications outside the output directory.
"""
import os, sys, hashlib, warnings, traceback, signal, json
from pathlib import Path
import pandas as pd
import numpy as np

# Keep caches warm (do NOT force rebuilds)
os.environ.setdefault('LSMS_NO_CACHE', '0')

REPL_DIR = Path(os.path.expanduser(
    '~/Projects/RiskSharing_Replication/external_data/LSMS_Library/lsms_library/countries/Uganda/var/'
))

PARQUETS = [
    'cluster_features.parquet',
    'earnings.parquet',
    'enterprise_income.parquet',
    'fct.parquet',
    'food_acquired.parquet',
    'food_expenditures.parquet',
    'food_prices.parquet',
    'food_quantities.parquet',
    'household_characteristics.parquet',
    'household_roster.parquet',
    'income.parquet',
    'interview_date.parquet',
    'locality.parquet',
    'nutrition.parquet',
    'other_features.parquet',
    'people_last7days.parquet',
    'shocks.parquet',
]

# Map parquet name -> (method_name, kwargs, notes)
# None method_name = no API equivalent
API_MAP = {
    'cluster_features':       ('cluster_features',       {},                      ''),
    'earnings':               ('earnings',                {},                      'in data_scheme'),
    'enterprise_income':      ('enterprise_income',       {},                      'in data_scheme'),
    'fct':                    ('fct',                     {},                      'in data_scheme'),
    'food_acquired':          ('food_acquired',           {},                      ''),
    'food_expenditures':      ('food_expenditures',       {},                      'auto-derived'),
    'food_prices':            ('food_prices',             {},                      'auto-derived'),
    'food_quantities':        ('food_quantities',         {},                      'auto-derived'),
    'household_characteristics': ('household_characteristics', {},                 'roster-derived'),
    'household_roster':       ('household_roster',        {},                      ''),
    'income':                 ('income',                  {},                      'in data_scheme'),
    'interview_date':         ('interview_date',          {},                      ''),
    'locality':               ('locality',                {},                      'deprecated'),
    'nutrition':              ('nutrition',               {},                      ''),
    'other_features':         (None,                     {},                      'deprecated/removed'),
    'people_last7days':       ('people_last7days',        {},                      'in data_scheme'),
    'shocks':                 ('shocks',                  {},                      ''),
}


def content_hash(df):
    try:
        h = pd.util.hash_pandas_object(df, index=True)
        return hashlib.sha256(h.values.tobytes()).hexdigest()[:16]
    except Exception:
        return 'hash_error'


def fingerprint(df):
    return {
        'shape': df.shape,
        'index_names': list(df.index.names),
        'columns': sorted(df.columns.tolist()),
        'dtypes': {c: str(t) for c, t in df.dtypes.items()},
        'content_hash': content_hash(df),
    }


def call_with_timeout(fn, kwargs, timeout_sec=180):
    """Call fn(**kwargs) with a SIGALRM timeout."""
    def _handler(signum, frame):
        raise TimeoutError(f"Timed out after {timeout_sec}s")
    old = signal.signal(signal.SIGALRM, _handler)
    signal.alarm(timeout_sec)
    try:
        return fn(**kwargs), None
    except Exception as e:
        return None, e
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)


def compare_dfs(rep_df, api_df, feature):
    """Detailed row-level comparison."""
    result = {}
    rep_fp = fingerprint(rep_df)
    api_fp = fingerprint(api_df)

    result['rep_shape'] = rep_fp['shape']
    result['api_shape'] = api_fp['shape']
    result['rep_index'] = rep_fp['index_names']
    result['api_index'] = api_fp['index_names']
    result['rep_cols'] = rep_fp['columns']
    result['api_cols'] = api_fp['columns']
    result['rep_hash'] = rep_fp['content_hash']
    result['api_hash'] = api_fp['content_hash']

    result['index_match'] = rep_fp['index_names'] == api_fp['index_names']
    result['cols_match'] = rep_fp['columns'] == api_fp['columns']
    result['hash_match'] = rep_fp['content_hash'] == api_fp['content_hash']
    result['shape_match'] = rep_fp['shape'] == api_fp['shape']

    # Extra/missing columns
    rep_col_set = set(rep_df.columns)
    api_col_set = set(api_df.columns)
    result['cols_only_rep'] = sorted(rep_col_set - api_col_set)
    result['cols_only_api'] = sorted(api_col_set - rep_col_set)

    # Index-level set comparison
    rep_idx_set = set(rep_fp['index_names']) - {None}
    api_idx_set = set(api_fp['index_names']) - {None}
    result['idx_only_rep'] = sorted(rep_idx_set - api_idx_set)
    result['idx_only_api'] = sorted(api_idx_set - rep_idx_set)

    # Dtype diffs for common columns
    common_cols = rep_col_set & api_col_set
    dtype_diffs = {}
    for c in sorted(common_cols):
        rt = str(rep_df[c].dtype)
        at = str(api_df[c].dtype)
        if rt != at:
            dtype_diffs[c] = {'rep': rt, 'api': at}
    result['dtype_diffs'] = dtype_diffs

    # Row-level comparison on common index + columns
    try:
        common_idx_names = [n for n in rep_fp['index_names'] if n in api_fp['index_names']]
        if common_idx_names and common_cols:
            # Reset to common index levels
            rep_r = rep_df.reset_index()
            api_r = api_df.reset_index()

            # Use common index columns
            rep_idx_cols = [c for c in common_idx_names if c in rep_r.columns]
            api_idx_cols = [c for c in common_idx_names if c in api_r.columns]
            if rep_idx_cols and api_idx_cols:
                rep_r2 = rep_r.set_index(rep_idx_cols)
                api_r2 = api_r.set_index(api_idx_cols)
                shared_idx = rep_r2.index.intersection(api_r2.index)
                result['rows_common'] = len(shared_idx)
                result['rows_only_rep'] = len(rep_r2.index.difference(api_r2.index))
                result['rows_only_api'] = len(api_r2.index.difference(rep_r2.index))

                # Sample MAD for up to 3 numeric common columns
                num_cols = [c for c in sorted(common_cols)
                            if pd.api.types.is_numeric_dtype(rep_df[c])
                            and pd.api.types.is_numeric_dtype(api_df[c])][:3]
                mad = {}
                for c in num_cols:
                    try:
                        rv = rep_r2.loc[shared_idx, c].astype(float)
                        av = api_r2.loc[shared_idx, c].astype(float)
                        d = (rv - av).abs().mean()
                        mad[c] = float(d) if pd.notna(d) else None
                    except Exception:
                        mad[c] = 'error'
                result['sample_mad'] = mad
    except Exception as e:
        result['row_compare_error'] = str(e)

    return result


def main():
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        uganda = ll.Country('Uganda')

    results = {}

    for parquet_name in PARQUETS:
        feature = parquet_name.replace('.parquet', '')
        print(f"\n{'='*60}\nProcessing: {feature}", flush=True)

        entry = {'feature': feature}

        # Load replication parquet
        repl_path = REPL_DIR / parquet_name
        if not repl_path.exists():
            entry['status'] = 'MISSING_REPL'
            results[feature] = entry
            print(f"  [MISSING] {repl_path}")
            continue

        try:
            rep_df = pd.read_parquet(repl_path, engine='pyarrow')
            entry['rep_shape'] = rep_df.shape
            entry['rep_index'] = list(rep_df.index.names)
            entry['rep_cols'] = sorted(rep_df.columns.tolist())
            entry['rep_hash'] = content_hash(rep_df)
            print(f"  Replication: shape={rep_df.shape}, index={list(rep_df.index.names)}, cols={sorted(rep_df.columns.tolist())[:5]}...")
        except Exception as e:
            entry['status'] = 'REPL_READ_ERROR'
            entry['error'] = str(e)
            results[feature] = entry
            print(f"  [REPL_READ_ERROR] {e}")
            continue

        # Determine API method
        api_info = API_MAP.get(feature, (None, {}, 'unknown'))
        method_name, kwargs, notes = api_info
        entry['method_name'] = method_name
        entry['notes'] = notes

        # Check if replication has 'm' index level
        has_m = 'm' in (rep_df.index.names or [])
        if has_m and method_name:
            # Try with market='Region'
            kwargs = dict(kwargs)
            kwargs['market'] = 'Region'
            entry['kwargs_used'] = "market='Region'"
        else:
            entry['kwargs_used'] = '{}'

        if method_name is None:
            entry['status'] = 'NO_API'
            entry.update({
                'api_shape': None, 'api_index': None, 'api_cols': None,
                'api_hash': None, 'index_match': None, 'cols_match': None,
                'hash_match': None, 'shape_match': None,
            })
            results[feature] = entry
            print(f"  [NO_API] {notes}")
            continue

        # Call API
        try:
            method = getattr(uganda, method_name)
        except AttributeError:
            entry['status'] = 'API_ERROR'
            entry['error'] = f'No method {method_name} on Country'
            results[feature] = entry
            print(f"  [API_ERROR] No method {method_name}")
            continue

        print(f"  Calling uganda.{method_name}({', '.join(f'{k}={v!r}' for k,v in kwargs.items())})...", flush=True)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            api_df, exc = call_with_timeout(method, kwargs, timeout_sec=180)

        if exc is not None:
            if isinstance(exc, TimeoutError):
                entry['status'] = 'API_TIMEOUT'
            else:
                entry['status'] = 'API_ERROR'
            entry['error'] = str(exc)
            # Still record replication stats
            entry['api_shape'] = None
            entry['api_index'] = None
            entry['api_cols'] = None
            entry['api_hash'] = None
            results[feature] = entry
            print(f"  [{'TIMEOUT' if isinstance(exc, TimeoutError) else 'ERROR'}] {exc}")
            continue

        print(f"  API: shape={api_df.shape}, index={list(api_df.index.names)}, cols={sorted(api_df.columns.tolist())[:5]}...")

        # Compare
        cmp = compare_dfs(rep_df, api_df, feature)
        entry.update(cmp)

        # Classify status
        if cmp['hash_match']:
            entry['status'] = 'CLEAN_MATCH'
        elif cmp['cols_match'] and cmp['index_match'] and not cmp['hash_match']:
            entry['status'] = 'CONTENT_DRIFT'
        elif not (cmp['cols_match'] and cmp['index_match']):
            entry['status'] = 'SCHEMA_DRIFT'
        else:
            entry['status'] = 'CONTENT_DRIFT'

        results[feature] = entry
        print(f"  Status: {entry['status']}")

    return results


if __name__ == '__main__':
    results = main()
    out_path = Path('/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/slurm_logs/uganda_replication_drift_2026-04-14/results.json')
    with open(out_path, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nResults saved to {out_path}")
