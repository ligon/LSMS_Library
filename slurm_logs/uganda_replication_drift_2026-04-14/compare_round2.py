#!/usr/bin/env python3
"""
Uganda replication-package vs. current API drift — ROUND 2.

Functional-equivalence-first methodology. READ-ONLY output; no source
modifications.

Usage:
    PYTHONPATH=. .venv/bin/python slurm_logs/uganda_replication_drift_2026-04-14/compare_round2.py
"""
import os, sys, hashlib, warnings, traceback, signal, json
from pathlib import Path
import pandas as pd
import numpy as np

# Use cached parquets — do NOT force rebuild
# (explicitly unset in case caller set it)
os.environ.pop('LSMS_NO_CACHE', None)

REPL_DIR = Path(os.path.expanduser(
    '~/Projects/RiskSharing_Replication/external_data/LSMS_Library/lsms_library/countries/Uganda/var/'
))

OUT_DIR = Path('/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/slurm_logs/uganda_replication_drift_2026-04-14')

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

# Map: parquet_name -> (method_name, kwargs, notes)
API_MAP = {
    'cluster_features':           ('cluster_features',       {},                    ''),
    'earnings':                   ('earnings',                {'market': 'Region'},  'in data_scheme'),
    'enterprise_income':          ('enterprise_income',       {'market': 'Region'},  'in data_scheme'),
    'fct':                        ('fct',                     {},                    'in data_scheme'),
    'food_acquired':              ('food_acquired',           {'market': 'Region'},  ''),
    'food_expenditures':          ('food_expenditures',       {'market': 'Region'},  'auto-derived'),
    'food_prices':                ('food_prices',             {'market': 'Region'},  'auto-derived'),
    'food_quantities':            ('food_quantities',         {'market': 'Region'},  'auto-derived'),
    'household_characteristics':  ('household_characteristics', {'market': 'Region'}, 'roster-derived'),
    'household_roster':           ('household_roster',        {},                    ''),
    'income':                     ('income',                  {'market': 'Region'},  'in data_scheme'),
    'interview_date':             ('interview_date',          {'market': 'Region'},  ''),
    'locality':                   ('locality',                {'market': 'Region'},  'deprecated'),
    'nutrition':                  ('nutrition',               {'market': 'Region'},  ''),
    'other_features':             (None,                     {},                    'deprecated/removed'),
    'people_last7days':           ('people_last7days',        {'market': 'Region'},  'in data_scheme'),
    'shocks':                     ('shocks',                  {'market': 'Region'},  ''),
}


def content_hash(df):
    try:
        h = pd.util.hash_pandas_object(df, index=True)
        return hashlib.sha256(h.values.tobytes()).hexdigest()[:16]
    except Exception:
        return 'hash_error'


def fingerprint(df):
    return {
        'shape': list(df.shape),
        'index_names': list(df.index.names),
        'columns': sorted(df.columns.tolist()),
        'dtypes': {c: str(t) for c, t in df.dtypes.items()},
        'content_hash': content_hash(df),
    }


def call_with_timeout(fn, kwargs, timeout_sec=240):
    """Call fn(**kwargs) with SIGALRM timeout."""
    def _handler(signum, frame):
        raise TimeoutError(f'Timed out after {timeout_sec}s')
    old = signal.signal(signal.SIGALRM, _handler)
    signal.alarm(timeout_sec)
    try:
        return fn(**kwargs), None
    except Exception as e:
        return None, e
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)


def row_compare(rep_df, api_df, common_idx_names):
    """Return row-comparison dict on the common index."""
    rep_r = rep_df.reset_index()
    api_r = api_df.reset_index()
    rep_keys = [c for c in common_idx_names if c in rep_r.columns]
    api_keys = [c for c in common_idx_names if c in api_r.columns]
    if not rep_keys or not api_keys:
        return {'error': 'no common index keys'}
    rep_r = rep_r.set_index(rep_keys)
    api_r = api_r.set_index(api_keys)

    # Cast index to string to ensure alignment
    rep_r.index = rep_r.index.astype(str) if len(rep_keys) == 1 else pd.MultiIndex.from_arrays(
        [rep_r.index.get_level_values(k).astype(str) for k in rep_keys])
    api_r.index = api_r.index.astype(str) if len(api_keys) == 1 else pd.MultiIndex.from_arrays(
        [api_r.index.get_level_values(k).astype(str) for k in api_keys])

    shared = rep_r.index.intersection(api_r.index)
    out = {
        'rows_common': int(len(shared)),
        'rows_only_rep': int(len(rep_r.index.difference(api_r.index))),
        'rows_only_api': int(len(api_r.index.difference(rep_r.index))),
    }
    common_cols = sorted(set(rep_df.columns) & set(api_df.columns))
    # Coerce numeric cols for MAD
    mad = {}
    for c in common_cols:
        try:
            rv = pd.to_numeric(rep_r.loc[shared, c], errors='coerce') if c in rep_r.columns else None
            av = pd.to_numeric(api_r.loc[shared, c], errors='coerce') if c in api_r.columns else None
            if rv is not None and av is not None and rv.notna().any() and av.notna().any():
                d = (rv - av).abs().mean()
                if pd.notna(d):
                    mad[c] = float(d)
        except Exception:
            pass
    out['sample_mad'] = mad
    total = out['rows_common'] + out['rows_only_rep'] + out['rows_only_api']
    if total > 0 and out['rows_only_rep'] == 0 and out['rows_only_api'] == 0:
        all_zero = all(v == 0.0 for v in mad.values())
        out['success'] = True
    elif out['rows_common'] > 0:
        out['success'] = 'partial'
    else:
        out['success'] = False
    return out


def fe_verdict(success, rows_common, rows_only_rep, rows_only_api):
    if success is True:
        return 'YES'
    elif success == 'partial':
        total = rows_common + rows_only_rep + rows_only_api
        return f'PARTIAL({rows_common}/{total})'
    return 'NO'


def functional_equivalence(feature, rep_df, api_df, entry):
    """
    Progressive FE checks per the task methodology.
    Returns dict with 'verdict' and 'attempts'.
    """
    attempts = []
    verdict = 'NO'

    rep_idx = set(rep_df.index.names) - {None}
    api_idx = set(api_df.index.names) - {None}
    rep_cols = set(rep_df.columns)
    api_cols = set(api_df.columns)

    # ------------------------------------------------------------------
    # Step 1: Dtype coercion — if only precision flavor differs, coerce
    # ------------------------------------------------------------------
    # (applied inline in MAD computation via pd.to_numeric)

    # ------------------------------------------------------------------
    # Step 2: food_expenditures — rename x <-> Expenditure
    # ------------------------------------------------------------------
    if feature == 'food_expenditures':
        try:
            if 'x' in rep_cols and 'Expenditure' in api_cols:
                api_t = api_df.rename(columns={'Expenditure': 'x'})
                stats = row_compare(rep_df, api_t, ['i', 't', 'm', 'j'])
                att = {'transform': "rename API 'Expenditure'->'x'", **stats,
                       'hash_match': content_hash(rep_df) == content_hash(api_t)}
                attempts.append(att)
                v = fe_verdict(stats.get('success'), stats.get('rows_common', 0),
                               stats.get('rows_only_rep', 0), stats.get('rows_only_api', 0))
                if v == 'YES':
                    verdict = 'YES'
                elif verdict == 'NO' and v.startswith('PARTIAL'):
                    verdict = v
        except Exception as e:
            attempts.append({'transform': 'food_expenditures rename', 'error': str(e)})

    # ------------------------------------------------------------------
    # Step 3: household_characteristics — column rename + index reorder
    # ------------------------------------------------------------------
    if feature == 'household_characteristics':
        try:
            col_map = {}
            for c in api_df.columns:
                if c == 'log HSize':
                    col_map[c] = c
                else:
                    parts = c.split(' ', 1)
                    if len(parts) == 2:
                        sex_part, age_part = parts
                        full_sex = 'Females' if sex_part == 'F' else 'Males'
                        age_old = age_part.replace('+', '-99') if age_part.endswith('+') else age_part
                        col_map[c] = f'{full_sex} {age_old}'
                    else:
                        col_map[c] = c
            api_t = api_df.rename(columns=col_map)
            if set(api_df.index.names) == set(rep_df.index.names):
                api_t = api_t.reorder_levels(rep_df.index.names)
            stats = row_compare(rep_df, api_t, ['i', 't', 'm'])
            att = {'transform': "col rename (F/M->Females/Males, 51+->51-99) + index reorder",
                   **stats,
                   'hash_match': content_hash(rep_df) == content_hash(api_t)}
            attempts.append(att)
            v = fe_verdict(stats.get('success'), stats.get('rows_common', 0),
                           stats.get('rows_only_rep', 0), stats.get('rows_only_api', 0))
            if v == 'YES':
                verdict = 'YES'
            elif verdict == 'NO' and v.startswith('PARTIAL'):
                verdict = v
        except Exception as e:
            attempts.append({'transform': 'household_characteristics col/idx transform', 'error': str(e)})

    # ------------------------------------------------------------------
    # Step 4: household_roster — kinship decomp + v-level drop
    # ------------------------------------------------------------------
    if feature == 'household_roster':
        try:
            api_t = api_df.copy()
            if 'v' in api_idx:
                api_t = api_t.reset_index('v', drop=True)
            api_t = api_t.drop(columns=['Generation', 'Distance', 'Affinity'], errors='ignore')
            if 'Relationship' in api_t.columns and 'Relation' in rep_cols:
                api_t = api_t.rename(columns={'Relationship': 'Relation'})
            common_c = sorted(set(rep_df.columns) & set(api_t.columns))
            rep_t = rep_df[common_c]
            api_t = api_t[common_c]
            if api_t.index.duplicated().any():
                api_t = api_t[~api_t.index.duplicated(keep='first')]
            stats = row_compare(rep_t, api_t, ['i', 't', 'pid'])
            # Age MAD
            try:
                rep_r2 = rep_t.reset_index().set_index(['i', 't', 'pid'])
                api_r2 = api_t.reset_index().set_index(['i', 't', 'pid'])
                shared2 = rep_r2.index.intersection(api_r2.index)
                age_mad = (pd.to_numeric(rep_r2.loc[shared2, 'Age'], errors='coerce') -
                           pd.to_numeric(api_r2.loc[shared2, 'Age'], errors='coerce')).abs().mean()
                stats['age_mad'] = float(age_mad) if pd.notna(age_mad) else None
            except Exception:
                stats['age_mad'] = None
            att = {'transform': "drop v + kinship cols, rename Relationship->Relation, compare (i,t,pid)",
                   **stats,
                   'hash_match': content_hash(rep_t) == content_hash(api_t)}
            attempts.append(att)
            v = fe_verdict(stats.get('success'), stats.get('rows_common', 0),
                           stats.get('rows_only_rep', 0), stats.get('rows_only_api', 0))
            if v == 'YES':
                verdict = 'YES'
            elif verdict == 'NO' and v.startswith('PARTIAL'):
                verdict = v
        except Exception as e:
            attempts.append({'transform': 'household_roster kinship+v', 'error': str(e)})

    # ------------------------------------------------------------------
    # Step 5: locality — column rename v -> Parish
    # ------------------------------------------------------------------
    if feature == 'locality':
        try:
            if 'v' in rep_cols and 'Parish' in api_cols:
                rep_t = rep_df.rename(columns={'v': 'Parish'})
                stats = row_compare(rep_t, api_df, ['i', 't', 'm'])
                att = {'transform': "rename rep 'v'->'Parish' to match API",
                       **stats,
                       'hash_match': content_hash(rep_t) == content_hash(api_df)}
                # Check value agreement
                try:
                    rep_r3 = rep_t.reset_index().set_index(['i', 't', 'm'])
                    api_r3 = api_df.reset_index().set_index(['i', 't', 'm'])
                    shared3 = rep_r3.index.intersection(api_r3.index)
                    match_rate = (rep_r3.loc[shared3, 'Parish'].values ==
                                  api_r3.loc[shared3, 'Parish'].values).mean()
                    att['value_match_rate'] = float(match_rate)
                    att['rows_common_compared'] = int(len(shared3))
                except Exception as e2:
                    att['value_match_error'] = str(e2)
                attempts.append(att)
                v = fe_verdict(stats.get('success'), stats.get('rows_common', 0),
                               stats.get('rows_only_rep', 0), stats.get('rows_only_api', 0))
                if v == 'YES':
                    verdict = 'YES'
                elif verdict == 'NO' and v.startswith('PARTIAL'):
                    verdict = v
        except Exception as e:
            attempts.append({'transform': 'locality rename', 'error': str(e)})

    # ------------------------------------------------------------------
    # Step 6: nutrition — compare on common (i,t,m) intersection
    # ------------------------------------------------------------------
    if feature == 'nutrition':
        try:
            common_idx_names = [n for n in rep_df.index.names if n in api_df.index.names]
            common_c = sorted(set(rep_df.columns) & set(api_df.columns))
            if common_idx_names and common_c:
                stats = row_compare(rep_df[common_c], api_df[common_c], common_idx_names)
                att = {'transform': f"compare on common {len(common_c)} columns + {common_idx_names} index",
                       **stats}
                attempts.append(att)
                v = fe_verdict(stats.get('success'), stats.get('rows_common', 0),
                               stats.get('rows_only_rep', 0), stats.get('rows_only_api', 0))
                if v == 'YES':
                    verdict = 'YES'
                elif verdict == 'NO' and v.startswith('PARTIAL'):
                    verdict = v
        except Exception as e:
            attempts.append({'transform': 'nutrition common col compare', 'error': str(e)})

    # ------------------------------------------------------------------
    # Step 7: interview_date — datetime precision coercion
    # ------------------------------------------------------------------
    if feature == 'interview_date':
        try:
            rep_t = rep_df.copy()
            api_t = api_df.copy()
            # Coerce both to datetime64[us] for comparison
            for c in rep_t.columns:
                if 'datetime' in str(rep_t[c].dtype):
                    rep_t[c] = rep_t[c].astype('datetime64[us]')
            for c in api_t.columns:
                if 'datetime' in str(api_t[c].dtype):
                    api_t[c] = api_t[c].astype('datetime64[us]')
            stats = row_compare(rep_t, api_t, ['i', 't', 'm'])
            att = {'transform': 'coerce datetime to [us] precision, compare on (i,t,m)',
                   **stats,
                   'hash_match': content_hash(rep_t) == content_hash(api_t)}
            attempts.append(att)
            v = fe_verdict(stats.get('success'), stats.get('rows_common', 0),
                           stats.get('rows_only_rep', 0), stats.get('rows_only_api', 0))
            if v == 'YES':
                verdict = 'YES'
            elif verdict == 'NO' and v.startswith('PARTIAL'):
                verdict = v
        except Exception as e:
            attempts.append({'transform': 'interview_date datetime coerce', 'error': str(e)})

    # ------------------------------------------------------------------
    # Step 8: food_prices / food_quantities — dtype coerce float
    # ------------------------------------------------------------------
    if feature in ('food_prices', 'food_quantities'):
        try:
            # Coerce all string columns to numeric
            api_t = api_df.copy()
            for c in api_t.columns:
                if str(api_t[c].dtype) in ('string', 'object'):
                    api_t[c] = pd.to_numeric(api_t[c], errors='coerce')
            stats = row_compare(rep_df, api_t, ['i', 't', 'm', 'j', 'u'])
            att = {'transform': 'coerce string->float64 in API, compare (i,t,m,j,u)',
                   **stats,
                   'hash_match': content_hash(rep_df) == content_hash(api_t)}
            attempts.append(att)
            v = fe_verdict(stats.get('success'), stats.get('rows_common', 0),
                           stats.get('rows_only_rep', 0), stats.get('rows_only_api', 0))
            if v == 'YES':
                verdict = 'YES'
            elif verdict == 'NO' and v.startswith('PARTIAL'):
                verdict = v
        except Exception as e:
            attempts.append({'transform': f'{feature} dtype coerce', 'error': str(e)})

    # ------------------------------------------------------------------
    # Step 9: earnings — compare on intersection (check column names)
    # ------------------------------------------------------------------
    if feature == 'earnings':
        try:
            # replication has columns 'earnings', 'level_1'
            # normalize column names to lowercase
            rep_t = rep_df.copy()
            api_t = api_df.copy()
            api_t.columns = [c.lower() for c in api_t.columns]
            common_c = sorted(set(rep_t.columns) & set(api_t.columns))
            if common_c:
                stats = row_compare(rep_t[common_c], api_t[common_c], ['i', 't', 'm'])
                att = {'transform': f"lowercase API cols, compare {common_c} on (i,t,m)",
                       **stats}
                attempts.append(att)
                v = fe_verdict(stats.get('success'), stats.get('rows_common', 0),
                               stats.get('rows_only_rep', 0), stats.get('rows_only_api', 0))
                if v == 'YES':
                    verdict = 'YES'
                elif verdict == 'NO' and v.startswith('PARTIAL'):
                    verdict = v
        except Exception as e:
            attempts.append({'transform': 'earnings col normalize', 'error': str(e)})

    # ------------------------------------------------------------------
    # Step 10: generic — compare on common index + columns
    # ------------------------------------------------------------------
    if verdict == 'NO' and feature not in ('cluster_features', 'other_features'):
        try:
            common_idx_names = [n for n in rep_df.index.names if n in api_df.index.names]
            common_c = sorted(set(rep_df.columns) & set(api_df.columns))
            if common_idx_names and common_c:
                stats = row_compare(rep_df[common_c], api_df[common_c], common_idx_names)
                att = {'transform': f"generic: intersect cols {common_c[:3]}..., align index {common_idx_names}",
                       **stats}
                attempts.append(att)
                v = fe_verdict(stats.get('success'), stats.get('rows_common', 0),
                               stats.get('rows_only_rep', 0), stats.get('rows_only_api', 0))
                if v == 'YES' and verdict == 'NO':
                    verdict = v
                elif verdict == 'NO' and v.startswith('PARTIAL'):
                    verdict = v
        except Exception as e:
            attempts.append({'transform': 'generic common col compare', 'error': str(e)})

    return {'verdict': verdict, 'attempts': attempts}


def compare_dfs(feature, rep_df, api_df):
    """High-level comparison dict."""
    rep_fp = fingerprint(rep_df)
    api_fp = fingerprint(api_df)

    rep_col_set = set(rep_df.columns)
    api_col_set = set(api_df.columns)
    rep_idx_set = set(rep_df.index.names) - {None}
    api_idx_set = set(api_df.index.names) - {None}

    result = {
        'rep_shape': rep_fp['shape'],
        'api_shape': api_fp['shape'],
        'rep_index': rep_fp['index_names'],
        'api_index': api_fp['index_names'],
        'rep_cols': rep_fp['columns'],
        'api_cols': api_fp['columns'],
        'rep_dtypes': rep_fp['dtypes'],
        'api_dtypes': api_fp['dtypes'],
        'rep_hash': rep_fp['content_hash'],
        'api_hash': api_fp['content_hash'],
        'index_match': rep_fp['index_names'] == api_fp['index_names'],
        'cols_match': rep_fp['columns'] == api_fp['columns'],
        'hash_match': rep_fp['content_hash'] == api_fp['content_hash'],
        'shape_match': rep_fp['shape'] == api_fp['shape'],
        'cols_only_rep': sorted(rep_col_set - api_col_set),
        'cols_only_api': sorted(api_col_set - rep_col_set),
        'idx_only_rep': sorted(rep_idx_set - api_idx_set),
        'idx_only_api': sorted(api_idx_set - rep_idx_set),
    }

    # Dtype diffs
    common_cols = rep_col_set & api_col_set
    dtype_diffs = {}
    for c in sorted(common_cols):
        rt = str(rep_df[c].dtype)
        at = str(api_df[c].dtype)
        if rt != at:
            dtype_diffs[c] = {'rep': rt, 'api': at}
    result['dtype_diffs'] = dtype_diffs

    # Quick row comparison on common index+cols
    common_idx_names = [n for n in rep_df.index.names if n in api_df.index.names and n is not None]
    if common_idx_names and common_cols:
        try:
            stats = row_compare(rep_df, api_df, common_idx_names)
            result.update({k: v for k, v in stats.items() if k != 'success'})
        except Exception as e:
            result['row_compare_error'] = str(e)

    # Classify status
    if result['hash_match']:
        result['status'] = 'CLEAN_MATCH'
    elif result['cols_match'] and result['index_match']:
        result['status'] = 'CONTENT_DRIFT'
    else:
        result['status'] = 'SCHEMA_DRIFT'

    # FE checks when needed
    if result['status'] in ('SCHEMA_DRIFT', 'CONTENT_DRIFT') or feature in (
            'food_prices', 'food_quantities', 'interview_date', 'nutrition',
            'earnings', 'enterprise_income', 'income', 'shocks', 'people_last7days', 'fct'):
        fe = functional_equivalence(feature, rep_df, api_df, result)
        result['functional_equivalence'] = fe
    else:
        result['functional_equivalence'] = {'verdict': 'n/a (clean match)', 'attempts': []}

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

        # --- Load replication parquet ---
        repl_path = REPL_DIR / parquet_name
        if not repl_path.exists():
            entry['status'] = 'MISSING_REPL'
            results[feature] = entry
            print(f"  [MISSING] {repl_path}")
            continue

        try:
            rep_df = pd.read_parquet(repl_path, engine='pyarrow')
            if rep_df.empty:
                entry['status'] = 'REPL_READ_ERROR'
                entry['error'] = 'Parquet is empty (0 rows or 0 bytes)'
                results[feature] = entry
                print(f"  [REPL_READ_ERROR] empty file")
                continue
            entry['rep_shape'] = list(rep_df.shape)
            entry['rep_index'] = list(rep_df.index.names)
            entry['rep_cols'] = sorted(rep_df.columns.tolist())
            print(f"  Replication: shape={rep_df.shape}, index={list(rep_df.index.names)}")
        except Exception as e:
            entry['status'] = 'REPL_READ_ERROR'
            entry['error'] = str(e)
            results[feature] = entry
            print(f"  [REPL_READ_ERROR] {e}")
            continue

        # --- API lookup ---
        api_info = API_MAP.get(feature, (None, {}, ''))
        method_name, kwargs, notes = api_info
        entry['method_name'] = method_name
        entry['notes'] = notes
        entry['kwargs_used'] = str(kwargs)

        if method_name is None:
            entry['status'] = 'NO_API'
            results[feature] = entry
            print(f"  [NO_API]")
            continue

        try:
            method = getattr(uganda, method_name)
        except AttributeError:
            entry['status'] = 'API_ERROR'
            entry['error'] = f'No method {method_name} on Country'
            results[feature] = entry
            print(f"  [API_ERROR] No method {method_name}")
            continue

        print(f"  Calling uganda.{method_name}({kwargs})...", flush=True)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            api_df, exc = call_with_timeout(method, kwargs, timeout_sec=240)

        if exc is not None:
            entry['status'] = 'API_TIMEOUT' if isinstance(exc, TimeoutError) else 'API_ERROR'
            entry['error'] = str(exc)
            results[feature] = entry
            print(f"  [{'TIMEOUT' if isinstance(exc, TimeoutError) else 'ERROR'}] {exc}")
            continue

        print(f"  API: shape={api_df.shape}, index={list(api_df.index.names)}, cols={sorted(api_df.columns.tolist())[:5]}")

        cmp = compare_dfs(feature, rep_df, api_df)
        entry.update(cmp)
        print(f"  Status: {entry['status']}")
        if 'functional_equivalence' in entry:
            print(f"  FE verdict: {entry['functional_equivalence']['verdict']}")

        results[feature] = entry

    return results


if __name__ == '__main__':
    results = main()
    out_path = OUT_DIR / 'results_round2.json'
    with open(out_path, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print(f'\nResults saved to {out_path}')
