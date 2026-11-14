"""derivation_inputs join and re-derivation checks for the new keys, on the candidate.

usage: python join_check.py COUNTRY FRAMEDIR   (FRAMEDIR from build_food.py; same root)
Writes FRAMEDIR/join.json.  Aggregates only.
"""
import json, os, sys, collections, warnings
import numpy as np
import pandas as pd
import lsms_library
CHECKOUT = os.environ['PYTHONPATH'].split(':')[0]
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(CHECKOUT) + os.sep)
from lsms_library import paths
assert os.path.realpath(str(paths.data_root())).startswith('/local/job')
import lsms_library as ll
from lsms_library.derivations import derivation_records
from lsms_library.local_tools import get_dataframe

warnings.simplefilter('ignore')
country, fdir = sys.argv[1], sys.argv[2]
c = ll.Country(country)
NEW = {
    'Togo': ['Togo::food_acquired::s07bq03a-regional-factor-divided-out',
             'Togo::food_acquired::s07bq03a-floored-count-recovered',
             'Togo::food_acquired::s07bq03a-floored-count-withheld',
             'Togo::food_acquired::7day-purchase-at-last-purchase-unit-value',
             'Togo::sample::region-milieu-stratum'],
    'CotedIvoire': ['CotedIvoire::food_acquired::s07bq03a-outlier-replaced-withheld',
                    'CotedIvoire::food_acquired::s07bq03a-partial-triplet-conversion',
                    'CotedIvoire::food_acquired::7day-purchase-at-last-purchase-unit-value',
                    'CotedIvoire::sample::region-milieu-stratum-upper-region',
                    'CotedIvoire::sample::region-milieu-stratum'],
    'Benin': ['Benin::food_acquired::7day-purchase-at-last-purchase-unit-value',
              'Benin::sample::region-milieu-stratum'],
}[country]
recs = derivation_records(country)
out = {}
for key in NEW:
    rec = recs[key]
    table = key.split('::')[1]
    served = pd.read_parquet(os.path.join(fdir, f'{table}.parquet'))
    keyed = served[served['Derivation'].fillna('').astype(str).str.split('+').apply(lambda ks: key in ks)]
    r = {'waves': rec.waves, 'served_keyed_rows': int(len(keyed)),
         'served_keyed_by_s': ({str(k): int(v) for k, v in keyed['s'].value_counts().items()}
                               if 's' in keyed.columns else None)}
    try:
        frames = [c.derivation_inputs(key, wave=w) for w in rec.waves]
        inp = pd.concat(frames)
        names = list(inp.index.names)
        r['inputs_rows'] = int(len(inp))
        r['inputs_index'] = names
        r['inputs_dup_keys'] = int(inp.index.duplicated().sum())
        sk = keyed.set_index(names).index
        ik = inp.index
        icount = collections.Counter(ik)
        scount = collections.Counter(sk)
        per_served = collections.Counter(min(icount.get(k, 0), 2) for k in sk)
        per_input = collections.Counter(min(scount.get(k, 0), 4) for k in ik)
        r['served_rows_by_input_matches'] = {str(k): v for k, v in sorted(per_served.items())}
        r['input_rows_by_served_matches'] = {str(k): v for k, v in sorted(per_input.items())}
        # Re-derivation, where the function reproduces a served column from the inputs.
        fn = rec.resolve()
        if key.endswith(('divided-out', 'count-recovered')):
            rec_q3 = fn(inp['s07bq03a'], inp['s07bq03b'], inp['s00q01'], inp['s07bq04'], inp['s07bq05'])
            q4 = pd.to_numeric(inp['s07bq04'], errors='coerce').fillna(0).to_numpy()
            q5 = pd.to_numeric(inp['s07bq05'], errors='coerce').fillna(0).to_numpy()
            resid = pd.Series(np.clip(rec_q3 - q4 - q5, 0, None), index=ik)
            one = keyed.set_index(names)
            one = one[~one.index.duplicated(keep=False)]
            uniq = resid[~resid.index.duplicated(keep=False)]
            both = one.index.intersection(uniq.index)
            diff = (one.loc[both, 'Quantity'].astype(float) - uniq.loc[both]).abs()
            r['rederive_rows'] = int(len(both))
            r['rederive_max_abs_diff'] = float(diff.max()) if len(diff) else None
            r['factor_filed_over_recovered'] = {str(k): int(v) for k, v in pd.Series(
                np.round(pd.to_numeric(inp['s07bq03a']).to_numpy() / rec_q3, 4)).value_counts().head(8).items()}
        elif key.endswith('withheld') and country == 'Togo':
            r['classes'] = {str(k): int(v) for k, v in pd.Series(
                fn(inp['s07bq03a'], inp['s07bq03b'], inp['s00q01'], inp['s07bq04'], inp['s07bq05'])).value_counts(dropna=False).items()}
            r['served_quantity_all_na'] = bool(keyed['Quantity'].isna().all())
            r['served_expenditure_all_na'] = bool(keyed['Expenditure'].isna().all())
            r['filed_q3_zero'] = int((pd.to_numeric(inp['s07bq03a']) == 0).sum())
            r['by_region'] = {str(k): int(v) for k, v in inp['s00q01'].value_counts().items()}
        elif country == 'CotedIvoire' and table == 'food_acquired' and 's07bq03a-' in key:
            cls = pd.Series(fn(inp['s07bq03a'])).value_counts(dropna=False)
            r['classes'] = {str(k): int(v) for k, v in cls.items()}
            r['served_quantity_all_na'] = bool(keyed['Quantity'].isna().all())
            r['served_expenditure_na_share'] = float(keyed['Expenditure'].isna().mean()) if len(keyed) else None
        elif table == 'sample':
            got = pd.Series([fn(a, b) for a, b in zip(inp['s00q01'], inp['s00q04'])], index=ik)
            one = keyed.set_index(names)['strata']
            both = one.index.intersection(got.index)
            r['rederive_rows'] = int(len(both))
            r['rederive_mismatch'] = int((one.loc[both].astype(str) != got.loc[both].astype(str)).sum())
    except Exception as exc:
        r['error'] = f'{type(exc).__name__}: {exc}'[:2000]
    out[key] = r
    print(key, json.dumps(r, default=str)[:1500], flush=True)

# Togo raw measures: phantom purchases on Jamais rows, before and after.
if country == 'Togo':
    from lsms_library.paths import countries_root
    T = countries_root() / 'Togo' / '2021-22' / 'Data'
    s07 = get_dataframe(str(T / 's07b_me_tgo2021.dta'), convert_categoricals=True)
    cov = get_dataframe(str(T / 's00_me_tgo2021.dta'), convert_categoricals=True)
    reg = cov.set_index(['grappe', 'menage'])['s00q01'].astype('string')
    region = reg.reindex(pd.MultiIndex.from_arrays([s07['grappe'], s07['menage']])).to_numpy()
    fn = recs['Togo::food_acquired::s07bq03a-regional-factor-divided-out'].resolve()
    cl = recs['Togo::food_acquired::s07bq03a-floored-count-withheld'].resolve()
    q3 = pd.to_numeric(s07['s07bq03a']).to_numpy(dtype=float)
    q4 = pd.to_numeric(s07['s07bq04']).fillna(0).to_numpy()
    q5 = pd.to_numeric(s07['s07bq05']).fillna(0).to_numpy()
    rq = fn(s07['s07bq03a'], s07['s07bq03b'], region, s07['s07bq04'], s07['s07bq05'])
    br = cl(s07['s07bq03a'], s07['s07bq03b'], region, s07['s07bq04'], s07['s07bq05'])
    jam = s07['s07bq06'].astype('string').str.strip().eq('Jamais').fillna(False).to_numpy()
    before = jam & (q3 - q4 - q5 > 0)
    after = jam & (rq - q4 - q5 > 0)
    regs = sorted(pd.Series(region).dropna().unique())
    out['raw'] = {
        'rows': int(len(s07)), 'branches': {str(k): int(v) for k, v in pd.Series(br).value_counts(dropna=False).items()},
        'branch_by_region': {r_: {str(k): int(v) for k, v in pd.Series(br[region == r_]).value_counts(dropna=False).items()} for r_ in regs},
        'jamais_rows': int(jam.sum()), 'jamais_phantom_before': int(before.sum()), 'jamais_phantom_after': int(after.sum()),
        'jamais_phantom_by_region': {r_: [int((before & (region == r_)).sum()), int((after & (region == r_)).sum())] for r_ in regs},
        'jamais_phantom_after_min_residual': float(np.nanmin((rq - q4 - q5)[after])) if after.any() else None,
        'recovered_below_q4_plus_q5': int(((rq - q4 - q5) < -1e-6 * np.maximum(1, q4 + q5)).sum()),
        'recovered_below_by_branch': {str(k): int(v) for k, v in pd.Series(br[(rq - q4 - q5) < -1e-6 * np.maximum(1, q4 + q5)]).value_counts().items()},
        'count_recovered_changed': int(((br == 'count-recovered') & (np.abs(rq - q3) > 1e-9)).sum()),
        'lome_og_rows_fit_other_factor': int(((region == 'Grand Lomé') & ((q4 > 0) | (q5 > 0)) & (br == 'divided')
                                              & ~(np.abs(q3 / 1.2 * 100 - np.round(q3 / 1.2 * 100)) <= 1e-6 * np.maximum(1, q3 / 1.2 * 100))
                                              & (np.abs(q3 / 0.9143 * 100 - np.round(q3 / 0.9143 * 100)) <= 1e-6 * np.maximum(1, q3 / 0.9143 * 100))).sum()),
        'lome_og_rows_fit_neither_001': int(((region == 'Grand Lomé') & ((q4 > 0) | (q5 > 0)) & (br == 'divided')
                                             & ~(np.abs(q3 / 1.2 * 100 - np.round(q3 / 1.2 * 100)) <= 1e-6 * np.maximum(1, q3 / 1.2 * 100))
                                             & ~(np.abs(q3 / 0.9143 * 100 - np.round(q3 / 0.9143 * 100)) <= 1e-6 * np.maximum(1, q3 / 0.9143 * 100))).sum()),
        'lome_og_rows': int(((region == 'Grand Lomé') & ((q4 > 0) | (q5 > 0)) & (q3 > 0)).sum()),
    }
    print('raw', json.dumps(out['raw'], ensure_ascii=False)[:3000], flush=True)

json.dump(out, open(os.path.join(fdir, 'join.json'), 'w'), indent=1, default=str, ensure_ascii=False)
print('DONE', flush=True)
