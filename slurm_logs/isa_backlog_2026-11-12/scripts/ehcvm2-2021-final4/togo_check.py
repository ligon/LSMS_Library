"""Togo 2021-22 cold checks on one checkout, after build_food.py in the same root.

usage: python togo_check.py FRAMEDIR OUT_JSON
Aggregates only: counts and sums, no raw rows.
"""
import collections
import json
import os
import sys
import warnings

import numpy as np
import pandas as pd

import lsms_library
CHECKOUT = os.environ['PYTHONPATH'].split(':')[0]
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(CHECKOUT) + os.sep), lsms_library.__file__
from lsms_library import paths
assert os.path.realpath(str(paths.data_root())).startswith('/local/job')
import lsms_library as ll
from lsms_library.derivations import derivation_records
from lsms_library.local_tools import get_dataframe

warnings.simplefilter('ignore')
fdir, out_json = sys.argv[1], sys.argv[2]
W = '2021-22'
P = 'Togo::food_acquired::'
K7 = P + '7day-purchase-at-last-purchase-unit-value'
KD = P + 's07bq03a-regional-factor-divided-out'
KC = P + 's07bq03a-floored-count-recovered'
KW = P + 's07bq03a-floored-count-withheld'
KX = P + 's07bq03a-purchase-removed-by-division'
c = ll.Country('Togo')
recs = derivation_records('Togo')
out = {'checkout': CHECKOUT, 'keys_registered': sorted(k for k in recs if k.startswith(P))}


def counts(s):
    return {str(k): int(v) for k, v in pd.Series(s).value_counts(dropna=False).items()}


fa_all = pd.read_parquet(os.path.join(fdir, 'food_acquired.parquet'))
fa = fa_all[fa_all['t'] == W].copy()
keys = fa['Derivation'].fillna('').astype(str).str.split('+')
pur = fa['s'] == 'purchased'


def keyed(key):
    return keys.apply(lambda ks: key in ks).to_numpy()


served = {
    'rows': int(len(fa)), 'by_s': counts(fa['s']),
    'purchased_quantity_na': int(fa.loc[pur, 'Quantity'].isna().sum()),
    'purchased_expenditure_na': int(fa.loc[pur, 'Expenditure'].isna().sum()),
    'purchased_expenditure_sum': float(fa.loc[pur, 'Expenditure'].sum()),
    'purchased_quantity_sum': float(fa.loc[pur, 'Quantity'].sum()),
    'nonpurchased_quantity_sum': {str(s): float(fa.loc[fa['s'] == s, 'Quantity'].sum())
                                  for s in ('produced', 'inkind')},
    'purchased_quantity_zero': int((fa.loc[pur, 'Quantity'] == 0).sum()),
    'keyed_rows': {k.split('::')[-1]: int(keyed(k).sum()) for k in (K7, KD, KC, KW, KX)},
    'withheld_keyed_nonpurchased': int((keyed(KW) & ~pur.to_numpy()).sum()),
    'withheld_keyed_quantity_notna': int(fa.loc[keyed(KW), 'Quantity'].notna().sum()),
    'withheld_keyed_expenditure_notna': int(fa.loc[keyed(KW), 'Expenditure'].notna().sum()),
    'count_keyed_quantity_le0': int((fa.loc[keyed(KC), 'Quantity'] <= 0).sum()),
    'quantity_na_not_withheld': int((fa['Quantity'].isna().to_numpy() & ~keyed(KW)).sum()),
    'index_unique': bool(not fa.set_index(['t', 'v', 'i', 'j', 'u', 's']).index.duplicated().any()),
}
out['served'] = served
print('served', json.dumps(served)[:2000], flush=True)

# --- the raw file, through the registered functions ---------------------------------
T = paths.countries_root() / 'Togo' / W / 'Data'
s07 = get_dataframe(str(T / 's07b_me_tgo2021.dta'), convert_categoricals=True)
cov = get_dataframe(str(T / 's00_me_tgo2021.dta'), convert_categoricals=True)
reg = cov.set_index(['grappe', 'menage'])['s00q01'].astype('string').str.strip()
region = reg.reindex(pd.MultiIndex.from_arrays([s07['grappe'], s07['menage']])).to_numpy()
recover = recs[KD].resolve()
classify = recs[KW].resolve()
args = (s07['s07bq03a'], s07['s07bq03b'], region, s07['s07bq04'], s07['s07bq05'])
value = np.asarray(recover(*args), dtype=float)
branch = pd.Series(classify(*args), dtype=object).to_numpy()
q3 = pd.to_numeric(s07['s07bq03a'], errors='coerce').to_numpy(dtype=float)
q4 = pd.to_numeric(s07['s07bq04'], errors='coerce').fillna(0).to_numpy()
q5 = pd.to_numeric(s07['s07bq05'], errors='coerce').fillna(0).to_numpy()
filed = q3 - q4 - q5
resid = value - q4 - q5
unit = s07['s07bq03b'].astype('string').str.strip().str.lower()
count = unit.eq('unité').fillna(False).to_numpy()
lome = region == 'Grand Lomé'
og = (q4 > 0) | (q5 > 0)
implied = ((q4 <= 0) & (q5 <= 0)) | (filed > 1e-6)
reg_s = pd.Series(region)
F = {'Centrale': 1.6725, 'Savanes': 1.338, 'Plateaux': 1.2265, 'Kara': 1.0035, 'Maritime': 1.0035,
     'Grand Lomé': 0.9143}
f = reg_s.map(F).astype(float).to_numpy()
q06 = s07['s07bq06'].astype('string').str.strip() if 's07bq06' in s07.columns else pd.Series([pd.NA] * len(s07))
q02 = s07['s07bq02'].astype('string').str.strip() if 's07bq02' in s07.columns else pd.Series([pd.NA] * len(s07))

zero = count & ~lome & (q3 == 0)
# The candidate's rule (ruling 2026-11-12, "NA row only for the 11"): a count that recovers to
# 0 takes the ordinary withheld test, so `implied` above stands.  The previous head's rule
# (`implied | zero`, 84722db39) is reported beside it.
implied_prev_rule = implied | zero
raw = {
    'withheld_implied_prev_rule': int(((branch == 'withheld') & implied_prev_rule).sum()),
    'rows': int(len(s07)), 'branches': counts(branch),
    'branch_count_outside_lome': counts(branch[count & ~lome]),
    'branch_count_lome': counts(branch[count & lome]),
    'count_recovered_value_zero': int(((branch == 'count-recovered') & (value == 0)).sum()),
    'count_recovered_value_le0': int(((branch == 'count-recovered') & (value <= 0)).sum()),
    'recovered_zero_rows': int(zero.sum()),
    'recovered_zero_branch': counts(branch[zero]),
    'recovered_zero_by_region': counts(reg_s[zero]),
    'recovered_zero_own_or_gift': int((zero & og).sum()),
    'recovered_zero_implied': int((zero & implied).sum()),
    'recovered_zero_og_with_q4q5_below_1_over_f': int((zero & og & ((q4 + q5) < 1 / f)).sum()),
    'recovered_zero_s07bq02': counts(q02[zero]),
    'recovered_zero_s07bq06': counts(q06[zero]),
    'recovered_zero_og_s07bq06': counts(q06[zero & og]),
    'recovered_zero_nonog_s07bq06': counts(q06[zero & ~og]),
    'recovered_zero_og_q4_positive': int((zero & (q4 > 0)).sum()),
    'recovered_zero_og_q5_positive': int((zero & (q5 > 0)).sum()),
    'recovered_zero_og_q4q5_values': counts(np.round((q4 + q5)[zero & og], 3)),
    'withheld_implied_total': int(((branch == 'withheld') & implied).sum()),
    'withheld_total': int((branch == 'withheld').sum()),
    'withheld_by_region': counts(reg_s[branch == 'withheld']),
    'withheld_filedpos_not_implied': int(((branch == 'withheld') & (filed > 0) & ~implied).sum()),
    'divided_or_count_value_zero_nonog': int((((branch == 'divided') | (branch == 'count-recovered'))
                                              & (value == 0) & ~og).sum()),
    'noncount_filed_zero_nonog': int((~count & (q3 == 0) & ~og).sum()),
}
rec_b = (branch == 'divided') | (branch == 'count-recovered')
dele = rec_b & (filed > 0) & ~(resid > 0)
raw['deleted'] = {
    'rows': int(dele.sum()),
    'by_branch': counts(branch[dele]),
    'by_s07bq06': counts(q06[dele]),
    'by_region': counts(reg_s[dele]),
    'filed_residual_le_1e-6': int((dele & (filed <= 1e-6)).sum()),
    'resid_exactly_zero': int((dele & (resid == 0)).sum()),
    'resid_negative': int((dele & (resid < 0)).sum()),
    'own_or_gift': int((dele & og).sum()),
}
# Partition of the rows the filed value served a purchased row (filed residual > 0).
fp = filed > 0
raw['filed_positive_partition'] = {
    'filed_positive': int(fp.sum()),
    'recovered_and_served': int((fp & rec_b & (resid > 0)).sum()),
    'deleted': int((fp & dele).sum()),
    'withheld_implied': int((fp & (branch == 'withheld') & implied).sum()),
    'withheld_not_implied': int((fp & (branch == 'withheld') & ~implied).sum()),
    'other': int((fp & ~rec_b & (branch != 'withheld')).sum()),
}
# Approximate FCFA the filed residual carried on the deleted rows, at the last-purchase unit
# value where the purchase unit label and size equal the consumption's (labels as filed,
# after the wave's two-label fold).  Approximate: the hook compares folded codes.
lp_u = s07['s07bq07b'].astype('string').str.strip().replace({'sachet (industrielle)': 'sachet',
                                                            'Sans os en tas': 'Sans os au tas'})
same = ((lp_u.str.lower() == s07['s07bq03b'].astype('string').str.strip().str.lower())
        & (s07['s07bq07c'].astype('string') == s07['s07bq03c'].astype('string'))
        & (pd.to_numeric(s07['s07bq07a'], errors='coerce') > 0)).fillna(False).to_numpy()
uv = (pd.to_numeric(s07['s07bq08'], errors='coerce') / pd.to_numeric(s07['s07bq07a'], errors='coerce')).to_numpy()
raw['deleted']['approx_filed_residual_value_fcfa'] = float(np.nansum(np.where(dele & same, filed * uv, np.nan)))
raw['deleted']['approx_valued_rows'] = int((dele & same & np.isfinite(uv)).sum())
out['raw'] = raw
print('raw', json.dumps(raw, ensure_ascii=False)[:3000], flush=True)

# --- derivation_inputs joins ------------------------------------------------------------
fa_idx = fa.set_index(['t', 'i', 'j'])
pur_keys = collections.Counter(fa_idx[pur.to_numpy()].index)
joins = {}
for key in (K7, KD, KC, KW, KX):
    if key not in recs:
        joins[key.split('::')[-1]] = 'not registered at this checkout'
        continue
    r = {'rows': recs[key].rows if not isinstance(recs[key].rows, dict) else dict(recs[key].rows)}
    try:
        inp = c.derivation_inputs(key, wave=W)
        r['inputs_rows'] = int(len(inp))
        r['inputs_index'] = list(inp.index.names)
        sk = fa_idx[keyed(key)].index
        icount, scount = collections.Counter(inp.index), collections.Counter(sk)
        r['served_keyed'] = int(len(sk))
        r['served_rows_by_input_matches'] = {str(k): v for k, v in sorted(collections.Counter(
            min(icount.get(k, 0), 3) for k in sk).items())}
        r['input_rows_by_served_matches'] = {str(k): v for k, v in sorted(collections.Counter(
            min(scount.get(k, 0), 4) for k in inp.index).items())}
        r['input_keys_absent_from_served_keyed'] = int(sum(1 for k in icount if k not in scount))
        r['served_keyed_absent_from_inputs'] = int(sum(1 for k in scount if k not in icount))
        if key in (KD, KC):
            fn = recs[key].resolve()
            got = np.asarray(fn(inp['s07bq03a'], inp['s07bq03b'], inp['s00q01'], inp['s07bq04'], inp['s07bq05']),
                             dtype=float)
            iq4 = pd.to_numeric(inp['s07bq04'], errors='coerce').fillna(0).to_numpy()
            iq5 = pd.to_numeric(inp['s07bq05'], errors='coerce').fillna(0).to_numpy()
            res = pd.Series(np.clip(got - iq4 - iq5, 0, None), index=inp.index)
            sums_in = res.groupby(level=[0, 1, 2]).sum()
            sums_sv = fa_idx[keyed(key)]['Quantity'].astype(float).groupby(level=[0, 1, 2]).sum()
            both = sums_in.index.intersection(sums_sv.index)
            r['groups_both'] = int(len(both))
            r['groups_sum_equal'] = int((np.abs(sums_in.loc[both] - sums_sv.loc[both]) <= 1e-9 * np.maximum(1, sums_sv.loc[both].abs())).sum())
            r['inputs_value_zero'] = int((got == 0).sum())
            r['factor_filed_over_recovered'] = counts(np.round(pd.to_numeric(inp['s07bq03a']).to_numpy() / got, 4))
        if key == KW:
            fn = recs[key].resolve()
            r['classes'] = counts(fn(inp['s07bq03a'], inp['s07bq03b'], inp['s00q01'], inp['s07bq04'], inp['s07bq05']))
            r['inputs_filed_zero'] = int((pd.to_numeric(inp['s07bq03a']) == 0).sum())
            r['inputs_by_region'] = counts(inp['s00q01'])
            iu = inp['s07bq03b'].astype('string').str.strip().str.lower().eq('unité').fillna(False).to_numpy()
            r['inputs_outside_lome_filed_zero'] = int((iu & (inp['s00q01'].astype('string') != 'Grand Lomé').to_numpy()
                                                      & (pd.to_numeric(inp['s07bq03a']) == 0).to_numpy()).sum())
        if key == KX:
            fn = recs[key].resolve()
            got = np.asarray(fn(inp['s07bq03a'], inp['s07bq03b'], inp['s00q01'], inp['s07bq04'], inp['s07bq05']),
                             dtype=float)
            iq4 = pd.to_numeric(inp['s07bq04'], errors='coerce').fillna(0).to_numpy()
            iq5 = pd.to_numeric(inp['s07bq05'], errors='coerce').fillna(0).to_numpy()
            ifl = pd.to_numeric(inp['s07bq03a'], errors='coerce').to_numpy() - iq4 - iq5
            r['inputs_filed_residual_positive'] = int((ifl > 0).sum())
            r['inputs_recovered_residual_le0'] = int((got - iq4 - iq5 <= 0).sum())
            r['inputs_keys_with_a_served_purchased_row'] = int(sum(1 for k in inp.index if pur_keys.get(k, 0)))
            r['inputs_i_multiset_equals_raw'] = None
            ids = ['E2_' + str(int(g)) + '0' + f'{int(m):02d}' for g, m in zip(s07['grappe'][dele], s07['menage'][dele])]
            r['inputs_i_multiset_equals_raw'] = bool(sorted(map(str, inp.index.get_level_values('i'))) == sorted(ids))
    except Exception as exc:  # noqa: BLE001
        r['error'] = f'{type(exc).__name__}: {exc}'[:2000]
    joins[key.split('::')[-1]] = r
    print(key, json.dumps(r, default=str)[:1500], flush=True)
out['joins'] = joins

# --- the 21 recovered-0 counts, at the served grain (t, i, j) ---------------------------
# Through the wave's own raw-input helper (the withheld key's inputs read it), so the j
# is the served one.  Its first six returns have the same meaning at both heads.
try:
    from lsms_library.derivations import resolve_callable
    helper = resolve_callable('lsms_library.countries.Togo.2021-22._.mapping:_inputs_s07bq03a')
    got = helper(W)
    rr, rbranch, _, rfiled_res, rq4, rq5 = got[:6]
    rq3 = pd.to_numeric(rr['s07bq03a'], errors='coerce').to_numpy(dtype=float)
    ru = rr['s07bq03b'].astype('string').str.strip().str.lower().eq('unité').fillna(False).to_numpy()
    rl = (rr['s00q01'].astype('string') == 'Grand Lomé').fillna(False).to_numpy()
    rz = (rbranch == 'withheld') & ru & ~rl & (rq3 == 0)
    rog = (rq4 > 0) | (rq5 > 0)
    s_lv = fa_idx['s'].to_numpy()
    kw = keyed(KW)
    by_s = {s: set(fa_idx.index[s_lv == s]) for s in ('purchased', 'produced', 'inkind')}
    withheld_keys = set(fa_idx.index[kw])
    zr = {}
    for name, m in (('own_or_gift', rz & rog), ('neither', rz & ~rog)):
        keys_m = list(rr.index[m])
        zr[name] = {
            'rows': int(m.sum()),
            'filed_residual_negative': int((rfiled_res[m] < 0).sum()),
            'with_produced_or_inkind_row': int(sum(1 for k in keys_m if k in by_s['produced'] or k in by_s['inkind'])),
            'with_withheld_keyed_purchased_row': int(sum(1 for k in keys_m if k in withheld_keys)),
            'with_any_purchased_row': int(sum(1 for k in keys_m if k in by_s['purchased'])),
            'with_no_served_row': int(sum(1 for k in keys_m if not any(k in v for v in by_s.values()))),
        }
    out['recovered_zero_served'] = zr
    print('recovered_zero_served', json.dumps(zr), flush=True)
except Exception as exc:  # noqa: BLE001
    out['recovered_zero_served'] = f'{type(exc).__name__}: {exc}'[:2000]

# --- attrs summary of the deletion key --------------------------------------------------
try:
    meta = json.load(open(os.path.join(fdir, 'meta.json')))
    summ = meta['tables']['food_acquired']['attrs'].get('derivations', {})
    out['attrs_summary'] = {k: v for k, v in (summ.get('Togo') or {}).items() if 's07bq03a' in k}
except Exception as exc:  # noqa: BLE001
    out['attrs_summary'] = f'{type(exc).__name__}: {exc}'

json.dump(out, open(out_json, 'w'), indent=1, default=str, ensure_ascii=False)
print('DONE', flush=True)
