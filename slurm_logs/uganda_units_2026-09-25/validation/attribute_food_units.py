#!/usr/bin/env python3
"""Reproduce Uganda raw-unit, kg-grain and recorded-FCT provenance aggregates.

Adapted from the original scratch split_old_blank.py, extra_compare.py,
attribute_kg_delta.py, quantity_grain_delta.py and check_fct_sources.py.
Inputs are outputs from validate_food_units.py plus versioned survey/config
sources. Raw survey records and household joins remain in memory; only aggregate
JSON and non-microdata copies of versioned Org/CSV inputs are written.
No Country builds, source edits, fixture updates, or upstream FCT API calls.
"""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import subprocess

import numpy as np
import pandas as pd
import pandas.testing as pdt
import lsms_library as ll
from lsms_library.local_tools import df_from_orgfile, format_id, get_dataframe, id_walk

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--checkout', type=Path, required=True)
parser.add_argument('--baseline-ref', required=True)
parser.add_argument('--candidate-ref', required=True)
parser.add_argument('--baseline-dir', type=Path, required=True)
parser.add_argument('--candidate-dir', type=Path, required=True)
parser.add_argument('--baseline-tag', default='base')
parser.add_argument('--candidate-tag', default='candidate')
parser.add_argument('--proposal', type=Path, required=True)
parser.add_argument('--work-dir', type=Path, required=True)
parser.add_argument('--report', type=Path, required=True)
args = parser.parse_args()
ROOT = args.checkout.resolve()
assert Path.cwd().resolve() == ROOT
assert Path(ll.__file__).resolve().is_relative_to(ROOT), ll.__file__
args.work_dir.mkdir(parents=True, exist_ok=True)
COUNTRY = Path('lsms_library/countries/Uganda')
KEYS = ['t', 'i', 'j', 's']


def git_bytes(ref, path):
    return subprocess.check_output(['git', 'show', f'{ref}:{path}'])


def copy_input(ref, rel, name):
    path = args.work_dir / name
    path.write_bytes(git_bytes(ref, COUNTRY / rel))
    return path


def read_frames(directory, tag):
    return {name: pd.read_pickle(directory / f'{tag}_{name}.pkl') for name in
            ['food_acquired', 'food_quantities', 'food_kg_factors', 'nutrition']}


B = read_frames(args.baseline_dir, args.baseline_tag)
C = read_frames(args.candidate_dir, args.candidate_tag)
base_org = copy_input(args.baseline_ref, '_/categorical_mapping.org', 'base-catalog.org')
candidate_org = copy_input(args.candidate_ref, '_/categorical_mapping.org', 'candidate-catalog.org')
u = df_from_orgfile(base_org, name='u')
food = df_from_orgfile(base_org, name='harmonize_food')
new_u = df_from_orgfile(candidate_org, name='u')
new_labels = dict(zip(new_u.Code.astype(int), new_u['Preferred Label'].astype(str)))
blank = u[u['Preferred Label'].isna() | u['Preferred Label'].astype(str).str.strip().eq('')]
codes = set(pd.to_numeric(blank.Code).astype(int))
fmap = {int(r['Code']): str(r['Preferred Label']).strip() for r in food.to_dict('records')
        if pd.notna(r['Preferred Label'])}
proposal = pd.read_csv(args.proposal)
physical = set(proposal.loc[proposal.Physical.astype(str).str.lower().eq('true'), 'Code'].astype(int))
assert all(new_labels[int(r.Code)] == str(r.Proposed_Label) for r in proposal.itertuples())
parts, raw_stats, source_sidecars = [], [], []
# Wave labels come from the versioned baseline catalog, not the current Country registry.
for wave in [column for column in u if column not in ('Code', 'Preferred Label')]:
    rel = COUNTRY / wave / '_/food_acquired.py'
    definitions = {}
    for node in ast.parse(git_bytes(args.baseline_ref, rel).decode()).body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
            if name == 'fn':
                definitions[name] = ast.literal_eval(node.value)
            if name == 'myvars':
                definitions[name] = (ast.literal_eval(node.value) if isinstance(node.value, ast.Dict)
                                     else {kw.arg: ast.literal_eval(kw.value) for kw in node.value.keywords})
    mv = definitions['myvars']
    fn = (ROOT / rel).parent / definitions['fn']
    sidecar = Path(str(fn.resolve()) + '.dvc')
    sidecar_rel = sidecar.relative_to(ROOT)
    assert sidecar.read_bytes() == git_bytes(args.baseline_ref, sidecar_rel), 'Raw source sidecar changed'
    source_sidecars.append({'path': str(sidecar_rel), 'sha256': hashlib.sha256(sidecar.read_bytes()).hexdigest()})
    raw = get_dataframe(fn, convert_categoricals=False)
    selected = raw.loc[raw[mv['units']].isna() | raw[mv['units']].isin(codes)].rename(
        columns={v: k for k, v in mv.items()}).copy()
    measures = [x for x in mv if x not in ['item', 'HHID', 'units']]
    selected = selected.dropna(subset=measures, how='all')
    selected['t'] = wave
    selected['i'] = selected.HHID.map(format_id)
    selected['j'] = selected.item.map(lambda x: fmap.get(x, x))
    selected['raw_code'] = selected.units
    selected['raw_category'] = selected.units.map(
        lambda x: 'missing_raw_unit' if pd.isna(x) else 'zero_code' if x == 0
        else 'promoted_physical_code' if int(x) in physical else 'recorded_code_residual')
    raw_stats.append({'wave': wave, 'selected_wide_rows': len(selected),
                      'by_raw_category': selected.raw_category.value_counts().to_dict()})
    for source, qcols, ecols in [
            ('purchased', ['quantity_home', 'quantity_away'], ['value_home', 'value_away']),
            ('produced', ['quantity_own'], ['value_own']),
            ('inkind', ['quantity_inkind'], ['value_inkind'])]:
        part = selected[['t', 'i', 'j', 'raw_code', 'raw_category']].copy()
        part['s'] = source
        part['Quantity'] = selected[qcols].sum(axis=1, min_count=1)
        part['Expenditure'] = selected[ecols].sum(axis=1, min_count=1)
        part = part.loc[(part.Quantity > 0) | (part.Expenditure > 0)].copy()
        part[['Quantity', 'Expenditure']] = part[['Quantity', 'Expenditure']].replace(0, np.nan)
        parts.append(part)
    print('READ', wave, 'raw rows', len(raw), 'selected wide', len(selected), flush=True)
long = pd.concat(parts, ignore_index=True)
updated = json.loads(git_bytes(args.baseline_ref, COUNTRY / '_/updated_ids.json'))
long = id_walk(long.set_index(KEYS), updated).reset_index()
group = long.groupby(KEYS, dropna=False)
agg = group[['Quantity', 'Expenditure']].sum(min_count=1)
agg['raw_category'] = group.raw_category.agg(lambda x: '|'.join(sorted(set(x))))
agg['raw_codes'] = group.raw_code.agg(lambda x: tuple(sorted({str(v) for v in x})))
base = B['food_acquired']
old = base.loc[base.index.get_level_values('u') == '---'].droplevel(['v', 'u']).reorder_levels(KEYS).sort_index()
agg = agg.sort_index()
agg[['Quantity', 'Expenditure']] = agg[['Quantity', 'Expenditure']].astype(old[['Quantity', 'Expenditure']].dtypes.to_dict())
pdt.assert_frame_equal(old[['Quantity', 'Expenditure']], agg[['Quantity', 'Expenditure']], check_exact=True)
old = old.join(agg[['raw_category', 'raw_codes']])
classification = {
    'baseline_blank_rows': len(old), 'raw_long_rows': len(long), 'matched_keys': len(old),
    'unmatched_served_keys': 0, 'extra_raw_keys': 0,
    'exact_quantity_expenditure_replay': True,
    'category_counts': old.raw_category.value_counts().to_dict(),
    'physical_catalog_codes': len(physical),
    'observed_physical_codes': long.loc[long.raw_category.eq('promoted_physical_code'), 'raw_code'].unique().tolist(),
    'raw_wide_summary': raw_stats,
    'identity_rule': 'existing id_walk with baseline committed updated_ids.json',
}
# Join the old row classification to candidate labels and assert row conservation.
flat = C['food_acquired'].reset_index().set_index(KEYS)
trans = flat.loc[flat.index.isin(old.index)].copy()
trans = trans.join(old[['raw_category', 'raw_codes', 'Quantity', 'Expenditure', 'Price']].rename(
    columns={name: 'old_' + name for name in ['Quantity', 'Expenditure', 'Price']}))
expected = {}
for index, row in old.iterrows():
    if row.raw_category in ('promoted_physical_code', 'recorded_code_residual'):
        unit = new_labels[int(float(row.raw_codes[0]))]
    elif pd.isna(row.Quantity) and pd.notna(row.Expenditure) and row.Expenditure > 0:
        unit = 'Value'
    else:
        unit = 'Unknown'
    expected[index] = unit
trans['expected_u'] = pd.Series(expected).reindex(trans.index).to_numpy()
trans = trans.loc[trans.u.eq(trans.expected_u)].copy()
assert trans.index.is_unique and len(trans) == len(old)
pdt.assert_series_equal(trans.Expenditure.sort_index(), old.Expenditure.sort_index(), check_exact=True)
value = trans.u.eq('Value')
pdt.assert_series_equal(trans.loc[~value, 'Quantity'].sort_index(), old.loc[trans.loc[~value].index, 'Quantity'].sort_index(), check_exact=True)
pdt.assert_series_equal(trans.loc[~value, 'Price'].sort_index(), old.loc[trans.loc[~value].index, 'Price'].sort_index(), check_exact=True)
pdt.assert_series_equal(trans.loc[value, 'Quantity'], trans.loc[value, 'Expenditure'], check_names=False, check_exact=True)
assert trans.loc[value, 'Price'].isna().all()
classification['transitions'] = trans.groupby(['raw_category', 'u']).size().rename('rows').reset_index().to_dict('records')
classification['exact_transition_checks'] = True
# Attribute every changed kg to the classified native row, without mixed-u totals.
bf = B['food_kg_factors']
bf = bf.loc[bf.index.get_level_values('u') == '---'].droplevel(['v', 'u']).reorder_levels(KEYS)
cf = C['food_kg_factors']
trans['kg_before'] = trans.old_Quantity * bf.reindex(trans.index).kg_per_unit
candidate_keys = trans.reset_index().set_index(cf.index.names).index
trans['kg_after'] = trans.Quantity * cf.reindex(candidate_keys).kg_per_unit.to_numpy()
attribution = trans.groupby(['raw_category', 'u']).agg(
    rows=('Quantity', 'size'), kg_before=('kg_before', 'sum'), kg_after=('kg_after', 'sum'))
attribution['kg_delta'] = attribution.kg_after - attribution.kg_before
bq = B['food_quantities']; cq = C['food_quantities']
kg_before = bq.loc[bq.index.get_level_values('u') == 'kg'].Quantity.sum()
kg_after = cq.loc[cq.index.get_level_values('u') == 'kg'].Quantity.sum()
np.testing.assert_allclose(attribution.kg_delta.sum(), kg_after - kg_before, rtol=1e-12, atol=1e-8)
kg_result = {'kg_delta': float(attribution.kg_delta.sum()), 'attribution': attribution.reset_index().to_dict('records')}
unknown = trans.loc[trans.u.eq('Unknown')]
other = base.loc[base.index.get_level_values('u') != '---'].reset_index().set_index(KEYS)
colliding = other.loc[other.index.isin(unknown.index) & other.Quantity.notna()]
grains = {
    'quantity_rows_delta': len(cq) - len(bq),
    'value_rows_added': int((cq.index.get_level_values('u') == 'Value').sum()),
    'unknown_rows_added': int((cq.index.get_level_values('u') == 'Unknown').sum()),
    'kg_rows_delta': int((cq.index.get_level_values('u') == 'kg').sum() - (bq.index.get_level_values('u') == 'kg').sum()),
    'unknown_rows_colliding_with_other_physical_inputs': len(colliding),
    'collision_summaries': [],
}
for index, row in colliding.iterrows():
    grains['collision_summaries'].append({'wave': index[0], 'food': index[2], 'source': index[3],
        'other_native_unit': row.u, 'other_native_quantity': float(row.Quantity),
        'unknown_quantity': float(unknown.loc[index].Quantity)})
assert grains['quantity_rows_delta'] == grains['value_rows_added'] + grains['unknown_rows_added'] + grains['kg_rows_delta']
bn = B['nutrition'].droplevel('v'); cn = C['nutrition'].droplevel('v')
grains['nutrition_all_household_row_delta_sums'] = cn.subtract(bn, fill_value=0).sum().to_dict()
# Reconstruct the committed CSV from versioned Org tables; do not execute Babel.
org = copy_input(args.baseline_ref, '_/nutrition.org', 'base-nutrition.org')
csv = copy_input(args.baseline_ref, '_/fct_uganda.csv', 'base-fct-uganda.csv')
fct = df_from_orgfile(org, name='fct', encoding='ISO-8859-1')
cross = df_from_orgfile(org, name='i_to_fct', encoding='ISO-8859-1')
nut = df_from_orgfile(org, name='fct_n_dict', encoding='ISO-8859-1')
fct['FCT Code'] = pd.to_numeric(fct['FCT Code'], errors='coerce')
fct = fct.set_index('FCT Code')
nut = nut[nut['FCT labels'].str.strip().ne('')]
mapping = dict(zip(nut['FCT labels'], nut['n']))
rows = []
for label, group in cross.groupby('Preferred Label', sort=False):
    code = pd.to_numeric(pd.Series([group['FCT Code'].iloc[-1]]), errors='coerce').iloc[0]
    if pd.notna(code) and code in fct.index:
        row = pd.to_numeric(fct.loc[code, list(mapping)], errors='coerce').rename(index=mapping) * 10
        row.name = label
        rows.append(row)
ref = pd.DataFrame(rows).fillna(0)
actual = pd.read_csv(csv, index_col='i')
idx = actual.index.intersection(ref.index); cols = actual.columns.intersection(ref.columns)
left = actual.loc[idx, cols].apply(pd.to_numeric, errors='coerce'); right = ref.loc[idx, cols]
delta = (left - right).abs()
bad = (delta > 1e-8) | left.isna().ne(right.isna())
assert not bad.any().any(), 'Recorded FCT source disagrees with committed CSV'
fct_result = {'org_foods': len(ref), 'csv_foods': len(actual), 'matched_foods': len(idx),
              'matched_nutrients': len(cols), 'cells': int(bad.size), 'differing_cells': int(bad.sum().sum()),
              'max_abs_delta': float(delta.max().max()),
              'csv_labels_without_org_reconstruction': actual.index.difference(ref.index).tolist(),
              'org_labels_without_csv': ref.index.difference(actual.index).tolist(),
              'source_basis': 'recorded Org per 100g, multiplied by 10 to per kg',
              'tolerance_absolute': 1e-8}
result = {'baseline_ref': args.baseline_ref, 'candidate_ref': args.candidate_ref,
          'import': ll.__file__, 'proposal_sha256': hashlib.sha256(args.proposal.read_bytes()).hexdigest(),
          'source_sidecars': source_sidecars, 'classification': classification,
          'kg_attribution': kg_result, 'quantity_grains': grains, 'fct_source': fct_result,
          'limitations': [
              'Survey records are reread through the library; numeric code identity is checked, not guessed from labels.',
              'Physical versus residual catalog decisions are inputs from the versioned proposal, not independently re-adjudicated.',
              'FCT check reconciles recorded Org inputs and CSV; original lab measurements and 24 USDA additions are not independently refetched.',
              'Fish range endpoint remains the existing parser policy, not an independently measured mass.',
              'No fixture regeneration, published-total matching, or Country cache rebuild is performed by this attribution driver.']}
args.report.write_text(json.dumps(result, indent=2, default=str) + '\n')
print(json.dumps(result, indent=2, default=str))
