"""Account for every organic source amount/unit without calling its decoder.

Only safe aggregate facts are exported. Raw frames remain in the private root.
Source semantics checked independently on 2026-10-21 (ledger section 3).
"""
from pathlib import Path
import json
import os
import argparse
import pandas as pd
import pyreadstat
import lsms_library as ll
from lsms_library.local_tools import format_id
from lsms_library.paths import countries_root, data_root
from lsms_library.transformations import _kg_factor_series

checkout = Path(os.environ['PYTHONPATH'].split(os.pathsep)[0]).resolve()
assert Path(ll.__file__).resolve() == checkout / 'lsms_library/__init__.py'
assert countries_root().resolve() == checkout / 'lsms_library/countries'
ap = argparse.ArgumentParser()
ap.add_argument('--manifest', required=True, type=Path)
ap.add_argument('--out', required=True, type=Path)
args = ap.parse_args()
root = data_root()
# Independent source dictionary; no import of Malawi's organic decoder/map.
labels = {2: 'Kilogramme', 10: 'Bucket', 11: 'Wheelbarrow',
          12: 'Ox-Cart', 13: 'Other (Specify)'}
source_labels = {2: 'kilogram', 10: 'bucket', 11: 'wheelbarrow',
                 12: 'ox cart', 13: 'other (specify)'}
expected_parts = []
sources = []
for item in json.loads(args.manifest.read_text()):
    rel = item['source']
    if 'ag_mod_d' not in rel.lower():
        continue
    h = item['md5']
    blob = root / 'dvc-cache' / h[:2] / h[2:]
    _, meta = pyreadstat.read_dta(str(blob), metadataonly=True)
    dictionary = meta.variable_value_labels['ag_d37b']
    assert {int(k): v.casefold() for k, v in dictionary.items()} == source_labels
    used = '2010-11/Data/Panel' not in rel
    record = {'source': rel, 'used_by_current_builder': used,
              'organic_unit_dictionary': dictionary,
              'quantity_label': dict(zip(meta.column_names, meta.column_labels))['ag_d37a'],
              'unit_label': dict(zip(meta.column_names, meta.column_labels))['ag_d37b']}
    if not used:
        sources.append(record)
        continue
    wave = rel.split('/')[3]
    household = ('case_id' if 'Cross_Sectional' in rel or wave == '2010-11'
                 else {'2013-14': 'y2_hhid', '2016-17': 'y3_hhid',
                       '2019-20': 'y4_hhid'}[wave])
    plotcols = ['ag_d00'] if wave in ('2010-11', '2013-14') else ['gardenid', 'plotid']
    columns = [household] + plotcols + ['ag_d36', 'ag_d37a', 'ag_d37b']
    df, _ = pyreadstat.read_dta(str(blob), usecols=columns, apply_value_formats=False)
    df = df.loc[df.ag_d36.eq(1)].copy()
    i = df[household].map(format_id).astype('string')
    if wave == '2016-17' and 'Cross_Sectional' in rel:
        i = 'cs-17-' + i
    plot = df[plotcols[0]].map(format_id).astype('string')
    if len(plotcols) == 2:
        plot += '_' + df[plotcols[1]].map(format_id).astype('string')
    q = pd.to_numeric(df.ag_d37a, errors='coerce').astype('Float64')
    code = pd.to_numeric(df.ag_d37b, errors='coerce').astype('Int64')
    unit = code.map(labels).astype('string')
    # Preserve source identity without letting Country's unrelated food map
    # interpret the bare numeric code (measured collision, dated amendment).
    unit = unit.fillna('Organic unit code ' + code.astype('string')).fillna('Unknown')
    expected_parts.append(pd.DataFrame({'t': wave, 'i': i, 'plot_id': plot,
                                        'Quantity': q, 'u': unit}))
    record.update({'affirmative_rows': len(df), 'positive_amounts': int(q.gt(0).sum()),
                   'zero_amounts': int(q.eq(0).sum()), 'missing_amounts': int(q.isna().sum()),
                   'missing_unit_codes': int(code.isna().sum()),
                   'unit_counts': {str(k): int(v) for k, v in unit.value_counts().items()}})
    sources.append(record)
expected = pd.concat(expected_parts, ignore_index=True)
key = ['t', 'i', 'plot_id']
assert not expected.duplicated(key).any()
observed_parts = []
for wave in expected.t.unique():
    wave_frame = pd.read_parquet(root / 'Malawi' / wave / '_' / 'plot_inputs.parquet')
    org = wave_frame.index.get_level_values('input') == 'Organic Fertilizer'
    part = wave_frame.loc[org].reset_index()
    assert part.Applied.eq(True).all()
    assert part['crop'].isna().all()
    observed_parts.append(part[key + ['Quantity', 'u']])
actual = pd.concat(observed_parts, ignore_index=True)
assert not actual.duplicated(key).any()
for frame in (expected, actual):
    # Raw Stata strings and parquet strings use distinct pandas NA sentinels.
    # Compare identical explicit types after asserting the keys are nonmissing.
    assert frame[key].notna().all().all()
    frame[key] = frame[key].astype(object)
    frame['Quantity'] = frame['Quantity'].astype('Float64')
    frame['u'] = frame['u'].astype('string')
expected = expected.set_index(key).sort_index()
actual = actual.set_index(key).sort_index()
pd.testing.assert_frame_equal(expected, actual, check_exact=True)
served = pd.read_pickle(root / 'cold-served.pkl')
organic = served.index.get_level_values('input') == 'Organic Fertilizer'
unlabelled = served.index.get_level_values('u').astype(str).str.startswith('Organic unit code ')
assert int(unlabelled.sum()) == 6
assert _kg_factor_series(served.loc[unlabelled]).isna().all()
# Reuse sanctioned ID/label finalization on the independently read source
# answers; compare every public quantity/unit at its full resulting key.
raw_expected = expected.reset_index().assign(input='Organic Fertilizer')
raw_expected['crop'] = pd.NA
raw_expected['Applied'] = pd.array([True] * len(raw_expected), dtype='boolean')
raw_expected['Purchased'] = pd.array([pd.NA] * len(raw_expected), dtype='boolean')
raw_expected['Quantity_purchased'] = pd.array([pd.NA] * len(raw_expected), dtype='Float64')
raw_expected['Improved'] = pd.array([pd.NA] * len(raw_expected), dtype='boolean')
raw_expected = raw_expected.set_index(['t', 'i', 'plot_id', 'input', 'crop', 'u'])
country = ll.Country('Malawi')
# Public loading initializes this crosswalk before finalization (country.py
# load_dataframe_with_dvc); preserve that established panel-ID convention.
_ = country.updated_ids
expected_served = country._finalize_result(
    raw_expected, country._materialization_entry('plot_inputs'), 'plot_inputs')
def public_answers(frame):
    out = frame.reset_index()[key + ['Quantity', 'u', 'Applied']].copy()
    out[key] = out[key].astype(object)
    out['u'] = out['u'].astype('string')
    return out.set_index(key).sort_index()
pd.testing.assert_frame_equal(public_answers(expected_served),
                              public_answers(served.loc[organic]), check_exact=True)
result = {'source_headers_checked': len(sources), 'loaded_sources': sum(s['used_by_current_builder'] for s in sources),
          'source_rows': len(expected), 'all_source_rows_unique_at_plot_key': True,
          'every_wave_organic_quantity_and_unit_exactly_matches_source': True,
          'every_public_organic_quantity_unit_and_applied_matches_finalized_source': True,
          'source_qualified_unit_codes_have_no_inferred_kg_factor': True,
          'source_positive_amounts': int(expected.Quantity.gt(0).sum()),
          'source_missing_amounts': int(expected.Quantity.isna().sum()),
          'source_zero_amounts': int(expected.Quantity.eq(0).sum()),
          'source_unit_counts': {str(k): int(v) for k, v in expected.u.value_counts().items()},
          'served_organic_rows': int(organic.sum()),
          'served_positive_amounts': int(served.loc[organic, 'Quantity'].gt(0).sum()),
          'served_missing_amounts': int(served.loc[organic, 'Quantity'].isna().sum()),
          'served_unit_counts': {str(k): int(v) for k, v in served.loc[organic].index.get_level_values('u').value_counts().items()},
          'remaining_native_null_units': int(served.index.get_level_values('u').isna().sum()),
          'remaining_null_units_with_amount': int((served.index.get_level_values('u').isna() & served.Quantity.notna()).sum()),
          'sources': sources}
assert result['served_organic_rows'] == len(expected) == 11075
assert result['served_positive_amounts'] == result['source_positive_amounts'] == 11068
assert result['served_missing_amounts'] == result['source_missing_amounts'] == 7
assert result['served_unit_counts'] == result['source_unit_counts']
args.out.write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({k: v for k, v in result.items() if k != 'sources'}))
