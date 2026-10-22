"""GH1198: source observations survive before explicit analyst reduction.

Synthetic sources have explicit synthetic release references. Real loader
bindings are checked separately; these frames do not claim survey provenance.
"""
import ast
import copy
import importlib.util
import inspect
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

from lsms_library.paths import countries_root
from lsms_library.transformations import harvest_kg


@pytest.fixture
def ug(monkeypatch):
    path = countries_root() / 'Uganda' / '_' / 'uganda.py'
    spec = importlib.util.spec_from_file_location('uganda_gh1198', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, '_crop_label_map', lambda: {1: 'Maize'})
    monkeypatch.setattr(module, '_harvest_unit_map', lambda: {22: 'sack', 1: 'Kg'})
    monkeypatch.setattr(module, '_harvest_condition_map', lambda: {20: 'fresh'})
    return module


def source(n=2):
    return pd.DataFrame({'hh': ['H'] * n, 'parcel': [1] * n,
                         'plot': [1] * n, 'crop': [1] * n,
                         'record': list(range(1, n + 1)),
                         'qty': [2.] * n, 'unit': [22] * n,
                         'condition': [20] * n, 'factor': [10.] * n,
                         'sold': [3.] * n, 'value': [90.] * n})


def colmap(*, native='record', slots=False):
    condition = {'qty': 'qty', 'unit': 'unit', 'condition': 'condition',
                 'qty_sold': 'sold', 'value_sold': 'value', 'kg_factor': 'factor'}
    if slots:
        condition['production_slot'] = 1
    return {'A': {'hhid': 'hh', 'parcel': 'parcel', 'plot': 'plot',
                  'crop': 'crop', 'production_id': native,
                  'conditions': [condition]}}


def build(ug, raw, cm=None, refs=None):
    # Invoke the real old signature on a parent checkout so counterfactuals
    # fail on observation loss, rather than only an unexpected new keyword.
    kwargs = {}
    if 'source_refs' in inspect.signature(ug.crop_production_for_wave).parameters:
        kwargs['source_refs'] = {'A': 'synthetic:release-A'} if refs is None else refs
    return ug.crop_production_for_wave(
        '2013-14', raw, None, None, cm or colmap(),
        **kwargs)


@pytest.mark.parametrize('kind', ['distinct', 'equal', 'partial', 'sale-only'])
def test_every_eligible_record_retains_its_own_complete_payload(ug, kind):
    raw = source()
    if kind == 'distinct':
        raw.loc[1, ['qty', 'factor', 'sold', 'value']] = [5., 20., 7., 350.]
    elif kind == 'partial':
        raw.loc[1, 'factor'] = np.nan
    elif kind == 'sale-only':
        raw.loc[1, 'qty'] = np.nan
    out = build(ug, raw)
    assert len(out) == 2 and out.index.is_unique
    assert list(out.index.get_level_values('production_obs')) == ['reported:1', 'reported:2']
    for output, field in [('Quantity', 'qty'), ('KgFactor', 'factor'),
                          ('Quantity_sold', 'sold'), ('Value_sold', 'value')]:
        expected = pd.Series(pd.array(raw[field].to_numpy(), dtype='Float64'),
                             index=out.index, name=output)
        pd.testing.assert_series_equal(out[output], expected, check_exact=True, rtol=0, atol=0)
    assert 'production_observation' not in out.attrs


def test_reported_id_is_stable_under_reordering(ug):
    raw = source(3)
    raw['qty'] = [2., 5., 7.]
    first = build(ug, raw)
    second = build(ug, raw.iloc[[2, 0, 1]])
    pd.testing.assert_frame_equal(second, first.iloc[[2, 0, 1]], check_exact=True, rtol=0, atol=0)


@pytest.mark.parametrize('missing', [None, np.nan, pd.NA, '', '  ', '.'])
def test_missing_id_values_use_explicit_physical_fallback(ug, missing):
    raw = source().astype({'record': 'object'})
    raw.loc[1, 'record'] = missing
    out = build(ug, raw)
    assert list(out.index.get_level_values('production_obs')) == [
        'reported:1', 'missing-id:row:synthetic:release-A:1']


def test_named_missing_id_column_is_loud(ug):
    with pytest.raises(ug.CropColmapError, match='production id'):
        build(ug, source().drop(columns='record'))


@pytest.mark.parametrize('equal', [True, False])
def test_repeated_id_suffixes_every_occurrence(ug, equal):
    raw = source()
    raw['record'] = 9
    if not equal:
        raw.loc[1, 'qty'] = 8.
    out = build(ug, raw)
    assert list(out.index.get_level_values('production_obs')) == [
        'reported:9/row:synthetic:release-A:0',
        'reported:9/row:synthetic:release-A:1']
    assert len(out) == 2 and out.index.is_unique
    assert out.Quantity.tolist() == raw.qty.tolist()


def test_physical_positions_precede_filtering_and_ignore_dataframe_labels(ug):
    raw = source(3)
    raw.index = [90, 90, -4]
    raw.loc[raw.index == -4, 'qty'] = 7.
    raw.iloc[1, raw.columns.get_loc('crop')] = 999
    out = build(ug, raw, colmap(native=None))
    assert list(out.index.get_level_values('production_obs')) == [
        'row:synthetic:release-A:0', 'row:synthetic:release-A:2']
    assert out.Quantity.tolist() == [2., 7.]


def test_constructed_release_and_order_semantics(ug):
    raw = source()
    raw['qty'] = [2., 8.]
    cm = colmap(native=None)
    first = build(ug, raw, cm)
    again = build(ug, raw, cm)
    pd.testing.assert_frame_equal(first, again, check_exact=True, rtol=0, atol=0)
    reordered = build(ug, raw.iloc[::-1], cm)
    assert reordered.index.equals(first.index)
    assert reordered.Quantity.tolist() == [8., 2.]
    changed_release = build(ug, raw, cm, {'A': 'synthetic:release-B'})
    assert not changed_release.index.equals(first.index)
    pd.testing.assert_frame_equal(first.droplevel('production_obs'),
                                  changed_release.droplevel('production_obs'),
                                  check_exact=True, rtol=0, atol=0)


def test_actual_condition_slots_remain_distinct_even_with_equal_payload(ug):
    cm = colmap(native=None, slots=True)
    second = copy.deepcopy(cm['A']['conditions'][0])
    second['production_slot'] = 2
    cm['A']['conditions'].append(second)
    out = build(ug, source(1), cm)
    assert list(out.index.get_level_values('production_obs')) == ['slot:1', 'slot:2']
    assert out.Quantity.tolist() == [2., 2.]


def test_repeated_slot_suffixes_all_source_rows(ug):
    out = build(ug, source(), colmap(native=None, slots=True))
    assert list(out.index.get_level_values('production_obs')) == [
        'slot:1/row:synthetic:release-A:0', 'slot:1/row:synthetic:release-A:1']


def test_reordered_condition_declarations_keep_actual_slot_bindings(ug):
    raw = source(1)
    raw['second_qty'] = 7.
    cm = colmap(native=None, slots=True)
    second = copy.deepcopy(cm['A']['conditions'][0])
    second.update(production_slot=2, qty='second_qty')
    cm['A']['conditions'].append(second)
    first = build(ug, raw, cm)
    cm['A']['conditions'].reverse()
    reordered = build(ug, raw, cm)
    assert dict(zip(first.index.get_level_values('production_obs'), first.Quantity)) == {
        'slot:1': 2., 'slot:2': 7.}
    assert dict(zip(reordered.index.get_level_values('production_obs'), reordered.Quantity)) == {
        'slot:1': 2., 'slot:2': 7.}


def test_duplicate_declared_slot_is_loud_even_with_distinct_condition_keys(ug, monkeypatch):
    raw = source(1)
    raw['second_condition'] = 24
    monkeypatch.setattr(ug, '_harvest_condition_map', lambda: {20: 'fresh', 24: 'dried'})
    cm = colmap(native=None, slots=True)
    second = copy.deepcopy(cm['A']['conditions'][0])
    second['condition'] = 'second_condition'
    cm['A']['conditions'].append(second)
    with pytest.raises(ValueError, match='physical source locator'):
        build(ug, raw, cm)


def test_duplicate_physical_source_row_slot_is_an_error(ug):
    raw = source(1)
    cm = colmap()
    cm['B'] = copy.deepcopy(cm['A'])
    with pytest.raises(ValueError, match='physical source locator'):
        ug.crop_production_for_wave('2013-14', raw, raw, None, cm,
                                   source_refs={'A': 'synthetic:same', 'B': 'synthetic:same'})


@pytest.mark.parametrize('reference', [None, '', '  ', 'missing:AGSEC5A.dta'])
def test_constructed_identity_requires_verifiable_explicit_source(ug, reference):
    with pytest.raises(ValueError, match='source'):
        build(ug, source(), colmap(native=None), {'A': reference})


def test_missing_source_reference_does_not_guess_from_wave(ug):
    with pytest.raises(ValueError, match='source'):
        build(ug, source(), colmap(native=None), {})


def test_unique_old_key_with_null_is_retained(ug):
    raw = source(1).astype({'hh': 'object'})
    raw.loc[0, 'hh'] = None
    out = build(ug, raw)
    assert len(out) == 1
    assert out.index.get_level_values('i').isna().all()
    assert out.Quantity.iloc[0] == 2.


def test_duplicate_null_old_keys_keep_the_old_exclusion(ug):
    raw = source().astype({'hh': 'object'})
    raw['hh'] = None
    assert build(ug, raw).empty


def test_nonunique_old_index_keeps_its_existing_null_key_boundary(ug):
    raw = source(3).astype({'hh': 'object'})
    raw.loc[2, 'hh'] = None
    out = build(ug, raw)
    assert len(out) == 2
    assert out.index.get_level_values('i').notna().all()
    assert out.index.get_level_values('production_obs').tolist() == ['reported:1', 'reported:2']


def test_empty_input_needs_no_constructed_identity(ug):
    assert build(ug, source(0), refs={}).empty


def test_conversion_uses_each_observations_own_factor_before_harvest_sum(ug):
    raw = source()
    raw['qty'], raw['factor'] = [2., 3.], [10., 20.]
    out = build(ug, raw)
    harvest = harvest_kg(out)
    assert len(harvest) == 1
    assert harvest.Harvest_kg.iloc[0] == 80.


@pytest.mark.parametrize('wave', ['2009-10', '2010-11', '2011-12', '2013-14',
                                  '2015-16', '2018-19', '2019-20'])
def test_actual_loader_fingerprints_the_same_literal_sources_it_reads(wave):
    path = countries_root() / 'Uganda' / wave / '_' / 'crop_production.py'
    tree = ast.parse(path.read_text())
    reads = {n.targets[0].id[-1].upper(): ast.literal_eval(n.value.args[0])
             for n in tree.body if isinstance(n, ast.Assign)
             and isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name)
             and n.value.func.id == '_try' and n.targets[0].id in ('df5a', 'df5b')}
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Name) and n.func.id == 'crop_production_for_wave']
    assert len(calls) == 1
    refs = next(k.value for k in calls[0].keywords if k.arg == 'source_refs')
    assert {ast.literal_eval(k): ast.literal_eval(v.args[0])
            for k, v in zip(refs.keys, refs.values)} == reads
    assert all(isinstance(v.func, ast.Name) and v.func.id == 'source_fingerprint'
               for v in refs.values)


def test_canonical_axis_has_honest_unresolved_sentinel():
    import lsms_library
    path = Path(lsms_library.__file__).with_name('data_info.yml')
    info = yaml.safe_load(path.read_text())['Index Info']
    assert info['index_info']['crop_production'].endswith('season, production_obs)')
    assert info['missing_level_sentinels']['crop_production']['production_obs'] == 'unresolved_observation'
