"""Disclosure of the existing nonseed all-payload-NA deletion, without recovery."""
import importlib.util

import pandas as pd
import pytest

from lsms_library.derivations import derivation_records
from lsms_library.paths import countries_root

KEY = 'Malawi::plot_inputs::nonseed-group-no-payload'
SEED = 'Malawi::plot_inputs::seed-line-no-payload'


@pytest.fixture(scope='module')
def binding():
    path = countries_root() / 'Malawi/2010-11/_/mapping.py'
    spec = importlib.util.spec_from_file_location('malawi_nonseed_1174', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _raw(rows):
    base = dict(case_id='a', ag_d00='p1', ag_d38=pd.NA,
                ag_d39a=2, ag_d39d=pd.NA, ag_d39f=pd.NA, ag_d39i=pd.NA,
                ag_d36=2, ag_d40=2, ag_d41a=pd.NA, ag_d41d=pd.NA)
    return pd.DataFrame([{**base, **row} for row in rows])


def _inputs(binding, monkeypatch, raw):
    from lsms_library import local_tools
    seen = []

    def read(path, **kwargs):
        seen.append((path, kwargs))
        return raw.copy(deep=True)

    monkeypatch.setattr(local_tools, 'get_dataframe', read)
    before = raw.copy(deep=True)
    result = binding.inputs_nonseed_group_no_payload('2010-11')
    pd.testing.assert_frame_equal(raw, before, check_exact=True)
    assert len(seen) == 1
    assert seen[0][0].endswith('/2010-11/Data/Full_Sample/Agriculture/ag_mod_d.dta')
    assert seen[0][1] == {'convert_categoricals': False}
    return result


def test_registry_documents_both_deletions_and_applied():
    records = derivation_records('Malawi')
    record = records[KEY]
    assert record['rows'] == 0
    assert record['waves'] == ['2010-11']
    assert 'Applied' in record['columns']
    assert 'Applied' in records[SEED]['columns']
    assert callable(record.resolve('function'))
    assert callable(record.resolve('inputs'))


@pytest.mark.parametrize('column,value', [('Quantity', 0), ('Quantity', 3),
                                         ('Applied', False), ('Applied', True),
                                         ('Purchased', False), ('Improved', False)])
def test_every_observed_payload_prevents_deletion(binding, column, value):
    index = pd.MultiIndex.from_tuples([('2010-11', 'DAP Fertilizer')], names=['t', 'input'])
    frame = pd.DataFrame({'Quantity': [pd.NA], 'Applied': [pd.NA]}, index=index)
    assert binding.derive_nonseed_group_no_payload(frame).tolist() == [True]
    frame[column] = value
    assert binding.derive_nonseed_group_no_payload(frame).tolist() == [False]


def test_predicate_does_not_absorb_seed_or_other_wave(binding):
    index = pd.MultiIndex.from_tuples([('2010-11', 'Seed'), ('2013-14', 'DAP Fertilizer')],
                                     names=['t', 'input'])
    frame = pd.DataFrame({'Quantity': [pd.NA, pd.NA]}, index=index)
    assert not binding.derive_nonseed_group_no_payload(frame).any()


def test_two_slots_return_one_original_line(binding, monkeypatch):
    raw = _raw([dict(ag_d39f=2, ag_d39b=11.5, original_text='  native  ')])
    out = _inputs(binding, monkeypatch, raw)
    assert len(out) == 1
    assert list(out.index.names) == ['t', 'i', 'input_obs']
    pd.testing.assert_frame_equal(out[raw.columns].reset_index(drop=True), raw,
                                  check_exact=True)
    assert out.index.get_level_values('input_obs')[0].endswith(':0')
    assert out.ag_d39b.iloc[0] == 11.5  # exposed raw, never substituted for total


def test_multiple_contributors_keep_all_release_positions(binding, monkeypatch):
    raw = _raw([dict(ag_d39b=1), dict(ag_d39b=2)])
    raw.index = [91, 91]  # DataFrame labels are not release-row coordinates
    out = _inputs(binding, monkeypatch, raw)
    assert len(out) == 2
    assert out.index.is_unique
    assert [x.rsplit(':', 1)[1] for x in out.index.get_level_values('input_obs')] == ['0', '1']
    assert out.ag_d39b.tolist() == [1, 2]


def test_an_answer_elsewhere_in_the_group_prevents_every_line_deletion(binding, monkeypatch):
    raw = _raw([dict(ag_d39b=1), dict(ag_d39d=4)])
    assert _inputs(binding, monkeypatch, raw).empty


def test_same_plot_other_input_does_not_leak_into_replay(binding, monkeypatch):
    raw = _raw([dict(native_text='deleted DAP'), dict(ag_d39a=4, ag_d39d=8,
                                                   native_text='retained Urea')])
    out = _inputs(binding, monkeypatch, raw)
    assert out.native_text.tolist() == ['deleted DAP']


def test_same_household_other_plot_does_not_leak_into_replay(binding, monkeypatch):
    raw = _raw([dict(ag_d00='p1'), dict(ag_d00='p2', ag_d39d=8)])
    out = _inputs(binding, monkeypatch, raw)
    assert out.ag_d00.tolist() == ['p1']


@pytest.mark.parametrize('answer', [1, 2])
def test_use_answer_preserves_blank_total_without_raw_quantity_recovery(binding, monkeypatch, answer):
    raw = _raw([dict(ag_d38=answer, ag_d39b=12)])
    assert _inputs(binding, monkeypatch, raw).empty


def test_full_raw_repeat_uses_first_release_position(binding, monkeypatch):
    raw = _raw([{}, {}])
    out = _inputs(binding, monkeypatch, raw)
    assert len(out) == 1
    assert out.index.get_level_values('input_obs')[0].endswith(':0')


def test_one_line_with_two_deleted_groups_is_returned_once(binding, monkeypatch):
    out = _inputs(binding, monkeypatch, _raw([dict(ag_d39f=4)]))
    assert len(out) == 1


def test_no_input_types_returns_empty_original_schema(binding, monkeypatch):
    raw = _raw([dict(ag_d39a=pd.NA)])
    out = _inputs(binding, monkeypatch, raw)
    assert out.empty
    assert set(raw.columns) <= set(out.columns)
    assert list(out.index.names) == ['t', 'i', 'input_obs']


def test_wrong_wave_is_refused_before_source_read(binding, monkeypatch):
    from lsms_library import local_tools
    monkeypatch.setattr(local_tools, 'get_dataframe', lambda *a, **k: pytest.fail('source read'))
    with pytest.raises(ValueError, match='2010-11'):
        binding.inputs_nonseed_group_no_payload('2013-14')
