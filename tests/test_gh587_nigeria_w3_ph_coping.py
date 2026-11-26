"""Each W1-W3 own PH day-count battery must survive beside the original PP answers.

Actual native driver/helper, invented providers only. No Country/Feature,
survey input, cache provider, Make or source acquisition is invoked.
"""
from copy import deepcopy
import importlib.util
from pathlib import Path
from typing import NamedTuple
import runpy
import sys

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

import lsms_library
import lsms_library.local_tools as tools


ROOT = Path(__file__).resolve().parents[1]
NIGERIA = ROOT / 'lsms_library/countries/Nigeria'


class VisitPair(NamedTuple):
    wave: str
    pp_quarter: str
    ph_quarter: str
    pp: str
    ph: str

    @property
    def driver(self):
        return NIGERIA / self.wave / '_/food_coping.py'


@pytest.fixture(params=(
    VisitPair('2010-11', '2010Q3', '2011Q1',
              '../Data/Post Planting Wave 1/Household/sect9_plantingw1.dta',
              '../Data/Post Harvest Wave 1/Household/sect12_harvestw1.dta'),
    VisitPair('2012-13', '2012Q3', '2013Q1',
              '../Data/Post Planting Wave 2/Household/sect9_plantingw2.dta',
              '../Data/Post Harvest Wave 2/Household/sect12_harvestw2.dta'),
    VisitPair('2015-16', '2015Q3', '2016Q1',
              '../Data/sect9_plantingw3.dta', '../Data/sect12_harvestw3.dta'),
), ids=('W1', 'W2', 'W3'))
def visit(request):
    return request.param


STRATEGIES = (
    'LessPreferred', 'LimitVariety', 'LimitPortion', 'ReduceMeals',
    'RestrictAdults', 'BorrowFood', 'NoFood', 'SleepHungry', 'WholeDayNoFood',
)
# Independent own-form a-i oracle, not read from the candidate's mapping.
PH_VALUES = ((0, 7), (1, 6), (2, 5), (3, 4), (4, 3),
             (5, 2), (6, 1), (7, 0), (1, 0))


@pytest.fixture
def native(monkeypatch, visit):
    assert Path(lsms_library.__file__).resolve().is_relative_to(ROOT)
    spec = importlib.util.spec_from_file_location('nigeria', NIGERIA / '_/nigeria.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert Path(module.__file__).resolve() == (NIGERIA / '_/nigeria.py').resolve()
    monkeypatch.setitem(sys.modules, 'nigeria', module)
    # Undo the driver's historical sys.path additions after each test too.
    monkeypatch.syspath_prepend(str(visit.driver.parent))
    return module


@pytest.fixture
def frames(visit):
    pp = pd.DataFrame({'hhid': [101, 202], **{
        's9q1' + c: pd.array([n % 8, (n + 2) % 8], dtype='Int64')
        for n, c in enumerate('abcdefghi')
    }})
    ph = pd.DataFrame({'hhid': [101, 303], **{
        's12q1' + c: pd.array(vals, dtype='Int64')
        for c, vals in zip('abcdefghi', PH_VALUES, strict=True)
    }})
    pp.attrs = {'opaque': {'source-policy': ['unchanged']}}
    ph.attrs = deepcopy(pp.attrs)
    return {visit.pp: pp, visit.ph: ph}


def execute(monkeypatch, native, frames, visit, *, failure=None):
    reads, writes = [], []

    def reader(path, **kwargs):
        assert path in (visit.pp, visit.ph), path
        assert kwargs == {'convert_categoricals': False}
        reads.append(path)
        if failure is not None and path == failure[0]:
            raise failure[1]
        return frames[path]

    def writer(frame, path, **kwargs):
        assert path == 'food_coping.parquet' and kwargs == {}
        writes.append(frame.copy(deep=True))

    monkeypatch.setattr(tools, 'get_dataframe', reader)
    monkeypatch.setattr(tools, 'to_parquet', writer)
    return reads, writes, lambda: runpy.run_path(str(visit.driver), run_name='__main__')


def ph_oracle(visit, values=PH_VALUES):
    records = [
        (visit.ph_quarter, str(hhid), strategy, value)
        for strategy, pair in zip(STRATEGIES, values, strict=True)
        for hhid, value in zip((101, 303), pair, strict=True)
        if not pd.isna(value)
    ]
    frame = pd.DataFrame(records, columns=['t', 'i', 'Strategy', 'Days'])
    frame['Days'] = pd.array(frame['Days'], dtype='Int64')
    return frame.set_index(['t', 'i', 'Strategy']).sort_index()


def test_exact_own_sources_rounds_and_single_writer(monkeypatch, native, frames, visit):
    reads, writes, run = execute(monkeypatch, native, frames, visit)
    run()
    assert reads == [visit.pp, visit.ph]
    assert len(writes) == 1
    assert set(writes[0].index.get_level_values('t')) == {visit.pp_quarter, visit.ph_quarter}
    assert writes[0].index.is_unique
    assert list(writes[0].columns) == ['Days']


def test_ph_matches_own_nine_item_value_oracle(monkeypatch, native, frames, visit):
    _, writes, run = execute(monkeypatch, native, frames, visit)
    run()
    actual = writes[0].xs(visit.ph_quarter, level='t', drop_level=False)
    expected = ph_oracle(visit)
    expected.attrs = deepcopy(frames[visit.ph].attrs)
    assert_frame_equal(actual, expected)


def test_pp_is_exact_unchanged_helper_counterfactual(monkeypatch, native, frames, visit):
    # The former producer did exactly this, before adding any PH input.
    expected = native.food_coping_for_wave(visit.pp_quarter, frames[visit.pp])
    _, writes, run = execute(monkeypatch, native, frames, visit)
    run()
    assert_frame_equal(writes[0].xs(visit.pp_quarter, level='t', drop_level=False), expected)


def test_inputs_and_nested_attrs_are_not_mutated(monkeypatch, native, frames, visit):
    before = {key: frame.copy(deep=True) for key, frame in frames.items()}
    _, _, run = execute(monkeypatch, native, frames, visit)
    run()
    for key in frames:
        assert_frame_equal(frames[key], before[key])
        assert frames[key].attrs == before[key].attrs


def test_zero_days_survive_and_genuine_missing_does_not_become_zero(
        monkeypatch, native, frames, visit):
    frames[visit.ph].loc[0, 's12q1a'] = pd.NA
    frames[visit.ph].loc[1, 's12q1a'] = 0
    _, writes, run = execute(monkeypatch, native, frames, visit)
    run()
    result = writes[0]
    assert (visit.ph_quarter, '101', 'LessPreferred') not in result.index
    assert result.loc[(visit.ph_quarter, '303', 'LessPreferred'), 'Days'] == 0
    assert len(result.xs(visit.ph_quarter, level='t')) == 17


@pytest.mark.parametrize('field', ['s12q1' + c for c in 'abcdefghi'])
def test_missing_ph_operand_refuses_before_output(monkeypatch, native, frames, visit, field):
    frames[visit.ph] = frames[visit.ph].drop(columns=field)
    _, writes, run = execute(monkeypatch, native, frames, visit)
    with pytest.raises(ValueError, match='missing PH day-count fields') as exc:
        run()
    assert field in str(exc.value)
    assert writes == []


@pytest.mark.parametrize('role', ['pp', 'ph'])
def test_reader_error_is_original_and_no_partial_output(monkeypatch, native, frames, visit, role):
    path = getattr(visit, role)
    error = OSError('invented unavailable own input')
    reads, writes, run = execute(monkeypatch, native, frames, visit, failure=(path, error))
    with pytest.raises(OSError) as exc:
        run()
    assert exc.value is error
    assert writes == []
    assert reads == ([visit.pp] if path == visit.pp else [visit.pp, visit.ph])


def test_duplicate_ph_household_is_not_deduplicated(monkeypatch, native, frames, visit):
    frames[visit.ph]['hhid'] = [101, 101]
    _, writes, run = execute(monkeypatch, native, frames, visit)
    with pytest.raises(AssertionError, match='Non-unique'):
        run()
    assert writes == []


def test_all_missing_ph_items_keep_exact_pp(monkeypatch, native, frames, visit):
    for field in ['s12q1' + c for c in 'abcdefghi']:
        frames[visit.ph][field] = pd.array([pd.NA, pd.NA], dtype='Int64')
    expected = native.food_coping_for_wave(visit.pp_quarter, frames[visit.pp])
    _, writes, run = execute(monkeypatch, native, frames, visit)
    run()
    assert_frame_equal(writes[0], expected)


def test_ph_does_not_borrow_pp_named_decoy_columns(monkeypatch, native, frames, visit):
    for field in ['s9q1' + c for c in 'abcdefghi']:
        frames[visit.ph][field] = [999, 999]
    frames[visit.ph]['unrelated'] = ['opaque', 'payload']
    _, writes, run = execute(monkeypatch, native, frames, visit)
    run()
    expected = ph_oracle(visit)
    expected.attrs = deepcopy(frames[visit.ph].attrs)
    assert_frame_equal(writes[0].xs(visit.ph_quarter, level='t', drop_level=False), expected)


@pytest.mark.parametrize('agree', [True, False])
def test_existing_concat_attrs_agreement_rule(monkeypatch, native, frames, visit, agree):
    if not agree:
        frames[visit.ph].attrs = {'opaque': {'source-policy': ['different']}}
    _, writes, run = execute(monkeypatch, native, frames, visit)
    run()
    assert writes[0].attrs == (frames[visit.pp].attrs if agree else {})
