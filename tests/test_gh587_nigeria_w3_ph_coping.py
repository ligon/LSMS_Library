"""W3's own PH day-count battery must survive beside the original PP answers.

Actual native driver/helper, invented providers only. No Country/Feature,
survey input, cache provider, Make or source acquisition is invoked.
"""
from copy import deepcopy
import importlib.util
from pathlib import Path
import runpy
import sys

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

import lsms_library
import lsms_library.local_tools as tools


ROOT = Path(__file__).resolve().parents[1]
NIGERIA = ROOT / 'lsms_library/countries/Nigeria'
DRIVER = NIGERIA / '2015-16/_/food_coping.py'
PP = '../Data/sect9_plantingw3.dta'
PH = '../Data/sect12_harvestw3.dta'
STRATEGIES = (
    'LessPreferred', 'LimitVariety', 'LimitPortion', 'ReduceMeals',
    'RestrictAdults', 'BorrowFood', 'NoFood', 'SleepHungry', 'WholeDayNoFood',
)
# Independent own-form a-i oracle, not read from the candidate's mapping.
PH_VALUES = ((0, 7), (1, 6), (2, 5), (3, 4), (4, 3),
             (5, 2), (6, 1), (7, 0), (1, 0))


@pytest.fixture
def native(monkeypatch):
    assert Path(lsms_library.__file__).resolve().is_relative_to(ROOT)
    spec = importlib.util.spec_from_file_location('nigeria', NIGERIA / '_/nigeria.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert Path(module.__file__).resolve() == (NIGERIA / '_/nigeria.py').resolve()
    monkeypatch.setitem(sys.modules, 'nigeria', module)
    # Undo the driver's historical sys.path additions after each test too.
    monkeypatch.syspath_prepend(str(DRIVER.parent))
    return module


@pytest.fixture
def frames():
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
    return {PP: pp, PH: ph}


def execute(monkeypatch, native, frames, *, failure=None):
    reads, writes = [], []

    def reader(path, **kwargs):
        assert path in (PP, PH), path
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
    return reads, writes, lambda: runpy.run_path(str(DRIVER), run_name='__main__')


def ph_oracle(values=PH_VALUES):
    records = [
        ('2016Q1', str(hhid), strategy, value)
        for strategy, pair in zip(STRATEGIES, values, strict=True)
        for hhid, value in zip((101, 303), pair, strict=True)
        if not pd.isna(value)
    ]
    frame = pd.DataFrame(records, columns=['t', 'i', 'Strategy', 'Days'])
    frame['Days'] = pd.array(frame['Days'], dtype='Int64')
    return frame.set_index(['t', 'i', 'Strategy']).sort_index()


def test_exact_own_sources_rounds_and_single_writer(monkeypatch, native, frames):
    reads, writes, run = execute(monkeypatch, native, frames)
    run()
    assert reads == [PP, PH]
    assert len(writes) == 1
    assert set(writes[0].index.get_level_values('t')) == {'2015Q3', '2016Q1'}
    assert writes[0].index.is_unique
    assert list(writes[0].columns) == ['Days']


def test_ph_matches_own_nine_item_value_oracle(monkeypatch, native, frames):
    _, writes, run = execute(monkeypatch, native, frames)
    run()
    actual = writes[0].xs('2016Q1', level='t', drop_level=False)
    expected = ph_oracle()
    expected.attrs = deepcopy(frames[PH].attrs)
    assert_frame_equal(actual, expected)


def test_pp_is_exact_unchanged_helper_counterfactual(monkeypatch, native, frames):
    # The former producer did exactly this, before adding any PH input.
    expected = native.food_coping_for_wave('2015Q3', frames[PP])
    _, writes, run = execute(monkeypatch, native, frames)
    run()
    assert_frame_equal(writes[0].xs('2015Q3', level='t', drop_level=False), expected)


def test_inputs_and_nested_attrs_are_not_mutated(monkeypatch, native, frames):
    before = {key: frame.copy(deep=True) for key, frame in frames.items()}
    _, _, run = execute(monkeypatch, native, frames)
    run()
    for key in frames:
        assert_frame_equal(frames[key], before[key])
        assert frames[key].attrs == before[key].attrs


def test_zero_days_survive_and_genuine_missing_does_not_become_zero(
        monkeypatch, native, frames):
    frames[PH].loc[0, 's12q1a'] = pd.NA
    frames[PH].loc[1, 's12q1a'] = 0
    _, writes, run = execute(monkeypatch, native, frames)
    run()
    result = writes[0]
    assert ('2016Q1', '101', 'LessPreferred') not in result.index
    assert result.loc[('2016Q1', '303', 'LessPreferred'), 'Days'] == 0
    assert len(result.xs('2016Q1', level='t')) == 17


@pytest.mark.parametrize('field', ['s12q1' + c for c in 'abcdefghi'])
def test_missing_ph_operand_refuses_before_output(monkeypatch, native, frames, field):
    frames[PH] = frames[PH].drop(columns=field)
    _, writes, run = execute(monkeypatch, native, frames)
    with pytest.raises(ValueError, match='missing PH day-count fields') as exc:
        run()
    assert field in str(exc.value)
    assert writes == []


@pytest.mark.parametrize('path', [PP, PH])
def test_reader_error_is_original_and_no_partial_output(monkeypatch, native, frames, path):
    error = OSError('invented unavailable own input')
    reads, writes, run = execute(monkeypatch, native, frames, failure=(path, error))
    with pytest.raises(OSError) as exc:
        run()
    assert exc.value is error
    assert writes == []
    assert reads == ([PP] if path == PP else [PP, PH])


def test_duplicate_ph_household_is_not_deduplicated(monkeypatch, native, frames):
    frames[PH]['hhid'] = [101, 101]
    _, writes, run = execute(monkeypatch, native, frames)
    with pytest.raises(AssertionError, match='Non-unique'):
        run()
    assert writes == []


def test_all_missing_ph_items_keep_exact_pp(monkeypatch, native, frames):
    for field in ['s12q1' + c for c in 'abcdefghi']:
        frames[PH][field] = pd.array([pd.NA, pd.NA], dtype='Int64')
    expected = native.food_coping_for_wave('2015Q3', frames[PP])
    _, writes, run = execute(monkeypatch, native, frames)
    run()
    assert_frame_equal(writes[0], expected)


def test_ph_does_not_borrow_pp_named_decoy_columns(monkeypatch, native, frames):
    for field in ['s9q1' + c for c in 'abcdefghi']:
        frames[PH][field] = [999, 999]
    frames[PH]['unrelated'] = ['opaque', 'payload']
    _, writes, run = execute(monkeypatch, native, frames)
    run()
    expected = ph_oracle()
    expected.attrs = deepcopy(frames[PH].attrs)
    assert_frame_equal(writes[0].xs('2016Q1', level='t', drop_level=False), expected)


@pytest.mark.parametrize('agree', [True, False])
def test_existing_concat_attrs_agreement_rule(monkeypatch, native, frames, agree):
    if not agree:
        frames[PH].attrs = {'opaque': {'source-policy': ['different']}}
    _, writes, run = execute(monkeypatch, native, frames)
    run()
    assert writes[0].attrs == (frames[PP].attrs if agree else {})
