"""GH #905: singleton numeric groups, exact legacy sums for every repeated key.

Invented operands only; no Country/Feature/getter/native/cache/Make route.
Prepared for ROOT's separate pure execution, not executed by the source worker.
The reference reconstructs the complete, exact dispatch-base module, without
repinning or modifying the existing #905 reference, fixtures or assertions.
Ledger: tanzania-queue-refill-20261109-v2 §§2–5.
"""
import hashlib
from types import ModuleType

import numpy as np
import pandas as pd
import pytest

from tests.tanzania_crop_marker_oracle import assert_strict
from tests.test_gh905_tanzania_vectorised_sums import (
    SOURCE, load_helper, source_case, stub_maps,
)


BASE_MODULE_SHA256 = '138fd458056125ccb354c620591311756595789277e520e24b05d62ad87b3040'
ORIGINAL_REDUCER = """        df = df.groupby(level=index, dropna=False).agg({
            'Quantity': _sum1,
            'Purchased': 'max', 'Quantity_purchased': _sum1,
            'Unit_purchased': _agreed_unit,
            'Improved': 'max',
        })
"""


def load_dispatch_base():
    """Reverse only the selected block; require the independently frozen hash."""
    source = SOURCE.read_text(encoding='utf-8')
    start = source.index('        # Ledger tanzania-queue-refill-20261109-v2 §5:')
    end = source.index("        df['Unit_purchased'] =", start)
    original = source[:start] + ORIGINAL_REDUCER + source[end:]
    assert hashlib.sha256(original.encode('utf-8')).hexdigest() == BASE_MODULE_SHA256
    module = ModuleType('_tanzania_queue_refill_dispatch_base')
    module.__file__ = str(SOURCE)
    exec(compile(original, str(SOURCE), 'exec'), module.__dict__)
    return module


def inputs(wave, duplicate_values):
    """One repeated organic key beside singletons, NA crop keys and coded seeds.

Use each real script's column names with explicitly invented frame contents.
Quantity and purchased quantity exercise the same un-clamped numeric domain.
The source's used/seed gates and all unit/flag operations remain active.
"""
    _, template, seeds, _ = source_case('plot_inputs', wave=wave)
    rows = []
    singles = [1., -1., -0., 0., np.inf, -np.inf, 5e-324, 1e308, pd.NA, np.nan]
    for household, values in [(20, duplicate_values)] + [
            (100 + pos, [value]) for pos, value in enumerate(singles)]:
        for pos, value in enumerate(values):
            row = dict.fromkeys(template.columns, pd.NA)
            row.update(hhid=household, plot=1, ag3a_41=1, ag3a_42=value,
                       ag3a_43=1 if pos % 2 else 2, ag3a_44=value)
            rows.append(row)
    sec3 = pd.DataFrame(rows, columns=template.columns)
    household = 'sdd_hhid' if wave == '2019-20' else 'y5_hhid'
    plot = 'plotnum' if wave == '2019-20' else 'plot_id'
    sec3 = sec3.rename(columns={'hhid': household, 'plot': plot})
    sec4 = seeds.rename(columns={'hhid': household, 'plot': plot, 'crop': 'cropid'})
    for frame in (sec3, sec4):
        frame.attrs = {'source': 'invented-20261109', 'wave': wave}
    return wave, sec3, sec4, dict(hhid=household, plot=plot, crop='cropid')


@pytest.fixture
def helpers(monkeypatch):
    before, after = load_dispatch_base(), load_helper()
    for module in (before, after):
        stub_maps(module, monkeypatch)
    return before, after


POPULATIONS = [
    pytest.param([1e16, 1., 1., -1e16], id='signed-cancellation'),
    pytest.param([1e16, -1e16, 1., 1.], id='signed-permutation'),
    pytest.param([pd.NA] * 4, id='all-na'),
    pytest.param([pd.NA, 1.5, pd.NA, -0.5], id='partial-na'),
    pytest.param([100., 0.1, 0.2, 1e-12], id='positive-mixed-scale'),
    pytest.param([-0., 0., -0., 0.], id='signed-zero'),
    pytest.param([np.inf, -np.inf, 1., pd.NA], id='infinite-cancellation'),
]


@pytest.mark.parametrize('wave', ['2019-20', '2020-21'])
@pytest.mark.parametrize('duplicate_values', POPULATIONS)
def test_actual_helpers_match_exact_dispatch_base(helpers, wave, duplicate_values):
    before, after = helpers
    args = inputs(wave, duplicate_values)
    old = before.plot_inputs_for_wave(*args)
    new = after.plot_inputs_for_wave(*args)
    # Full axes/domains/codes, EA masks, values, dtypes, row order and typed attrs.
    # This is stronger than the inherited four-ULP bound; never relax that bound.
    assert_strict(new, old)
    for column in ('Quantity', 'Quantity_purchased'):
        a = old[column].dropna().to_numpy(dtype='float64')
        b = new[column].dropna().to_numpy(dtype='float64')
        np.testing.assert_array_equal(np.signbit(a), np.signbit(b))


@pytest.mark.parametrize('wave', ['2019-20', '2020-21'])
def test_no_singleton_numeric_callbacks_and_full_duplicate_operands(
        helpers, monkeypatch, wave):
    before, after = helpers
    args = inputs(wave, [1e16, 1., 1., -1e16])
    original_sum = pd.Series.sum
    calls = []

    def spy(series, *a, **kw):
        if kw.get('min_count') == 1 and series.name in (
                'Quantity', 'Quantity_purchased'):
            calls.append(series.copy(deep=True))
        return original_sum(series, *a, **kw)

    monkeypatch.setattr(pd.Series, 'sum', spy)
    old = before.plot_inputs_for_wave(*args)
    old_calls = calls[:]
    calls.clear()
    new = after.plot_inputs_for_wave(*args)
    new_calls = calls[:]
    assert_strict(new, old)
    assert len(old_calls) == 2 * len(old)
    assert len(new_calls) == 2
    assert all(len(series) == 4 for series in new_calls)
    repeated_calls = [series for series in old_calls if len(series) > 1]
    assert len(repeated_calls) == len(new_calls)
    for legacy, current in zip(repeated_calls, new_calls):
        assert legacy.name == current.name
        pd.testing.assert_series_equal(legacy.reset_index(drop=True),
                                       current.reset_index(drop=True),
                                       check_exact=True)
    # Both complete signed sequences use scalar sum, retaining 0 rather than 2.
    assert new.xs('20', level='i')['Quantity'].tolist() == [0.]
    assert new.xs('20', level='i')['Quantity_purchased'].tolist() == [0.]


@pytest.mark.parametrize('wave', ['2019-20', '2020-21'])
@pytest.mark.parametrize('empty', [False, True])
def test_unique_and_empty_routes_are_unchanged(helpers, wave, empty):
    before, after = helpers
    t, sec3, sec4, colmap = inputs(wave, [1.])
    if empty:
        sec3, sec4 = sec3.iloc[:0], sec4.iloc[:0]
    old = before.plot_inputs_for_wave(t, sec3, sec4, colmap)
    new = after.plot_inputs_for_wave(t, sec3, sec4, colmap)
    assert_strict(new, old)


@pytest.mark.parametrize('wave', ['2019-20', '2020-21'])
def test_unresolved_seed_purchase_unit_conflict_still_refuses(helpers, wave):
    before, after = helpers
    t, sec3, sec4, colmap = inputs(wave, [1., 2.])
    seeds = pd.concat([sec4.iloc[[0]]] * 2, ignore_index=True)
    seeds['cropid'] = pd.array([pd.NA, pd.NA], dtype='Int64')
    seeds['ag4a_10_1'] = pd.array([1., 2.], dtype='Float64')
    seeds['ag4a_10c_1'] = pd.array([1., 2.], dtype='Float64')
    seeds['ag4a_10c_2'] = pd.array([1, 2], dtype='Int64')
    messages = []
    for module in (before, after):
        with pytest.raises(ValueError, match='purchased quantities in different') as caught:
            module.plot_inputs_for_wave(t, sec3.iloc[:0], seeds, colmap)
        messages.append(str(caught.value))
    assert messages[0] == messages[1]
