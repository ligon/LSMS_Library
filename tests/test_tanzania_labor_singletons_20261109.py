"""#905 labor: singletons avoid callbacks; every multirow sum stays scalar.

Source-bound invented controls, prepared for ROOT's separate pure execution.
No native source, Country/Feature, provider, cache or Make route is exercised.
Ledger: 905-tanzania-labor-singletons-20261109 §§2–5. Existing references,
fixtures, digest and four-ULP assertions are deliberately untouched.
"""
import ast
import copy
import hashlib
from types import ModuleType

import numpy as np
import pandas as pd
import pytest

from tests.tanzania_crop_marker_oracle import assert_strict, restore_original_function
from tests.test_gh905_tanzania_signed_sums import (
    LEGACY_LIVESTOCK, OLD_PUBLIC_SHA256, VECTORIZED_LIVESTOCK,
)
from tests.test_gh905_tanzania_vectorised_sums import SOURCE, load_helper, source_case


BASE_MODULE_SHA256 = '138fd458056125ccb354c620591311756595789277e520e24b05d62ad87b3040'
BASE_LABOR_SHA256 = '9fcea5ddf45d86206c2873676d229c9a7c106d1768f31d63875f167930a2b9b7'
ORIGINAL_REDUCER = """    if not df.index.is_unique:
        def _sum1(s):
            return s.sum(min_count=1)
        df = df.groupby(level=['t', 'i', 'plot_id', 'source']).agg({
            'PersonDays': _sum1, 'Wage': _sum1,
        })
"""


def labor_function(source):
    return next(node for node in ast.parse(source).body
                if isinstance(node, ast.FunctionDef) and node.name == 'plot_labor_for_wave')


def reduction_branch(source):
    return next(node for node in labor_function(source).body
                if isinstance(node, ast.If)
                and ast.dump(node.test) == ast.dump(ast.parse('not df.index.is_unique', mode='eval').body))


def function_text(source):
    node = labor_function(source)
    return ''.join(source.splitlines(keepends=True)[node.lineno - 1:node.end_lineno])


def source_without_input_reduction():
    """Reverse only the already-reviewed input block for labor source checks."""
    source = SOURCE.read_text(encoding='utf-8')
    marker = '        # Ledger tanzania-queue-refill-20261109-v2 §5:'
    if marker in source:
        from tests.test_tanzania_queue_refill_20261109 import ORIGINAL_REDUCER as input_reducer
        assert source.count(marker) == 1
        start = source.index(marker)
        end = source.index("        df['Unit_purchased'] =", start)
        source = source[:start] + input_reducer + source[end:]
    return source


def dispatch_base_source():
    """Inverse ONLY the selected labor block, requiring the frozen FULL hash.

    This is the exact dispatch-base helper, not a separately implemented
    reduction. The independent historical905 reconstruction is also checked,
    retaining its original digest and byte-identical labor function.
    """
    source = source_without_input_reduction()
    branch = reduction_branch(source)
    lines = source.splitlines(keepends=True)
    original = (''.join(lines[:branch.lineno - 1]) + ORIGINAL_REDUCER
                + ''.join(lines[branch.end_lineno:]))
    assert hashlib.sha256(original.encode()).hexdigest() == BASE_MODULE_SHA256
    assert hashlib.sha256(function_text(original).encode()).hexdigest() == BASE_LABOR_SHA256
    assert original.count(VECTORIZED_LIVESTOCK) == 1
    historical = restore_original_function(original).replace(
        VECTORIZED_LIVESTOCK, LEGACY_LIVESTOCK, 1)
    assert hashlib.sha256(historical.encode()).hexdigest() == OLD_PUBLIC_SHA256
    assert function_text(historical) == function_text(original)
    return original


@pytest.fixture
def helpers():
    module = ModuleType('_tanzania_labor_singletons_dispatch_base')
    module.__file__ = str(SOURCE)
    exec(compile(dispatch_base_source(), str(SOURCE), 'exec'), module.__dict__)
    return module, load_helper()


def assert_labor_equal(actual, expected):
    # Includes complete axis domains/codes/order, dtypes, extension arrays,
    # masks, exact nonmissing numeric values and typed attrs (#1206).
    assert_strict(actual, expected)
    for column in ('PersonDays', 'Wage'):
        a = expected[column].to_numpy(dtype='float64', na_value=np.nan)
        b = actual[column].to_numpy(dtype='float64', na_value=np.nan)
        np.testing.assert_array_equal(np.isfinite(a), np.isfinite(b))
        np.testing.assert_array_equal(np.signbit(a[~np.isnan(a)]),
                                      np.signbit(b[~np.isnan(b)]))
        if np.isfinite(a).any():
            np.testing.assert_array_max_ulp(a[np.isfinite(a)], b[np.isfinite(b)], maxulp=4)


def compare_helpers(helpers, args):
    before, after = helpers
    snapshots = [copy.deepcopy(arg) for arg in args]
    results = []
    for module in (before, after):
        results.append(module.plot_labor_for_wave(*args))
        for current, frozen in zip(args, snapshots):
            if isinstance(current, pd.DataFrame):
                assert_strict(current, frozen)
            else:
                assert type(current) is type(frozen)
                assert current == frozen
    assert_labor_equal(results[1], results[0])
    return results


def inputs(wave, wages, days):
    """One repeated plot beside many singleton plots; both real column maps.

    All values are invented. Day clamps and row sums run in the actual helper;
    signed Wage is left in its real un-clamped numeric domain. Family rows
    exist only where a family day total is reported, including zero.
    """
    _, template, _ = source_case('plot_labor', wave=wave)
    rows = []
    singles = [1., -1., -0., 0., np.inf, -np.inf, 5e-324, 1e308, pd.NA, np.nan]
    groups = [(20, wages, days)] + [
        (100 + pos, [value], [value]) for pos, value in enumerate(singles)]
    for household, group_wages, group_days in groups:
        assert len(group_wages) == len(group_days)
        for wage, day in zip(group_wages, group_days):
            row = dict.fromkeys(template.columns, pd.NA)
            row.update(hhid=household, plot=1, ag3a_73=1,
                       ag3a_74_1a=day, ag3a_74_1d=wage)
            if wave == '2020-21':
                row['ag3a_72c_1'] = day
            else:
                row['ag3a_72b_1'] = 987  # roster ID, never person-days
            rows.append(row)
    raw = pd.DataFrame(rows, columns=template.columns)
    household = 'sdd_hhid' if wave == '2019-20' else 'y5_hhid'
    plot = 'plotnum' if wave == '2019-20' else 'plot_id'
    raw = raw.rename(columns={'hhid': household, 'plot': plot})
    raw.attrs = {'source': 'invented-labor-20261109', 'wave': wave,
                 'tags': ('905', 4), 'nested': {'synthetic': True}}
    return wave, raw, dict(hhid=household, plot=plot)


POPULATIONS = [
    pytest.param([1e16, 1., 1., -1e16], [0.1, 0.2, 0.3, 0.4], id='signed-cancellation'),
    pytest.param([1e16, -1e16, 1., 1.], [0.4, 0.3, 0.2, 0.1], id='signed-permutation'),
    pytest.param([100., 0.1, 0.2, 1e-12], [100., 0.1, 0.2, 1e-12], id='nonnegative-mixed'),
    pytest.param([1.5, pd.NA, 0., 2.25], [1.5, pd.NA, 0., 2.25], id='nullable'),
    pytest.param([pd.NA] * 4, [pd.NA] * 4, id='all-na'),
    pytest.param([-0., 0., -0., 0.], [-0., 0., -0., 0.], id='signed-zero'),
    pytest.param([np.inf, -np.inf, 1., pd.NA], [np.inf, -np.inf, 1., pd.NA], id='infinite-cancellation'),
    pytest.param([np.inf, 1., pd.NA, 2.], [730., 731., pd.NA, 2.], id='positive-infinity'),
    pytest.param([-np.inf, 1., pd.NA, 2.], [-1., 1., pd.NA, 2.], id='negative-infinity'),
    pytest.param([1e308, -1e308, 5e-324, 1e-200], [1e308, -1e308, 5e-324, 1e-200], id='extreme-mixed-scale'),
]


@pytest.mark.parametrize('wave', ['2019-20', '2020-21'])
@pytest.mark.parametrize('wages,days', POPULATIONS)
def test_labor_actual_helpers_match_frozen_dispatch_base(helpers, wave, wages, days):
    args = inputs(wave, wages, days)
    # Nondefault, nonunique raw row labels must not alter positional assembly.
    args[1].index = pd.Index([17] * len(args[1]), name='raw_row')
    old, new = compare_helpers(helpers, args)
    if wave == '2019-20':
        assert set(new.index.get_level_values('source')) == {'hired'}
    else:
        assert new.xs('family', level='source')['Wage'].isna().all()
    assert len(old) > 8  # one collision must exercise many singleton groups


@pytest.mark.parametrize('wave', ['2019-20', '2020-21'])
def test_labor_callbacks_only_reduce_complete_multirow_groups(helpers, monkeypatch, wave):
    args = inputs(wave, [1e16, 1., 1., -1e16], [0.1, 0.2, 0.3, 0.4])
    original_sum = pd.Series.sum
    calls = []

    def spy(series, *a, **kw):
        if kw.get('min_count') == 1 and series.name in ('PersonDays', 'Wage'):
            calls.append(series.copy(deep=True))
        return original_sum(series, *a, **kw)

    monkeypatch.setattr(pd.Series, 'sum', spy)
    old = helpers[0].plot_labor_for_wave(*args)
    old_calls = calls[:]
    calls.clear()
    new = helpers[1].plot_labor_for_wave(*args)
    assert_labor_equal(new, old)
    assert len(old_calls) == 2 * len(old)
    assert len(calls) == (2 if wave == '2019-20' else 4)
    assert all(len(series) == 4 for series in calls)
    repeated = [series for series in old_calls if len(series) > 1]
    assert len(repeated) == len(calls)
    for legacy, current in zip(repeated, calls):
        pd.testing.assert_series_equal(current, legacy, check_exact=True)
        np.testing.assert_array_equal(current.isna().to_numpy(), legacy.isna().to_numpy())
    # Retain the signed counterexample's OLD 0, never the grouped answer 2.
    assert new.xs(('20', 'hired'), level=('i', 'source'))['Wage'].tolist() == [0.]


@pytest.mark.parametrize('wave', ['2019-20', '2020-21'])
def test_labor_unique_route_keeps_unsorted_source_order(helpers, monkeypatch, wave):
    t, raw, colmap = inputs(wave, [3.], [2.])
    raw = raw.iloc[::-1].copy()
    # An isolated missing key survives when no duplicate activates groupby.
    raw.loc[raw.index[0], colmap['hhid']] = None
    original_sum = pd.Series.sum
    calls = []

    def spy(series, *a, **kw):
        if kw.get('min_count') == 1 and series.name in ('PersonDays', 'Wage'):
            calls.append(series.name)
        return original_sum(series, *a, **kw)

    monkeypatch.setattr(pd.Series, 'sum', spy)
    _, new = compare_helpers(helpers, (t, raw, colmap))
    assert new.index.get_level_values('i').isna().sum() == 1
    assert calls == []


@pytest.mark.parametrize('wave', ['2019-20', '2020-21'])
@pytest.mark.parametrize('case', ['empty', 'gated-out', 'missing-day', 'missing-hire',
                                  'missing-household', 'missing-plot'])
def test_labor_exact_legacy_refusals_and_input_conservation(helpers, wave, case):
    t, raw, colmap = inputs(wave, [1., 2.], [1., 2.])
    if case == 'empty':
        raw = raw.iloc[:0].copy()
    elif case == 'gated-out':
        raw['ag3a_73'] = pd.NA
        for column in raw:
            if column.startswith(('ag3a_72c_', 'ag3a_72g_', 'ag3a_72k_')):
                raw[column] = pd.NA
    else:
        column = {'missing-day': 'ag3a_74_1a', 'missing-hire': 'ag3a_73',
                  'missing-household': colmap['hhid'], 'missing-plot': colmap['plot']}[case]
        raw = raw.drop(columns=column)
    frozen, frozen_colmap = raw.copy(deep=True), copy.deepcopy(colmap)
    refusals = []
    for module in helpers:
        with pytest.raises(Exception) as caught:
            module.plot_labor_for_wave(t, raw, colmap)
        refusals.append((type(caught.value), caught.value.args))
        assert_strict(raw, frozen)
        assert colmap == frozen_colmap
    assert refusals[1] == refusals[0]
    if case in ('empty', 'gated-out'):
        # Compare the real legacy refusal; do not invent healthy empty support.
        assert refusals[0] == (ValueError, ('No objects to concatenate',))
    else:
        assert refusals[0][0] is KeyError


@pytest.mark.parametrize('wave', ['2019-20', '2020-21'])
def test_labor_existing_clamps_coercion_and_family_hired_gates(helpers, wave):
    t, template, colmap = inputs(wave, [1.], [1.])
    rows = []
    for household, hire, day, wage in [('boundary', 1, 730., -7.),
                                      ('no-hire', '2', 731., np.inf),
                                      ('family-only', None, '4', '9'),
                                      ('unknown-answer', '3', 0., pd.NA)]:
        row = dict.fromkeys(template.columns, pd.NA)
        row.update({colmap['hhid']: household, colmap['plot']: 1,
                    'ag3a_73': hire, 'ag3a_74_1a': day, 'ag3a_74_1d': wage})
        row.update(ag3a_72b_1=9999, ag3a_72c_bad=700, ag3a_72c_1x=700)
        if wave == '2020-21':
            row.update(ag3a_72c_1=day, ag3a_72g_2=2., ag3a_72k_3=731.)
        rows.append(row)
    rows[0].update(ag3a_74_1b=731., ag3a_74_1c=-1., ag3a_74_2a='3.25',
                   ag3a_74_2b='bad', ag3a_74_2c=np.inf)
    raw = pd.DataFrame(rows)
    raw.attrs = copy.deepcopy(template.attrs)
    _, new = compare_helpers(helpers, (t, raw, colmap))
    hired = new.xs('hired', level='source')
    assert hired.xs('boundary', level='i').iloc[0].tolist() == [733.25, -7.]
    assert hired.xs('no-hire', level='i').iloc[0].tolist() == [0., 0.]
    assert 'family-only' not in hired.index.get_level_values('i')
    assert hired.xs('unknown-answer', level='i')['PersonDays'].tolist() == [0.]
    if wave == '2019-20':
        assert set(new.index.get_level_values('source')) == {'hired'}
    else:
        family = new.xs('family', level='source')
        assert family.xs('boundary', level='i')['PersonDays'].tolist() == [732.]
        assert family.xs('family-only', level='i')['PersonDays'].tolist() == [6.]
        assert family['Wage'].isna().all()


@pytest.mark.parametrize('wave', ['2019-20', '2020-21'])
@pytest.mark.parametrize('only_null_keys', [False, True])
def test_labor_legacy_groupby_sort_dropna_and_domains(helpers, wave, only_null_keys):
    t, raw, colmap = inputs(wave, [1., 2.], [1., 2.])
    if only_null_keys:
        raw = raw.iloc[:2].copy()
        raw[colmap['hhid']] = None
    else:
        extra = raw.iloc[:3].copy()
        extra[colmap['hhid']] = None
        extra[colmap['plot']] = [1, 1, None]
        raw = pd.concat([raw.iloc[::-1], extra], ignore_index=True)
    # A nonempty eligible source with no nonnull group key is distinct from
    # pd.concat([]). Compare its actual legacy outcome, including any refusal.
    frozen = raw.copy(deep=True)
    try:
        old = helpers[0].plot_labor_for_wave(t, raw, colmap)
    except Exception as legacy:
        with pytest.raises(type(legacy)) as caught:
            helpers[1].plot_labor_for_wave(t, raw, colmap)
        assert caught.value.args == legacy.args
    else:
        new = helpers[1].plot_labor_for_wave(t, raw, colmap)
        assert_labor_equal(new, old)
        assert not new.index.get_level_values('i').isna().any()
        assert new.index.is_monotonic_increasing
        if only_null_keys:
            assert len(new) == 0
    assert_strict(raw, frozen)


def test_labor_every_executable_ast_outside_reduction_is_frozen():
    current = source_without_input_reduction()
    original = dispatch_base_source()
    trees = []
    for source in (original, current):
        tree = ast.parse(source)
        function = next(node for node in tree.body
                        if isinstance(node, ast.FunctionDef) and node.name == 'plot_labor_for_wave')
        branch = reduction_branch(source)
        # Replace only this direct statement, preserving the entire rest of
        # the function and every other executable top-level/nested AST node.
        position = next(pos for pos, node in enumerate(function.body)
                        if node.lineno == branch.lineno)
        function.body[position] = ast.Pass()
        trees.append(ast.dump(tree, include_attributes=False))
    assert trees[0] == trees[1]
