"""GH #323: `_normalize_dataframe_index` must not silently collapse a
non-unique DECLARED index with `groupby().first()`.

Two things are under test here:

1. The CLASS-level mechanism.  A country may DECLARE how duplicate index
   tuples collapse, via an `aggregation:` block in its data_scheme.yml.  That
   block predates this fix (SkunkWorks/grain_aggregation_policy.org) but
   *nothing read it* -- it was documentary prose while the code applied
   `.first()` regardless.  These tests pin it as enforcement.

2. The EthiopiaRHS INSTANCE.  Its food roster carries rows that are
   indistinguishable in the canonical grain (data-entry double-punches).  The
   declared reducer is `sum`, so those rows would be DOUBLE-COUNTED if the
   country hook did not drop them first.  The dedup and the sum are a matched
   pair; the test that catches a regression in either is the 1989 case, where
   the source records one line (qty 2.0 / 0.5 Birr) that was punched twice.
"""
import warnings

import pandas as pd
import pytest

from lsms_library.country import _declared_aggregation, _normalize_dataframe_index


def _dup_frame():
    """Two rows colliding on the declared index (t, i, j), carrying DIFFERENT
    quantities -- i.e. real measurements, not duplicates."""
    return pd.DataFrame({
        't': ['1997', '1997'],
        'i': ['h1', 'h1'],
        'j': ['Berbere', 'Berbere'],
        'Quantity': [1.0, 30.0],
        'Expenditure': [2.0, 60.0],
    }).set_index(['t', 'i', 'j'])


# --------------------------------------------------------------------------
# 1. the declared-policy mechanism
# --------------------------------------------------------------------------

def test_declared_aggregation_is_parsed():
    entry = {'index': '(t, i, j)', 'aggregation': {'Quantity': 'sum'}}
    assert _declared_aggregation(entry) == {'Quantity': 'sum'}


def test_declared_aggregation_rejects_unknown_reducer():
    """A typo'd reducer must RAISE, not silently degrade to first() -- which
    would reintroduce exactly the silent loss the block exists to prevent."""
    entry = {'index': '(t, i)', 'aggregation': {'Quantity': 'sumn'}}
    with pytest.raises(ValueError, match='unknown reducer'):
        _declared_aggregation(entry)


def test_declared_sum_is_honoured_not_first():
    """THE class-level regression.  Pre-fix the `aggregation:` block was inert
    and this collapsed to Quantity == 1.0 (the FIRST row), silently destroying
    the 30.0.  It must SUM to 31.0."""
    entry = {'index': '(t, i, j)',
             'aggregation': {'Quantity': 'sum', 'Expenditure': 'sum'}}
    out = _normalize_dataframe_index(_dup_frame(), entry, None, 'some_table')
    assert len(out) == 1
    assert out['Quantity'].iloc[0] == 31.0, 'declared `sum` was ignored'
    assert out['Expenditure'].iloc[0] == 62.0


def test_declared_policy_suppresses_the_data_loss_warning():
    """A DECLARED collapse is intended, so it must not cry wolf."""
    entry = {'index': '(t, i, j)',
             'aggregation': {'Quantity': 'sum', 'Expenditure': 'sum'}}
    with warnings.catch_warnings():
        warnings.simplefilter('error', RuntimeWarning)
        _normalize_dataframe_index(_dup_frame(), entry, None, 'some_table')


def test_undeclared_collapse_still_warns():
    """No policy + colliding rows == the #323 bug.  Stay loud."""
    entry = {'index': '(t, i, j)'}   # no aggregation block
    with pytest.warns(RuntimeWarning, match='GH #323'):
        out = _normalize_dataframe_index(_dup_frame(), entry, None, 'some_table')
    assert len(out) == 1  # historical first() behaviour preserved by default


def test_strict_mode_turns_undeclared_collapse_into_an_error(monkeypatch):
    """The enforcement lever: LSMS_STRICT_INDEX=1 makes an undeclared collapse
    fail loudly instead of silently discarding rows."""
    monkeypatch.setenv('LSMS_STRICT_INDEX', '1')
    entry = {'index': '(t, i, j)'}
    with pytest.raises(ValueError, match='GH #323'):
        _normalize_dataframe_index(_dup_frame(), entry, None, 'some_table')


def test_level_keyed_policy_reduces_rowwise():
    """The pre-existing `{visit: first}` form (Malawi et al.) keeps meaning
    'take the first row' -- honouring it must not change their output."""
    df = pd.DataFrame({
        't': ['2016', '2016'],
        'i': ['h1', 'h1'],
        'visit': [1, 2],
        'Int_t': ['2016-01-01', '2016-06-01'],
    }).set_index(['t', 'i', 'visit'])
    entry = {'index': '(t, i)', 'aggregation': {'visit': 'first'}}
    out = _normalize_dataframe_index(df, entry, None, 'interview_date')
    assert len(out) == 1
    assert out['Int_t'].iloc[0] == '2016-01-01'


# --------------------------------------------------------------------------
# 2. the EthiopiaRHS instance
# --------------------------------------------------------------------------

def test_ethiopiarhs_declares_a_food_acquired_policy():
    """`sum` is only SAFE because the hook dedups first; if someone deletes the
    declaration, the 127 real repeat measurements get destroyed by first()."""
    import yaml
    from lsms_library.paths import countries_root

    ds = yaml.safe_load(
        (countries_root() / 'EthiopiaRHS' / '_' / 'data_scheme.yml')
        .read_text().replace('!make', '')
    )
    agg = ds['Data Scheme']['food_acquired']['aggregation']
    assert agg == {'Quantity': 'sum', 'Expenditure': 'sum'}


def test_ethiopiarhs_drops_double_punched_rows():
    """The 1989 smoking gun: food89.dta rows 865/866 are identical in EVERY
    source column (hh 20120, foodcode 34, qty 2.0, unit 9, value 0.5).  They
    are one measurement punched twice.  Summing them reports 4.0 / 1.0 -- a
    pure overstatement.  Pre-fix this returned 2 rows and the framework summed
    them; the hook must collapse them to 1.
    """
    import importlib.util

    from lsms_library.paths import countries_root

    spec = importlib.util.spec_from_file_location(
        'erhs_mod', countries_root() / 'EthiopiaRHS' / '_' / 'ethiopiarhs.py')
    erhs = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(erhs)

    punched = pd.DataFrame({
        'i': ['20120', '20120'],
        'j': ['Fenugreek (Abish)', 'Fenugreek (Abish)'],
        'u': ['9', '9'],
        's': ['purchased', 'purchased'],
        'Quantity': [2.0, 2.0],
        'Expenditure': [0.5, 0.5],
    })
    out = erhs._drop_double_punched(punched)
    assert len(out) == 1, 'byte-identical double-punch was not dropped'
    assert out['Quantity'].iloc[0] == 2.0
    assert out['Expenditure'].iloc[0] == 0.5


def test_ethiopiarhs_keeps_rows_that_differ_in_measurement():
    """The other half of the matched pair: rows that DIFFER are real repeat
    acquisitions (1997 hh 10_93 produced Berbere 1.0 kg AND 30.0 kg).  The
    dedup must NOT touch them -- the declared `sum` folds them together."""
    import importlib.util

    from lsms_library.paths import countries_root

    spec = importlib.util.spec_from_file_location(
        'erhs_mod', countries_root() / 'EthiopiaRHS' / '_' / 'ethiopiarhs.py')
    erhs = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(erhs)

    real = pd.DataFrame({
        'i': ['10_93', '10_93'],
        'j': ['Berbere', 'Berbere'],
        'u': ['kg', 'kg'],
        's': ['produced', 'produced'],
        'Quantity': [1.0, 30.0],
        'Expenditure': [float('nan'), float('nan')],
    })
    out = erhs._drop_double_punched(real)
    assert len(out) == 2, 'real repeat measurements were destroyed'
