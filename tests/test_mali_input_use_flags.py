"""Mali plot_inputs: the survey's plot-level GROUP use answers are served.

The EAC-I instrument asks "used inorganic fertilizer on this plot?" and
"used phytosanitary products on this plot?" once per plot, then a quantity
per type.  The answers are served as their own rows (input
'Inorganic Fertilizer?' / 'Phytosanitary products?') with ``Applied``
True/False, no quantity and ``u = Unknown`` (maintainer decision on GH #1118,
2026-10-22).  A label ending in '?' is a use answer, which ``nitrogen_kg``
skips (``transformations.is_use_answer_label``).
The module is loaded from THIS checkout's source file.
"""
import importlib.util
from pathlib import Path

import pandas as pd
import pytest
import lsms_library as ll
from lsms_library.transformations import U_UNKNOWN


@pytest.fixture
def mali(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    assert Path(ll.__file__).resolve().is_relative_to(root), ll.__file__
    source = root / 'lsms_library/countries/Mali/_/mali.py'
    spec = importlib.util.spec_from_file_location('mali_use_flags_source', source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # The categorical helpers resolve tables from the native script cwd.
    monkeypatch.chdir(source.parent)
    return module


def row(input_code, **changes):
    result = dict(t='2014-15', i='h1', plot_id='1_1', input=input_code, crop=pd.NA,
                  u=pd.NA, Quantity=pd.NA, Quantity_purchased=pd.NA,
                  Purchased=pd.NA, Improved=pd.NA)
    result.update(changes)
    return result


@pytest.mark.parametrize('code, label', [('inorganic_fertilizer', 'Inorganic Fertilizer?'),
                                         ('phytosanitary', 'Phytosanitary products?')])
@pytest.mark.parametrize('answer', [True, False])
def test_group_answer_is_served_yes_and_no(mali, code, label, answer):
    got = mali.plot_inputs_finalize(pd.DataFrame([row(code, Applied=answer)]))
    assert len(got) == 1
    assert got.index.get_level_values('input').tolist() == [label]
    assert got['Applied'].tolist() == [answer]
    assert got['Quantity'].isna().all()
    assert got['u'].tolist() == [U_UNKNOWN]
    assert str(got['Applied'].dtype) == 'boolean'


def test_missing_group_answer_carries_no_content(mali):
    got = mali.plot_inputs_finalize(pd.DataFrame([row('inorganic_fertilizer', Applied=pd.NA)]))
    assert len(got) == 0


def test_type_rows_without_applied_column_get_na(mali):
    got = mali.plot_inputs_finalize(pd.DataFrame([row('urea', u='Kg', Quantity=50.)]))
    assert got['Applied'].isna().all()
    assert got['Quantity'].tolist() == [50.]
    assert got['u'].tolist() == ['Kg']


def test_group_row_sits_beside_zero_type_rows(mali):
    frame = pd.DataFrame([row('urea', u='Kg', Quantity=40.),
                          row('dap', Quantity=0.),
                          row('inorganic_fertilizer', Applied=True)])
    got = mali.plot_inputs_finalize(frame)
    assert sorted(got.index.get_level_values('input')) == ['DAP', 'Inorganic Fertilizer?', 'Urea']
    applied = got['Applied']
    assert applied.xs('Inorganic Fertilizer?', level='input').tolist() == [True]
    assert applied.drop('Inorganic Fertilizer?', level='input').isna().all()
    # The zero row keeps its recorded zero; only u is the sentinel.
    dap = got.xs('DAP', level='input')
    assert dap['Quantity'].tolist() == [0.] and dap['u'].tolist() == [U_UNKNOWN]


def test_duplicate_group_answers_reduce_with_any(mali):
    got = mali.plot_inputs_finalize(pd.DataFrame([row('phytosanitary', Applied=False),
                                                  row('phytosanitary', Applied=True)]))
    assert len(got) == 1
    assert got['Applied'].tolist() == [True]


def test_nitrogen_kg_skips_use_answer_rows_without_warning():
    import warnings
    from lsms_library import transformations as T
    names = ['t', 'i', 'plot_id', 'input', 'crop']
    base = pd.DataFrame({'Quantity': [50.0], 'u': ['Kg']},
                        index=pd.MultiIndex.from_tuples([('2014-15', 'h1', '1_1', 'Urea', None)], names=names))
    answers = pd.DataFrame({'Quantity': [pd.NA, pd.NA], 'u': [U_UNKNOWN, U_UNKNOWN]},
                           index=pd.MultiIndex.from_tuples([('2014-15', 'h1', '1_1', 'Inorganic Fertilizer?', None),
                                                            ('2014-15', 'h1', '1_1', 'Phytosanitary products?', None)],
                                                           names=names))
    with warnings.catch_warnings():
        warnings.simplefilter('error', T.NutrientCoverageWarning)
        alone = T.nitrogen_kg(base)
        mixed = T.nitrogen_kg(pd.concat([base, answers]))
    pd.testing.assert_frame_equal(alone, mixed)
    assert mixed.attrs['nitrogen_input_match'] == alone.attrs['nitrogen_input_match']
    assert T.is_use_answer_label('Inorganic Fertilizer?')
    assert not T.is_use_answer_label('Inorganic Fertilizer')
