"""Mali plot_inputs: a row that is not crop-specific carries a marker, not NaN.

`crop` is a declared index level of Mali's plot_inputs.  Before this change
every fertilizer / pesticide type row and both group use-answer rows served
`crop` = NaN -- 145,530 of 179,817 rows (Site I's count) -- and the first
full-index groupby deleted them: `Feature('plot_inputs')(['Mali'])` grouped
on its full index kept 34,287 rows.  `mali.plot_inputs_finalize` now fills
`mali.CROP_NA = '(not crop-specific)'`, the sentinel Niger defines
(`Niger/_/niger.py:658-668`, filled at `:693`) and all seven EHCVM siblings
inline.  Niger's comment also covers "any seed row whose crop could not be
resolved", so a seed row whose recorded crop harmonize_food does not map
(Mali: `Non-Exploitée` / `Non exploitée`, `Echalotte`, `Poireaux` -- 45
served rows) gets the marker too.  The cross-country canonical marker is
GH #1119's open decision.  The module is loaded from THIS checkout's source.
"""
import importlib.util
from pathlib import Path

import pandas as pd
import pytest
import lsms_library as ll

MARKER = '(not crop-specific)'


@pytest.fixture
def mali(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    assert Path(ll.__file__).resolve().is_relative_to(root), ll.__file__
    source = root / 'lsms_library/countries/Mali/_/mali.py'
    spec = importlib.util.spec_from_file_location('mali_crop_marker_source', source)
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


def test_marker_is_the_ehcvm_spelling(mali):
    assert mali.CROP_NA == MARKER
    niger = (Path(mali.__file__).resolve().parents[2] / 'Niger/_/niger.py').read_text(
        encoding='utf-8')
    assert f"CROP_NA = '{MARKER}'" in niger


@pytest.mark.parametrize('code', ['urea', 'dap', 'npk', 'other_inorganic', 'manure',
                                  'compost', 'other_organic', 'pesticide', 'fungicide',
                                  'herbicide', 'other_phytosanitary'])
def test_type_rows_carry_the_marker(mali, code):
    got = mali.plot_inputs_finalize(pd.DataFrame([row(code, u='Kg', Quantity=4.)]))
    assert len(got) == 1
    assert got.index.get_level_values('crop').tolist() == [MARKER]
    assert got['Quantity'].tolist() == [4.]


@pytest.mark.parametrize('code', ['inorganic_fertilizer', 'phytosanitary'])
@pytest.mark.parametrize('answer', [True, False])
def test_use_answer_rows_carry_the_marker(mali, code, answer):
    got = mali.plot_inputs_finalize(pd.DataFrame([row(code, Applied=answer)]))
    assert got.index.get_level_values('crop').tolist() == [MARKER]
    assert got['Applied'].tolist() == [answer]


def test_seed_crop_mapped_is_kept_and_unmapped_gets_the_marker(mali):
    frame = pd.DataFrame([row('seed', crop='Ail', u='Kg', Quantity=2.),
                          row('seed', plot_id='1_2', crop='Non-Exploitée', u='Kg',
                              Quantity=1.)])
    got = mali.plot_inputs_finalize(frame)
    crops = dict(zip(got.index.get_level_values('plot_id'),
                     got.index.get_level_values('crop')))
    assert crops == {'1_1': 'Ail', '1_2': MARKER}


def test_no_nan_on_crop_and_the_collapse_is_unchanged(mali):
    """Two urea lines on one plot still sum (the 2014-15 s2c repeated-key
    case); the marker changes the key's VALUE, not group membership."""
    frame = pd.DataFrame([row('urea', u='Kg', Quantity=10.),
                          row('urea', u='Kg', Quantity=5.),
                          row('phytosanitary', Applied=False),
                          row('phytosanitary', Applied=True),
                          row('seed', crop='Ail', u='Kg', Quantity=2.)])
    got = mali.plot_inputs_finalize(frame)
    assert got.index.is_unique
    assert not pd.isna(got.index.get_level_values('crop')).any()
    assert len(got) == 3
    assert got.xs('Urea', level='input')['Quantity'].tolist() == [15.]
    assert got.xs('Phytosanitary products?', level='input')['Applied'].tolist() == [True]
    # A full-index groupby (what Feature() and a user's aggregation run) keeps
    # every row.
    assert int(got.groupby(list(got.index.names)).size().sum()) == len(got)
