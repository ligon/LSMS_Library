"""GH #1123: recorded ECVMA slots survive even when all payloads agree."""
import importlib.util
from pathlib import Path
import runpy
import sys

import numpy as np
import pandas as pd
import pytest

from lsms_library import local_tools
from lsms_library.paths import countries_root


@pytest.fixture
def niger():
    path = countries_root() / 'Niger/_/niger.py'
    spec = importlib.util.spec_from_file_location('niger_slots_test', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('wave', ['2011-12', '2014-15'])
def test_actual_driver_preserves_slots_payload_and_eligibility(monkeypatch, wave):
    early = wave == '2011-12'
    pre = 'as02cq' if early else 'AS02CQ'
    suffix = lambda value: value if early else value.upper()
    numeric = pd.DataFrame({
        'hid': [703] * 5, 'GRAPPE': [7]*5, 'MENAGE': [3]*5,
        'EXTENSION': [2]*5,
        pre+'02': [11, 12, 13, 14, 1], pre+'03': [1, 1, 1, 2, 1],
        pre+'04': [1, 1, 1, 1, np.nan],
        pre+suffix('05a'): [0, 0, np.nan, 999, 2],
        pre+suffix('05b'): [1]*5,
        pre+'07': [2, 2, 9, 1, 1],
        pre+suffix('08a'): [np.nan, np.nan, np.nan, 999, 1.25],
    })
    labelled = numeric.copy()
    labelled[pre+'02'] = ['semences']*4 + ['engrais organique']
    labelled[pre+'04'] = ['mil']*4 + [pd.NA]
    labelled[pre+suffix('05b')] = 'kg'
    written = []
    monkeypatch.setattr(local_tools, 'get_dataframe', lambda path, convert_categoricals=True:
                        (labelled if convert_categoricals else numeric).copy())
    monkeypatch.setattr(local_tools, 'to_parquet', lambda df, *a, **kw: written.append(df))
    root = countries_root()/'Niger'
    monkeypatch.chdir(root/wave/'_')
    monkeypatch.syspath_prepend(str(root/'_'))
    monkeypatch.delitem(sys.modules, 'niger', raising=False)
    runpy.run_path(str(root/wave/'_/plot_inputs.py'))
    assert len(written) == 1
    result = written[0]
    assert len(result) == 4 and result.index.is_unique
    assert result.Quantity.tolist()[:2] == [0, 0]
    assert pd.isna(result.Quantity.iloc[2])
    assert result.Quantity_purchased.iloc[3] == 1.25
    assert result.Purchased.isna().tolist() == [False, False, True, False]
    assert result.index.get_level_values('i').tolist() == (['703']*4 if early else ['70032']*4)
    tokens = result.index.get_level_values('input_obs')
    assert [token.split('=')[-1] for token in tokens] == ['11','12','13','1']
    assert all(token.startswith('slot:Niger/'+wave+'/Data/') for token in tokens)
    assert result.Quantity.dtype == pd.Float64Dtype()
    assert result.Purchased.dtype == pd.BooleanDtype()


@pytest.mark.parametrize('slots,households', [([11,11],['h','h']),
    ([11,None],['h','h']), ([11,11.5],['h','h']), ([11,12],['h',None])])
def test_ambiguous_native_key_refuses(niger, slots, households):
    path = countries_root()/'Niger/2011-12/Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas2c_p1.dta'
    with pytest.raises(ValueError):
        niger._ecvma_input_observations(pd.Series(households),
            pd.Series(slots,name='as02cq02'),path,script_path=__file__)


def test_same_slot_different_household_is_not_a_duplicate(niger):
    path = countries_root()/'Niger/2011-12/Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas2c_p1.dta'
    result = niger._ecvma_input_observations(pd.Series(['h','g']),
        pd.Series([11,11],name='as02cq02'),path,script_path=__file__)
    assert result.iloc[0] == result.iloc[1]  # uniqueness is household x slot
    with pytest.raises(ValueError, match='sidecar'):
        niger._ecvma_input_observations(pd.Series(['h']),pd.Series([11]),
            path.with_name('absent.dta'),script_path=__file__)
