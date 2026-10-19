"""Nigeria food_acquired: a blank unit code is the missing-unit sentinel.

GH #1041.  ``nigeria.food_acquired_for_wave`` used to fill a NaN raw unit
code with the literal ``'None'``.  The Nigeria ``u`` table has no ``'None'``
key, so the value could only mean "no unit recorded" -- but ``'None'`` is not
in ``transformations._U_SENTINELS``, so ``food_kg_factors`` treated it as a
real unit and gave its rows pooled price-ratio kilograms: 847 rows (2010Q3
112, 2011Q1 98, 2012Q3 154, 2013Q1 463, 2015Q3 14, 2016Q1 6), sources
``unit`` 466 / ``item_unit`` 381, 1,133 kg.  It now writes ``U_UNKNOWN``,
which the ladder serves ``none``.

Three tiers: a static check on the source, a synthetic-frame test of the
function (no microdata), and a cache-gated guardrail on the BUILT table.
"""
import importlib.util

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root
from lsms_library.transformations import U_UNKNOWN, food_kg_factors

COUNTRY = 'Nigeria'
NIGERIA_PY = countries_root() / COUNTRY / '_' / 'nigeria.py'


def _nigeria_module():
    spec = importlib.util.spec_from_file_location('_nigeria_1041', NIGERIA_PY)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_source_no_longer_mints_the_none_unit():
    src = NIGERIA_PY.read_text()
    assert "fillna('None')" not in src
    assert 'fillna("None")' not in src
    assert 'fillna(U_UNKNOWN)' in src


def test_blank_unit_code_becomes_the_sentinel(monkeypatch):
    """A NaN raw unit code is served U_UNKNOWN; a recorded code is decoded."""
    from lsms_library import local_tools

    nigeria = _nigeria_module()
    raw = pd.DataFrame({
        'hhid': [1, 2, 3],
        'item_cd': [10, 10, 10],
        'q_buy': [2.0, 3.0, 1.0],
        'u_buy': [1.0, np.nan, 999.0],
        'spend': [100.0, 150.0, 50.0],
    })
    monkeypatch.setattr(local_tools, 'get_dataframe', lambda *a, **k: raw.copy())
    # harmonized_food_labels resolves categorical_mapping.org cwd-relatively
    monkeypatch.chdir(NIGERIA_PY.parent)
    out = nigeria.food_acquired_for_wave(
        'synthetic.csv', '2015Q3',
        {'purchased': {'quantity': 'q_buy', 'unit': 'u_buy'}},
        expenditure='spend')
    u = out.reset_index().set_index('i')['u']
    assert u['2'] == U_UNKNOWN
    assert u['1'] == nigeria.food_unit_labels('2015Q3')[1]
    # an unlisted code keeps its raw code; it is never folded into the sentinel
    assert u['3'] != U_UNKNOWN
    assert 'None' not in set(u)


# --- built-table guardrail -------------------------------------------------

@pytest.fixture(scope='module')
def food_acquired():
    ll = pytest.importorskip('lsms_library')
    try:
        df = ll.Country(COUNTRY).food_acquired()
    except Exception as exc:                      # noqa: BLE001 - data absent
        pytest.skip(f'Nigeria food_acquired unavailable: {exc}')
    if df is None or df.empty:
        pytest.skip('Nigeria food_acquired empty (no microdata)')
    return df


@pytest.mark.requires_s3
@pytest.mark.filterwarnings('ignore::UserWarning')
def test_no_row_is_served_the_none_unit(food_acquired):
    u = food_acquired.index.get_level_values('u').astype(str)
    assert not (u == 'None').any(), int((u == 'None').sum())


@pytest.mark.requires_s3
@pytest.mark.filterwarnings('ignore::UserWarning')
def test_missing_unit_rows_get_no_kilograms(food_acquired):
    u = food_acquired.index.get_level_values('u').astype(str)
    missing = u == U_UNKNOWN
    # 847 on the 2026-10-18 build; a floor, not a pin, so a wave added later
    # does not fail this test for the wrong reason.
    assert missing.sum() > 0
    factors = food_kg_factors(food_acquired)
    fu = factors.index.get_level_values('u').astype(str)
    served = factors.loc[fu == U_UNKNOWN]
    assert len(served) == int(missing.sum())
    assert served['KgFactorSource'].eq('none').all(), (
        served['KgFactorSource'].value_counts().to_dict())
    assert served['kg_per_unit'].isna().all()
