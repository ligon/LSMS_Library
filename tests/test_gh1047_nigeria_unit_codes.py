"""Nigeria unit codes: a raw code is an integer string, and crop ``u`` is never NaN.

GH #1047, two halves of one rule (a unit is always a real, greppable value):

1. ``nigeria.food_acquired_for_wave`` keeps a raw unit code that the wave's
   column of the ``u`` table does not list (GH #607).  The CSV unit columns
   are float, and the kept code was stringified without ``format_id``, so the
   one such code that reaches the served table -- 2015Q3 ``purchased`` code 65,
   an item code keypunched into a unit field (``CONTENTS.org``, "Stray unit
   codes") -- was served as ``'65.0'``.  It is now ``'65'``; the labelled
   path is untouched.  (The only other unlisted code on a wired source,
   2012Q3 ``inkind`` 58, is on a row with no quantity and never reaches the
   table.)
2. ``nigeria.crop_production_for_wave`` served ``u = NaN`` on 13,138 rows
   (3,053 / 3,009 / 1,694 / 3,467 / 1,915 in 2011Q1 / 2013Q1 / 2016Q1 /
   2019Q1 / 2024Q1).  Every unit ``_canon_unit`` cannot place is now
   ``U_UNKNOWN`` -- the GH #1041 ruling, applied to the crop side.  That
   includes 2011Q1's 12 rows whose code (5, 0) has no value label: unlike an
   unlisted FOOD code they do not keep the raw code, because crop ``u`` is a
   label space and the API-time ``u`` table is keyed on food codes -- a kept
   ``'5'`` was measured served as ``'mudu'`` (11 rows).  No row is dropped.

Two tiers: synthetic-frame tests of the functions (no microdata), and a
cache-gated guardrail on the BUILT tables.
"""
import importlib.util
import re

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root
from lsms_library.transformations import U_UNKNOWN

COUNTRY = 'Nigeria'
NIGERIA_PY = countries_root() / COUNTRY / '_' / 'nigeria.py'
FLOAT_STRING = re.compile(r'^-?\d+\.\d+$')


def _nigeria_module():
    spec = importlib.util.spec_from_file_location('_nigeria_1047', NIGERIA_PY)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# --- 1. food_acquired: the kept raw code ------------------------------------

def test_unlisted_food_code_is_an_integer_string(monkeypatch):
    from lsms_library import local_tools

    nigeria = _nigeria_module()
    raw = pd.DataFrame({
        'hhid': [1, 2, 3, 4],
        'item_cd': [10, 10, 10, 10],
        'q_buy': [2.0, 3.0, 1.0, 1.0],
        'u_buy': [1.0, 65.0, np.nan, 131.0],
        'spend': [100.0, 150.0, 50.0, 20.0],
    })
    monkeypatch.setattr(local_tools, 'get_dataframe', lambda *a, **k: raw.copy())
    # harmonized_food_labels resolves categorical_mapping.org cwd-relatively
    monkeypatch.chdir(NIGERIA_PY.parent)
    labels = nigeria.food_unit_labels('2015Q3')
    assert 65 not in labels                     # the premise: 65 is unlisted
    out = nigeria.food_acquired_for_wave(
        'synthetic.csv', '2015Q3',
        {'purchased': {'quantity': 'q_buy', 'unit': 'u_buy'}},
        expenditure='spend')
    u = out.reset_index().set_index('i')['u']
    assert u['2'] == '65'
    assert u['3'] == U_UNKNOWN
    # the labelled path is unchanged: a listed code is decoded per round
    assert u['1'] == labels[1]
    assert u['4'] == labels[131]
    assert not any(FLOAT_STRING.match(str(x)) for x in u), list(u)


# --- 2. crop_production: u is never NaN ------------------------------------

def _crop_frame(raw_units, decoded_units, *, with_unit=True):
    n = len(raw_units)
    df = pd.DataFrame({
        'hhid': [f'{k + 1}' for k in range(n)],
        'plotid': ['1'] * n,
        'cropcode': [1010] * n,
        'qty': [10.0] * n,
        'unit': raw_units,
    })
    dec = df.copy()
    dec['unit'] = decoded_units
    fr = dict(df=df, dec=dec, hhid='hhid', plot='plotid', crop='cropcode',
              qty='qty', perennial=False)
    if with_unit:
        fr['unit'] = 'unit'
    return fr


def test_crop_unit_blank_or_unplaceable_is_unknown():
    nigeria = _nigeria_module()
    fr = _crop_frame([1.0, np.nan, 5.0, 0.0],
                     ['1. KILOGRAMS', np.nan, '5.0', '0.0'])
    out = nigeria.crop_production_for_wave('2011Q1', [fr], {1010: 'Maize'})
    u = out.reset_index().set_index('i')['u']
    assert u['1'] == 'Kg'                       # labelled: canonicalised
    assert u['2'] == U_UNKNOWN                  # blank: no unit recorded
    assert u['3'] == U_UNKNOWN                  # unlabelled code: not placed
    assert u['4'] == U_UNKNOWN
    assert not out['u'].isna().any()
    assert len(out) == 4                        # nothing dropped


def test_a_raw_crop_code_would_be_relabelled_as_a_food_unit():
    """Why an unplaceable crop code is NOT kept raw: the country's ``u`` table
    is applied to crop_production's ``u`` column at API time, keyed on FOOD
    unit codes, so a raw ``'5'`` would be served as that table's Preferred
    Label for 5 (``mudu``, the W2 food meaning).  If this ever stops holding,
    the decision in ``crop_production_for_wave`` can be revisited."""
    from lsms_library.local_tools import df_from_orgfile
    tab = df_from_orgfile(str(NIGERIA_PY.parent / 'categorical_mapping.org'), name='u')
    codes = dict(zip(tab['Code'].astype(int), tab['Preferred Label']))
    assert codes.get(5) == 'mudu'
    assert U_UNKNOWN not in set(tab['Code'].astype(str))


def test_crop_frame_without_a_unit_column_is_unknown():
    nigeria = _nigeria_module()
    fr = _crop_frame([1.0, 2.0], ['1. KILOGRAMS', '2. GRAMS'], with_unit=False)
    out = nigeria.crop_production_for_wave('2019Q1', [fr], {1010: 'Maize'})
    assert (out['u'] == U_UNKNOWN).all(), out['u'].tolist()


# --- built-table guardrail -------------------------------------------------

def _built(table):
    ll = pytest.importorskip('lsms_library')
    try:
        df = getattr(ll.Country(COUNTRY), table)()
    except Exception as exc:                      # noqa: BLE001 - data absent
        pytest.skip(f'Nigeria {table} unavailable: {exc}')
    if df is None or df.empty:
        pytest.skip(f'Nigeria {table} empty (no microdata)')
    return df


@pytest.fixture(scope='module')
def food_acquired():
    return _built('food_acquired')


@pytest.fixture(scope='module')
def crop_production():
    return _built('crop_production')


@pytest.mark.requires_s3
@pytest.mark.filterwarnings('ignore::UserWarning')
def test_no_food_unit_is_float_stringified(food_acquired):
    u = pd.Series(food_acquired.index.get_level_values('u').astype(str))
    bad = u[u.str.match(FLOAT_STRING.pattern)]
    assert bad.empty, bad.value_counts().to_dict()


@pytest.mark.requires_s3
@pytest.mark.filterwarnings('ignore::UserWarning')
def test_no_crop_row_has_a_nan_unit(crop_production):
    u = (crop_production['u'] if 'u' in crop_production.columns
         else crop_production.index.get_level_values('u').to_series())
    assert not u.isna().any(), int(u.isna().sum())
    assert not u.astype(str).str.match(FLOAT_STRING.pattern).any()
    # no raw code in the label space (a bare '5' is relabelled 'mudu')
    assert not u.astype(str).str.fullmatch(r'\d+').any()
    # 13,138 on the 2026-10-18 build; a floor, not a pin.
    assert (u == U_UNKNOWN).sum() > 10000
