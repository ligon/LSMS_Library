"""Nigeria unit codes: a raw code is an integer string, and crop ``u`` is never NaN.

GH #1047, two halves of one rule (a recorded amount always carries a real,
greppable unit value):

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
3. ``plot_inputs`` served ``u = NaN`` on 6,742 rows.  1,018 of them were NOT
   missing: W4/W5 record input units the harvest canonicaliser does not know
   (``210. STEMS``, ``280. METRIC CUP``, ``260. SEEDLING/PLANT``,
   ``21. TRACTORS``), now placed by ``_canon_input_unit``.  Of the other
   5,724, the 178 that record an amount (164 positive, 14 zero) and no unit
   are ``U_UNKNOWN``, filled once AFTER the per-channel collapse; the 5,546
   that record NO quantity in any channel keep ``u = NA`` (dispatcher
   decision 2026-10-19 -- ``u`` is a column here, and ``Unknown`` would
   claim an amount).

Folding an unplaceable recorded LABEL into ``Unknown`` silently would be the
same loss under the ruling's name, so ``_canon_units`` REPORTS it
(``UnplacedUnitLabelWarning``); a blank or a bare unlabelled code stays
silent.

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


def test_an_unplaceable_unit_label_is_reported_not_silently_unknown():
    """The D3 failure mode: a recorded LABEL the canonicaliser does not know
    must be NAMED.  Blanks and bare unlabelled codes are missing units and
    stay silent."""
    nigeria = _nigeria_module()
    fr = _crop_frame([1.0, np.nan, 5.0, 99.0],
                     ['1. KILOGRAMS', np.nan, '5.0', '99. CALABASH'])
    with pytest.warns(nigeria.UnplacedUnitLabelWarning, match='99. CALABASH') as rec:
        out = nigeria.crop_production_for_wave('2011Q1', [fr], {1010: 'Maize'})
    msgs = [str(w.message) for w in rec
            if issubclass(w.category, nigeria.UnplacedUnitLabelWarning)]
    assert len(msgs) == 1 and "'99. CALABASH': 1" in msgs[0], msgs
    assert '5.0' not in msgs[0]                 # an unlabelled code is silent
    u = out.reset_index().set_index('i')['u']
    assert u['4'] == U_UNKNOWN                  # still served, never NaN
    assert len(out) == 4


def test_missing_units_raise_no_report():
    import warnings
    nigeria = _nigeria_module()
    labels = pd.Series(['1. KILOGRAMS', np.nan, '', '5.0', '0', '210. STEMS'])
    with warnings.catch_warnings():
        warnings.simplefilter('error', nigeria.UnplacedUnitLabelWarning)
        out = nigeria._canon_units(labels, canon=nigeria._canon_input_unit)
    assert out.tolist()[0] == 'Kg' and out.tolist()[-1] == 'Stem'
    assert out.iloc[1:5].isna().all()


@pytest.mark.parametrize('label, expected', [
    ('210. STEMS', 'Stem'),
    ('260. SEEDLING/PLANT', 'Seedling/Plant'),
    ('280. METRIC CUP', 'Metric Cup'),
    ('21. TRACTORS', 'Tractor'),
])
def test_input_only_units_are_placed_and_kept_out_of_canon_unit(label, expected):
    """Placed for plot_inputs; NOT added to ``_canon_unit``, which
    crop_production and community_prices also use."""
    nigeria = _nigeria_module()
    assert nigeria._canon_input_unit(label) == expected
    assert pd.isna(nigeria._canon_unit(label))


def test_metric_cup_is_the_u_tables_own_label():
    """280 is the W4 codebook's 'Metric Cup', already a Preferred Label."""
    from lsms_library.local_tools import df_from_orgfile
    tab = df_from_orgfile(str(NIGERIA_PY.parent / 'categorical_mapping.org'), name='u')
    codes = dict(zip(tab['Code'].astype(int), tab['Preferred Label']))
    assert codes.get(280) == 'Metric Cup'


def _seed_frames(rows):
    """rows: (qty0, unit0, qty1, unit1) per plot-crop, two acquisition channels."""
    df = pd.DataFrame({
        'hhid': [f'{k + 1}' for k in range(len(rows))],
        'plotid': ['1'] * len(rows),
        'cropcode': [1010] * len(rows),
        'q0': [r[0] for r in rows], 'u0': [np.nan] * len(rows),
        'q1': [r[2] for r in rows], 'u1': [np.nan] * len(rows),
    })
    dec = df.copy()
    dec['u0'] = [r[1] for r in rows]
    dec['u1'] = [r[3] for r in rows]
    return df, dec


def test_plot_inputs_unit_is_nan_only_without_an_amount_and_is_filled_after_the_collapse():
    """An AMOUNT with no unit is ``U_UNKNOWN`` (the #1041 ruling); a row that
    records no quantity in any channel keeps ``u = NA`` (dispatcher decision
    2026-10-19: ``u`` is a column here, and ``Unknown`` would claim an
    amount the row does not have)."""
    nigeria = _nigeria_module()
    df, dec = _seed_frames([
        (2.0, '1. KILOGRAMS', np.nan, np.nan),     # one channel, Kg
        (3.0, np.nan, np.nan, np.nan),             # a quantity, no unit
        (np.nan, np.nan, np.nan, np.nan),          # nothing recorded
        (np.nan, np.nan, np.nan, '1. KILOGRAMS'),  # no qty; unit on channel 1
        (4.0, '210. STEMS', np.nan, np.nan),       # an input-only unit
        (0.0, np.nan, np.nan, np.nan),             # a zero is an amount
    ])
    seeds = nigeria.seed_rows_for_wave(
        df, dec, 'hhid', 'plotid', 'cropcode',
        channels=[dict(qty='q0', unit='u0'), dict(qty='q1', unit='u1')],
        crop_labels={1010: 'Maize'})
    out = nigeria.assemble_plot_inputs('2018Q3', [seeds])
    u = out.reset_index().set_index('i')['u']
    q = out.reset_index().set_index('i')['Quantity']
    # NA only where there is no amount; never where there is one
    assert out.loc[out['u'].isna(), 'Quantity'].isna().all(), out.to_dict()
    assert u['1'] == 'Kg'
    assert u['2'] == U_UNKNOWN
    assert pd.isna(q['3']) and pd.isna(u['3']), (q['3'], u['3'])
    # a per-CHANNEL fill would hand row 4 channel 0's 'Unknown' instead; a
    # recorded unit on a row with no amount is kept as recorded
    assert u['4'] == 'Kg'
    assert u['5'] == 'Stem'
    assert q['6'] == 0 and u['6'] == U_UNKNOWN, (q['6'], u['6'])
    assert len(out) == 6


def test_crop_frame_without_a_unit_column_is_unknown():
    nigeria = _nigeria_module()
    fr = _crop_frame([1.0, 2.0], ['1. KILOGRAMS', '2. GRAMS'], with_unit=False)
    out = nigeria.crop_production_for_wave('2019Q1', [fr], {1010: 'Maize'})
    # notna first: `NA == 'Unknown'` is NA, which `.all()` skips
    assert out['u'].notna().all(), out['u'].tolist()
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


@pytest.fixture(scope='module')
def plot_inputs():
    return _built('plot_inputs')


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
    # 13,138 on the 2026-10-18 build; a floor and a ceiling, not a pin.
    assert 10000 < (u == U_UNKNOWN).sum() < 14000


@pytest.mark.requires_s3
@pytest.mark.filterwarnings('ignore::UserWarning')
def test_no_plot_input_has_a_nan_unit(plot_inputs):
    """``u`` is NA only on a row with no quantity (dispatcher decision
    2026-10-19); a row with an amount and no unit is ``U_UNKNOWN``."""
    u = plot_inputs['u']
    q = plot_inputs['Quantity']
    # NA only where Quantity is NA -- an amount always carries a unit value
    assert q[u.isna()].isna().all(), int((u.isna() & q.notna()).sum())
    # and a row with no amount is never given Unknown
    assert not ((u == U_UNKNOWN) & q.isna()).any(), int(
        ((u == U_UNKNOWN) & q.isna()).sum())
    # 5,546 no-quantity seed rows on the 2026-10-19 build; floor and ceiling
    # catch a fill that over-fires (0) or a new class of NaN unit.
    assert 5000 < u.isna().sum() < 6000, int(u.isna().sum())
    assert not u.astype(str).str.fullmatch(r'[\d.]+').any()
    # The input-only units are placed, not folded into Unknown.
    served = set(u.dropna())
    assert {'Stem', 'Metric Cup', 'Seedling/Plant', 'Tractor'} <= served, served
    # 178 on the 2026-10-19 build (164 with a quantity, 14 with a zero).  The
    # ceiling is what catches the four labels above falling back to Unknown
    # (1,018 more rows).
    assert 100 < (u == U_UNKNOWN).sum() < 300, int((u == U_UNKNOWN).sum())
