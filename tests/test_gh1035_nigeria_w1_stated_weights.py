"""Nigeria W1 post-harvest food units keep the weight the survey states.

GH #1035 (ruling @ligon 2026-10-18: "Keep the survey-stated weight in the
unit label (e.g. ``Sack/Bag Small (20 kg)``), so ``_parse_explicit_metric``
uses it").  ``sect10b_harvestw1.dta`` labels its sized unit codes 11-95 with
a weight -- ``Sack/Bag: Small (20 kg)``, ``Basin: Small (10 kg)`` -- and the
``2010-11`` column of the ``u`` table used to drop it (``Sack/Bag Small``),
so 3,809 food rows of round 2011Q1 were given price-ratio kilograms
(``item_unit`` / ``unit``) where the survey itself states the weight.

Three tiers: config-only tests of the ``u`` table's ``2010-11`` column (no
microdata), a cache-gated check against the survey's own W1 conversion file,
and a cache-gated guardrail on the BUILT table's kg ladder.
"""
import importlib.util
import re

import pandas as pd
import pytest

from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root
from lsms_library.transformations import _parse_explicit_metric, food_kg_factors

COUNTRY = 'Nigeria'
ORG = countries_root() / COUNTRY / '_' / 'categorical_mapping.org'

# Transcribed from sect10b_harvestw1.dta, value-label set SP10BQ2B (shared by
# s10bq2b/3b/5b/6b) -- the stated weight of each sized code.  The three
# Bundle labels are truncated at Stata's 60 characters ('...: Small (1',
# '...: Medium (', '...: Big (40'); their 15 / 25 / 40 kg are from the
# survey's own w1agnsconversion.csv (nscode 61/62/63, constant across all
# crops), which agrees with every other row here.
STATED = {
    11: 20, 12: 50, 13: 100, 14: 120,          # Sack/Bag
    21: 15, 22: 30, 23: 50, 24: 75,            # Basket
    31: 10, 32: 25, 33: 40, 34: 75,            # Basin
    41: 5, 42: 8, 43: 15,                      # Bunch of plantain / FFB
    51: 3, 52: 5, 53: 8,                       # Tuber of yam
    61: 15, 62: 25, 63: 40,                    # Bundle (truncated in .dta)
    71: 60, 72: 85, 73: 110, 74: 150,          # Wheel barrow
    81: 1500, 82: 2000, 83: 2500,              # Pick-up van ('1,500 kg')
}
# Jerry can / keg / rubber of palm oil: litres ('Small(10 Lt.)').
STATED_LITRES = {91: 10, 92: 20, 93: 25, 94: 50, 95: 200}


def _nigeria_module():
    path = countries_root() / COUNTRY / '_' / 'nigeria.py'
    spec = importlib.util.spec_from_file_location('_nigeria_1035', path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope='module')
def w1ph():
    """``{int code: label}`` the 2011Q1 build decodes with."""
    d = _nigeria_module().food_unit_labels('2011Q1')
    return {k: v for k, v in d.items() if isinstance(k, int)}


@pytest.fixture(scope='module')
def table():
    return df_from_orgfile(str(ORG), name='u')


def test_every_sized_w1_code_states_its_weight(w1ph):
    """All 33 sized post-harvest codes are decoded, each with its weight."""
    sized = {c: lab for c, lab in w1ph.items() if c >= 11}
    assert set(sized) == set(STATED) | set(STATED_LITRES)
    for c, lab in sized.items():
        assert re.search(r'\(\d+(?:\.\d+)? (?:kg|l)\)$', lab), (c, lab)


@pytest.mark.parametrize('code', sorted(STATED))
def test_stated_mass_parses_to_the_survey_weight(w1ph, code):
    lab = w1ph[code]
    assert _parse_explicit_metric(lab) == STATED[code], lab
    # a mass is a mass whatever the volume convention
    assert _parse_explicit_metric(lab, volume_as_mass=False) == STATED[code]


@pytest.mark.parametrize('code', sorted(STATED_LITRES))
def test_jerry_can_is_a_volume(w1ph, code):
    """Palm-oil jerry cans state LITRES: served as kg only under the
    default ``volume_as_mass=True``, declined (-> inference) otherwise."""
    lab = w1ph[code]
    assert _parse_explicit_metric(lab) == STATED_LITRES[code], lab
    assert _parse_explicit_metric(lab, volume_as_mass=False) is None


def test_every_parenthesised_number_in_the_w1_column_round_trips(table):
    """Parametrised over the COLUMN, not a hand list: any label in the
    ``2010-11`` column that states a number in parentheses must parse to
    exactly that number, so a future spelling the parser misreads fails
    here rather than serving a wrong weight."""
    for lab in table['2010-11'].dropna().astype(str):
        m = re.search(r'\((\d+(?:\.\d+)?) (kg|l)\)', lab)
        if m:
            assert _parse_explicit_metric(lab) == float(m.group(1)), lab


def test_no_label_uses_a_thousands_separator(table):
    """``_parse_explicit_metric('Pick-up Van Small (1,500 kg)')`` is 500.0:
    the pattern reads the digits adjacent to the unit token, so the
    survey's own comma spelling would understate a 1,500 kg van 3x."""
    for col in table.columns:
        for lab in table[col].dropna().astype(str):
            assert not re.search(r'\d,\d{3}', lab), (col, lab)


def test_colliding_codes_keep_their_other_wave_meanings(table):
    """The weight goes in the ``2010-11`` column only; the Preferred Label of
    a code that collides across waves stays the W3/W4 meaning (GH #607)."""
    t = table.assign(Code=table['Code'].astype(float).astype(int)).set_index('Code')
    assert t.loc[11, 'Preferred Label'] == 'paint rubber'
    assert t.loc[11, '2012-13'] == 'small derica'
    assert t.loc[91, 'Preferred Label'] == 'medium heap'
    # a W1-only code's Preferred Label carries the same stated weight
    assert t.loc[22, 'Preferred Label'] == t.loc[22, '2010-11'] == 'Basket Medium (30 kg)'


# --- the survey's own conversion file -----------------------------------------

@pytest.mark.requires_s3
def test_weights_agree_with_the_surveys_w1_conversion_file():
    """``w1agnsconversion.csv`` ships with GHS-Panel W1: a conversion value
    per (crop, nonstandard unit code).  Its value is constant across crops
    for every sized code and equals the label weight -- the evidence for the
    three truncated Bundle labels."""
    from lsms_library.local_tools import get_dataframe

    try:
        conv = get_dataframe(str(countries_root() / COUNTRY / '2010-11' / 'Data'
                                 / 'w1agnsconversion.csv'))
    except Exception as exc:                      # noqa: BLE001 - data absent
        pytest.skip(f'w1agnsconversion.csv unavailable: {exc}')
    by_code = conv.groupby('nscode')['conversion'].agg(['min', 'max'])
    expected = {**STATED, **STATED_LITRES}
    assert set(by_code.index) == set(expected)
    for code, kg in expected.items():
        assert by_code.loc[code, 'min'] == by_code.loc[code, 'max'] == kg, code


# --- built-table guardrail -----------------------------------------------------

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
def test_stated_weight_rows_are_served_their_stated_kilograms(food_acquired, w1ph):
    """Every 2011Q1 row on a sized code is served the ``metric`` rung at the
    stated weight -- no row on these labels is left to price-ratio
    inference (3,809 rows on the 2026-10-19 build: 2,617 purchased, 1,062
    produced, 130 in-kind)."""
    stated = {w1ph[c]: kg for c, kg in {**STATED, **STATED_LITRES}.items()}
    factors = food_kg_factors(food_acquired)
    u = factors.index.get_level_values('u').astype(str)
    t = factors.index.get_level_values('t').astype(str)
    on = pd.Series(u, index=factors.index).isin(stated).to_numpy()
    assert on.sum() > 0
    served = factors.loc[on]
    assert served['KgFactorSource'].eq('metric').all(), (
        served['KgFactorSource'].value_counts().to_dict())
    want = pd.Series(u[on]).map(stated).to_numpy()
    assert (served['kg_per_unit'].to_numpy() == want).all()
    # the labels are W1 post-harvest only
    assert set(t[on]) == {'2011Q1'}
    # and no 2011Q1 row still carries a weightless sized label
    old = {re.sub(r' \(.*\)$', '', lab) for lab in stated}
    assert not (pd.Series(u).isin(old) & (pd.Series(t) == '2011Q1')).any()
