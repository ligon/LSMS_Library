"""Nigeria W1 post-harvest: the card weight serves a FRACTION of a container.

GH #1035.  ``sect10b_harvestw1.dta`` labels its sized unit codes 11-95 with
the container weight of the agriculture production-unit card --
``Sack/Bag: Small (20 kg)``, ``Basin: Small (10 kg)`` -- which the ``u``
table's ``2010-11`` column drops (``Sack/Bag Small``).

Ruling (@ligon 2026-10-18), AMENDED 2026-10-19 after red team r1: the
survey-stated container weight applies only where ``Quantity < 1``; where
``Quantity >= 1``, or where the quantity equals the card weight, kilograms
come from price-ratio inference.  So the ``u`` labels stay WEIGHTLESS (the
``metric`` rung must never read a weight off them) and the card weight is
served through the existing ``survey_kg`` rung as ``Quantity_kg = Quantity x
card_kg`` on the fraction rows only, stamped
``Nigeria::food_acquired::w1-card-weight`` (``2010-11/_/mapping.py``,
``Nigeria/_/derivations.yml``).

Tiers: config-only tests of the card table, the rule and the stamp (no
microdata); a cache-gated check against the survey's own W1 conversion file;
and cache-gated guardrails on the BUILT table, its kg ladder and the
derivation's ``inputs`` callable.
"""
import ast
import importlib.util
import re

import numpy as np
import pandas as pd
import pytest

from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root
from lsms_library.transformations import _parse_explicit_metric, food_kg_factors

COUNTRY = 'Nigeria'
WAVE_DIR = countries_root() / COUNTRY / '2010-11' / '_'
ORG = countries_root() / COUNTRY / '_' / 'categorical_mapping.org'
KEY = 'Nigeria::food_acquired::w1-card-weight'

# Transcribed independently of mapping.py, from sect10b_harvestw1.dta,
# value-label set SP10BQ2B (shared by s10bq2b/3b/5b/6b): the weight each
# sized code states.  The three Bundle labels are truncated at Stata's 60
# characters ('...: Small (1', '...: Medium (', '...: Big (40'); their 15 / 25
# / 40 kg are from the survey's own w1agnsconversion.csv (nscode 61/62/63,
# constant across all crops), which agrees with every other row here.
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
# Jerry can / keg / rubber of palm oil: LITRES ('Small(10 Lt.)').
STATED_LITRES = {91: 10, 92: 20, 93: 25, 94: 50, 95: 200}


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope='module')
def M():
    """The wave module that carries the card table, rule and stamp."""
    return _load(WAVE_DIR / 'mapping.py', '_nigeria_2010_11_mapping_1035')


@pytest.fixture(scope='module')
def w1ph():
    """``{int code: label}`` the 2011Q1 build decodes with."""
    d = _load(countries_root() / COUNTRY / '_' / 'nigeria.py',
              '_nigeria_1035').food_unit_labels('2011Q1')
    return {k: v for k, v in d.items() if isinstance(k, int)}


@pytest.fixture(scope='module')
def table():
    return df_from_orgfile(str(ORG), name='u')


# --- the card table is data, and the labels stay weightless -------------------

def test_card_table_is_the_survey_card(M):
    assert M.W1_PH_CARD_KG == {c: float(kg) for c, kg in STATED.items()}
    assert M.W1_PH_CARD_LITRES == {c: float(l) for c, l in STATED_LITRES.items()}


def test_every_sized_code_is_decoded(w1ph):
    sized = {c for c in w1ph if c >= 11}
    assert sized == set(STATED) | set(STATED_LITRES)


def test_w1_labels_state_no_weight(w1ph):
    """The amended ruling serves the card weight ONLY to a fraction of a
    container.  A weight in the label would let the ``metric`` rung serve it
    to every row on the label -- the r1 design the amendment withdrew (on
    Quantity >= 1 rows the card weight priced at a median 0.032x the item's
    own Kg price)."""
    for c in set(STATED) | set(STATED_LITRES):
        lab = w1ph[c]
        assert not re.search(r'\d', lab), (c, lab)
        assert _parse_explicit_metric(lab) is None, (c, lab)


def test_card_labels_are_unique(M):
    """The stamp maps the served label back to its code's card weight."""
    by_label = M._card_kg_by_label()
    assert len(by_label) == len(M.W1_PH_CARD_KG)


def test_no_label_uses_a_thousands_separator(table):
    """``_parse_explicit_metric('Pick-up Van Small (1,500 kg)')`` is 500.0:
    the pattern reads the digits adjacent to the unit token, so a comma
    spelling would understate a 1,500 kg van 3x if one were ever added."""
    for col in table.columns:
        for lab in table[col].dropna().astype(str):
            assert not re.search(r'\d,\d{3}', lab), (col, lab)


def test_colliding_codes_keep_their_other_wave_meanings(table):
    """Only the ``2010-11`` column carries W1-PH meanings; the Preferred
    Label of a code that collides across waves is the W3/W4 meaning (#607)."""
    t = table.assign(Code=table['Code'].astype(float).astype(int)).set_index('Code')
    assert t.loc[11, 'Preferred Label'] == 'paint rubber'
    assert t.loc[11, '2010-11'] == 'Sack/Bag Small'
    assert t.loc[11, '2012-13'] == 'small derica'
    assert t.loc[91, 'Preferred Label'] == 'medium heap'
    assert t.loc[22, 'Preferred Label'] == t.loc[22, '2010-11'] == 'Basket Medium'


# --- the wave script and the inputs callable read the same source -----------

def test_mapping_constants_match_the_wave_script(M):
    """``food_acquired.py`` keeps its source path and variables as LITERALS
    (the cache hash folds the DVC fingerprint of string-literal sources);
    ``mapping.W1_PH_*`` repeats them for the ``inputs`` callable.  Read the
    script with ``ast`` -- it reads data at import -- and hold the two equal."""
    tree = ast.parse((WAVE_DIR / 'food_acquired.py').read_text())
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and getattr(n.func, 'id', None) == 'food_acquired_for_wave']
    harvest = [c for c in calls if ast.literal_eval(c.args[1]) == M.W1_PH_ROUND]
    assert len(harvest) == 1
    call = harvest[0]
    assert ast.literal_eval(call.args[0]) == M.W1_PH_FOOD_FILE
    kw = {k.arg: ast.literal_eval(k.value) for k in call.keywords}
    assert kw['sources'] == M.W1_PH_SOURCES
    assert kw['expenditure'] == M.W1_PH_EXPENDITURE
    # and the harvest frame is the one the stamp is applied to
    stamped = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
               and getattr(n.func, 'id', None) == 'stamp_w1_card_weight']
    assert len(stamped) == 1 and stamped[0].args[0] is call


# --- the rule -----------------------------------------------------------------

@pytest.mark.parametrize('q, card, want', [
    (0.05, 20.0, 1.0),        # bread: 0.05 of a 20 kg sack is a 1 kg loaf
    (0.5, 10.0, 5.0),
    (0.999, 3.0, 2.997),
    (1.0, 20.0, np.nan),      # one container: a count -> inference
    (2.0, 10.0, np.nan),      # beef, 2 basins at N470 -> inference
    (15.0, 15.0, np.nan),     # quantity == card weight -> inference
    (0.0, 20.0, np.nan),
    (-0.5, 20.0, np.nan),
    (np.nan, 20.0, np.nan),
    (0.5, np.nan, np.nan),    # no stated mass (a litre code, or Kg)
    (0.5, 0.0, np.nan),
])
def test_derive_rule(M, q, card, want):
    got = M.derive_w1_card_weight_kg([q], [card])[0]
    if np.isnan(want):
        assert np.isnan(got)
    else:
        assert got == pytest.approx(want)


def test_a_fraction_can_never_equal_its_card_weight(M):
    """Every stated mass is >= 3 kg, so ``0 < Quantity < 1`` never selects a
    row whose quantity IS the card weight (165 such rows in 2011Q1, a kg
    figure keyed into the quantity field) -- the ruling's third clause holds
    by construction rather than by a second test in the rule."""
    assert min(M.W1_PH_CARD_KG.values()) >= 1


def _frame(rows):
    idx = pd.MultiIndex.from_tuples([r[:5] for r in rows],
                                    names=['t', 'i', 'j', 'u', 's'])
    return pd.DataFrame({'Quantity': [r[5] for r in rows],
                         'Expenditure': [r[6] for r in rows]}, index=idx)


def test_stamp_selects_exactly_the_fraction_rows_on_mass_codes(M):
    df = _frame([
        ('2011Q1', '1', 'Bread', 'Sack/Bag Small', 'purchased', 0.05, 150.0),
        ('2011Q1', '1', 'Beef', 'Basin Small', 'purchased', 2.0, 470.0),
        ('2011Q1', '1', 'Millet', 'Bundle Small', 'produced', 15.0, np.nan),
        ('2011Q1', '1', 'Malt', 'Jerry Can Small', 'purchased', 0.03, 110.0),
        ('2011Q1', '1', 'Rice', 'Kg', 'purchased', 0.5, 100.0),
        ('2011Q1', '2', 'Yam', 'Yam Tuber Small', 'inkind', 0.5, np.nan),
        # a post-PLANTING row on a sized spelling is never stamped
        ('2010Q3', '1', 'Bread', 'Sack/Bag Small', 'purchased', 0.05, 150.0),
    ])
    out = M.stamp_w1_card_weight(df)
    assert out.index.equals(df.index)
    assert out[['Quantity', 'Expenditure']].equals(df[['Quantity', 'Expenditure']])
    want = [1.0, np.nan, np.nan, np.nan, np.nan, 1.5, np.nan]
    np.testing.assert_allclose(out['Quantity_kg'].to_numpy(dtype=float), want)
    stamped = out['Derivation'].notna().to_numpy()
    assert (stamped == out['Quantity_kg'].notna().to_numpy()).all()
    assert set(out['Derivation'].dropna()) == {KEY}


def test_stamp_refuses_a_frame_that_already_has_survey_kilograms(M):
    df = _frame([('2011Q1', '1', 'Bread', 'Sack/Bag Small', 'purchased', 0.05, 150.0)])
    with pytest.raises(ValueError):
        M.stamp_w1_card_weight(df.assign(Quantity_kg=1.0))


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


# --- built-table guardrails ----------------------------------------------------

@pytest.fixture(scope='module')
def country():
    ll = pytest.importorskip('lsms_library')
    return ll.Country(COUNTRY)


@pytest.fixture(scope='module')
def food_acquired(country):
    try:
        df = country.food_acquired()
    except Exception as exc:                      # noqa: BLE001 - data absent
        pytest.skip(f'Nigeria food_acquired unavailable: {exc}')
    if df is None or df.empty:
        pytest.skip('Nigeria food_acquired empty (no microdata)')
    return df


@pytest.mark.requires_s3
@pytest.mark.filterwarnings('ignore::UserWarning')
def test_the_stamp_is_on_exactly_the_derived_rows(food_acquired):
    """1,856 rows on the 2026-10-19 build (1,659 purchased, 148 produced, 49
    in-kind; 4,740.73 kg), all 2011Q1."""
    d = food_acquired['Derivation']
    t = food_acquired.index.get_level_values('t').astype(str)
    assert (d.dropna() == KEY).all()
    assert d.notna().sum() > 0
    assert set(t[d.notna().to_numpy()]) == {'2011Q1'}
    q11 = t == '2011Q1'
    assert (d.notna().to_numpy()[q11]
            == food_acquired['Quantity_kg'].notna().to_numpy()[q11]).all()
    summ = food_acquired.attrs['derivations'][COUNTRY][KEY]
    assert summ['in'] == 'food_acquired'
    assert summ['columns'] == ['Quantity_kg']
    assert summ['rows'] == int((d == KEY).sum())


@pytest.mark.requires_s3
@pytest.mark.filterwarnings('ignore::UserWarning')
def test_kg_ladder_on_the_sized_labels(food_acquired, w1ph):
    """Mass labels: a fraction of a container is served ``survey_kg`` at
    ``Quantity x card weight``; one or more containers (which includes every
    quantity equal to the card weight) is served by inference, never by a
    stated weight.  Litre labels are never stamped.  No sized label reaches
    the ``metric`` rung."""
    mass = {w1ph[c]: kg for c, kg in STATED.items()}
    litres = {w1ph[c] for c in STATED_LITRES}
    f = food_kg_factors(food_acquired)
    u = pd.Series(f.index.get_level_values('u').astype(str))
    t = pd.Series(f.index.get_level_values('t').astype(str))
    q = food_acquired['Quantity'].reindex(f.index).to_numpy()
    src = f['KgFactorSource'].to_numpy()
    sized = u.isin(set(mass) | litres).to_numpy()
    w1 = (t == '2011Q1').to_numpy()
    assert set(t[sized & w1]) == {'2011Q1'}
    # GH #1034: W3 shares these native names, but not W1's stated card.
    later_shared = sized & ~w1
    assert later_shared.any()
    assert not (src[later_shared] == 'survey_kg').any()
    stamp = food_acquired['Derivation'].reindex(f.index).to_numpy()
    assert not pd.Series(stamp[later_shared]).eq(KEY).fillna(False).any()
    assert not (src[sized] == 'metric').any()
    on_mass = w1 & u.isin(mass).to_numpy() & (q > 0)
    frac = on_mass & (q < 1)
    whole = on_mass & (q >= 1)
    assert frac.sum() > 0 and whole.sum() > 0
    assert (src[frac] == 'survey_kg').all()
    np.testing.assert_allclose(f['kg_per_unit'].to_numpy()[frac],
                               u[frac].map(mass).to_numpy())
    assert not (src[whole] == 'survey_kg').any()
    assert not (src[u.isin(litres).to_numpy()] == 'survey_kg').any()


@pytest.mark.requires_s3
@pytest.mark.filterwarnings('ignore::UserWarning')
def test_inputs_join_to_the_served_rows(country, food_acquired, M):
    """``derivation_inputs`` returns one raw row per derived served row, on
    the served row's own key, and re-deriving from it gives the served
    ``Quantity_kg``."""
    inp = country.derivation_inputs(KEY)
    served = food_acquired[food_acquired['Derivation'] == KEY]
    if 'v' in served.index.names:
        served = served.droplevel('v')
    served = served.reorder_levels(list(inp.index.names))
    assert inp.index.is_unique
    assert set(served.index) == set(inp.index)
    j = served.join(inp, how='inner')
    s = j.index.get_level_values('s')
    raw_q = np.where(s == 'purchased', j['s10bq3a'],
                     np.where(s == 'produced', j['s10bq5a'], j['s10bq6a']))
    np.testing.assert_allclose(raw_q, j['Quantity'].to_numpy())
    card = pd.Series(j.index.get_level_values('u')).map(M._card_kg_by_label())
    np.testing.assert_allclose(
        M.derive_w1_card_weight_kg(raw_q, card), j['Quantity_kg'].to_numpy())
