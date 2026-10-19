"""Nigeria food_acquired: unit codes are decoded PER WAVE, not wave-blind.

GH #607.  The GHS-Panel food-module unit codes collide across waves -- 4 is
millilitre in W1 but centilitre from W2 on; 5/6 are piece/other in W1
post-planting but mudu/olodo in W2; 10/11/14 are W2's medium derica / small
derica / tiya but bin/basket / paint rubber / tin from W3 on; W1
post-harvest's 11..95 are sized sacks, basins, bunches, tubers and jerry
cans.  The framework's automatic categorical mapping keys on ``Code`` alone,
so before the fix every wave was served the W3/W4 meaning: 40,854 rows
relabelled, including 1,272 millilitre rows served as centilitre (a 10x kg
error).  ``nigeria.food_acquired_for_wave`` now decodes ``u`` from the
round's own wave column of the ``u`` table.

Two tiers: config-only tests (the decode dicts against the table and against
the rounds' ``.dta`` value labels, transcribed below) and a cache-gated
guardrail on the BUILT table.
"""
import importlib.util

import pytest

from lsms_library.paths import countries_root

COUNTRY = 'Nigeria'
ROUNDS = {'2010Q3': '2010-11', '2011Q1': '2010-11',
          '2012Q3': '2012-13', '2013Q1': '2012-13',
          '2015Q3': '2015-16', '2016Q1': '2015-16',
          '2018Q3': '2018-19', '2019Q1': '2018-19'}


def _nigeria_module():
    path = countries_root() / COUNTRY / '_' / 'nigeria.py'
    spec = importlib.util.spec_from_file_location('_nigeria_607', path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope='module')
def nigeria():
    return _nigeria_module()


@pytest.mark.parametrize('t,wave', sorted(ROUNDS.items()))
def test_round_maps_to_its_wave(nigeria, t, wave):
    assert nigeria.round_wave(t) == wave


@pytest.mark.parametrize('t', sorted(ROUNDS))
def test_decode_reads_the_waves_own_column(nigeria, t):
    """Each round's decode dict IS its wave column, codes '---' excluded."""
    from lsms_library.local_tools import df_from_orgfile

    tab = df_from_orgfile(
        str(countries_root() / COUNTRY / '_' / 'categorical_mapping.org'),
        name='u')
    wave = ROUNDS[t]
    expected = {int(float(c)): str(lab).strip()
                for c, lab in zip(tab['Code'], tab[wave])
                if isinstance(lab, str) and lab.strip() not in ('', '---')}
    d = nigeria.food_unit_labels(t)
    assert {k: v for k, v in d.items() if isinstance(k, int)} == expected
    # the CSV sources arrive as floats; every spelling must resolve
    for c, lab in expected.items():
        assert d[float(c)] == d[str(c)] == d[f'{c}.0'] == lab


# Transcribed from the rounds' own Stata value labels (the CSVs carry none).
DTA_FACTS = {
    # W1 post-planting sect7b_plantingw1.dta: 1 kilogram .. 6 other
    '2010Q3': {4: 'ml', 5: 'piece', 6: 'other'},
    # W1 post-harvest sect10b_harvestw1.dta: SP10BQ2B, 1-4 then sized units
    # that state their weight ('Sack/Bag: Small (20 kg)'), kept since GH #1035
    # (tests/test_gh1035_nigeria_w1_stated_weights.py).
    '2011Q1': {4: 'ml', 11: 'Sack/Bag Small (20 kg)',
               21: 'Basket Small (15 kg)', 31: 'Basin Small (10 kg)',
               41: 'Bunch Small (5 kg)', 51: 'Yam Tuber Small (3 kg)',
               61: 'Bundle Small (15 kg)', 91: 'Jerry Can Small (10 l)'},
    # W2 both rounds: 4 'centilitre (cl)', 10/11 derica, 14 tiya
    '2012Q3': {4: 'cl', 5: 'mudu', 6: 'olodo', 10: 'medium derica',
               11: 'small derica', 14: 'tiya'},
    '2013Q1': {4: 'cl', 10: 'medium derica', 11: 'small derica', 14: 'tiya'},
    # W3/W4: 4 CENTILITRES, 10 BIN/BASKET, 11 PAINT RUBBER, 14 TIN
    '2015Q3': {4: 'cl', 10: 'bin/basket', 11: 'paint rubber', 14: 'tin'},
    '2018Q3': {4: 'cl', 10: 'bin/basket', 11: 'paint rubber', 14: 'tin'},
}


@pytest.mark.parametrize('t', sorted(DTA_FACTS))
def test_decode_agrees_with_the_dta_labels(nigeria, t):
    d = nigeria.food_unit_labels(t)
    assert {c: d.get(c) for c in DTA_FACTS[t]} == DTA_FACTS[t]


def test_w2_code_4_is_centilitre_not_millilitre(nigeria):
    """Both W2 .dta files label 4 'centilitre (cl)'; W2 code-4 rows have a
    median quantity of 75 (the 75 cl bottle, as in W3), against 365-600 for
    W1's genuine millilitre rows.  Relabelling W2 to ml would put a 10x kg
    error on ~13.9k rows in the other direction."""
    assert nigeria.food_unit_labels('2012Q3')[4] == 'cl'
    assert nigeria.food_unit_labels('2010Q3')[4] == 'ml'


# --- built-table guardrail -------------------------------------------------

# Labels a round can only carry if a code was decoded with ANOTHER wave's
# meaning.
FORBIDDEN = {
    '2010Q3': {'cl', 'mudu', 'olodo'},
    '2011Q1': {'cl', 'large mudu', 'paint rubber', 'large congo',
               'medium heap', 'medium kobiowu', 'medium derica'},
    '2012Q3': {'ml', 'tin', 'bin/basket'},
    '2013Q1': {'ml', 'tin', 'bin/basket'},
}


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
@pytest.mark.parametrize('t', sorted(FORBIDDEN))
def test_no_round_serves_another_waves_unit(food_acquired, t):
    u = food_acquired.xs(t, level='t').index.get_level_values('u')
    bad = sorted(set(u) & FORBIDDEN[t])
    assert not bad, f'{t} serves other-wave unit labels: {bad}'


@pytest.mark.requires_s3
@pytest.mark.filterwarnings('ignore::UserWarning')
def test_w1_millilitre_is_served_as_ml(food_acquired):
    u = food_acquired.index.get_level_values('u')
    t = food_acquired.index.get_level_values('t')
    assert ((u == 'ml') & t.isin(['2010Q3', '2011Q1'])).sum() > 0
