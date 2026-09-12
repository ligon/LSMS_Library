"""Data-gated checks for GhanaLSS ``community_prices`` (GH #562 phase 3a).

The table is each wave's OWN market price survey at grain (t, v, j, u, obs)
with the reported columns Price / NumberOfUnits / Description.  These tests
pin the invariants the wave scripts and ``_/glss_prices.py`` promise:

* the declared index is unique (obs makes the three vendor observations rows,
  so no collapse is needed);
* v is the price survey's cluster id on ``sample().v``'s keyspace -- every
  wave's price clusters join the household clusters at or above the floor
  measured when the feature was built (1998-99 is the low one: 251 of 253);
* the food share of j sits on the wave's ``harmonize_food`` axis at or above
  the measured floor, so community prices JOIN ``food_acquired.j``;
* Price is populated on every row (an empty slot is not a reported price);
* pre-2007 waves are in old cedis and post-2007 in GHS: the maize median
  moves by four orders of magnitude across the 2007 redenomination.

Skips (rather than fails) when the GhanaLSS microdata is not available.
"""
import pandas as pd
import pytest

import lsms_library as ll

pytestmark = pytest.mark.slow

WAVES = ['1987-88', '1988-89', '1991-92', '1998-99', '2012-13', '2016-17']

# Measured 2026-09-02 (slurm_logs/ghana_audit/community_prices/verify_run.log);
# each floor is a little below the measurement so a small data revision does
# not trip it, but a broken key (0%) or a broken decode does.
V_IN_SAMPLE_FLOOR = 0.93      # 1988-89: 106/112 = 0.946; 1987-88 163/165; 1998-99 251/253; others 1.0
J_ON_FOOD_AXIS_FLOOR = {      # share of PRICE ROWS whose j is on harmonize_food
    '1987-88': 0.40, '1988-89': 0.40, '1991-92': 0.50, '1998-99': 0.50,
    '2012-13': 0.60, '2016-17': 0.30,
}


@pytest.fixture(scope='module')
def ghana():
    try:
        c = ll.Country('GhanaLSS')
        cp = c.community_prices()
    except Exception as e:  # pragma: no cover - data not available
        pytest.skip(f'GhanaLSS community_prices not buildable here: {e}')
    if cp is None or len(cp) == 0:
        pytest.skip('GhanaLSS community_prices is empty here')
    return c, cp


def test_index_and_columns(ghana):
    _, cp = ghana
    assert list(cp.index.names) == ['t', 'v', 'j', 'u', 'obs']
    assert cp.index.is_unique
    for col in ('Price', 'NumberOfUnits', 'Description'):
        assert col in cp.columns
    assert set(cp.index.get_level_values('t')) == set(WAVES)


def test_price_reported_on_every_row(ghana):
    _, cp = ghana
    assert cp['Price'].notna().all()
    assert (cp['Price'] > 0).mean() > 0.99


def test_obs_starts_at_one_and_is_mostly_the_form_slot(ghana):
    _, cp = ghana
    obs = cp.index.get_level_values('obs')
    assert obs.min() == 1
    # The form has three slots; rows beyond obs 3 are repeat records, brand
    # lines, or several price items folding onto one j (measured 81-95% <= 3).
    assert (obs <= 3).mean() > 0.75


def test_v_joins_sample_v(ghana):
    c, cp = ghana
    sv = c.sample().reset_index()[['t', 'v']].drop_duplicates()
    flat = cp.reset_index()[['t', 'v']].drop_duplicates()
    for t, g in flat.groupby('t'):
        s = set(sv.loc[sv['t'] == t, 'v'])
        pv = set(g['v'])
        share = len(pv & s) / len(pv)
        assert share >= V_IN_SAMPLE_FLOOR, f'{t}: only {share:.3f} of price clusters in sample().v'


def test_j_on_harmonize_food_axis(ghana):
    from lsms_library.local_tools import df_from_orgfile
    from lsms_library.paths import countries_root
    _, cp = ghana
    flat = cp.reset_index()[['t', 'j']]
    for t, g in flat.groupby('t'):
        hf = df_from_orgfile(countries_root() / 'GhanaLSS' / t / '_' / 'categorical_mapping.org',
                             name='harmonize_food', to_numeric=False)
        hf.columns = [x.strip() for x in hf.columns]
        axis = set(hf['Preferred Label'].astype(str).str.strip()) - {''}
        share = g['j'].isin(axis).mean()
        assert share >= J_ON_FOOD_AXIS_FLOOR[t], f'{t}: {share:.3f} of price rows on the food axis'


def test_redenomination_visible_in_maize(ghana):
    _, cp = ghana
    flat = cp.reset_index()
    maize = flat[flat['j'].isin(['Maize', 'Maize (cob)', 'Maize (grain)'])]
    old = maize[maize['t'] == '1998-99']['Price'].median()
    new = maize[maize['t'] == '2012-13']['Price'].median()
    assert pd.notna(old) and pd.notna(new)
    # 1999 cedis per kg (hundreds) vs 2013 GHS per kg (a few) -- ~10^4 apart.
    assert old / new > 50
