"""Nigeria food_acquired: the acquisition-source split must not be inverted.

GH #591.  The GHS-Panel questionnaire was renumbered after wave 1 and the
wave scripts did not follow: q5a stayed bound to ``Produced`` in every wave,
but from W2 on q5a is the quantity consumed out of PURCHASES (own production
moved to q6a).  Consequences, all silent, all graded `sane`:

  * the purchased row held (total - purchases) ~= 0 while the expenditure
    rode on it, so Price = Expenditure / 0 = inf and food_prices_from_acquired
    DELETED the row -- 161 to 1,106 surviving price rows per wave against a
    120k-160k-row food_acquired;
  * food_quantities reported ~87-94% of Nigerian food as own-produced from
    2012 on (the mirror image of the truth: wave 1 says ~6%).

These are guardrails, not unit tests: they assert properties of the BUILT
country table that would have caught the bug, and they are cheap once the
cache is warm.  Skipped when the Nigeria microdata isn't available.
"""
import pytest

pytestmark = pytest.mark.filterwarnings('ignore::UserWarning')

COUNTRY = 'Nigeria'

# Wave 1 (the only wave that was never broken) reports ~4-6% of food quantity
# as own-produced.  The broken waves reported 87-94%.  25% is a wide,
# deliberately un-tuned band: it passes on every wave post-fix (2.9-8.2%) and
# fails on all six broken waves by a factor of 3.5 or more.
MAX_PRODUCED_SHARE = 0.25


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


@pytest.fixture(scope='module')
def food_prices():
    ll = pytest.importorskip('lsms_library')
    try:
        df = ll.Country(COUNTRY).food_prices()
    except Exception as exc:                      # noqa: BLE001
        pytest.skip(f'Nigeria food_prices unavailable: {exc}')
    if df is None or df.empty:
        pytest.skip('Nigeria food_prices empty (no microdata)')
    return df


def test_all_waves_present(food_acquired):
    ts = set(food_acquired.index.get_level_values('t'))
    assert ts >= {'2010Q3', '2011Q1', '2012Q3', '2013Q1',
                  '2015Q3', '2016Q1', '2018Q3', '2019Q1'}


def test_source_split_is_not_inverted(food_acquired):
    """Purchased quantity must exceed own-produced quantity in EVERY wave.

    Pre-fix this fails for all six waves from 2012Q3 on (e.g. 2013Q1:
    produced 8.97M vs purchased 1.03M -- the mirror image of the truth).
    """
    q = (food_acquired.groupby(['t', 's'])['Quantity'].sum()
         .unstack(fill_value=0))
    bad = {t: (row.get('purchased', 0), row.get('produced', 0))
           for t, row in q.iterrows()
           if not row.get('purchased', 0) > row.get('produced', 0)}
    assert not bad, f'produced > purchased (inverted s-split) in: {bad}'


def test_produced_share_is_plausible(food_acquired):
    """Own production is a minority of Nigerian food acquisition in every wave.

    Pre-fix: 87-94% for 2012Q3 onward.
    """
    q = (food_acquired.groupby(['t', 's'])['Quantity'].sum()
         .unstack(fill_value=0))
    share = q.get('produced', 0) / q.sum(axis=1)
    bad = share[share > MAX_PRODUCED_SHARE]
    assert bad.empty, (
        f'implausible own-production share (> {MAX_PRODUCED_SHARE:.0%}) in:\n'
        f'{bad.round(3).to_string()}')


def test_gifts_are_their_own_acquisition_source(food_acquired):
    """q6a/q7a (gifts) must land on s='inkind', not be folded into purchased."""
    assert 'inkind' in set(food_acquired.index.get_level_values('s'))


def test_food_prices_does_not_collapse(food_acquired, food_prices):
    """Every wave must price the bulk of its purchased rows.

    This is the #591 symptom stated as an invariant: food_prices row count per
    wave must be within a factor of 2 of that wave's purchased-row count.
    Pre-fix, six waves priced 0.2%-1.6% of their purchased rows.
    """
    purchased = (food_acquired.xs('purchased', level='s')
                 .groupby('t').size())
    priced = food_prices.groupby('t').size()
    ratios = (priced / purchased).dropna()
    bad = ratios[(ratios < 0.5) | (ratios > 2.0)]
    assert bad.empty, (
        'food_prices row count is not within 2x of the purchased-row count '
        f'for:\n{ratios.round(4).to_string()}')


def test_no_infinite_prices_survive(food_prices):
    import numpy as np
    assert np.isfinite(food_prices['Price'].astype('float64')).all()
