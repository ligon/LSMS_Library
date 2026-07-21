"""GH #637 -- Uganda plot_inputs must carry `season` in its grain.

`uganda.plot_inputs_for_wave` reads AGSEC3A (season 1) and AGSEC3B (season 2)
-- two DISTINCT plot-season questionnaires -- and, before this fix, indexed the
result on (t, i, plot, input, j) with no season level.  The same plot id
appears in both files, so an input applied in season A and again in season B
landed on ONE index tuple.  The builder's own de-dup step then

    Quantity / Quantity_purchased  ->  sum()
    Purchased / Improved           ->  max()
    u                              ->  first()

which SUMS QUANTITIES ACROSS SEASONS and picks one native unit at random.

THE FIX IS TO THE IDENTIFIER, NOT TO A REDUCER (GH #323 doctrine D1).  The
missing level is `season`, and the repo had already decided this: the sibling
`plot_labor` builder reads THE SAME TWO FILES and carries `season` explicitly,
with the reason stated in Uganda/_/data_scheme.yml --

    "collapsing the two seasons into one (plot, source) row would require
     summing across seasons (a transformation)"

-- and `crop_production` carries it too.  `plot_inputs` was the only one of the
three UNPS plot-level features that did not.

Measured before the fix (7 waves): 83,019 rows returned against 84,216
reported line-items -- 1,197 destroyed, ~95% of them season-A/season-B pairs.

INSTRUMENT NOTE (inherited from the #323 tests -- do not undo it).  Do NOT
assert that the returned index is unique.  The builder collapses it, so
uniqueness holds BY CONSTRUCTION and such a test passes with the bug fully
present.  The bug is that ROWS DISAPPEAR and that surviving rows carry summed
quantities under a borrowed unit.  These tests assert on exact row counts and
on the specific rows that were being merged.
"""
import pandas as pd
import pytest

from lsms_library.country import Country

# Reported input line-items per wave.  Pre-fix the API returned the sum of
# these MINUS 1,197 (230/179/157/113/92/172/254 respectively).
EXPECTED_ROWS_BY_WAVE = {
    '2009-10': 13993,
    '2010-11': 13554,
    '2011-12': 12353,
    '2013-14': 11834,
    '2015-16': 11981,
    '2018-19': 10367,
    '2019-20': 10134,
}
EXPECTED_ROWS = sum(EXPECTED_ROWS_BY_WAVE.values())          # 84_216


@pytest.fixture(scope='module')
def plot_inputs():
    try:
        c = Country('Uganda')
    except Exception as exc:                                   # pragma: no cover
        pytest.skip(f'Uganda unavailable: {exc}')
    try:
        return c.plot_inputs()
    except Exception as exc:                                   # pragma: no cover
        pytest.skip(f'Uganda plot_inputs could not be built: '
                    f'{type(exc).__name__}: {exc}')


def test_season_is_an_index_level(plot_inputs):
    """`season` must be part of the grain, not folded away."""
    assert 'season' in plot_inputs.index.names, (
        f'Uganda plot_inputs index is {list(plot_inputs.index.names)} with no '
        f'`season` level.  AGSEC3A and AGSEC3B are distinct plot-SEASON '
        f'questionnaires and the same plot id appears in both, so without '
        f'`season` a season-A application and a season-B application of the '
        f'same input collapse onto one row -- summing quantities across '
        f'seasons and taking one unit at random (GH #637).'
    )


def test_both_seasons_are_present(plot_inputs):
    """Season B must actually survive -- not be silently emptied."""
    seasons = set(plot_inputs.reset_index()['season'].dropna().unique())
    assert seasons == {'A', 'B'}, f'expected seasons A and B, got {seasons}'


def test_keeps_every_reported_line_item(plot_inputs):
    """Every reported input line-item must survive to the API."""
    assert len(plot_inputs) == EXPECTED_ROWS, (
        f'Uganda plot_inputs has {len(plot_inputs)} rows, expected '
        f'{EXPECTED_ROWS}.  {EXPECTED_ROWS - len(plot_inputs)} reported '
        f'line-item(s) are being discarded -- most likely a season-A row and a '
        f'season-B row for the same (plot, input, j) being merged (GH #637).'
    )


def test_per_wave_row_counts(plot_inputs):
    got = plot_inputs.reset_index().groupby('t').size().to_dict()
    assert got == EXPECTED_ROWS_BY_WAVE, (
        f'per-wave plot_inputs row counts changed: {got} != '
        f'{EXPECTED_ROWS_BY_WAVE}'
    )


def test_kilograms_are_not_summed_with_litres(plot_inputs):
    """The sharpest pre-fix symptom, on the row that showed it.

    Uganda 2009-10 household 1063000910, plot 1063000910-1-1, applied
    70 LITRES of insecticide in season A and 1 KILOGRAM in season B.  On the
    old seasonless key those two rows collided; `sum()` produced Quantity=71
    and `first()` labelled the whole thing 'Litre' -- a quantity that exists
    nowhere in the survey, in a unit that is wrong for part of it.

    Assert the exact (season, Quantity, u) TRIPLES.  Merely checking that a
    row with Quantity=70 exists passes with the bug present.
    """
    flat = plot_inputs.reset_index()
    rows = flat[(flat['t'] == '2009-10')
                & (flat['plot'] == '1063000910-1-1')
                & (flat['input'] == 'Insecticide')]
    if rows.empty:                                             # pragma: no cover
        pytest.skip('household 1063000910 not present')
    got = {(r['season'], float(r['Quantity']), r['u'])
           for _, r in rows.iterrows() if pd.notna(r['Quantity'])}
    expected = {('A', 70.0, 'Litre'), ('B', 1.0, 'Kg')}
    assert got == expected, (
        f'expected the season-A (70 Litre) and season-B (1 Kg) insecticide '
        f'applications to stay SEPARATE; got {got}.  A single row carrying '
        f'Quantity=71.0 means kilograms were summed with litres by the '
        f'seasonless collapse (GH #637).'
    )


def test_no_row_merges_two_seasons_of_the_same_input(plot_inputs):
    """No (t, i, plot, input, j) group may hold two units across seasons.

    Generalises the case above: for every plot-input reported in BOTH seasons,
    the two rows must still be distinguishable.  Pre-fix these were exactly
    the groups whose `u` was resolved arbitrarily.
    """
    flat = plot_inputs.reset_index()
    key = ['t', 'i', 'plot', 'input', 'j']
    both = flat[flat.duplicated(key, keep=False)]
    if both.empty:                                             # pragma: no cover
        pytest.fail('no plot-input appears in both seasons -- the fixture '
                    'looks wrong, since season B carries >4,000 rows')
    per_group = both.groupby(key)['season'].nunique()
    assert (per_group > 1).all(), (
        f'{int((per_group <= 1).sum())} duplicated (t,i,plot,input,j) groups '
        f'do NOT differ by season, so `season` is not what separates them; '
        f'the grain is still incomplete (GH #637).'
    )
