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

Measured cold across the 7 waves (re-derived 2026-07-21): the builder
constructs 84,489 rows.  Pre-fix it returned 83,019 of them; post-fix it
returns 84,216.  Of the 1,342 colliding tuples on the old 5-level key, 1,196
(89%) were season-A/season-B pairs.

WHAT THESE TESTS DO NOT CLAIM.  84,216 is NOT "every reported line-item".  The
builder's de-dup step still discards 273 of the 84,489 rows it constructs --
135 deleted outright by ``groupby(dropna=True)`` because ``plot`` is NaN, and
138 merged away across 135 same-season duplicate groups, every one of them an
``input='Seed'`` pair from AGSEC4A.  That residual is pre-existing (pre-fix it
was 1,470), is documented with per-wave counts in
``uganda.plot_inputs_for_wave``, and is tracked on GH #637.  The numbers below
are POST-COLLAPSE row counts, and they are pinned so that residual cannot grow
unnoticed.

INSTRUMENT NOTE (inherited from the #323 tests -- do not undo it).  Do NOT
assert that the returned index is unique.  The builder collapses it, so
uniqueness holds BY CONSTRUCTION and such a test passes with the bug fully
present.  The bug is that ROWS DISAPPEAR and that surviving rows carry summed
quantities under a borrowed unit.  These tests assert on exact row counts and
on the specific rows that were being merged.
"""
import pandas as pd
import pytest

from tests.conftest import requires_s3
from lsms_library.country import Country

# Building Uganda hits DVC -> S3.  The CI ``unit-tests`` job is deliberately
# data-free, so skip the module there explicitly rather than letting it error.
pytestmark = requires_s3

# Rows the API returns per wave, POST-collapse (see the module docstring: this
# is not the same as the number of reported line-items).  Pre-fix the API
# returned the sum of these MINUS 1,197 (230/179/157/113/92/172/254
# respectively).
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
    """Build Uganda plot_inputs, and let a genuine build failure FAIL.

    Deliberately no ``try/except Exception -> pytest.skip``.  That shape was
    here in the first draft and it makes every test in this module vacuous:
    the most likely way for this fix to regress is for the build to raise, and
    a blanket skip reports that as green.  The only environmental reason to
    skip is missing S3 credentials, and that is handled twice over -- by the
    ``requires_s3`` marker above and by ``tests/conftest.py``'s
    ``pytest_runtest_makereport`` hook, which converts a
    ``NoCredentialsError`` (and only that, and only when credentials really
    are absent) into a skip.
    """
    return Country('Uganda').plot_inputs()


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


def test_total_row_count(plot_inputs):
    """Pin the total row count the API returns.

    NOT "every reported line-item survives" -- 273 of the 84,489 constructed
    rows are still discarded by the builder's de-dup step (see the module
    docstring).  This pins the post-collapse total so that residual cannot
    grow: pre-fix this number was 83,019, and the 1,197-row gap was the
    season-A/season-B merge.  This is one of the two tests that actually
    discriminate the fix; the other is ``test_per_wave_row_counts``.
    """
    assert len(plot_inputs) == EXPECTED_ROWS, (
        f'Uganda plot_inputs has {len(plot_inputs)} rows, expected '
        f'{EXPECTED_ROWS}.  {EXPECTED_ROWS - len(plot_inputs)} row(s) are '
        f'being discarded beyond the known residual -- most likely a season-A '
        f'row and a season-B row for the same (plot, input, j) being merged '
        f'(GH #637).'
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
        # NOT a skip: if the build succeeded at all, this insecticide pair is
        # in it.  Its disappearance is the regression, not a missing fixture.
        pytest.fail('household 1063000910 / plot 1063000910-1-1 / Insecticide '
                    'is absent from Uganda plot_inputs; it is present in '
                    'AGSEC3A (70 Litre) and AGSEC3B (1 Kg) of 2009-10')
    got = {(r['season'], float(r['Quantity']), r['u'])
           for _, r in rows.iterrows() if pd.notna(r['Quantity'])}
    expected = {('A', 70.0, 'Litre'), ('B', 1.0, 'Kg')}
    assert got == expected, (
        f'expected the season-A (70 Litre) and season-B (1 Kg) insecticide '
        f'applications to stay SEPARATE; got {got}.  A single row carrying '
        f'Quantity=71.0 means kilograms were summed with litres by the '
        f'seasonless collapse (GH #637).'
    )


def test_2013_14_nan_plot_seed_row_survives(plot_inputs):
    """Pin the one non-season row-count change, so it is not a silent effect.

    Adding `season` makes the 2013-14 index unique, so the builder's de-dup
    collapse no longer runs in that wave -- and therefore no longer DELETES a
    row whose `plot` is NaN via ``groupby(..., dropna=True)``.  So one of the
    "+113 recovered" rows in 2013-14 is not a season split at all, and
    2013-14's `Quantity` total rises by 3.0 (335,392.005 -> 335,395.005).
    Every other wave's total is unchanged, which is why "Quantity totals are
    unchanged" is not a true statement about this change.

    Fails pre-fix: the row is not in the output at all.
    """
    flat = plot_inputs.reset_index()
    row = flat[(flat['t'] == '2013-14') & flat['plot'].isna()
               & (flat['input'] == 'Seed') & (flat['j'] == 'Cassava')]
    assert len(row) == 1, (
        f'expected exactly one 2013-14 NaN-`plot` Seed/Cassava row (household '
        f'2113000606, Quantity 3.0, "Sack (100 kgs)"); got {len(row)}.  '
        f'Pre-fix it was deleted by `groupby(dropna=True)` inside the de-dup '
        f'collapse (GH #637).'
    )
    assert float(row['Quantity'].iloc[0]) == 3.0
    total = flat.loc[flat['t'] == '2013-14', 'Quantity'].sum()
    assert round(float(total), 3) == 335395.005, (
        f'2013-14 Quantity total is {total}, expected 335395.005 '
        f'(pre-fix: 335392.005)'
    )


def test_no_row_merges_two_seasons_of_the_same_input(plot_inputs):
    """Every 5-key duplicate differs by `season` -- a WEAK test, stated so.

    HONEST LABEL (adversarial review, 2026-07-21).  This does NOT test that
    quantities stopped being summed across seasons, despite what an earlier
    docstring here implied.  The builder collapses on the full 6-key, so any
    surviving (t, i, plot, input, j) duplicate MUST differ on `season` by
    construction, and the ``(per_group > 1).all()`` assertion is therefore
    close to tautological on any frame the builder can produce.

    What it does still catch is one real regression: both source files being
    tagged with the same season (or `season` becoming constant), which empties
    the 5-key duplicate set and trips the ``both.empty`` branch below.

    The tests that actually discriminate the fix are ``test_total_row_count``
    and ``test_per_wave_row_counts`` (exact counts, 84,216 vs the pre-fix
    83,019) and ``test_kilograms_are_not_summed_with_litres`` (the specific
    merged rows).
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
