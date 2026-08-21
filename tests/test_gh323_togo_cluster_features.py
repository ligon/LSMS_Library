"""GH #323 (Togo) -- cluster_features must be built AT cluster grain.

Togo's `cluster_features` is declared `(t, v)` -- one row per grappe -- but its
`df_main` source is the EHCVM *household* cover page
(`2018/Data1/s00_me_tgo2018.dta`: 6,171 households across 540 grappes).  The raw
extraction therefore emitted 6,171 rows for a 540-row table and handed the
framework 5,631 EXCESS rows -- every one of the 6,171 sits on a duplicated
`(t, v)` tuple, since no grappe holds fewer than 3 households -- to reduce with
`groupby().first()`.

That is an EXTRACTION bug, not an aggregation one.  Per decision D1 of
`slurm_logs/DESIGN_grain_collapse_sites_2026-07-13.org` the core does not
aggregate, so the fix is to stop manufacturing the duplicates: `2018/_/mapping.py`
aliases the SHIPPED reducer as its `cluster_features` df_edit hook ---

    from lsms_library.transformations import (
        collapse_to_cluster_grain as cluster_features,
    )

--- exactly as that helper's docstring prescribes.  A private hand-rolled copy
was written first and then removed (PR #632 review, Finding 1); the tests below
pin the alias, because a copy is free to drift and this one already had: its
`nunique(dropna=False)` guard read NaN as CONTRADICTION rather than as absence.

That divergence was LATENT, NOT LIVE -- measured against pristine
`origin/development`, cold (2026-08-20): on the 6,171-row pre-projection frame
the copy's guard finds a varying column in ZERO of the 540 grappes, so it would
NOT have raised and the build would NOT have died.  Togo's geo source is already
one row per grappe, so a GPS-less grappe is WHOLLY GPS-less (5 grappes, 56
household rows; 0 grappes partly GPS-less) and `nunique(dropna=False)` on an
all-NaN group is still 1.  The copy was wrong in CONTRACT, one upstream edit
from being wrong in FACT -- which is why it is pinned by a test rather than
described as a bug that fired.

INSTRUMENT NOTE (inherited from the CotedIvoire / Benin-Togo tests -- do not
undo it).  Do NOT assert that the *API* index is unique: the framework collapse
makes it unique BY CONSTRUCTION, so such an assertion passes with the bug fully
present.  The observable is the PRE-COLLAPSE wave frame, which is why
`test_wave_frame_is_already_at_cluster_grain` reaches for `grab_data` rather
than `Country.cluster_features()`.

DISCRIMINATION (measured on pristine `development`, cold, 2026-07-22).  Four of
the six tests here FAIL without the fix:
`test_wave_frame_is_already_at_cluster_grain` (6,171 rows),
`test_hook_is_the_shipped_reducer_not_a_private_copy`,
`test_projection_hook_raises_rather_than_picking_a_row` and
`test_nan_is_absence_not_contradiction` (no hook at all).  The remaining two --
`test_api_still_returns_every_cluster` and
`test_region_and_rural_are_constant_within_a_grappe` -- pass BOTH WITH AND
WITHOUT the change, deliberately: they pin the invariant the fix must not
break (no cluster lost) and the source fact the fix rests on (constancy within
a grappe).  They are not evidence that the fix is present; the four named above
are.

This collapse destroyed no VALUE (all 540 grappes are constant in Region and
Rural, so `.first()` landed on the right answer), and post-PR #614 the core is
correctly silent about a lossless collapse.  The fix therefore buys
correctness-by-construction, not rows: it converts an unchecked prose assumption
into an enforced one, in exactly the cell where the assumption failing would
return silently WRONG data rather than merely lossy data (cf. Kazakhstan 1996
cluster v=126, CotedIvoire grappe 648).
"""
import numpy as np
import pandas as pd
import pytest

from lsms_library.country import Country
from lsms_library.transformations import (
    collapse_to_cluster_grain,
    GrainConflict,
)

# The S3 guard is reached by MARKER, never by import (GH #648, commit 4a57ebf9).
# Both import spellings are broken, and an earlier cut of this module shipped the
# second one and turned CI red at COLLECTION -- before any mark could apply:
#   * `from conftest import ...` resolves to the repo-ROOT `conftest.py`, which
#     has no such name;
#   * `from tests.conftest import ...` resolves `tests` to the top-level `tests`
#     package that the `fooddatacentral` dependency installs into site-packages,
#     whose `__init__.py` is a broken `from .tests import *` --
#     `ModuleNotFoundError: No module named 'tests.tests'`.  It passes locally
#     only because `.venv/.../lsms_library.pth` puts the repo root on sys.path.
pytestmark = pytest.mark.requires_s3

N_GRAPPES = 540          # distinct grappe in s00_me_tgo2018.dta / grappe_gps
N_HOUSEHOLDS = 6171      # rows in the household cover page (the pre-fix count)
N_EXCESS = N_HOUSEHOLDS - N_GRAPPES      # 5,631 rows duplicating an earlier (t, v)


@pytest.fixture(scope='module')
def togo():
    """Deliberately does NOT swallow build errors.

    An earlier draft wrapped this in `except Exception: pytest.skip(...)`, which
    turns a genuinely broken build into a green run -- the end-to-end tests
    become vacuous exactly when they matter.  The only environmental excuse is
    absent S3 credentials, and that is handled twice over: the module-level
    `pytestmark = pytest.mark.requires_s3` above, and `tests/conftest.py`'s
    `pytest_runtest_makereport` hook, which converts a missing-credentials
    failure (and ONLY that) into a skip.
    """
    return Country('Togo')


@pytest.fixture(scope='module')
def wave_frame(togo):
    return togo['2018'].grab_data('cluster_features')


def test_wave_frame_is_already_at_cluster_grain(wave_frame):
    """The extraction -- not the framework collapse -- must set the grain.

    DISCRIMINATES: fails on pristine `development` with 6,171 rows (the
    household cover page passed straight through).
    """
    assert len(wave_frame) == N_GRAPPES, (
        f'Togo/2018 cluster_features wave frame has {len(wave_frame)} rows for '
        f'a {N_GRAPPES}-cluster table. The household cover page '
        f'(s00_me_tgo2018.dta, {N_HOUSEHOLDS} households) is reaching the '
        f'framework un-projected, leaving {len(wave_frame) - N_GRAPPES} excess '
        f'rows on already-seen (t, v) tuples for groupby().first() to reduce '
        f'(GH #323). Project in the extraction -- see the '
        f'collapse_to_cluster_grain alias in Togo/2018/_/mapping.py.'
    )
    assert wave_frame.index.is_unique, (
        'Togo/2018 cluster_features wave frame still carries duplicate (t, v) '
        'tuples; the framework would reduce them with groupby().first().'
    )


def test_api_still_returns_every_cluster(togo):
    """The projection must not cost a cluster (it recovers no rows either).

    DOES NOT DISCRIMINATE -- passes with and without the fix, by design: 540
    before, 540 after.  It pins the invariant the fix must not break.  The
    discriminating tests are `test_wave_frame_is_already_at_cluster_grain` and
    `test_hook_is_the_shipped_reducer_not_a_private_copy`.
    """
    df = togo.cluster_features()
    assert len(df) == N_GRAPPES, (
        f'Togo cluster_features returned {len(df)} rows, expected {N_GRAPPES}.'
    )
    assert set(df.index.names) == {'t', 'v'}


def test_region_and_rural_are_constant_within_a_grappe(togo):
    """The invariant the projection relies on, checked against the source.

    This is the claim that `.first()` used to make silently.  If it ever stops
    holding, the reducer raises `GrainConflict` and this test tells you why.

    DOES NOT DISCRIMINATE -- it is a fact about the survey, true before and
    after the fix.  Its job is to detect the day the fact stops being true.
    """
    from lsms_library.local_tools import get_dataframe
    import os
    wave_dir = togo.file_path / '2018' / '_'
    assert wave_dir.exists(), f'{wave_dir} missing'
    cwd = os.getcwd()
    try:
        os.chdir(wave_dir)
        cover = get_dataframe('../Data1/s00_me_tgo2018.dta',
                              convert_categoricals=True)
    finally:
        os.chdir(cwd)

    n = cover.groupby('grappe')[['s00q01', 's00q04']].nunique(dropna=False)
    bad_region = n.index[n['s00q01'] > 1].tolist()
    bad_rural = n.index[n['s00q04'] > 1].tolist()
    assert not bad_region and not bad_rural, (
        f'Region varies within grappe(s) {bad_region[:5]}; Rural varies within '
        f'grappe(s) {bad_rural[:5]}. The cluster_features projection is no '
        f'longer value-lossless -- do NOT reduce it; fix the cluster key.'
    )
    assert cover['grappe'].nunique() == N_GRAPPES
    assert len(cover) == N_HOUSEHOLDS
    # every one of the 6,171 rows sat on a duplicated (t, v): no singleton grappe
    assert (cover.groupby('grappe').size() > 1).all()


def test_hook_is_the_shipped_reducer_not_a_private_copy(togo):
    """The hook must BE `collapse_to_cluster_grain`, not a look-alike.

    DISCRIMINATES: on pristine `development` there is no hook at all, and
    against the first draft of this PR the hook was a private 40-line copy whose
    NaN semantics contradicted `reduce_to_agreed`'s.  Identity, not behaviour,
    is asserted on purpose -- a copy is free to drift, and this one already had
    (PR #632 review, Findings 1-3).
    """
    hook = togo['2018'].formatting_functions.get('cluster_features')
    assert hook is not None, (
        'Togo/2018 has no cluster_features df_edit hook; the household cover '
        'page would reach the framework at household grain (GH #323).'
    )
    assert hook is collapse_to_cluster_grain, (
        f'Togo/2018 cluster_features hook is {hook!r}, not the shipped '
        f'lsms_library.transformations.collapse_to_cluster_grain. Alias the '
        f'helper; do not re-implement it.'
    )


def _conflicting_frame():
    idx = pd.MultiIndex.from_tuples(
        [('2018', '001'), ('2018', '001'), ('2018', '002')], names=['t', 'v'])
    return pd.DataFrame(
        {'Region': ['Maritime', 'Kara', 'Kara'],
         'Rural': ['Rural', 'Rural', 'Urban']}, index=idx)


def test_projection_hook_raises_rather_than_picking_a_row(togo):
    """Negative test: the reducer must FAIL LOUDLY, not silently pick a winner.

    A quiet wrong answer is the failure mode this whole issue is about, so the
    hook's value is entirely in this branch.  Without it the frame would just
    be de-duplicated and one Region kept at random.

    DISCRIMINATES: fails on pristine `development` (no hook).  This test pins
    that Togo is wired to the reducer with the default `on_conflict='raise'`;
    `reduce_to_agreed`'s own contract is owned by
    `tests/test_gh323_explicit_reducers.py`.
    """
    hook = togo['2018'].formatting_functions.get('cluster_features')
    assert hook is not None
    with pytest.raises(GrainConflict, match='grain conflict'):
        hook(_conflicting_frame())

    # ... and it must stay quiet (and lossless) when the invariant holds.
    idx = _conflicting_frame().index
    consistent = pd.DataFrame(
        {'Region': ['Maritime', 'Maritime', 'Kara'],
         'Rural': ['Rural', 'Rural', 'Urban']}, index=idx)
    out = hook(consistent)
    assert len(out) == 2 and out.index.is_unique
    assert out.loc[('2018', '001'), 'Region'] == 'Maritime'


def test_nan_is_absence_not_contradiction(togo):
    """A grappe that reports a value in one row and nothing in another COMPLETES.

    DISCRIMINATES, and it is the specific reason the private copy had to go: its
    `nunique(dropna=False)` guard counted NaN as a distinct value, so the
    SYNTHETIC frame below raises `ValueError` under the copy while the shipped
    helper completes it.

    On TODAY'S Togo data that divergence never fires -- LATENT, not live.  The
    geo source is already one row per grappe, so its 5 GPS-less grappes (56
    household rows in the 6,171-row pre-projection frame) are WHOLLY GPS-less
    and `nunique(dropna=False)` on an all-NaN group is 1; measured cold against
    pristine `origin/development`, the copy's guard flags 0 of 540 grappes.  A
    single upstream edit that made one grappe PARTLY GPS-less would make it
    live -- and the repo's doctrine, pinned by
    `tests/test_gh323_grain_contract.py::
    test_p2_complementary_missingness_is_COMPLETION_not_fabrication`, is that
    NaN is ABSENCE.

    Fails on pristine `development` too (no hook).
    """
    hook = togo['2018'].formatting_functions.get('cluster_features')
    assert hook is not None
    idx = pd.MultiIndex.from_tuples(
        [('2018', '001'), ('2018', '001')], names=['t', 'v'])
    complementary = pd.DataFrame(
        {'Region': ['Maritime', np.nan], 'Latitude': [np.nan, 6.1]}, index=idx)
    out = hook(complementary)
    assert len(out) == 1
    assert out.loc[('2018', '001'), 'Region'] == 'Maritime'
    assert out.loc[('2018', '001'), 'Latitude'] == 6.1
