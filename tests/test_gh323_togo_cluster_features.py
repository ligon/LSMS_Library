"""GH #323 (Togo) -- cluster_features must be built AT cluster grain.

Togo's `cluster_features` is declared `(t, v)` -- one row per grappe -- but its
`df_main` source is the EHCVM *household* cover page
(`2018/Data1/s00_me_tgo2018.dta`: 6,171 households across 540 grappes).  The raw
extraction therefore emitted 6,171 rows for a 540-row table and handed the
framework 5,631 rows sitting on a duplicated `(t, v)` tuple to reduce with
`groupby().first()`.

That is an EXTRACTION bug, not an aggregation one.  Per decision D1 of
`slurm_logs/DESIGN_grain_collapse_sites_2026-07-13.org` the core does not
aggregate, so the fix is to stop manufacturing the duplicates -- a
`cluster_features(df)` hook in `2018/_/mapping.py` that projects to cluster
grain and RAISES if any column varies within a grappe.

INSTRUMENT NOTE (inherited from the CotedIvoire / Benin-Togo tests -- do not
undo it).  Do NOT assert that the *API* index is unique: the framework collapse
makes it unique BY CONSTRUCTION, so such an assertion passes with the bug fully
present.  The observable is the PRE-COLLAPSE wave frame, which is why
`test_wave_frame_is_already_at_cluster_grain` reaches for `grab_data` rather
than `Country.cluster_features()`.

This collapse destroyed no VALUE (all 540 grappes are constant in Region and
Rural, so `.first()` landed on the right answer), and post-PR #614 the core is
correctly silent about a lossless collapse.  The fix therefore buys
correctness-by-construction, not rows: it converts an unchecked prose assumption
into an enforced one, in exactly the cell where the assumption failing would
return silently WRONG data rather than merely lossy data (cf. Kazakhstan 1996
cluster v=126, CotedIvoire grappe 648).
"""
import pandas as pd
import pytest

from lsms_library.country import Country

N_GRAPPES = 540          # distinct grappe in s00_me_tgo2018.dta / grappe_gps
N_HOUSEHOLDS = 6171      # rows in the household cover page (the pre-fix count)


@pytest.fixture(scope='module')
def togo():
    try:
        return Country('Togo')
    except Exception as exc:                                    # pragma: no cover
        pytest.skip(f'Togo unavailable: {exc}')


@pytest.fixture(scope='module')
def wave_frame(togo):
    try:
        return togo['2018'].grab_data('cluster_features')
    except Exception as exc:                                    # pragma: no cover
        pytest.skip(f'Togo/2018 cluster_features could not be built: '
                    f'{type(exc).__name__}: {exc}')


def test_wave_frame_is_already_at_cluster_grain(wave_frame):
    """The extraction -- not the framework collapse -- must set the grain.

    FAILS on pristine `development` with 6,171 rows: the household cover page
    passed straight through.
    """
    assert len(wave_frame) == N_GRAPPES, (
        f'Togo/2018 cluster_features wave frame has {len(wave_frame)} rows for '
        f'a {N_GRAPPES}-cluster table. The household cover page '
        f'(s00_me_tgo2018.dta, {N_HOUSEHOLDS} households) is reaching the '
        f'framework un-projected, leaving {len(wave_frame) - N_GRAPPES} rows on '
        f'a duplicated (t, v) tuple for groupby().first() to reduce (GH #323). '
        f'Project in the extraction -- see cluster_features() in '
        f'Togo/2018/_/mapping.py.'
    )
    assert wave_frame.index.is_unique, (
        'Togo/2018 cluster_features wave frame still carries duplicate (t, v) '
        'tuples; the framework would reduce them with groupby().first().'
    )


def test_api_still_returns_every_cluster(togo):
    """The projection must not cost a cluster (it recovers no rows either)."""
    df = togo.cluster_features()
    assert len(df) == N_GRAPPES, (
        f'Togo cluster_features returned {len(df)} rows, expected {N_GRAPPES}.'
    )
    assert set(df.index.names) == {'t', 'v'}


def test_region_and_rural_are_constant_within_a_grappe(togo):
    """The invariant the projection relies on, checked against the source.

    This is the claim that `.first()` used to make silently.  If it ever stops
    holding, the hook raises and this test tells you why.
    """
    from lsms_library.local_tools import get_dataframe
    import os
    wave_dir = togo.file_path / '2018' / '_'
    if not wave_dir.exists():                                   # pragma: no cover
        pytest.skip('Togo/2018/_ not present')
    cwd = os.getcwd()
    try:
        os.chdir(wave_dir)
        cover = get_dataframe('../Data1/s00_me_tgo2018.dta',
                              convert_categoricals=True)
    except Exception as exc:                                    # pragma: no cover
        pytest.skip(f'cover page unavailable: {exc}')
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


def test_projection_hook_raises_rather_than_picking_a_row(togo):
    """Negative test: the guard must FAIL LOUDLY, not silently pick a winner.

    A quiet wrong answer is the failure mode this whole issue is about, so the
    hook's value is entirely in this branch.  Without it the frame would just
    be de-duplicated and one Region kept at random.
    """
    hook = togo['2018'].formatting_functions.get('cluster_features')
    assert hook is not None, (
        'Togo/2018 has no cluster_features df_edit hook; the household cover '
        'page would reach the framework at household grain (GH #323).'
    )
    idx = pd.MultiIndex.from_tuples(
        [('2018', '001'), ('2018', '001'), ('2018', '002')], names=['t', 'v'])
    conflicting = pd.DataFrame(
        {'Region': ['Maritime', 'Kara', 'Kara'],
         'Rural': ['Rural', 'Rural', 'Urban']}, index=idx)
    with pytest.raises(ValueError, match='vary WITHIN a cluster'):
        hook(conflicting)

    # ... and it must stay quiet (and lossless) when the invariant holds.
    consistent = pd.DataFrame(
        {'Region': ['Maritime', 'Maritime', 'Kara'],
         'Rural': ['Rural', 'Rural', 'Urban']}, index=idx)
    out = hook(consistent)
    assert len(out) == 2 and out.index.is_unique
    assert out.loc[('2018', '001'), 'Region'] == 'Maritime'
