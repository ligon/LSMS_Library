"""Togo `nonfood_expenditures` -- EHCVM section 9, parts B..F (GH #750).

Two things are pinned here, and they fail in different ways.

1. THE STRUCTURAL FACTS, which need no microdata.  The five
   `Togo_survey2018_nonfooditems*.csv.dvc` sidecars were `dvc add`ed in 2021
   and never pushed; their blobs are on no remote and in no workspace, so they
   are unrecoverable and have been deleted.  The two `_forEthan.dta` sidecars
   in the same directory ARE on the remote and must stay.  A future cleanup
   that swept the whole directory would look like tidying and would be a real
   loss, so both halves are asserted.

2. THE WINDOW MAPPING, which needs the data.  s09b..s09f are RECALL-WINDOW
   modules -- the instrument says so in as many words ("PARTIE B: DEPENSES NON
   ALIMENTAIRES DES 7 DERNIERS JOURS", ... 30 DERNIERS JOURS, 3 / 6 / 12
   DERNIERS MOIS; 2018/Documentation/tgo_ehcvm1_qnr_household_excel_vague1.xls,
   sheets S9b__Conso_NA .. S9f__Conso_NA).  `Expenditure` is therefore the
   amount spent over THAT window and is NOT annualised.

   The test that a window mapping is not scrambled is that median expenditure
   RISES with the length of the window.  It does: 500 / 900 / 1,200 / 3,000 /
   3,500 FCFA.  Asserting monotonicity rather than the five literals leaves the
   check meaningful if the numbers move, while still failing loudly if the five
   files are ever wired to the wrong five labels -- which is the mistake this
   feature is actually exposed to.

Deliberately NOT asserted: post-collapse index uniqueness.  Per the #323
doctrine (see tests/test_gh323_benin_togo.py), the framework collapses a
non-unique declared index, so API-level uniqueness holds by construction and
would pass with the bug fully present.  What is asserted instead is the exact
row count -- every reported line-item must survive -- and injectivity of the
label map, which is what would destroy rows here.
"""
import pandas as pd
import pytest

from lsms_library.country import Country
from lsms_library.paths import countries_root

# 17,932 (7d) + 27,871 (30d) + 10,999 (3m) + 32,326 (6m) + 19,317 (12m).
# Every source row is a reported purchase (the modules ship pre-filtered to
# the gate s09?q02 == 1), so every one of them must reach the API.
TOGO_NONFOOD_ROWS = 108445

WINDOWS_IN_ORDER = ['7 days', '30 days', '3 months', '6 months', '12 months']

RETIRED_SIDECARS = [
    'Togo_survey2018_nonfooditems7days.csv.dvc',
    'Togo_survey2018_nonfooditems30days.csv.dvc',
    'Togo_survey2018_nonfooditems3months.csv.dvc',
    'Togo_survey2018_nonfooditems6months.csv.dvc',
    'Togo_survey2018_nonfooditems12months.csv.dvc',
]

KEPT_SIDECARS = [
    'Togo_survey2018_fooditems_forEthan.dta.dvc',
    'Togo_survey2018_hhroster_forEthan.dta.dvc',
]


def _data_dir():
    return countries_root() / 'Togo' / '2018' / 'Data'


def test_unrecoverable_nonfooditems_sidecars_are_gone():
    """The five orphan CSV sidecars must not come back (GH #750)."""
    d = _data_dir()
    present = [n for n in RETIRED_SIDECARS if (d / n).exists()]
    assert not present, (
        f'{present} are back in {d}.  Those blobs were dvc-added in 2021 and '
        f'never pushed: they are on no remote and in no workspace, so the '
        f'sidecars point at nothing and `dvc pull` cannot succeed for anyone.  '
        f'The published EHCVM modules 2018/Data1/s09b..s09f_me_tgo2018.dta '
        f'carry the same five recall windows.')


def test_forEthan_sidecars_are_kept():
    """The two `_forEthan` sidecars ARE on the remote -- do not sweep them."""
    d = _data_dir()
    missing = [n for n in KEPT_SIDECARS if not (d / n).exists()]
    assert not missing, (
        f'{missing} were removed from {d}.  Unlike the five nonfooditems CSVs, '
        f'these two blobs are present on the S3 remote; deleting their '
        f'sidecars discards recoverable data.')


@pytest.fixture(scope='module')
def nonfood():
    try:
        c = Country('Togo')
    except Exception as exc:                                   # pragma: no cover
        pytest.skip(f'Togo unavailable: {exc}')
    try:
        return c.nonfood_expenditures()
    except Exception as exc:                                   # pragma: no cover
        pytest.skip(f'Togo nonfood_expenditures could not be built: '
                    f'{type(exc).__name__}: {exc}')


def test_every_reported_line_item_survives(nonfood):
    assert len(nonfood) == TOGO_NONFOOD_ROWS, (
        f'Togo/nonfood_expenditures has {len(nonfood)} rows, expected '
        f'{TOGO_NONFOOD_ROWS} -- the sum of the five s09 modules.  Rows are '
        f'being dropped between the wave script and the API.')


def test_recall_window_is_carried_and_not_annualised(nonfood):
    """The window must survive to the API, or the number is uninterpretable."""
    assert 'RecallWindow' in nonfood.columns, (
        'RecallWindow is gone.  Expenditure is reported over a window that '
        'differs by item (7 days to 12 months); without the window the numbers '
        'cannot be compared or scaled.')
    assert set(nonfood['RecallWindow'].dropna()) == set(WINDOWS_IN_ORDER)


def test_median_expenditure_rises_with_the_recall_window(nonfood):
    """A 7-day median far below a 12-month median, or the mapping is wrong."""
    med = (nonfood.groupby('RecallWindow', observed=True)['Expenditure']
                  .median().reindex(WINDOWS_IN_ORDER))
    assert med.notna().all(), f'a window has no expenditure at all: {med}'
    diffs = med.diff().dropna()
    assert (diffs >= 0).all(), (
        f'median expenditure does not rise with the recall window: '
        f'{med.to_dict()}.  The five source files are almost certainly wired '
        f'to the wrong five window labels.')
    assert med['7 days'] < med['12 months'], med.to_dict()


def test_item_labels_are_a_subset_of_the_declared_table(nonfood):
    """Every delivered `j` must come from `nonfood_items`, and be unique.

    `j` is an index level, so two items sharing a Preferred Label are silently
    pooled by the framework collapse -- the harmonize_seed_crop failure of
    GH #323.  Injectivity is asserted at build time too; this is the API-side
    twin.
    """
    c = Country('Togo')
    tbl = c.categorical_mapping.get('nonfood_items')
    if tbl is None:                                            # pragma: no cover
        pytest.skip('nonfood_items absent from categorical_mapping.org')
    declared = [str(x).strip() for x in tbl['Preferred Label']]
    assert len(declared) == len(set(declared)), (
        'nonfood_items Preferred Labels are not unique; duplicates: '
        f'{sorted({x for x in declared if declared.count(x) > 1})}')
    delivered = set(nonfood.index.get_level_values('j'))
    assert delivered <= set(declared), sorted(delivered - set(declared))


def test_household_ids_all_resolve_to_sample(nonfood):
    """No orphan households: `i` must match sample()'s composite EHCVM id."""
    s = Country('Togo').sample()
    orphans = (set(nonfood.index.get_level_values('i'))
               - set(s.index.get_level_values('i')))
    assert not orphans, (
        f'{len(orphans)} nonfood households are absent from sample(); the '
        f'composite (grappe, menage) id has drifted from togo.i(). '
        f'Examples: {sorted(orphans)[:5]}')
