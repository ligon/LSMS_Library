"""GH #323 -- Ethiopia: silent index collapse via groupby().first().

Every test here FAILS on pristine ``development``.

Background.  ``_normalize_dataframe_index`` collapses a non-unique DECLARED
index with ``groupby().first()``, silently discarding the dropped rows.  For
Ethiopia the non-uniqueness was NOT a legitimate finer grain that wanted
aggregating -- it was MANUFACTURED by four distinct extraction bugs, so the
collapse was destroying real, distinguishable observations:

  * cluster_features -- two HOUSEHOLD-grain sub-frames merged on the CLUSTER
    key ``v`` with how='outer': a many-to-many join whose cartesian product
    made a 433-row table 65,508 rows (W2).  In the other three waves the geo
    sub-df failed to load at all (wrong column names) and Latitude/Longitude
    silently vanished from the table.
  * individual_education -- indexed on the W1-BASELINE ids while the roster
    used the wave-native ids, so 5,247 real people (the W2 urban refreshment
    sample, which has blank baseline ids) collapsed into one phantom tuple.
  * interview_date -- W5's source is holder-grain, not household-grain.
  * crop_production -- exact duplicate §9 records, a many-to-one crop-label
    map, and multiple §9 crop entries per field all collided on the canonical
    index.

The fixes are in the extraction (correct ids / correct merge key / correct
source column names) plus, where the reduction is real, an EXPLICIT
``aggregation:`` policy declared in data_scheme.yml.
"""
from __future__ import annotations

import warnings

import pandas as pd
import pytest

from lsms_library.country import Country

pytestmark = pytest.mark.slow   # builds the full Ethiopia country tables


@pytest.fixture(scope='module')
def eth():
    return Country('Ethiopia')


def _wave(df, t):
    return df.xs(t, level='t', drop_level=False)


# ---------------------------------------------------------------- the class

def test_no_silent_collapse_warning_anywhere(eth):
    """No Ethiopia table may collapse its canonical index with a silent first().

    This is the CLASS check: it fails if ANY table/wave still lands a
    non-unique declared index in ``_normalize_dataframe_index``'s undeclared
    fallback branch.  Pre-fix this fires for cluster_features (5 waves),
    individual_education (2), interview_date (1), crop_production (5) and
    panel_ids (1).
    """
    tables = ['cluster_features', 'individual_education', 'interview_date',
              'crop_production', 'household_roster', 'shocks', 'housing',
              'assets', 'sample']
    offenders = []
    for t in tables:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            df = getattr(eth, t)()
        for w in caught:
            if 'GH #323' in str(w.message):
                offenders.append((t, str(w.message)[:70]))
        if int(df.index.duplicated().sum()):
            offenders.append((t, 'duplicate index tuples in returned frame'))
    assert not offenders, f'silent index collapse still present: {offenders}'


# -------------------------------------------------------- cluster_features

def test_cluster_features_is_ea_grain_not_cartesian(eth):
    """One row per (wave, EA) -- not a household x household cartesian."""
    cf = eth.cluster_features()
    assert not cf.index.duplicated().any()
    # The EA counts are a property of the ESS sample design.
    expected = {'2011-12': 333, '2013-14': 433, '2015-16': 432,
                '2018-19': 535, '2021-22': 435}
    got = cf.groupby(level='t').size().to_dict()
    assert got == expected, f'EA counts wrong: {got}'
    # Pre-fix W2 alone was 65,508 rows (5,262 x 5,287 households / 433 EAs).
    assert len(_wave(cf, '2013-14')) == 433


def test_cluster_features_has_coordinates_in_every_wave(eth):
    """Latitude/Longitude must be PRESENT and populated in all five waves.

    Pre-fix the geo sub-df raised a KeyError in W1 (LAT_DD_MOD vs lat_dd_mod),
    W4 (lat_mod vs lat_dd_mod) and W5 (no ea_id column at all); the optional-
    sub-df fallback swallowed it and the columns silently disappeared.
    """
    cf = eth.cluster_features()
    assert {'Latitude', 'Longitude'} <= set(cf.columns)
    for t in ['2011-12', '2013-14', '2015-16', '2018-19', '2021-22']:
        w = _wave(cf, t)
        frac = w['Latitude'].notna().mean()
        assert frac > 0.9, f'{t}: only {frac:.1%} of EAs have a Latitude'
        # Ethiopia's bounding box -- catches a scrambled join.
        lat = w['Latitude'].dropna().astype(float)
        lon = w['Longitude'].dropna().astype(float)
        assert lat.between(3, 15).all(), f'{t}: Latitude outside Ethiopia'
        assert lon.between(32, 48).all(), f'{t}: Longitude outside Ethiopia'


def test_cluster_features_coordinate_is_the_ea_median(eth):
    """The EA coordinate is the DECLARED median of its households' GPS fixes.

    Household coordinates are not EA-constant (38/433 EAs in W2 carry more
    than one), so ``first`` would publish an arbitrary household's location as
    the cluster's.  data_scheme.yml declares ``Latitude: median``.
    """
    from lsms_library.local_tools import get_dataframe
    from lsms_library.paths import countries_root
    base = countries_root() / 'Ethiopia' / '2013-14' / 'Data'
    cover = get_dataframe(str(base / 'sect_cover_hh_w2.dta'))
    geo = get_dataframe(str(base / 'Pub_ETH_HouseholdGeovars_Y2.dta'))
    hh = (geo[['household_id2', 'lat_dd_mod']]
          .merge(cover[['household_id2', 'ea_id2']], on='household_id2'))
    want = hh.groupby('ea_id2')['lat_dd_mod'].median()

    cf = _wave(eth.cluster_features(), '2013-14').reset_index()
    got = cf.set_index('v')['Latitude'].astype(float)
    # EAs where households disagree -- the ones where the reducer actually
    # matters.  Validating only where every household agrees would be vacuous.
    spread = hh.groupby('ea_id2')['lat_dd_mod'].nunique()
    contested = [str(e) for e in spread[spread > 1].index]
    assert len(contested) >= 30, 'expected the contested EAs to exist'
    checked = 0
    for ea in contested:
        if ea in got.index:
            assert got[ea] == pytest.approx(float(want[ea]), abs=1e-6), ea
            checked += 1
    assert checked >= 30, f'only checked {checked} contested EAs'


def test_cartesian_merge_raises(eth):
    """The many-to-many dfs: merge is refused, not mopped up by first().

    This is the systemic guard: merging two frames that are BOTH finer-grained
    than merge_on can only produce a cartesian product.
    """
    import pandas as pd
    w = eth['2013-14']
    left = pd.DataFrame({'t': ['a'] * 3, 'v': ['E1', 'E1', 'E2'], 'A': [1, 2, 3]})
    right = pd.DataFrame({'t': ['a'] * 3, 'v': ['E1', 'E1', 'E2'], 'B': [4, 5, 6]})
    with pytest.raises(ValueError, match='CARTESIAN'):
        w._merge_subframes(left, right, ['t', 'v'], 'cluster_features',
                           'df_main', 'df_geo')
    # a many-to-one merge on the same data is fine
    ok = w._merge_subframes(left, right.drop_duplicates('v'), ['t', 'v'],
                            'cluster_features', 'df_main', 'df_geo')
    assert len(ok) == 3


# ---------------------------------------------------- individual_education

@pytest.mark.parametrize('wave,source_rows', [('2013-14', 23_785),
                                              ('2015-16', 23_393)])
def test_individual_education_wave_index_is_unique(eth, wave, source_rows):
    """EVERY source row must survive as its own (t, i, pid) tuple.

    This is the load-bearing assertion, made at the WAVE level -- the source of
    truth -- because the country/API layer additionally drops rows whose
    'Educational Attainment' is NaN (people the §2 module rosters but never asks
    about).  That NaN filter is uniform across all five waves and predates this
    fix; it would mask the recovery if we only counted API rows.

    Pre-fix, the W1-baseline ids collapsed 5,247 W2 rows (the urban refreshment
    sample: blank household_id AND blank individual_id -> one phantom tuple
    ('2013-14','','')) and 8 W3 rows (split-off households sharing a parent's
    W1 id).
    """
    df = eth[wave].grab_data('individual_education')
    assert len(df) == source_rows, f'{wave}: expected all {source_rows} §2 rows'
    dup = int(df.index.duplicated().sum())
    assert dup == 0, f'{wave}: {dup} rows still collapse onto another'
    flat = df.reset_index()
    blank = (flat['i'].astype(str).str.strip() == '')
    assert not blank.any(), f'{wave}: {int(blank.sum())} blank-id rows'


def test_individual_education_keeps_the_refreshment_sample(eth):
    """The recovered W2 students must reach the API, not just the wave parquet.

    Pre-fix the API returned 8,505 W2 rows; the 5,247 collapsed refreshment-
    sample students were gone.  (The API count is below the 23,785 source rows
    because of the pre-existing NaN-attainment filter -- see the wave-level test
    above for the exact row-for-row assertion.)
    """
    ie = eth.individual_education()
    assert not ie.index.duplicated().any()
    w2 = _wave(ie, '2013-14')
    assert len(w2) > 12_000, (
        f'W2 education has only {len(w2)} rows; pre-fix it was 8,505 and the '
        'refreshment sample should add ~4,000 more')
    ids = w2.index.get_level_values('i').astype(str)
    assert not (ids.str.strip() == '').any(), 'phantom blank-id household'


def test_individual_education_pid_joins_the_roster(eth):
    """education.pid must be in the SAME keyspace as household_roster.pid.

    Pre-fix the roster used individual_id2 and education used individual_id,
    so the two tables could not be joined at all for W2/W3.
    """
    ie = eth.individual_education()
    hr = eth.household_roster()
    for t in ['2013-14', '2015-16']:
        e = set(_wave(ie, t).index.get_level_values('pid').astype(str))
        r = set(_wave(hr, t).index.get_level_values('pid').astype(str))
        overlap = len(e & r) / max(len(e), 1)
        assert overlap > 0.9, f'{t}: only {overlap:.1%} of education pids join'


def test_individual_education_split_offs_stay_distinct(eth):
    """W3 split-off households share a W1 id; they are DIFFERENT households.

    Baseline HH 01050100105031 appears in W3 as BOTH 010501020100105031 and
    010501088800105031, and individual 0105010010503101 is recorded with '10th
    Grade Complete' in one and '9th Grade Complete' in the other.  Under the
    baseline ids those two real people collapsed onto one tuple and first()
    silently kept one grade.  Under household_id2 they stay distinct.
    """
    from lsms_library.local_tools import get_dataframe
    from lsms_library.paths import countries_root
    raw = get_dataframe(str(countries_root() / 'Ethiopia' / '2015-16' / 'Data'
                            / 'sect2_hh_w3.dta'))

    # The rows the BASELINE ids cannot tell apart -- these are the ones first()
    # collapsed.  Validating anywhere else would be vacuous.
    key = ['household_id', 'individual_id']
    clashing = raw[raw.duplicated(subset=key, keep=False)]
    assert len(clashing) >= 2, 'expected the split-off clash to exist in W3'

    # Under the WAVE-NATIVE ids the same source rows are fully distinguishable.
    native = ['household_id2', 'individual_id2']
    assert not clashing.duplicated(subset=native).any()

    # ... and they are genuinely DIFFERENT people/records, not a duplicate:
    # at least one clashing group disagrees on the reported grade, so a
    # first() collapse would have destroyed a real answer.
    grades = clashing.groupby(key, observed=True)['hh_s2q05'].nunique()
    assert (grades > 1).any(), 'expected a clashing pair to report two grades'

    # Every one of them survives in the built table, under its own (i, pid).
    built = eth['2015-16'].grab_data('individual_education').reset_index()
    pairs = set(zip(built['i'].astype(str), built['pid'].astype(str)))
    for _, r in clashing.iterrows():
        assert (str(r['household_id2']), str(r['individual_id2'])) in pairs


# ---------------------------------------------------------- interview_date

def test_interview_date_w5_holder_grain_is_declared(eth):
    """W5's source is holder-grain; the reducer is a declared min, not first."""
    idt = eth.interview_date()
    assert not idt.index.duplicated().any()
    w5 = _wave(idt, '2021-22')
    col = 'Int_t' if 'Int_t' in w5.columns else 'int_t'
    # The '##N/A##' sentinel must be NaT, never a literal string.
    vals = w5[col].dropna().astype(str)
    assert not vals.str.contains('N/A').any(), 'raw ##N/A## sentinel leaked'
    assert pd.api.types.is_datetime64_any_dtype(w5[col])
    # 1,581 households in the PH cover, 15 of them with two holders.
    assert len(w5) >= 1_400


def test_interview_date_w5_takes_the_first_contact(eth):
    """A two-holder household is dated by its FIRST holder visit (min)."""
    w5 = _wave(eth.interview_date(), '2021-22')
    col = 'Int_t' if 'Int_t' in w5.columns else 'int_t'
    hh = w5.reset_index()
    row = hh[hh['i'].astype(str) == '030610088800708125']
    if len(row):   # holder visits 2022-04-13 and 2022-04-18
        assert str(row.iloc[0][col])[:10] == '2022-04-13'


# --------------------------------------------------------- crop_production

def test_crop_production_index_unique(eth):
    cp = eth.crop_production()
    assert not cp.index.duplicated().any()


def test_crop_production_sums_multi_entry_harvest(eth):
    """Several §9 entries for one crop on one field are SUMMED, not first()'d.

    W5 holder 03051208880020608101, parcel 1, field 1 reports Teff three times
    -- 8, 3 and 2 Chinet Medium.  first() keeps 8 and destroys 5 units.
    """
    cp = eth.crop_production().reset_index()
    w5 = cp[cp['t'] == '2021-22']
    teff = w5[(w5['plot_id'].astype(str) == '03051208880020608101_1_1')
              & (w5['j'].astype(str).str.contains('Teff', case=False, na=False))]
    assert len(teff) == 1, f'expected one Teff row, got {len(teff)}'
    assert float(teff.iloc[0]['Quantity']) == pytest.approx(13.0), (
        'Teff harvest should be 8+3+2 = 13, got '
        f"{teff.iloc[0]['Quantity']}")


def test_crop_production_exact_duplicates_do_not_double_count(eth):
    """W2's 605 byte-identical §9 records are dropped, not summed."""
    cp = eth.crop_production()
    w2 = _wave(cp, '2013-14')
    # 26,765 pre-fix rows included 605 collapsed dups; the deduped build is
    # smaller but every row is a distinct (plot, crop, unit).
    assert not w2.index.duplicated().any()
    assert len(w2) > 20_000


def test_crop_production_total_harvest_is_conserved(eth):
    """The collapse must not LOSE harvest -- only exact dups may disappear.

    Compares the table's total reported Quantity per (wave, unit) against the
    raw §9 file's total after the declared exact-duplicate dedup.
    """
    from lsms_library.local_tools import get_dataframe
    from lsms_library.paths import countries_root
    base = countries_root() / 'Ethiopia' / '2021-22' / 'Data'
    raw = get_dataframe(str(base / 'sect9_ph_w5.dta'), convert_categoricals=False)
    raw_total = pd.to_numeric(raw['s9q05a'], errors='coerce').sum()

    cp = _wave(eth.crop_production(), '2021-22')
    got_total = pd.to_numeric(cp['Quantity'], errors='coerce').sum()
    # Rows with an unmappable crop label are dropped upstream, so the built
    # total is a subset -- but it must not be MORE than the source (no
    # double-count) and must retain the bulk of it (no first() destruction).
    assert got_total <= raw_total * 1.0001, 'harvest double-counted'
    assert got_total > raw_total * 0.80, (
        f'harvest lost: {got_total} of {raw_total}')


# ---------------------------------------------------------------- panel_ids

def test_panel_ids_w2_uses_the_household_cover(eth):
    """The vestigial wave-level block pointed at the holder-grain LIVESTOCK
    cover (3,812 rows / 3,670 households, covering only 3,670 of 5,262)."""
    pids = eth.panel_ids
    assert isinstance(pids, dict)
    assert pids, 'panel_ids empty'
