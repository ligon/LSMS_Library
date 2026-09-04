"""Data-gated checks for GhanaLSS ``nutrition`` (GH #563).

``nutrition`` is ``food_quantities`` in kilograms times per-kilogram nutrient
densities from the FAO/INFOODS West African Food Composition Table 2019
(``GhanaLSS/_/fct_west_africa.org``), at the ``(i, t)`` grain both existing
precedents use so ``Feature('nutrition')`` can assemble the three countries.

What these tests actually pin, and why each earns its place:

* **the shape contract** -- index ``(i, t)``, columns a subset of the
  canonical ``Preferred Label`` axis in ``Ethiopia/_/nutrient_labels.org``.
  A fourth nutrient vocabulary is the failure mode #563 explicitly forbids.

* **Vitamin K is absent, deliberately.**  The WAFCT has no phylloquinone
  component.  Zero-filling it would assert that Ghanaian foods contain no
  vitamin K, so the column is omitted -- and that omission is pinned, because
  a well-meaning ``fillna(0)`` would silently reintroduce the claim.

* **the value-only waves produce no rows.**  1987-88 and 1988-89 are 100%
  ``u='Value'`` (expenditure, not quantity), so after ``nutrition.py`` drops
  non-physical units nothing survives.  Empty is the CORRECT answer here.

* **the regression net for the ``u='Value'`` kg-factor defect**
  (``slurm_logs/ghana_audit/ISSUE_value_kg_factor.org``).
  ``transformations.conversion_to_kgs`` infers a kg-per-cedi factor of
  0.49139 for ``u='Value'`` rows, and the conversion then relabels them
  ``u='kg'``.  Served that way, GhanaLSS households "acquire" up to 251,592
  kg of food in a month and per-capita Energy runs to millions of kcal.
  ``test_energy_not_absurd`` is the test that goes red if the filter in
  ``nutrition.py`` is ever removed, or if the upstream defect is "fixed" in a
  way that reintroduces it.

  Note it asserts a CEILING of 10,000 kcal/capita/day, not the physiological
  1,500-3,500 band.  The band would be false by design: see
  ``test_energy_2016_17_measured``.

Skips (rather than fails) when the GhanaLSS microdata is not available.
"""
import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root

pytestmark = pytest.mark.slow

#: 100% of these waves' food_acquired rows carry u='Value'.
VALUE_ONLY_WAVES = ['1987-88', '1988-89']

#: Measured 2026-09-04 on a cold build (slurm_logs/ghana_audit/
#: nutrition_verify_*.log).  A little below the measurement so a small data
#: revision does not trip it, but a broken label map (0%) does.
FCT_COVERAGE_FLOOR = 0.80           # measured 0.8826 of kg overall
FCT_COVERAGE_FLOOR_2016_17 = 0.85   # measured 0.9251

#: The defect ceiling.  Median per-capita daily Energy above this is not a
#: diet, it is a currency unit.  Measured max across waves is ~1,001.
ABSURD_KCAL_CAP_DAY = 10_000

#: Measured 2026-09-04, 2016-17, assuming a 30-day recall.  Deliberately a
#: WIDE interval around the measurement rather than the physiological band --
#: see test_energy_2016_17_measured's docstring.
KCAL_2016_17 = (600, 2_000)

DAYS = 30.0


@pytest.fixture(scope='module')
def nutrition():
    try:
        c = ll.Country('GhanaLSS')
        n = c.nutrition()
    except Exception as e:  # pragma: no cover - data not available
        pytest.skip(f'GhanaLSS nutrition not buildable here: {e}')
    if n is None or len(n) == 0:
        pytest.skip('GhanaLSS nutrition is empty here')
    return c, n


@pytest.fixture(scope='module')
def canonical_nutrients():
    labels = df_from_orgfile(countries_root() / 'Ethiopia' / '_' / 'nutrient_labels.org')
    return [s.strip() for s in labels['Preferred Label']]


def test_grain_is_household_wave(nutrition):
    """(i, t), unique -- the grain Feature('nutrition') assembles on."""
    _c, n = nutrition
    assert list(n.index.names) == ['i', 't'], list(n.index.names)
    assert n.index.is_unique, 'nutrition must be one row per household per wave'


def test_no_fourth_nutrient_vocabulary(nutrition, canonical_nutrients):
    """Every column is on Ethiopia's canonical Preferred Label axis (#563)."""
    _c, n = nutrition
    extra = sorted(set(n.columns) - set(canonical_nutrients))
    assert not extra, f'non-canonical nutrient columns: {extra}'
    assert 'Energy' in n.columns and 'Protein' in n.columns


def test_vitamin_k_omitted_not_zero_filled(nutrition):
    """The WAFCT has no phylloquinone; a zero would be a claim, not a datum."""
    _c, n = nutrition
    assert 'Vitamin K' not in n.columns, (
        'Vitamin K is absent from the West African FCT.  If it has been added, '
        'it must come from a real source -- not fillna(0), which asserts that '
        'Ghanaian foods contain none.')


def test_all_values_finite_and_nonnegative(nutrition):
    _c, n = nutrition
    assert n.notna().all().all(), 'nutrition must not carry NaN'
    assert (n >= 0).all().all(), 'a negative nutrient amount is impossible'


def test_value_only_waves_are_empty(nutrition):
    """1987-88 / 1988-89 are 100% u='Value': no physical quantity exists.

    Non-empty here means the u='Value' rows are being converted to kilograms
    again -- see ISSUE_value_kg_factor.org.
    """
    _c, n = nutrition
    waves = set(n.index.get_level_values('t').astype(str))
    for w in VALUE_ONLY_WAVES:
        assert w not in waves, (
            f'{w} is expenditure-only, so it can carry no nutrient intake; '
            'rows here mean cedis are being served as kilograms')


def test_energy_not_absurd(nutrition):
    """THE regression net for the u='Value' kg-factor defect.

    Remove nutrition.py's non-physical-unit filter and this goes red by
    several orders of magnitude.
    """
    c, n = nutrition
    hc = c.household_characteristics()
    size = hc['n'] if 'n' in hc.columns else hc.sum(axis=1)
    d = n[['Energy']].join(size.rename('n'), how='inner')
    d = d[d['n'] > 0]
    assert len(d), 'no nutrition row joined a household size'
    med = (d['Energy'] / d['n'] / DAYS).groupby(d.index.get_level_values('t')).median()
    bad = med[med > ABSURD_KCAL_CAP_DAY]
    assert bad.empty, (
        f'median per-capita daily Energy above {ABSURD_KCAL_CAP_DAY:,} kcal in '
        f'{bad.to_dict()} -- this is the u=\'Value\' contamination, not a diet')


def test_energy_2016_17_measured(nutrition):
    """2016-17 is the only wave with physical quantities for purchases too.

    The assertion is a WIDE interval around the 2026-09-04 measurement
    (~1,001 kcal/capita/day), NOT the physiological 1,500-3,500 band.  The
    band is not assertable here and pretending otherwise would be dishonest:

    * ~15% of 2016-17 expenditure is on u='Value' rows (restaurants, cooked
      meals) that carry real intake and are necessarily dropped;
    * 7.5% of its kilograms map to no FCT row, and the three labels missing
      from food_items.org#food_label alone are worth +27% of its Energy;
    * the 30-day recall is an approximation of ~7 visits over about a month;
    * densities are per edible portion but quantities are as acquired.

    Each of those pushes the number down or blurs it.  The test pins that the
    build has not MOVED, which is what a regression test can honestly do.
    """
    c, n = nutrition
    if '2016-17' not in set(n.index.get_level_values('t').astype(str)):
        pytest.skip('2016-17 not built here')
    hc = c.household_characteristics()
    size = hc['n'] if 'n' in hc.columns else hc.sum(axis=1)
    d = n[['Energy']].join(size.rename('n'), how='inner')
    d = d[(d['n'] > 0) & (d.index.get_level_values('t').astype(str) == '2016-17')]
    med = (d['Energy'] / d['n'] / DAYS).median()
    lo, hi = KCAL_2016_17
    assert lo <= med <= hi, (
        f'2016-17 median per-capita daily Energy {med:,.0f} kcal outside the '
        f'measured interval [{lo:,}, {hi:,}]')


def test_fct_coverage_of_mass(nutrition):
    """The share of kilograms reaching an FCT row, overall and for 2016-17."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        'glss_nutrition', countries_root() / 'GhanaLSS' / '_' / 'nutrition.py')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    root = countries_root()
    fct = mod._load_fct(root)
    by_wave, by_label = mod._load_food_codes(root)
    q = mod._quantities(root).rename('Quantity').reset_index()
    q = q.merge(by_wave[['t', 'j', 'FCT Code']], on=['t', 'j'], how='left')
    q['FCT Code'] = q['FCT Code'].where(q['FCT Code'].notna(),
                                        q['j'].map(by_label)).fillna('')
    ok = q['FCT Code'].isin(fct.index)

    share = q.loc[ok, 'Quantity'].sum() / q['Quantity'].sum()
    assert share >= FCT_COVERAGE_FLOOR, (
        f'only {share:.2%} of kilograms map to an FCT row '
        f'(floor {FCT_COVERAGE_FLOOR:.0%})')

    g = q[q['t'] == '2016-17']
    if len(g):
        s16 = g.loc[ok[g.index], 'Quantity'].sum() / g['Quantity'].sum()
        assert s16 >= FCT_COVERAGE_FLOOR_2016_17, (
            f'2016-17 coverage {s16:.2%} below floor '
            f'{FCT_COVERAGE_FLOOR_2016_17:.0%}')


def test_fct_table_is_well_formed():
    """The committed FCT extract: string codes, canonical nutrient columns."""
    fct = df_from_orgfile(countries_root() / 'GhanaLSS' / '_' / 'fct_west_africa.org',
                          name='fct_west_africa')
    assert len(fct) > 1000, f'expected the full WAFCT, got {len(fct)} rows'
    assert fct['FCT Code'].is_unique
    # NN_NNN strings -- pandas must not have coerced them to numbers.
    assert fct['FCT Code'].str.fullmatch(r'\d\d_\d\d\d').all(), (
        'FCT codes must stay NN_NNN strings')
    labels = df_from_orgfile(countries_root() / 'Ethiopia' / '_' / 'nutrient_labels.org')
    canon = {s.strip() for s in labels['Preferred Label']}
    nutrients = set(fct.columns) - {'FCT Code', 'FCT Label', 'Category', 'EDIBLE1'}
    assert nutrients <= canon, sorted(nutrients - canon)
    # Spot-check against the published table: white rice ~344 kcal/100 g EP.
    rice = pd.to_numeric(fct.set_index('FCT Code').loc['01_037', 'Energy'])
    assert 300 <= rice <= 380, f'01_037 Energy {rice} kcal/100 g looks wrong'


def test_food_labels_carry_fct_codes():
    """food_items.org#food_label is the single food-label table and now
    carries the FCT Code column (no second food-label table was created)."""
    lab = df_from_orgfile(countries_root() / 'GhanaLSS' / '_' / 'food_items.org',
                          name='food_label')
    assert 'FCT Code' in lab.columns
    coded = (lab['FCT Code'].astype(str).str.strip() != '').sum()
    assert coded >= 150, f'only {coded} of {len(lab)} labels carry an FCT Code'
