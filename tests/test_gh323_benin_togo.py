"""GH #323 -- Benin / Togo must not silently collapse a non-unique declared index.

`_normalize_dataframe_index` reduces a non-unique DECLARED index with
groupby().first(), silently discarding the dropped rows.  Benin and Togo trip it
in exactly one way, and it is the same way CotedIvoire did (see
`tests/test_gh323_cotedivoire.py`, case B):

  plot_inputs -- INDEX_INCOMPLETE.  `crop` is an index level of
  (t, i, input, crop, u), and harmonize_input maps EVERY seed label onto the
  single input 'Seed'.  A non-injective harmonize_seed_crop therefore lands two
  DISTINCT reported seed line-items on one index tuple.  Four labels shared an
  'Autre crop' catch-all bucket; THREE of them actually occur in the EHCVM s16b
  roster of both countries, and they co-occur within a household often enough to
  destroy 71 reported rows in Benin and 165 in Togo.

THE FIX IS TO THE IDENTIFIER, NOT TO A REDUCER.  Per the #323 doctrine,
duplicates on a declared index mean the identifier is broken or a level is
missing -- never that a reducer should be declared.  Here the identifier was
lossy: `crop` threw away the distinction between "other cereals", "tuber
cuttings" and "other seeds".  Splitting the bucket restores injectivity and the
rows come back.

DO NOT "FIX" THIS BY ADDING `Purchased` TO THE INDEX.  The colliding Togo rows
happen to differ in `Purchased` (grappe 101 / menage 9: 16 Charrette purchased
vs 3 Charrette own-production), which makes that look tempting.  It is wrong:
`Purchased` is a measured ATTRIBUTE of a line-item, not part of its identity --
it would not separate two distinct seeds that were both purchased, and it would
corrupt the declared grain.  The two rows are two different planting materials,
not a purchase-status split of one.

INSTRUMENT NOTE (inherited from the CotedIvoire tests -- do not undo it).  Do
NOT assert that the API's returned index is unique: `_normalize_dataframe_index`
collapses it, so API-level uniqueness holds BY CONSTRUCTION and such a test
passes even with the bug fully present.  The bug is that ROWS DISAPPEAR.  So
these tests assert on exact row counts, on the specific rows that were being
eaten, and on the injectivity of the mapping -- never on post-collapse
uniqueness.
"""
import pandas as pd
import pytest

from lsms_library.country import Country

# Row counts the API must return.
#
# Benin:  the wave script emits 10,605 reported line-items and ALL of them must
#         survive.  Pre-fix the API returned 10,534 -- 71 destroyed.
BENIN_ROWS = 10605
#
# Togo:   the wave script emits 13,733 reported line-items.  Pre-fix the API
#         returned 13,565 -- 168 missing.  165 of those were destroyed by the
#         #323 collapse and are recovered here.  The remaining 3 are HOLLOW rows
#         (Quantity, Purchased and Quantity_purchased ALL <NA> -- the household
#         named an input but reported nothing about it) and are dropped by the
#         framework's deliberate, table-agnostic `dropna(how='all')` safety net
#         in country.py.  That is not a #323 collapse: it destroys no reported
#         value, and it is not this fix's business to change it.
TOGO_ROWS = 13730

# The three seed labels that actually occur in the s16b roster of BOTH
# countries.  harmonize_seed_crop must keep them apart.
LABELS_IN_USE = ["Autres semences",
                 "Semences d'autres céréales",
                 "Plants/boutures de tubercules"]


@pytest.fixture(scope='module', params=['Benin', 'Togo'])
def country(request):
    try:
        return Country(request.param)
    except Exception as exc:                                   # pragma: no cover
        pytest.skip(f'{request.param} unavailable: {exc}')


def _plot_inputs(c):
    try:
        return c.plot_inputs()
    except Exception as exc:                                   # pragma: no cover
        pytest.skip(f'{c.name} plot_inputs could not be built: '
                    f'{type(exc).__name__}: {exc}')


def test_plot_inputs_keeps_every_reported_row(country):
    """Every reported input line-item must survive to the API."""
    expected = {'Benin': BENIN_ROWS, 'Togo': TOGO_ROWS}[country.name]
    df = _plot_inputs(country)
    assert len(df) == expected, (
        f'{country.name}/plot_inputs has {len(df)} rows, expected {expected}. '
        f'{expected - len(df)} reported line-item(s) are being silently '
        f'discarded by the framework collapse (GH #323) -- most likely a '
        f'non-injective harmonize_seed_crop merging distinct seed items into '
        f'one crop bucket.'
    )


def test_harmonize_seed_crop_is_injective_over_labels_in_use(country):
    """harmonize_seed_crop must be injective over the labels actually present.

    `crop` is an index level and harmonize_input maps EVERY seed label onto the
    single input 'Seed', so two seed labels sharing a Preferred Label collide on
    one index tuple and the framework's groupby().first() eats one of them.
    """
    m = country.categorical_mapping.get('harmonize_seed_crop')
    if m is None:                                              # pragma: no cover
        pytest.skip('harmonize_seed_crop absent')
    m = m.set_index('Original Label')['Preferred Label']
    present = [lab for lab in LABELS_IN_USE if lab in m.index]
    assert len(present) == len(LABELS_IN_USE), (
        f'expected all of {LABELS_IN_USE} in harmonize_seed_crop; '
        f'got {present}'
    )
    crops = [m[lab] for lab in present]
    assert len(set(crops)) == len(crops), (
        f'{country.name}: harmonize_seed_crop is NON-INJECTIVE over the seed '
        f'labels that actually occur in s16b: {dict(zip(present, crops))}. '
        f'Two distinct reported seed items will collide on the plot_inputs '
        f'index (t, i, input, crop, u) and one will be silently discarded '
        f'(GH #323).  Give each label its own Preferred Label.'
    )


@pytest.mark.parametrize('cname,hh,pairs', [
    # Benin grappe 133 / menage 53 reported BOTH
    #   "Autres semences"                9 kg   (own production)
    #   "Plants/boutures de tubercules"  6 kg   (own production)  <- was eaten
    # Both mapped to crop 'Autre crop', colliding on (i, Seed, Autre crop, Kg).
    ('Benin', '133053', {('Autre crop', 9.0), ('Autre tubercule', 6.0)}),
    # Togo grappe 101 / menage 9 reported BOTH
    #   "Plants/boutures de tubercules" 16 Charrette  (market-purchased)
    #   "Autres semences"                3 Charrette  (own production)
    # Both mapped to crop 'Autre crop', colliding on (i, Seed, Autre crop,
    # Charrette).  NB assert the exact (crop, Quantity) PAIR: merely checking
    # that 3.0 appears among this household's seed rows passes even with the bug
    # present, because it separately reports 1.0 and 6.0 for other crops.
    ('Togo', '101009', {('Autre tubercule', 16.0), ('Autre crop', 3.0)}),
])
def test_plot_inputs_recovers_the_discarded_line_item(country, cname, hh, pairs):
    """The exact rows the catch-all bucket used to eat."""
    if country.name != cname:
        pytest.skip(f'case is for {cname}')
    flat = _plot_inputs(country).reset_index()
    seeds = flat[(flat['i'] == hh) & (flat['input'] == 'Seed')]
    if seeds.empty:                                            # pragma: no cover
        pytest.skip(f'household {hh} not present')
    got = {(r['crop'], float(r['Quantity']))
           for _, r in seeds.iterrows() if pd.notna(r['Quantity'])}
    missing = pairs - got
    assert not missing, (
        f'{cname} household {hh}: {missing} still discarded -- '
        f'harmonize_seed_crop is merging distinct seed labels into one '
        f'"Autre crop" bucket (GH #323).  got {got}'
    )
