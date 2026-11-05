"""`yield_kg(on='parcel')` joins a key both features already share (GH #1460).

Ruling, @ligon 2026-11-04: extend #1110's per-country route to `yield_kg`.  The
area key used to be cut at its first `_` everywhere, so `{field}_{parcel}` on both
sides matched nothing, and Malawi lost its `{garden}_{plot}` waves with no signal.
"""
import warnings

import pandas as pd
import pytest

from lsms_library.transformations import (
    PlotGrainMismatchWarning,
    _YIELD_LAND_ROUTE_HELD,
    _YIELD_LITERAL_LAND_COUNTRIES,
    yield_kg,
)

EHCVM = ['Benin', 'Burkina_Faso', 'CotedIvoire', 'Guinea-Bissau', 'Mali', 'Niger',
         'Senegal', 'Togo']
ROUTED = sorted(_YIELD_LITERAL_LAND_COUNTRIES)
HELD = sorted(_YIELD_LAND_ROUTE_HELD)


def _frames(country, plots=('1_1', '1_2'), attrs=True):
    """Two plots of one field, each with its own area and harvest."""
    cp = pd.DataFrame({'Quantity': [10.0, 30.0]}, index=pd.MultiIndex.from_tuples(
        [('w1', 'h1', p, 'Maize', 'Kg') for p in plots],
        names=['t', 'i', 'plot_id', 'j', 'u']))
    pf = pd.DataFrame({'Area': [2.0, 3.0]}, index=pd.MultiIndex.from_tuples(
        [('w1', 'h1', p) for p in plots], names=['t', 'i', 'plot_id']))
    if attrs:
        cp.attrs['country'] = pf.attrs['country'] = country
    return cp, pf


@pytest.mark.parametrize('country', ROUTED)
def test_each_listed_country_joins_the_whole_key(country):
    cp, pf = _frames(country)
    with warnings.catch_warnings():
        warnings.simplefilter('error', PlotGrainMismatchWarning)
        out = yield_kg(cp, pf)
    assert list(out.index.names) == ['t', 'i', 'parcel']
    assert out.loc[('w1', 'h1', '1_1'), 'Yield_kg'] == 5.0
    assert out.loc[('w1', 'h1', '1_2'), 'Yield_kg'] == 10.0


@pytest.mark.parametrize('country', ROUTED)
def test_it_is_the_plot_grain_under_the_old_level_name(country):
    cp, pf = _frames(country)
    parcel = yield_kg(cp, pf)
    plot = yield_kg(cp, pf, on='plot').rename_axis(index={'plot_id': 'parcel'})
    pd.testing.assert_frame_equal(parcel, plot)


def test_no_area_is_pooled_to_a_coarser_land_unit():
    """The first token of `1_1` and `1_2` is one field.  A fall-back to that key
    would divide 40 kg by 5 ha and serve one row."""
    cp, pf = _frames('Niger')
    assert len(yield_kg(cp, pf)) == 2


def test_malawi_keeps_both_key_shapes_in_one_frame():
    cp, pf = _frames('Malawi', plots=('R1', 'G1_R1'))
    out = yield_kg(cp, pf)
    assert sorted(out.index.get_level_values('parcel')) == ['G1_R1', 'R1']


@pytest.mark.parametrize('country,plots,expected', [
    ('Uganda', ('h1-1-1', 'h1-2-1'), {'1', '2'}),
    ('Tanzania', ('1', '2'), {'1', '2'}),
])
def test_other_countries_keep_their_decoding(country, plots, expected):
    cp, pf = _frames(country, plots=plots)
    pf.index = pd.MultiIndex.from_tuples(
        [('w1', 'h1', p.split('-')[1] if '-' in p else p) for p in plots],
        names=['t', 'i', 'plot_id'])
    assert set(yield_kg(cp, pf).index.get_level_values('parcel')) == expected


def test_no_metadata_keeps_the_old_route_and_warns():
    cp, pf = _frames('Niger', attrs=False)
    with pytest.warns(PlotGrainMismatchWarning, match="on='plot'"):
        assert yield_kg(cp, pf).empty


def test_disagreeing_metadata_keeps_the_old_route():
    cp, pf = _frames('Niger')
    pf.attrs['country'] = 'Mali'
    with pytest.warns(PlotGrainMismatchWarning):
        assert yield_kg(cp, pf).empty


def test_one_labelled_country_selects_the_route_on_a_feature_shaped_frame():
    cp, pf = _frames('Niger', attrs=False)
    cp = pd.concat({'Niger': cp}, names=['country'])
    pf = pd.concat({'Niger': pf}, names=['country'])
    assert len(yield_kg(cp, pf)) == 2


def test_a_label_outranks_a_stale_scalar_attr():
    cp, pf = _frames('Niger')
    cp = pd.concat({'Uganda': cp}, names=['country'])
    cp.attrs['country'] = 'Niger'
    with pytest.warns(PlotGrainMismatchWarning):
        assert yield_kg(cp, pf).empty


def test_a_pooled_frame_keeps_the_old_route():
    """Crop transforms do not group on country yet (GH #1475)."""
    a, pa = _frames('Niger', attrs=False)
    cp = pd.concat({'Niger': a, 'Mali': a}, names=['country'])
    pf = pd.concat({'Niger': pa, 'Mali': pa}, names=['country'])
    with pytest.warns(PlotGrainMismatchWarning):
        assert yield_kg(cp, pf).empty


def test_the_list_is_the_one_the_ruling_names_less_what_is_held():
    assert _YIELD_LITERAL_LAND_COUNTRIES | _YIELD_LAND_ROUTE_HELD == {
        'Ethiopia', 'Malawi', *EHCVM}
    assert not _YIELD_LITERAL_LAND_COUNTRIES & _YIELD_LAND_ROUTE_HELD


@pytest.mark.parametrize('country', HELD)
class TestAHeldCountry:
    """Its area mixes hectares and square metres (GH #1531), so nothing changes."""

    def test_the_default_stays_empty_and_warns(self, country):
        cp, pf = _frames(country)
        with pytest.warns(PlotGrainMismatchWarning, match="on='plot'"):
            assert yield_kg(cp, pf).empty

    def test_on_plot_still_serves_it(self, country):
        cp, pf = _frames(country)
        with warnings.catch_warnings():
            warnings.simplefilter('error', PlotGrainMismatchWarning)
            out = yield_kg(cp, pf, on='plot')
        assert out.loc[('w1', 'h1', '1_1'), 'Yield_kg'] == 5.0
        assert out.loc[('w1', 'h1', '1_2'), 'Yield_kg'] == 10.0


def _wave(t, plot, i='h1', area=2.0):
    """One harvest row and one plot in wave *t*."""
    cp = pd.DataFrame({'Quantity': [10.0]}, index=pd.MultiIndex.from_tuples(
        [(t, i, plot, 'Maize', 'Kg')], names=['t', 'i', 'plot_id', 'j', 'u']))
    pf = pd.DataFrame({'Area': [area]}, index=pd.MultiIndex.from_tuples(
        [(t, i, plot)], names=['t', 'i', 'plot_id']))
    return cp, pf


def _country_frames(**waves_by_country):
    """A Feature-shaped pair, ``{country: [(t, plot, i), ...]}``."""
    cps, pfs = {}, {}
    for country, waves in waves_by_country.items():
        frames = [_wave(t, plot, i) for t, plot, i in waves]
        cps[country] = pd.concat([c for c, _ in frames])
        pfs[country] = pd.concat([f for _, f in frames])
    return (pd.concat(cps, names=['country']), pd.concat(pfs, names=['country']))


class TestAWaveThatJoinsNothing:
    """A wave lost beside waves that join used to go without a word: a real Feature of
    Malawi and Ethiopia served 18,919 yields where Malawi alone serves 54,419."""

    def test_a_pooled_country_names_each_lost_wave(self):
        cp, pf = _country_frames(
            Malawi=[('w1', 'R1', 'h1'), ('w2', 'G1_R1', 'h1')],
            Ethiopia=[('w1', '1_1', 'h2')])
        with pytest.warns(PlotGrainMismatchWarning) as caught:
            out = yield_kg(cp, pf)
        message = str(caught[0].message)
        assert 'Malawi w2' in message and 'Ethiopia w1' in message
        assert 'Malawi w1' not in message
        assert 'one country at a time' in message
        assert out.index.get_level_values('parcel').tolist() == ['R1']

    def test_it_is_per_country_within_a_wave(self):
        """Uganda's rows keep w1 non-empty, and Malawi's are still lost."""
        cp, pf = _country_frames(
            Malawi=[('w1', 'G1_R1', 'h1')],
            Uganda=[('w1', '1', 'h2')])
        with pytest.warns(PlotGrainMismatchWarning, match='Malawi w1') as caught:
            out = yield_kg(cp, pf)
        assert 'Uganda' not in str(caught[0].message)
        assert len(out) == 1

    def test_a_pooled_frame_where_every_wave_joins_is_silent(self):
        cp, pf = _country_frames(
            Malawi=[('w1', 'R1', 'h1')],
            Uganda=[('w1', '1', 'h2'), ('w2', '2', 'h2')])
        with warnings.catch_warnings():
            warnings.simplefilter('error', PlotGrainMismatchWarning)
            assert len(yield_kg(cp, pf)) == 3

    def test_labels_lost_by_a_cross_section(self):
        cp, pf = _country_frames(Malawi=[('w1', 'R1', 'h1'), ('w2', 'G1_R1', 'h1')],
                                 Ethiopia=[('w1', '1', 'h2')])
        cp, pf = cp.xs('Malawi', level='country'), pf.xs('Malawi', level='country')
        with pytest.warns(PlotGrainMismatchWarning, match='w2') as caught:
            assert len(yield_kg(cp, pf)) == 1
        assert 'one country at a time' in str(caught[0].message)

    def test_attrs_lost_on_one_side(self):
        a, pa = _wave('w1', 'R1')
        b, pb = _wave('w2', 'G1_R1')
        cp, pf = pd.concat([a, b]), pd.concat([pa, pb])
        pf.attrs['country'] = 'Malawi'
        with pytest.warns(PlotGrainMismatchWarning, match='w2'):
            assert len(yield_kg(cp, pf)) == 1

    def test_the_right_route_is_silent(self):
        a, pa = _wave('w1', 'R1')
        b, pb = _wave('w2', 'G1_R1')
        cp, pf = pd.concat([a, b]), pd.concat([pa, pb])
        cp.attrs['country'] = pf.attrs['country'] = 'Malawi'
        with warnings.catch_warnings():
            warnings.simplefilter('error', PlotGrainMismatchWarning)
            assert len(yield_kg(cp, pf)) == 2

    def test_a_wave_on_one_side_only_is_not_a_lost_join(self):
        """A wave with no plot roster has nothing to join."""
        a, pa = _wave('w1', 'R1')
        b, _ = _wave('w2', 'R1')
        cp = pd.concat([a, b])
        cp.attrs['country'] = pa.attrs['country'] = 'Malawi'
        with warnings.catch_warnings():
            warnings.simplefilter('error', PlotGrainMismatchWarning)
            assert len(yield_kg(cp, pa)) == 1

    def test_a_named_country_is_offered_the_other_grain(self):
        """Uganda keeps its decoding, and a wave lost there is still named."""
        a, pa = _wave('w1', 'h1-1-1')
        pa.index = pa.index.set_levels(['1'], level='plot_id')
        b, pb = _wave('w2', 'h1-1-1')
        pb.index = pb.index.set_levels(['9'], level='plot_id')
        cp, pf = pd.concat([a, b]), pd.concat([pa, pb])
        cp.attrs['country'] = pf.attrs['country'] = 'Uganda'
        with pytest.warns(PlotGrainMismatchWarning, match="Uganda w2.*on='plot'"):
            assert len(yield_kg(cp, pf)) == 1

    def test_on_plot_is_judged_the_same_way(self):
        a, pa = _wave('w1', '1')
        b, pb = _wave('w2', '1')
        pb.index = pb.index.set_levels(['2'], level='plot_id')
        cp, pf = pd.concat([a, b]), pd.concat([pa, pb])
        with pytest.warns(PlotGrainMismatchWarning, match="w2.*on='parcel'"):
            assert len(yield_kg(cp, pf, on='plot')) == 1
