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
    _YIELD_LITERAL_LAND_COUNTRIES,
    yield_kg,
)

EHCVM = ['Benin', 'Burkina_Faso', 'CotedIvoire', 'Guinea-Bissau', 'Mali', 'Niger',
         'Senegal', 'Togo']


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


@pytest.mark.parametrize('country', ['Ethiopia', 'Malawi', *EHCVM])
def test_each_listed_country_joins_the_whole_key(country):
    cp, pf = _frames(country)
    with warnings.catch_warnings():
        warnings.simplefilter('error', PlotGrainMismatchWarning)
        out = yield_kg(cp, pf)
    assert list(out.index.names) == ['t', 'i', 'parcel']
    assert out.loc[('w1', 'h1', '1_1'), 'Yield_kg'] == 5.0
    assert out.loc[('w1', 'h1', '1_2'), 'Yield_kg'] == 10.0


@pytest.mark.parametrize('country', ['Ethiopia', 'Malawi', *EHCVM])
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


def test_the_list_is_the_one_the_ruling_names():
    assert _YIELD_LITERAL_LAND_COUNTRIES == {'Ethiopia', 'Malawi', *EHCVM}
