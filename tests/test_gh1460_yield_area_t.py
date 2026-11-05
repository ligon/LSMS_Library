"""`yield_kg(area_t=...)`: plots measured in one round, harvest in another (GH #1460).

Nigeria's `plot_features` is on the post-planting quarter and its `crop_production`
on the post-harvest one, so the two share no `t` and `yield_kg` returned nothing.
"""
import warnings

import pandas as pd
import pytest

from lsms_library.transformations import (
    PlotGrainMismatchWarning,
    pp_ph_area_t,
    yield_kg,
)


def _cp(t='2011Q1', plot='1'):
    return pd.DataFrame({'Quantity': [10.0]}, index=pd.MultiIndex.from_tuples(
        [(t, 'h1', plot, 'Maize', 'Kg')], names=['t', 'i', 'plot_id', 'crop', 'u']))


def _pf(rows):
    return pd.DataFrame({'Area': [a for *_, a in rows]},
                        index=pd.MultiIndex.from_tuples(
                            [r[:3] for r in rows], names=['t', 'i', 'plot_id']))


PAIR = {'2010Q3': '2011Q1'}


@pytest.mark.parametrize('on,level', [('plot', 'plot_id'), ('parcel', 'parcel')])
def test_the_rounds_join(on, level):
    out = yield_kg(_cp(), _pf([('2010Q3', 'h1', '1', 2.0)]), on=on, area_t=PAIR)
    assert out.loc[('2011Q1', 'h1', '1'), 'Yield_kg'] == 5.0
    assert list(out.index.names) == ['t', 'i', level]


def test_without_it_they_do_not_and_the_warning_says_why():
    with pytest.warns(PlotGrainMismatchWarning, match='share no wave'):
        assert yield_kg(_cp(), _pf([('2010Q3', 'h1', '1', 2.0)])).empty


def test_a_round_it_does_not_name_stays():
    pf = _pf([('2010Q3', 'h1', '1', 2.0), ('2012Q3', 'h1', '1', 4.0)])
    cp = pd.concat([_cp(), _cp(t='2012Q3')])
    out = yield_kg(cp, pf, area_t=PAIR)
    assert out.loc[('2011Q1', 'h1', '1'), 'Yield_kg'] == 5.0
    assert out.loc[('2012Q3', 'h1', '1'), 'Yield_kg'] == 2.5


def test_a_target_already_served_is_refused():
    """It would add two rounds' areas for one plot and halve the yield."""
    pf = _pf([('2010Q3', 'h1', '1', 2.0), ('2011Q1', 'h1', '1', 2.0)])
    with pytest.raises(ValueError, match='already'):
        yield_kg(_cp(), pf, area_t=PAIR)


def test_two_rounds_onto_one_target_are_refused():
    """It would sum 2 and 3 and serve 2.0 where 5.0 is right."""
    pf = _pf([('2010Q3', 'h1', '1', 2.0), ('2011Q3', 'h1', '1', 3.0)])
    with pytest.raises(ValueError, match='more than one round'):
        yield_kg(_cp(), pf, area_t={'2010Q3': '2011Q1', '2011Q3': '2011Q1'})


def test_the_refusal_does_not_depend_on_what_is_served():
    pf = _pf([('2010Q3', 'h1', '1', 2.0)])
    with pytest.raises(ValueError, match='more than one round'):
        yield_kg(_cp(), pf, area_t={'2010Q3': '2011Q1', '2011Q3': '2011Q1'})


def test_a_lost_wave_is_judged_on_the_harvest_round():
    """The area's rows are on `2013Q1` once moved, so that is the wave named."""
    cp = pd.concat({'Nigeria': pd.concat([_cp(), _cp(t='2013Q1')])}, names=['country'])
    pf = pd.concat({'Nigeria': _pf([('2010Q3', 'h1', '1', 2.0), ('2012Q3', 'h1', '9', 2.0)])},
                   names=['country'])
    pair = {'2010Q3': '2011Q1', '2012Q3': '2013Q1'}
    with pytest.warns(PlotGrainMismatchWarning, match='Nigeria 2013Q1') as caught:
        assert len(yield_kg(cp, pf, area_t=pair)) == 1
    assert '2011Q1' not in str(caught[0].message)


def test_it_leaves_the_operands_alone():
    pf = _pf([('2010Q3', 'h1', '1', 2.0)])
    before = pf.copy()
    yield_kg(_cp(), pf, area_t=PAIR)
    pd.testing.assert_frame_equal(pf, before)


def test_no_area_t_is_the_old_call():
    pf = _pf([('2011Q1', 'h1', '1', 2.0)])
    pd.testing.assert_frame_equal(yield_kg(_cp(), pf), yield_kg(_cp(), pf, area_t={}))


class TestThePairs:
    def test_the_earlier_round_of_a_folder_is_planting(self):
        got = pp_ph_area_t({'2010Q3': '2010-11', '2011Q1': '2010-11'})
        assert got == {'2010Q3': '2011Q1'}

    def test_a_folder_of_one_round_has_nothing_to_pair(self):
        assert pp_ph_area_t({'2019-20': '2019-20'}) == {}

    def test_more_than_two_rounds_is_refused(self):
        rounds = {w: '2008-15' for w in ('2008-09', '2010-11', '2012-13', '2014-15')}
        with pytest.raises(ValueError, match='not a post-planting'):
            pp_ph_area_t(rounds)

    def test_a_country_with_one_round_per_folder_has_no_pairs(self):
        import lsms_library as ll
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            assert pp_ph_area_t(ll.Country('Togo')) == {}

    def test_nigeria(self):
        import lsms_library as ll
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            got = pp_ph_area_t(ll.Country('Nigeria'))
        assert got == {'2010Q3': '2011Q1', '2012Q3': '2013Q1', '2015Q3': '2016Q1',
                       '2018Q3': '2019Q1', '2023Q3': '2024Q1'}


@pytest.mark.requires_s3
def test_nigeria_yields_appear():
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        c = ll.Country('Nigeria')
        cp, pf = c.crop_production(), c.plot_features()
        out = yield_kg(cp, pf, area_t=pp_ph_area_t(c))
    assert len(out) > 0
    assert set(out.index.get_level_values('t').astype(str)) <= set(c.wave_folder_map)
    assert not set(out.index.get_level_values('t').astype(str)) & {
        '2010Q3', '2012Q3', '2015Q3', '2018Q3', '2023Q3'}
