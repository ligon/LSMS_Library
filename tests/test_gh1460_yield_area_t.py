"""`yield_kg(area_t=...)`: plots measured in one round, harvest in another (GH #1460).

Nigeria's `plot_features` is on the post-planting quarter and its `crop_production`
on the post-harvest one, so the two share no `t` and `yield_kg` returned nothing.
Since GH #587 its W1-W3 post-harvest rounds serve plot areas too (GH #1701).
"""
import inspect
import itertools
import textwrap
import types
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


class _InventedNigeria:
    """Nigeria's rounds with invented rows: one plot, 10 kg, in every harvest round.

    Its area is 2 ha at post planting in every wave and 5 ha at post harvest in
    W1-W3, so a yield of 5 used the first and 2 the second (10/7 used both).
    """

    wave_folder_map = {
        '2010Q3': '2010-11', '2011Q1': '2010-11', '2012Q3': '2012-13',
        '2013Q1': '2012-13', '2015Q3': '2015-16', '2016Q1': '2015-16',
        '2018Q3': '2018-19', '2019Q1': '2018-19', '2023Q3': '2023-24',
        '2024Q1': '2023-24'}
    waves = list(wave_folder_map)

    def crop_production(self):
        return pd.concat([_cp(t=t) for t in self.waves if t.endswith('Q1')])

    def plot_features(self):
        return _pf([(t, 'h1', '1', 2.0) for t in self.waves if t.endswith('Q3')]
                   + [(t, 'h1', '1', 5.0) for t in ('2011Q1', '2013Q1', '2016Q1')])


def _by_round(yields):
    assert yields.index.is_unique
    return dict(zip(yields.index.get_level_values('t'), yields['Yield_kg']))


class TestTheDocumentedExample:
    """`pp_ph_area_t`'s docstring example, run as printed (GH #1701).

    The whole-frame call it used to print began to raise when GH #587 served
    W1-W3 post-harvest areas, and no test without S3 ran it.
    """

    @pytest.fixture
    def printed(self):
        lines = inspect.getdoc(pp_ph_area_t).splitlines()
        start = 1 + next(n for n, line in enumerate(lines) if line.endswith('::'))
        block = itertools.takewhile(lambda line: not line or line.startswith(' '),
                                    lines[start:])
        names = {'ll': types.SimpleNamespace(
                     Country={'Nigeria': _InventedNigeria()}.__getitem__),
                 'yield_kg': yield_kg, 'pp_ph_area_t': pp_ph_area_t}
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            exec(textwrap.dedent('\n'.join(block)), names)
        return names

    def test_post_planting_area_moved_serves_every_wave(self, printed):
        assert _by_round(printed['moved']) == dict.fromkeys(
            ['2011Q1', '2013Q1', '2016Q1', '2019Q1', '2024Q1'], 5.0)

    def test_post_harvest_area_serves_w1_to_w3_only(self, printed):
        assert _by_round(printed['direct']) == dict.fromkeys(
            ['2011Q1', '2013Q1', '2016Q1'], 2.0)

    def test_its_post_harvest_rounds_are_the_ones_nigeria_serves(self, printed):
        """The wave scripts append post-harvest rows through this adapter only."""
        from lsms_library.countries.Nigeria._ import nigeria_ph_plot_features as ph
        assert set(printed['ph']) == {ph.ph_plot_source(folder)['t']
                                      for folder in ph._SOURCES}

    def test_the_whole_frame_is_still_refused(self):
        c = _InventedNigeria()
        with pytest.raises(ValueError, match=(
                r"\['2011Q1', '2013Q1', '2016Q1'\], which plot_features already")):
            yield_kg(c.crop_production(), c.plot_features(), area_t=pp_ph_area_t(c))


@pytest.mark.requires_s3
def test_nigeria_yields_appear():
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        c = ll.Country('Nigeria')
        cp, pf = c.crop_production(), c.plot_features()
        pair = pp_ph_area_t(c)
        # GH #587 adds original PH area records to W1–W3. The explicit
        # PP→PH mapping is only valid for PP rows; mapping a frame that already
        # contains those PH targets must still refuse double-counting.
        pp = pf.loc[pf.index.get_level_values('t').astype(str).isin(pair)]
        out = yield_kg(cp, pp, area_t=pair)
        ph_rounds = {'2011Q1', '2013Q1', '2016Q1'}
        ph = pf.loc[pf.index.get_level_values('t').astype(str).isin(ph_rounds)]
        assert ph_rounds <= set(ph.index.get_level_values('t').astype(str))
        ph_out = yield_kg(cp, ph)
        with pytest.raises(ValueError, match='already'):
            yield_kg(cp, pf, area_t=pair)
    assert len(out) > 0
    assert set(out.index.get_level_values('t').astype(str)) <= set(c.wave_folder_map)
    assert not set(out.index.get_level_values('t').astype(str)) & {
        '2010Q3', '2012Q3', '2015Q3', '2018Q3', '2023Q3'}
    assert len(ph_out) > 0
    assert set(ph_out.index.get_level_values('t').astype(str)) <= ph_rounds
