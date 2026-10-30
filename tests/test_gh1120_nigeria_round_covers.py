"""GH #1120 D1 / GH #1261 -- Nigeria: each round's `sample` comes from its OWN cover.

Nigeria is post-planting / post-harvest: every wave folder holds two rounds
(``t`` = ``YYYYQ3`` PP and ``YYYYQ1`` PH), and each round ships its own
household cover page (``secta_planting{wN}`` / ``secta_harvest{wN}``).  Until
2026-10-23 one ``data_info.yml`` block per folder read the PP cover for BOTH
rounds, so a PH round carried the PP round's households and clusters.  On W2
that left 44 households of the PH cover with no 2013Q1 ``v`` -- the 108
NaN-``v`` ``plot_labor`` rows of GH #1120 -- and made the 39 geo-only
households of GH #1261 arrive with a null ``v`` (deleted by the cluster-grain
collapse).

Ruling (@ligon, 2026-10-23, GH #1120 D1): per round, ``sample()`` takes
households from that round's own cover, with that cover's weight.
``cluster_features`` follows the same covers (one keyspace per round) and
joins the geovariables LEFT onto the round's cover (#1261).  The ruling's
general form ("from the cover the survey recorded the household in"; Ethiopia:
the agriculture cover) also brings in the 21 W2 households only the PP
AGRICULTURE cover lists, for 2012Q3 -- the 63 NaN-``v`` ``plot_inputs`` rows.

Retained declaration/routing/roster tests; source integration is verified
separately on explicitly preflighted cached inputs. No acquisition is required.
"""
import importlib.util
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

FOLDERS = ['2010-11', '2012-13', '2015-16', '2018-19', '2023-24']
NIGERIA = countries_root() / 'Nigeria'


@pytest.fixture(scope='module')
def ng():
    path = NIGERIA / '_' / 'nigeria.py'
    spec = importlib.util.spec_from_file_location('_nigeria_1120', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestDeclaration:
    @pytest.mark.parametrize('folder', FOLDERS)
    def test_no_yaml_block_reads_one_cover_for_both_rounds(self, folder):
        info = load_yaml(NIGERIA / folder / '_' / 'data_info.yml')
        assert 'sample' not in info and 'cluster_features' not in info

    @pytest.mark.parametrize('folder', FOLDERS)
    def test_wave_scripts_build_both_tables(self, folder):
        for table, fn in (('sample', 'sample_for_wave'),
                          ('cluster_features', 'cluster_features_for_wave')):
            src = (NIGERIA / folder / '_' / f'{table}.py').read_text(encoding='utf-8')
            assert f"{fn}('{folder}')" in src
            assert f"to_parquet(" in src and f"'{table}.parquet'" in src

    @pytest.mark.parametrize('folder', FOLDERS)
    def test_each_round_names_its_own_cover(self, ng, folder):
        spec = ng.ROUND_COVERS[folder]
        assert 'planting' in spec['pp'] and 'harvest' in spec['ph']
        assert spec['pp'] != spec['ph']

    def test_only_w2_has_a_supplement_cover(self, ng):
        sup = {f: s.get('pp_supplement') for f, s in ng.ROUND_COVERS.items()}
        assert sup['2012-13'] == 'Post Planting Wave 2/Agriculture/secta_plantingw2.dta'
        assert all(v is None for f, v in sup.items() if f != '2012-13')
        assert not any('ph_supplement' in s for s in ng.ROUND_COVERS.values())

    def test_scheme_declares_script_path(self):
        scheme = load_yaml(NIGERIA / '_' / 'data_scheme.yml')['Data Scheme']
        assert scheme['sample'].get('materialize') == 'make'
        assert scheme['cluster_features'].get('materialize') == 'make'

    def test_makefile_has_the_wave_rules(self):
        mk = (NIGERIA / '_' / 'Makefile').read_text(encoding='utf-8')
        assert '../%/_/sample.parquet: ../%/_/sample.py' in mk
        assert '../%/_/cluster_features.parquet: ../%/_/cluster_features.py' in mk


def _cover(ids, vs, weight):
    df = pd.DataFrame({'v': vs, 'weight': weight, 'panel_weight': weight,
                       'strata': 'z', 'Rural': 'Rural', 'Region': 'z',
                       'District': 's'}, index=pd.Index(ids, name='i'))
    return df


class TestRouting:
    """``round_covers`` / ``sample_for_wave`` on synthetic covers."""

    @pytest.fixture
    def fake(self, ng, monkeypatch):
        spec = ng.ROUND_COVERS['2012-13']
        covers = {
            spec['pp']: _cover(['a', 'b', 'c'], ['1/1/1', '1/1/1', '1/1/2'], [1.0, 2.0, 3.0]),
            spec['ph']: _cover(['a', 'b', 'd'], ['1/1/1', 'moved-b', '1/1/3'], [1.0, 2.0, np.nan]),
            spec['pp_supplement']: _cover(['a', 'd'], ['1/1/9', '1/1/3'], [np.nan, np.nan]),
        }
        seen = []

        def fake_cover(fn, spec_, *, weights=True):
            rel = fn.split('/Data/', 1)[1]
            seen.append((rel, weights))
            out = covers[rel].copy()
            if not weights:
                out['weight'] = np.nan
                out['panel_weight'] = np.nan
            return out

        monkeypatch.setattr(ng, '_cover_frame', fake_cover)
        # The rosters: 'e' is recorded by the PH roster only (D1: located by
        # it); 'a' is on a cover too, and the cover must win.
        rosters = {
            spec['pp_roster']: _cover(['a'], ['9/9/9'], [np.nan]),
            spec['ph_roster']: _cover(['a', 'e'], ['9/9/9', 'moved-e'], [np.nan, np.nan]),
        }

        def fake_roster(fn, spec_, cover_fn):
            rel = fn.split('/Data/', 1)[1]
            seen.append((rel, 'roster'))
            return rosters[rel].copy()

        monkeypatch.setattr(ng, '_roster_locator', fake_roster)
        return seen

    def test_pp_round_reads_pp_cover_and_ph_round_reads_ph_cover(self, ng, fake):
        s = ng.sample_for_wave('2012-13')
        assert list(s.index.names) == ['t', 'i']
        q3 = s.xs('2012Q3', level='t')
        q1 = s.xs('2013Q1', level='t')
        # PP: the PP household cover, plus 'd' from the agriculture cover.
        assert sorted(q3.index) == ['a', 'b', 'c', 'd']
        # PH: the PH cover's own households -- 'c' (not revisited) is gone,
        # 'd' (PH-only) is there.
        assert sorted(q1.index) == ['a', 'b', 'd', 'e']
        # A mover is the PH cover's singleton in Q1 only.
        assert q3.loc['b', 'v'] == '1/1/1' and q1.loc['b', 'v'] == 'moved-b'
        # Each round carries its own cover's weight.
        assert q3.loc['c', 'weight'] == 3.0
        assert pd.isna(q1.loc['d', 'weight'])

    def test_supplement_never_overrides_the_household_cover(self, ng, fake):
        s = ng.sample_for_wave('2012-13').xs('2012Q3', level='t')
        # 'a' is on both PP covers: the household cover's v and weight win.
        assert s.loc['a', 'v'] == '1/1/1' and s.loc['a', 'weight'] == 1.0
        # 'd' is supplement-only: its v, with no weight.
        assert s.loc['d', 'v'] == '1/1/3'
        assert pd.isna(s.loc['d', 'weight']) and pd.isna(s.loc['d', 'panel_weight'])
        assert ('Post Planting Wave 2/Agriculture/secta_plantingw2.dta', False) in fake

    def test_roster_locates_a_household_no_cover_lists(self, ng, fake):
        s = ng.sample_for_wave('2012-13')
        q1 = s.xs('2013Q1', level='t')
        # 'e': only the PH roster records it -- its v, no weight (D1).
        assert q1.loc['e', 'v'] == 'moved-e'
        assert pd.isna(q1.loc['e', 'weight']) and pd.isna(q1.loc['e', 'panel_weight'])
        # The roster never overrides a cover: 'a' keeps the cover's v.
        assert q1.loc['a', 'v'] == '1/1/1' and q1.loc['a', 'weight'] == 1.0
        assert s.xs('2012Q3', level='t').loc['a', 'v'] == '1/1/1'

    def test_cluster_features_joins_geo_left_onto_each_round(self, ng, fake, monkeypatch):
        import lsms_library.local_tools as lt
        geo = pd.DataFrame({'Latitude': [1.0, 2.0, 3.0], 'Longitude': [4.0, 5.0, 6.0]},
                           index=pd.Index(['a', 'd', 'z'], name='i'))
        monkeypatch.setattr(lt, 'df_data_grabber', lambda *a, **k: geo.copy())
        cf = ng.cluster_features_for_wave('2012-13')
        assert list(cf.index.names) == ['t', 'i', 'v']
        r = cf.reset_index()
        # 'z' is geo-only: on no cover, in no round (GH #1261 -- a left join).
        assert not r.i.eq('z').any()
        assert sorted(r[r.t == '2012Q3'].i) == ['a', 'b', 'c', 'd']
        assert sorted(r[r.t == '2013Q1'].i) == ['a', 'b', 'd', 'e']
        got = r.set_index(['t', 'i'])['Latitude']
        assert got[('2013Q1', 'd')] == 2.0 and pd.isna(got[('2012Q3', 'c')])

    def test_w5_joins_acquired_geo_left_and_preserves_round_identity(self, ng, monkeypatch):
        import lsms_library.local_tools as lt
        spec = ng.ROUND_COVERS['2023-24']
        assert spec['geo'] == ('nga_householdgeovariables_y5.dta', 'lat_mod', 'lon_mod')
        covers = {spec['pp']: _cover(['a'], ['1/1/1'], [1.0]),
                  spec['ph']: _cover(['a'], ['1/1/2'], [2.0])}
        monkeypatch.setattr(ng, '_cover_frame',
                            lambda fn, spec_, weights=True: covers[fn.split('/Data/', 1)[1]].copy())
        monkeypatch.setattr(ng, '_roster_locator',
                            lambda fn, spec_, cover_fn: _cover(['a'], ['1/1/1'], [np.nan]))
        # This routing test supplies a nonzero PH EA; actual labelled/raw
        # extraction and its zero-EA rule are tested in same_location.py.
        monkeypatch.setattr(ng, '_ph_same_location_answers', lambda folder: pd.DataFrame({
            'hhid': ['a'], 'state': [1], 'lga': [1], 'ea': [2],
            'hh_moved': [2], 'hh_moved_label': ['2. NO, IN ORIGINAL DWELLING'],
            'pp_hhid': ['a'], 'pp_state': [1], 'pp_lga': [1], 'pp_ea': [1],
        }, index=pd.Index(['a'], name='i')), raising=False)
        geo = pd.DataFrame({'Latitude': [1.5, 9.0], 'Longitude': [4.5, 9.0]},
                           index=pd.Index(['a', 'outside'], name='i'))
        def fake_geo(fn, idxvars, **columns):
            assert fn.endswith('/Data/nga_householdgeovariables_y5.dta')
            assert idxvars == {'i': 'hhid'}
            assert columns == {'Latitude': 'lat_mod', 'Longitude': 'lon_mod'}
            return geo.copy()
        monkeypatch.setattr(lt, 'df_data_grabber', fake_geo)
        cf = ng.cluster_features_for_wave('2023-24')
        assert set(cf.index.get_level_values('i')) == {'a'}
        assert set(cf.index) == {('2023Q3', 'a', '1/1/1'), ('2024Q1', 'a', '1/1/2')}
        assert cf['Latitude'].eq(1.5).all() and cf['Longitude'].eq(4.5).all()


class TestRosterLocator:
    """``_roster_locator``: D1 location from a roster, the D2 singleton."""

    @pytest.fixture
    def files(self, ng, monkeypatch):
        import lsms_library.local_tools as lt
        roster_raw = pd.DataFrame({
            'hhid': [1, 1, 2, 3, 3],
            'zone': [5, 5, 5, 5, 5], 'state': [32, 32, 32, 32, 32],
            'lga': [3201, 3201, 3201, 3201, 3201],
            'ea': [10, 10, 0, np.nan, np.nan],
            'sector': [2, 2, 2, 2, 2]})
        roster_dec = roster_raw.assign(zone='5. SOUTH SOUTH', state='32. Rivers',
                                       sector='2. RURAL')
        cover_raw = pd.DataFrame({'hhid': [9], 'zone': [5], 'state': [32],
                                  'lga': [3201], 'ea': [10], 'sector': [2]})
        cover_dec = cover_raw.assign(zone='south south', state='rivers',
                                     sector='2. RURAL')
        frames = {('roster', False): roster_raw, ('roster', True): roster_dec,
                  ('cover', False): cover_raw, ('cover', True): cover_dec}

        def fake_get(fn, convert_categoricals=True, **k):
            return frames[(fn, convert_categoricals)].copy()

        monkeypatch.setattr(lt, 'get_dataframe', fake_get)
        return frames

    def test_located_moved_and_unlocated(self, ng, files):
        out = ng._roster_locator('roster', ng.ROUND_COVERS['2015-16'], 'cover')
        assert out.loc['1', 'v'] == '32/3201/10'          # D1: the roster's EA
        assert out.loc['2', 'v'] == 'moved-2'             # ea == 0: the Moved sentinel
        assert out.loc['3', 'v'] == 'unlocated-3'         # D2: no location recorded
        assert out['weight'].isna().all() and out['panel_weight'].isna().all()

    def test_labels_are_the_covers_spelling(self, ng, files):
        out = ng._roster_locator('roster', ng.ROUND_COVERS['2015-16'], 'cover')
        assert set(out['Region']) == {'south south'}
        assert set(out['District']) == {'rivers'}
        assert set(out['Rural']) == {'Rural'}

    def test_a_household_with_two_locations_raises(self, ng, files):
        files[('roster', False)].loc[1, 'ea'] = 11
        with pytest.raises(AssertionError, match='two locations'):
            ng._roster_locator('roster', ng.ROUND_COVERS['2015-16'], 'cover')

    def test_singleton_spelling_is_not_the_mover_one(self, ng):
        assert ng.unlocated_id(17) == 'unlocated-17'
        assert ng.UNLOCATED_PREFIX == 'unlocated-'   # shared with Uganda
        # The id part is formatted exactly as cluster_id formats a mover's.
        for raw in (17, 17.0, '17', '0017'):
            moved = ng.cluster_id(1, 101, 0, raw)
            assert moved.startswith('moved-')
            assert ng.unlocated_id(raw) == 'unlocated-' + moved[len('moved-'):]

    @pytest.mark.parametrize('folder', FOLDERS)
    def test_every_round_names_its_roster(self, ng, folder):
        spec = ng.ROUND_COVERS[folder]
        assert 'sect1_planting' in spec['pp_roster']
        assert 'sect1_harvest' in spec['ph_roster']
