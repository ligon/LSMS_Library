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

Three tiers: ``TestDeclaration`` (config only), ``TestRouting`` (the builder on
synthetic covers, no microdata) and ``TestBuilt`` (real tables; skipped
without S3 credentials).  No assertion prints a household id.
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
        assert sorted(q1.index) == ['a', 'b', 'd']
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
        assert sorted(r[r.t == '2013Q1'].i) == ['a', 'b', 'd']
        got = r.set_index(['t', 'i'])['Latitude']
        assert got[('2013Q1', 'd')] == 2.0 and pd.isna(got[('2012Q3', 'c')])

    def test_w5_has_no_geo_and_serves_na_coordinates(self, ng, monkeypatch):
        spec = ng.ROUND_COVERS['2023-24']
        assert spec['geo'] is None
        covers = {spec['pp']: _cover(['a'], ['1/1/1'], [1.0]),
                  spec['ph']: _cover(['a'], ['1/1/1'], [1.0])}
        monkeypatch.setattr(ng, '_cover_frame',
                            lambda fn, spec_, weights=True: covers[fn.split('/Data/', 1)[1]].copy())
        cf = ng.cluster_features_for_wave('2023-24')
        assert cf['Latitude'].isna().all() and cf['Longitude'].isna().all()


def _aws_creds_available() -> bool:
    spec = importlib.util.spec_from_file_location(
        '_shared_conftest', Path(__file__).with_name('conftest.py'))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.aws_creds_available()


@pytest.mark.slow
@pytest.mark.requires_s3
@pytest.mark.skipif(not _aws_creds_available(),
                    reason='needs S3 credentials to build Nigeria')
class TestBuilt:
    @pytest.fixture(scope='class')
    def nigeria(self):
        from lsms_library.country import Country
        return Country('Nigeria')

    @staticmethod
    def _nan_v(df):
        r = df.reset_index()
        return {str(t): int(n) for t, n in r[r['v'].isna()].groupby('t').size().items()}

    def test_plot_labor_post_harvest_households_get_a_cluster(self, nigeria):
        # development: {'2013Q1': 108} (29 households, all on the PH cover).
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            got = self._nan_v(nigeria.plot_labor())
        assert got == {}, got

    def test_plot_inputs_agriculture_cover_households_get_a_cluster(self, nigeria):
        # development: {'2012Q3': 63} (20 households, all on the W2 PP
        # agriculture cover and not on the PP household cover).
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            got = self._nan_v(nigeria.plot_inputs())
        assert got == {}, got

    def test_cluster_features_has_no_null_key_rows(self):
        # GH #1261: the outer merge admitted 39 (W2) and 1 (W3) geo-only
        # households with a null `v`; the left join onto the cover admits none.
        from lsms_library.country import Country, grain_reports
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            Country('Nigeria').cluster_features()
        nan_rows = {str(r.get('wave')): int(r.get('nan_key_rows') or 0)
                    for r in grain_reports('Nigeria', 'cluster_features')}
        n = sum(nan_rows.values())
        assert n == 0, nan_rows

    def test_w2_post_harvest_clusters_are_served(self, nigeria):
        # Two EAs have no household on W2's PP household cover; their PH-cover
        # (and PP agriculture cover) households now put them in both rounds.
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            cf = nigeria.cluster_features()
        r = cf.reset_index()
        real = r[~r.v.astype(str).str.startswith('moved-')]
        got = {str(t): int(n) for t, n in real.groupby('t').v.nunique().items()
               if t in ('2012Q3', '2013Q1')}
        assert got == {'2012Q3': 497, '2013Q1': 497}, got
