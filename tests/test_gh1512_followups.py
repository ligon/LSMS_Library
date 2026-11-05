"""The four small follow-ups from #1487's review (GH #1512).  Data-free.

1. A kilogram subset whose waves declare no food table skips the all-waves read.
2. A subset the legacy path would answer "absent" raises the failure instead.
3. The grain replay at the L2-country gate is scoped to the served waves.
4. A kilogram subset's failure says that it read other waves.
"""
import warnings

import pandas as pd
import pytest

import lsms_library.country as C
from lsms_library.local_tools import to_parquet
from lsms_library.paths import countries_root, data_root


def _food(y, root, country='Malawi'):
    rows = [(y, f'{y}h{k}', 'Rice', 'Tin', 'purchased', 1.0, 150.0) for k in range(8)]
    rows += [(y, f'{y}k{k}', 'Rice', 'Kg', 'purchased', 1.0, 100.0) for k in range(10)]
    out = pd.DataFrame(rows, columns=['t', 'i', 'j', 'u', 's', 'Quantity', 'Expenditure'])
    out = out.set_index(['t', 'i', 'j', 'u', 's'])
    path = root / country / y / '_' / 'food_acquired.parquet'
    path.parent.mkdir(parents=True, exist_ok=True)
    to_parquet(out, path, absolute_path=True)
    return out


def _isolate(tmp_path, monkeypatch):
    root = tmp_path / 'data'
    monkeypatch.setenv('LSMS_DATA_DIR', str(root))
    monkeypatch.setenv('LSMS_BUILD_BACKEND', 'dvc')
    monkeypatch.setenv('LSMS_BUILD_WORKERS', '1')
    monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
    monkeypatch.delenv('LSMS_GRAIN_STRICT', raising=False)
    data_root.cache_clear()
    monkeypatch.setattr(C.Country, '_join_v_from_sample', lambda self, df, *a, **k: df)
    return root


def _country(name):
    c = C.Country(name, preload_panel_ids=False)
    c._panel_ids_attempted = True
    c._updated_ids_cache = {}
    return c


@pytest.fixture
def healthy(tmp_path, monkeypatch):
    """Mali's real registry: 2017-18 declares no `food_acquired`."""
    root = _isolate(tmp_path, monkeypatch)
    built = []

    def grab(wave, request, *, _with_origin=False):
        built.append(wave.year)
        out = _food(wave.year, root, 'Mali')
        return (out, 'parquet') if _with_origin else out

    monkeypatch.setattr(C.Wave, 'grab_data', grab)
    try:
        yield _country('Mali'), built
    finally:
        data_root.cache_clear()


@pytest.fixture
def broken(tmp_path, monkeypatch):
    """Malawi's real registry over controlled frames; its first wave fails."""
    root = _isolate(tmp_path, monkeypatch)
    c = _country('Malawi')
    waves = [w for w in c.waves if 'food_acquired' in c[w].data_scheme]
    bad, good = waves[0], waves[1]

    def grab(wave, request, *, _with_origin=False):
        if wave.year == bad:
            raise RuntimeError(f'source of {bad} unavailable')
        out = _food(wave.year, root)
        return (out, 'parquet') if _with_origin else out

    monkeypatch.setattr(C.Wave, 'grab_data', grab)
    try:
        yield c, good, bad
    finally:
        data_root.cache_clear()


class TestAWaveWithNoFoodTableSkipsTheRead:
    @pytest.mark.parametrize('table,kw', [
        ('food_quantities', {}), ('food_quantities', {'units': 'kgs'}),
        ('food_prices', {}), ('food_prices', {'units': 'kgvalue'})])
    def test_no_wave_is_built(self, healthy, table, kw):
        c, built = healthy
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            got = getattr(c, table)(waves=['2017-18'], **kw)
        assert built == [] and len(got) == 0

    def test_it_is_the_shape_of_the_read_it_replaces(self, healthy, monkeypatch):
        c, _ = healthy
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            got = c.food_quantities(waves=['2017-18'])
            monkeypatch.setattr(C.Country, '_proven_absent', lambda *a: False)
            old = _country('Mali').food_quantities(waves=['2017-18'])
        pd.testing.assert_frame_equal(got, old, check_exact=True)
        assert sum(got.attrs['kg_factor_sources'].values()) == 0

    def test_a_wave_with_the_table_still_reads_every_wave(self, healthy):
        c, built = healthy
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            assert len(c.food_quantities(waves=['2018-19']))
        assert set(built) == {'2014-15', '2018-19', '2021-22'}

    def test_a_country_builder_keeps_the_read(self, healthy, monkeypatch):
        c, built = healthy
        monkeypatch.setattr(C.Country, '_has_country_builder', lambda self, m: True)
        assert not c._proven_absent('food_acquired', ['2017-18'])


class TestProvenAbsent:
    def test_none_means_every_wave(self):
        assert not _country('Mali')._proven_absent('food_acquired', None)

    def test_every_wave_named_is_not_a_subset(self):
        c = _country('Mali')
        assert not c._proven_absent('food_acquired', list(c.waves))

    def test_a_declaring_wave_defeats_it(self):
        c = _country('Mali')
        assert not c._proven_absent('food_acquired', ['2017-18', '2018-19'])

    def test_an_undeclared_table_is_not_its_question(self):
        assert not _country('Mali')._proven_absent('no_such_table', ['2017-18'])

    def test_a_real_country_script_counts_as_a_builder(self):
        c = _country('Uganda')
        table = 'food_acquired'
        waves = [w for w in c.waves if table not in c[w].data_scheme]
        if waves:
            assert not c._proven_absent(table, waves[:1])
        assert c._has_country_builder(table)


class TestAFailureIsNotAbsence:
    @pytest.mark.parametrize('kw', [{}, {'basis': 'purchased'}])
    def test_a_broken_requested_wave_raises(self, broken, kw):
        c, _, bad = broken
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            with pytest.raises(RuntimeError, match='unavailable'):
                c.food_expenditures(waves=[bad], **kw)

    def test_one_broken_wave_of_two_raises(self, broken):
        c, good, bad = broken
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            with pytest.raises(RuntimeError, match='unavailable'):
                c.food_expenditures(waves=[good, bad])

    def test_a_healthy_wave_still_serves(self, broken):
        c, good, _ = broken
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            assert len(c.food_expenditures(waves=[good])) == 18

    def test_a_real_absence_still_answers_empty(self, healthy):
        c, built = healthy
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            assert len(c.food_expenditures(waves=['2017-18'])) == 0
        assert built == []


class TestTheErrorSaysTheRule:
    def _notes(self, c, good):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            with pytest.raises(RuntimeError) as caught:
                c.food_quantities(waves=[good])
        return getattr(caught.value, '__notes__', [])

    def test_a_kilogram_subset_says_it_read_other_waves(self, broken):
        c, good, _ = broken
        notes = self._notes(c, good)
        assert len(notes) == 1
        assert 'reads every wave' in notes[0] and good in notes[0]
        assert "units='units'" in notes[0]

    def test_a_failure_of_a_requested_wave_elsewhere_has_no_such_note(self, broken):
        c, _, bad = broken
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            with pytest.raises(RuntimeError) as caught:
                c.food_expenditures(waves=[bad])
        assert not getattr(caught.value, '__notes__', [])


class TestGrainReportScope:
    def _reports(self):
        return [{'wave': 'w1', 'n': 1}, {'wave': 'w2', 'n': 2}, {'wave': None, 'n': 3},
                {'n': 4}]

    def test_it_keeps_the_served_wave_and_any_that_names_none(self):
        c = _country('Mali')
        got = c._grain_reports_for_waves(self._reports(), ['w1'])
        assert [r['n'] for r in got] == [1, 3, 4]

    def test_a_report_naming_the_folder_counts_as_either_round(self):
        c = _country('Nigeria')
        c.waves  # populates wave_folder_map
        reports = [{'wave': '2015-16', 'n': 1}, {'wave': '2012-13', 'n': 2}]
        assert [r['n'] for r in c._grain_reports_for_waves(reports, ['2016Q1'])] == [1]

    def test_no_stamp_passes_through(self):
        assert _country('Mali')._grain_reports_for_waves(None, ['w1']) is None


@pytest.fixture
def two_waves(tmp_path, monkeypatch):
    """A synthetic country whose first wave destroys a row in a collapse."""
    croot = tmp_path / 'countries'
    droot = tmp_path / 'data'
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(croot))
    monkeypatch.setenv('LSMS_DATA_DIR', str(droot))
    monkeypatch.setenv('LSMS_ISSUES_LOG', str(tmp_path / 'issues.log'))
    monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
    monkeypatch.delenv('LSMS_GRAIN_STRICT', raising=False)
    countries_root.cache_clear()
    data_root.cache_clear()
    (croot / 'Fakeland' / '_').mkdir(parents=True)
    (croot / 'Fakeland' / '_' / 'data_scheme.yml').write_text(
        'Country: Fakeland\nWaves: ["2001", "2002"]\nData Scheme:\n'
        '  housing:\n    index: (t, i)\n    Roof: str\n')
    for wave, body in (('2001', 'h1,Tin\nh1,Grass\nh2,Tin\n'), ('2002', 'h1,Tin\nh2,Tin\n')):
        (croot / 'Fakeland' / wave / '_').mkdir(parents=True)
        source = tmp_path / f'{wave}.csv'
        source.write_text('id,roof\n' + body)
        (croot / 'Fakeland' / wave / '_' / 'data_info.yml').write_text(
            f'housing:\n  file: {source}\n  idxvars: {{i: id}}\n  myvars:\n    Roof: roof\n')
    try:
        yield
    finally:
        countries_root.cache_clear()
        data_root.cache_clear()


class TestTheGateEndToEnd:
    def _warm(self):
        C._GRAIN_LEDGER.clear()
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            C.Country('Fakeland', preload_panel_ids=False).housing()
        C._GRAIN_LEDGER.clear()

    def _read(self, **kw):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            C.Country('Fakeland', preload_panel_ids=False).housing(**kw)
        return [w for w in caught if issubclass(w.category, C.GrainCollapseWarning)]

    def test_the_wave_that_collapsed_is_told(self, two_waves):
        self._warm()
        assert self._read(waves=['2001'])

    def test_another_wave_is_not(self, two_waves):
        self._warm()
        assert not self._read(waves=['2002'])

    def test_every_wave_is(self, two_waves):
        self._warm()
        assert self._read()

    def test_the_strict_lever_fails_only_the_wave_that_collapsed(self, two_waves, monkeypatch):
        self._warm()
        monkeypatch.setenv('LSMS_GRAIN_STRICT', '1')
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            assert len(C.Country('Fakeland', preload_panel_ids=False).housing(waves=['2002']))
            with pytest.raises(C.GrainCollapseError):
                C.Country('Fakeland', preload_panel_ids=False).housing(waves=['2001'])
