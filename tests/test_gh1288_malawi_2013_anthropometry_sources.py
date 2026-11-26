"""Real Malawi IHS3/IHPS native input discovery and private-copy resolution."""
from pathlib import Path
import runpy
import sys
from types import SimpleNamespace

import pytest

import lsms_library.country as country_module
import lsms_library.local_tools as tools
from lsms_library.country import Country
from lsms_library.local_tools import scan_script_data_refs, source_fingerprint

COUNTRY = Path(__file__).resolve().parents[1] / 'lsms_library/countries/Malawi'
@pytest.fixture(params=(
    ('2010-11', 'Full_Sample/Household', ('hh_mod_v.dta', 'hh_mod_b.dta')),
    ('2013-14', '', ('HH_MOD_V_13.dta', 'HH_MOD_B_13.dta')),
), ids=('IHS3', 'IHPS2013'))
def source_paths(request):
    wave, nested, names = request.param
    root = COUNTRY / wave
    return root, root / '_/anthropometry.py', {root / 'Data' / nested / n for n in names}


def test_scanner_discovers_the_real_sidecars_for_private_copy(source_paths):
    wave, script, sources = source_paths
    refs = {x for x in scan_script_data_refs(script) if x.lower().endswith('.dta')}
    assert {(script.parent / x).resolve() for x in refs} == sources
    assert {(wave / 'Data' / x).resolve() for x in refs} == sources
    for source in sources:
        assert source.with_name(source.name + '.dvc').is_file()
        assert source_fingerprint(source).startswith(f'dvc:{source.name}:')
    if wave.name == '2010-11':
        # These same basenames exist for Panel, but IHS3 anthropometry
        # must continue to use the distinct Full_Sample release.
        panel_sidecars = {wave / 'Data/Panel/Household' / (p.name + '.dvc')
                          for p in sources}
        assert all(p.is_file() for p in panel_sidecars)
        assert not {(script.parent / (x + '.dvc')).resolve()
                    for x in refs} & panel_sidecars


def test_existing_private_collector_contains_both_raw_sources(source_paths):
    wave, _, expected = source_paths

    class MetadataOnly:
        name = 'Malawi'
        file_path = COUNTRY
        waves = [wave.name]
        resources = {'Data Scheme': {'anthropometry': {}}}

        def __getitem__(self, year):
            assert year == wave.name
            return SimpleNamespace(file_path=wave, resources={})

        def _make_other_references(self, table):
            assert table == 'anthropometry'
            return {}

        def _make_wave_inputs(self, table):
            assert table == 'anthropometry'
            return {Path(f'Malawi/{wave.name}/_/anthropometry.parquet'):
                    ([wave.name], False)}

    record, sources = Country._make_declared_inputs(
        MetadataOnly(), 'anthropometry', _with_sources=True)
    # Preserve the unchanged compiler f-string's existing missing marker.
    marker = Path('/_/anthropometry.parquet')
    assert sources == expected | {marker}
    # Discovered source sidecars are covered by the wave hash, not repeated here.
    assert record == {'tables': {}, 'sources': {str(marker): 'missing:anthropometry.parquet'}}


def test_ihs3_full_sample_sources_participate_in_the_wave_hash(monkeypatch):
    wave = Country('Malawi', preload_panel_ids=False)['2010-11']
    before = wave._input_hash('anthropometry')
    assert before is not None
    for module in ('hh_mod_b.dta', 'hh_mod_v.dta'):
        for release in ('Full_Sample', 'Panel'):
            changed = COUNTRY / '2010-11/Data' / release / 'Household' / module

            def probe(path):
                digest = source_fingerprint(path)
                return digest + ':updated' if path.resolve() == changed else digest

            monkeypatch.setattr(country_module, 'source_fingerprint', probe)
            after = wave._input_hash('anthropometry')
            assert (before != after) == (release == 'Full_Sample')


def test_ihs3_driver_still_reads_the_same_paths_and_writes_once(monkeypatch):
    from pandas import DataFrame

    v = '../Data/Full_Sample/Household/hh_mod_v.dta'
    b = '../Data/Full_Sample/Household/hh_mod_b.dta'
    replies = {v: object(), b: object()}
    seen = []
    output = DataFrame({'Weight': [10]})
    piece = object()

    def read(path, **kwargs):
        seen.append(path)
        assert kwargs == {}
        return replies[path]

    def block(health, roster, **kwargs):
        assert health is replies[v] and roster is replies[b]
        assert kwargs == dict(t='2010-11', hh_id_health='case_id',
                              pid_health='id_code', hh_id_roster='case_id',
                              pid_roster='id_code')
        return piece

    def assemble(wave, inputs):
        assert wave == '2010-11' and inputs == [piece]
        return output

    def write(frame, name):
        assert frame is output
        seen.append(name)

    monkeypatch.setattr(tools, 'get_dataframe', read)
    monkeypatch.setattr(tools, 'to_parquet', write)
    monkeypatch.setitem(sys.modules, 'malawi', SimpleNamespace(
        _anthropometry_block=block, assemble_anthropometry=assemble))
    monkeypatch.syspath_prepend(str(COUNTRY / '_'))
    runpy.run_path(str(COUNTRY / '2010-11/_/anthropometry.py'))
    assert seen == [v, b, 'anthropometry.parquet']
