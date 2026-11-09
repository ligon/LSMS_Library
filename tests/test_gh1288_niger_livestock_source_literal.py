"""The real Niger2014 source must enter the existing private Make closure.

Metadata only: no Country construction, raw source read or native producer.
"""
from pathlib import Path
from types import SimpleNamespace

from lsms_library.country import Country
from lsms_library.local_tools import scan_script_data_refs, source_fingerprint

COUNTRIES = Path(__file__).resolve().parents[1] / 'lsms_library' / 'countries'
COUNTRY = COUNTRIES / 'Niger'
WAVE = COUNTRY / '2014-15'
SOURCE = WAVE / 'Data/NER_2014_ECVMA-II_v02_M_STATA8/ECVMA2_AS4AP2.dta'


def test_native_literal_resolves_the_real_committed_sidecar():
    script = WAVE / '_/livestock.py'
    raw = {(script.parent / ref).resolve() for ref in scan_script_data_refs(script)
           if ref.lower().endswith('.dta')}
    assert raw == {SOURCE}
    assert SOURCE.with_name(SOURCE.name + '.dvc').is_file()
    assert source_fingerprint(SOURCE).startswith('dvc:ECVMA2_AS4AP2.dta:')


def test_existing_make_preparation_collects_the_real_source():
    class MetadataOnly:
        name = 'Niger'
        file_path = COUNTRY
        waves = ['2014-15']
        resources = {'Data Scheme': {'livestock': {}}}

        def __getitem__(self, year):
            assert year == '2014-15'
            return SimpleNamespace(file_path=WAVE, resources={})

        def _make_other_references(self, table):
            assert table == 'livestock'
            return {}

        def _make_wave_inputs(self, table):
            assert table == 'livestock'
            return {Path('Niger/2014-15/_/livestock.parquet'): (['2014-15'], False)}

    record, sources = Country._make_declared_inputs(
        MetadataOnly(), 'livestock', _with_sources=True)
    assert sources == {SOURCE}
    assert record == {'tables': {}, 'sources': {str(SOURCE): source_fingerprint(SOURCE)}}
