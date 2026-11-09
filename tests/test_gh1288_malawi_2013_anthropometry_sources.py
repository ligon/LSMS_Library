"""Real Malawi2013 native inputs: existing hash and private-copy resolution."""
from pathlib import Path
from types import SimpleNamespace

from lsms_library.country import Country
from lsms_library.local_tools import scan_script_data_refs, source_fingerprint

COUNTRY = Path(__file__).resolve().parents[1] / 'lsms_library/countries/Malawi'
WAVE = COUNTRY / '2013-14'
SCRIPT = WAVE / '_/anthropometry.py'
NAMES = ['HH_MOD_V_13.dta', 'HH_MOD_B_13.dta']
SOURCES = {WAVE / 'Data' / name for name in NAMES}


def test_scanner_discovers_the_real_sidecars_for_private_copy():
    refs = {x for x in scan_script_data_refs(SCRIPT) if x.lower().endswith('.dta')}
    assert {(SCRIPT.parent / x).resolve() for x in refs} == SOURCES
    for source in SOURCES:
        assert source.with_name(source.name + '.dvc').is_file()
        assert source_fingerprint(source).startswith(f'dvc:{source.name}:')
    # Old bare literals already resolved for the wave hash under Data, but
    # script-relative private copying pointed at absent _/ sources instead.
    assert {WAVE / 'Data' / name for name in NAMES} == SOURCES
    assert not SOURCES.intersection({SCRIPT.parent / name for name in NAMES})
    assert all(not (SCRIPT.parent / (name + '.dvc')).exists() for name in NAMES)


def test_existing_private_collector_contains_both_raw_sources():
    class MetadataOnly:
        name = 'Malawi'
        file_path = COUNTRY
        waves = ['2013-14']
        resources = {'Data Scheme': {'anthropometry': {}}}

        def __getitem__(self, year):
            assert year == '2013-14'
            return SimpleNamespace(file_path=WAVE, resources={})

        def _make_other_references(self, table):
            assert table == 'anthropometry'
            return {}

        def _make_wave_inputs(self, table):
            assert table == 'anthropometry'
            return {Path('Malawi/2013-14/_/anthropometry.parquet'):
                    (['2013-14'], False)}

    record, sources = Country._make_declared_inputs(
        MetadataOnly(), 'anthropometry', _with_sources=True)
    # Preserve the unchanged compiler f-string's existing missing marker.
    marker = Path('/_/anthropometry.parquet')
    assert sources == SOURCES | {marker}
    # Actual Data sidecars were already covered by the wave hash.
    assert record == {'tables': {}, 'sources': {str(marker): 'missing:anthropometry.parquet'}}
