"""Niger's proved native raw paths enter the existing scanner/Make closure.

Metadata only: no Country construction, raw source read or native producer.
"""
from pathlib import Path
from types import SimpleNamespace

import pytest

from lsms_library.country import Country
from lsms_library.local_tools import scan_script_data_refs, source_fingerprint

COUNTRIES = Path(__file__).resolve().parents[1] / 'lsms_library' / 'countries'
COUNTRY = COUNTRIES / 'Niger'
CASES = [
    ('2014-15', 'livestock', ['ECVMA2_AS4AP2.dta']),
    ('2014-15', 'plot_labor', ['ECVMA2_AS2AP1.dta', 'ECVMA2_AS2AP2.dta']),
    ('2014-15', 'crop_production', ['ECVMA2_AS2E1P2.dta', 'ECVMA2_AS2E2P2.dta']),
    ('2014-15', 'people_last7days', ['ECVMA2_MS04P1.dta', 'ECVMA2_MS01P1.dta']),
    ('2014-15', 'community_prices', ['comprixcs07.dta']),
    ('2011-12', 'community_prices', ['ecvmacoms07_p1.dta', 'ecvmacoms07_p2.dta']),
]


def expected_sources(wave, names):
    directory = ('NER_2014_ECVMA-II_v02_M_STATA8' if wave == '2014-15'
                 else 'NER_2011_ECVMA_v01_M_Stata8')
    return {COUNTRY / wave / 'Data' / directory / name for name in names}


@pytest.mark.parametrize('wave,table,names', CASES)
def test_native_literal_resolves_the_real_committed_sidecar(wave, table, names):
    script = COUNTRY / wave / '_' / f'{table}.py'
    raw = {(script.parent / ref).resolve() for ref in scan_script_data_refs(script)
           if ref.lower().endswith('.dta')}
    expected = expected_sources(wave, names)
    assert raw == expected
    for source in expected:
        assert source.with_name(source.name + '.dvc').is_file()
        assert source_fingerprint(source).startswith(f'dvc:{source.name}:')


@pytest.mark.parametrize('wave,table,names', CASES)
def test_existing_make_preparation_collects_the_real_source(wave, table, names):
    class MetadataOnly:
        name = 'Niger'
        file_path = COUNTRY
        waves = [wave]
        resources = {'Data Scheme': {table: {}}}

        def __getitem__(self, year):
            assert year == wave
            return SimpleNamespace(file_path=COUNTRY / wave, resources={})

        def _make_other_references(self, method):
            assert method == table
            return {}

        def _make_wave_inputs(self, method):
            assert method == table
            return {Path('Niger') / wave / '_' / f'{table}.parquet': ([wave], False)}

    record, sources = Country._make_declared_inputs(
        MetadataOnly(), table, _with_sources=True)
    assert sources == expected_sources(wave, names)
    # Each wave hash already covers its sources; do not duplicate them here.
    assert record == {'tables': {}, 'sources': {}}
