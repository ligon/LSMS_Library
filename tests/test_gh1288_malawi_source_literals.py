"""Real script scanner and private Make-input metadata, without source reads."""
from pathlib import Path
from types import SimpleNamespace

import pytest

from lsms_library.country import Country
from lsms_library.local_tools import scan_script_data_refs, source_fingerprint

COUNTRY = (Path(__file__).resolve().parents[1]
           / 'lsms_library/countries/Malawi')
WAVE = COUNTRY / '2010-11'
CASES = [
    ('livestock', ('Agriculture/ag_mod_r1.dta',)),
    ('crop_production', ('Agriculture/ag_mod_g.dta', 'Agriculture/ag_mod_p.dta',
                         'Agriculture/ag_mod_i.dta', 'Agriculture/ag_mod_q.dta')),
    ('plot_inputs', ('Agriculture/ag_mod_d.dta', 'Agriculture/ag_mod_g.dta',
                     'Agriculture/ag_mod_h.dta')),
    ('plot_labor', ('Agriculture/ag_mod_d.dta',)),
    ('people_last7days', ('Household/hh_mod_e.dta',)),
]


@pytest.mark.parametrize('table,relative_sources', CASES)
def test_real_script_refs_include_all_committed_native_sources(table, relative_sources):
    script = WAVE / '_' / f'{table}.py'
    refs = set(scan_script_data_refs(script))
    expected = {WAVE / 'Data/Full_Sample' / name for name in relative_sources}
    resolved = {(script.parent / ref).resolve() for ref in refs
                if ref.lower().endswith('.dta')}
    # The untouched labor provenance argument still contains its bare name;
    # the actual native getter source must also be discoverable in full.
    extra = {script.parent / 'ag_mod_d.dta'} if table == 'plot_labor' else set()
    assert resolved == expected | extra
    for source in expected:
        assert source.with_name(source.name + '.dvc').is_file()
        assert source_fingerprint(source).startswith(f'dvc:{source.name}:')


@pytest.mark.parametrize('table,relative_sources', CASES)
def test_private_make_input_collection_sees_real_sources(table, relative_sources):
    class MetadataOnly:
        name = 'Malawi'
        file_path = COUNTRY
        waves = ['2010-11']
        resources = {'Data Scheme': {table: {}}}

        def __getitem__(self, year):
            assert year == '2010-11'
            return SimpleNamespace(file_path=WAVE, resources={})

        def _make_other_references(self, method):
            assert method == table
            return {}

        def _make_wave_inputs(self, method):
            assert method == table
            return {Path(f'Malawi/2010-11/_/{table}.parquet'):
                    (['2010-11'], False)}

    record, sources = Country._make_declared_inputs(
        MetadataOnly(), table, _with_sources=True)
    expected = {WAVE / 'Data/Full_Sample' / name for name in relative_sources}
    extra = {WAVE / '_/ag_mod_d.dta'} if table == 'plot_labor' else set()
    assert sources == expected | extra
    # Correct wave literals are already authenticated in the existing wave
    # hash. Preserve the harmless old provenance literal's missing marker.
    assert record == {'tables': {}, 'sources': {
        str(path): 'missing:ag_mod_d.dta' for path in extra}}
