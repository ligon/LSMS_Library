"""Nigeria W1–W3 nonfood declarations: metadata and guard checks only.

No native producer, survey source, Country constructor or cache is used.
The original countries-root reader arguments remain authoritative.
"""
import ast
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from lsms_library.country import Country
from lsms_library.local_tools import scan_script_data_refs


COUNTRIES = Path(__file__).resolve().parents[1] / 'lsms_library/countries'
COUNTRY = COUNTRIES / 'Nigeria'
CASES = [
    ('2010-11', '2011Q1', '2010Q3',
     ('sect11a_harvestw1.csv', 'sect11b_harvestw1.csv',
      'sect11c_harvestw1.csv', 'sect11d_harvestw1.csv',
      'sect11e_harvestw1.csv'),
     ('sect81_plantingw1.csv', 'sect82_plantingw1.csv',
      'sect83_plantingw1.csv', 'sect84_plantingw1.csv',
      'sect85_plantingw1.csv')),
    ('2012-13', '2013Q1', '2012Q3',
     ('sect11a_harvestw2.csv', 'sect11b_harvestw2.csv',
      'sect11c_harvestw2.csv', 'sect11d_harvestw2.csv',
      'sect11e_harvestw2.csv'),
     ('sect8a_plantingw2.csv', 'sect8b_plantingw2.csv',
      'sect8c_plantingw2.csv', 'sect8d_plantingw2.csv',
      'sect8e_plantingw2.csv')),
    ('2015-16', '2016Q1', '2015Q3',
     ('sect11a_harvestw3.csv', 'sect11b_harvestw3.csv',
      'sect11c_harvestw3.csv', 'sect11d_harvestw3.csv',
      'sect11e_harvestw3.csv'),
     ('sect8a_plantingw3.csv', 'sect8b_plantingw3.csv',
      'sect8c_plantingw3.csv')),
    ('2018-19', '2019Q1', '2018Q3',
     ('sect11a_harvestw4.csv', 'sect11b_harvestw4.csv',
      'sect11c_harvestw4.csv', 'sect11d_harvestw4.csv'),
     ('sect8a_plantingw4.csv', 'sect8b_plantingw4.csv',
      'sect8c_plantingw4.csv')),
]
PARAMETERS = 'wave,ph,pp,harvest,planting'


@pytest.mark.parametrize(PARAMETERS, CASES)
def test_scanner_resolves_all_original_sources(wave, ph, pp, harvest, planting):
    script = COUNTRY / wave / '_/nonfood_expenditures.py'
    refs = {ref for ref in scan_script_data_refs(script) if ref.endswith('.csv')}
    names = harvest + planting
    reader_refs = {f'Nigeria/{wave}/Data/{name}' for name in names}
    declared_refs = {f'../Data/{name}' for name in names}
    # The original strings stay intact, including their old missing markers.
    assert refs == reader_refs | declared_refs
    for ref in declared_refs:
        beside_script = (script.parent / ref).resolve()
        under_wave_data = (COUNTRY / wave / 'Data' / ref).resolve()
        assert beside_script == under_wave_data
        assert beside_script in {COUNTRIES / path for path in reader_refs}
        assert beside_script.with_name(beside_script.name + '.dvc').is_file()


@pytest.mark.parametrize(PARAMETERS, CASES)
def test_private_preparation_collects_all_sources(wave, ph, pp, harvest, planting):
    class MetadataOnly:
        name = 'Nigeria'
        file_path = COUNTRY
        waves = [ph, pp]
        resources = {'Data Scheme': {'nonfood_expenditures': {}}}

        def __getitem__(self, quarter):
            assert quarter in (ph, pp)
            return SimpleNamespace(file_path=COUNTRY / wave, resources={})

        def _make_other_references(self, method):
            assert method == 'nonfood_expenditures'
            return {}

        def _make_wave_inputs(self, method):
            assert method == 'nonfood_expenditures'
            return {Path('Nigeria') / wave / '_/nonfood_expenditures.parquet':
                    ([ph, pp], False)}

    record, sources = Country._make_declared_inputs(
        MetadataOnly(), 'nonfood_expenditures', _with_sources=True)
    names = harvest + planting
    expected = {COUNTRY / wave / 'Data' / name for name in names}
    # Existing root-qualified literals and the country compiler's format
    # string remain discoverable artifacts. Do not hide their missing markers.
    legacy = {COUNTRY / wave / '_' / 'Nigeria' / wave / 'Data' / name
              for name in names}
    compiler_fragment = COUNTRY / '%s/_/nonfood_expenditures.parquet'
    assert sources == expected | legacy | {compiler_fragment}
    assert record == {'tables': {}, 'sources': {
        str(path): f'missing:{path.name}'
        for path in legacy | {compiler_fragment}}}


def _declarations(wave):
    """Extract only literal metadata and its actual guard, never a producer."""
    script = COUNTRY / wave / '_/nonfood_expenditures.py'
    tree = ast.parse(script.read_text())
    assignments = {node.targets[0].id: node for node in tree.body
                   if isinstance(node, ast.Assign)
                   and isinstance(node.targets[0], ast.Name)}
    declaration = assignments['_DECLARED_NONFOOD_SOURCES']
    guard = tree.body[tree.body.index(declaration) + 1]
    assert isinstance(guard, ast.If)
    return (script, ast.literal_eval(assignments['files'].value),
            ast.literal_eval(declaration.value), guard)


@pytest.mark.parametrize(PARAMETERS, CASES)
def test_guard_preserves_ordered_reader_arguments(wave, ph, pp, harvest, planting):
    script, reader_files, declarations, guard = _declarations(wave)
    expected = [(ph, [f'Nigeria/{wave}/Data/{name}' for name in harvest]),
                (pp, [f'Nigeria/{wave}/Data/{name}' for name in planting])]
    assert list(reader_files.items()) == expected
    original = deepcopy(reader_files)
    scope = {'files': reader_files, '_DECLARED_NONFOOD_SOURCES': declarations}
    exec(compile(ast.Module(body=[guard], type_ignores=[]), str(script), 'exec'), scope)
    assert list(reader_files.items()) == list(original.items())


@pytest.mark.parametrize('wave', [case[0] for case in CASES])
@pytest.mark.parametrize('drift', ['reader_path', 'declaration_path', 'declaration_anchor',
                                  'file_order', 'round_order'])
@pytest.mark.parametrize('optimize', [0, 2])
def test_drift_refuses_before_getter(wave, drift, optimize):
    script, reader_files, declarations, guard = _declarations(wave)
    first = next(iter(reader_files))
    if drift == 'reader_path':
        reader_files[first][0] = 'Nigeria/other-wave/Data/other-source.csv'
    elif drift == 'declaration_path':
        declarations[first][0] = '../Data/other-source.csv'
    elif drift == 'declaration_anchor':
        declarations[first][0] = declarations[first][0].removeprefix('../')
    elif drift == 'file_order':
        reader_files[first].reverse()
    else:
        reader_files = dict(reversed(list(reader_files.items())))
    # A sentinel immediately after the actual guard stands for entering the
    # unchanged getter loop. No reader or scientific processing is executed.
    sentinel = ast.parse('getter_entered = True').body[0]
    scope = {'files': reader_files, '_DECLARED_NONFOOD_SOURCES': declarations}
    code = compile(ast.Module(body=[guard, sentinel], type_ignores=[]),
                   str(script), 'exec', optimize=optimize)
    with pytest.raises(RuntimeError, match='source declaration differs from files'):
        exec(code, scope)
    assert 'getter_entered' not in scope
