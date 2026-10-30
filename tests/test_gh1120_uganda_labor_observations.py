"""Missing plots identify observations, never a fabricated shared plot."""
import ast
import hashlib
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root


@pytest.fixture
def u():
    spec = importlib.util.spec_from_file_location(
        'labor_observations_uganda', countries_root() / 'Uganda/_/uganda.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def refs(tmp_path, monkeypatch):
    import lsms_library.paths as paths
    monkeypatch.setattr(paths, 'countries_root', lambda: tmp_path)
    result = {}
    for season in 'AB':
        source = tmp_path / f'Uganda/2009-10/Data/AGSEC3{season}.dta'
        source.parent.mkdir(parents=True, exist_ok=True)
        source.with_suffix('.dta.dvc').write_text(
            f'outs:\n- md5: {"a" * 32}\n  path: {source.name}\n')
        result[season] = str(source)
    return result


def raw():
    # Labels deliberately repeat and are unrelated to physical source positions.
    return pd.DataFrame({
        'HHID': ['1013000101'] * 7, 'a3aq1': [1, None, None, None, 1, None, None],
        'a3aq3': [1, None, None, None, 1, None, None],
        'a3aq38': [1, None, None, 0, 1, None, None],
        'a3aq39': [2., None, 4., None, 5., None, None],
        'a3aq41': [1, None, None, 2, 1, 1, None],
        'a3aq42a': [1., None, 3., None, 7., None, None],
        'a3aq42b': [None] * 7, 'a3aq42c': [None] * 7,
        'a3aq43': [100., None, 200., None, 50., 0., 90.],
    }, index=[99, 4, 4, 2, 88, 8, 6])


def test_original_positions_payload_masks_and_known_max(u, refs):
    result = u.plot_labor_for_wave('2009-10', raw(), None,
                                 u.LABOR_COLMAPS['2009-10'], source_refs=refs)
    assert result.index.names == ['t', 'i', 'plot_id', 'source', 'season', 'labor_obs']
    assert result.index.is_unique
    assert list(result.dtypes) == [pd.Float64Dtype(), pd.Float64Dtype()]
    known = result.loc[result.index.get_level_values('plot_id') != 'unknown_plot']
    assert set(known.index.get_level_values('labor_obs')) == {'unresolved_observation'}
    assert known.PersonDays.tolist() == [5., 7.]
    assert known.Wage.isna().tolist() == [True, False]
    assert known.Wage.dropna().tolist() == [100.]
    missing = result.reset_index().query("plot_id == 'unknown_plot'")
    prefix = 'row:' + u._labor_source_reference(refs['A'], '2009-10') + ':'
    assert set(missing.labor_obs) == {prefix + str(i) for i in (2, 3, 5, 6)}
    assert len(missing) == 6  # blank physical row 1 never enters
    hired = missing.query("source == 'hired'").set_index('labor_obs')
    assert hired.loc[prefix + '2', 'PersonDays'] == 3.
    assert hired.loc[prefix + '3', 'PersonDays'] == 0.
    assert pd.isna(hired.loc[prefix + '5', 'PersonDays'])
    assert hired.loc[prefix + '5', 'Wage'] == 0.
    assert pd.isna(hired.loc[prefix + '6', 'PersonDays'])
    assert hired.loc[prefix + '6', 'Wage'] == 90.


def test_equal_payloads_source_files_categories_and_seasons_stay_distinct(u, refs):
    a = raw().iloc[[2, 2]].copy()
    b = a.rename(columns=lambda c: c.replace('a3aq', 'a3bq'))
    result = u.plot_labor_for_wave('2009-10', a, b, u.LABOR_COLMAPS['2009-10'],
                                 source_refs=refs)
    assert len(result) == 8 and result.index.is_unique
    for season in 'AB':
        take = result.xs(season, level='season')
        prefix = 'row:' + u._labor_source_reference(refs[season], '2009-10') + ':'
        assert set(take.index.get_level_values('labor_obs')) == {prefix+'0', prefix+'1'}
        assert set(take.index.get_level_values('source')) == {'family', 'hired'}
    # Even a shared physical file remains distinct by season.
    shared = u.plot_labor_for_wave('2009-10', a, b, u.LABOR_COLMAPS['2009-10'],
                                 source_refs={'A': refs['A'], 'B': refs['A']})
    assert len(shared) == 8 and shared.index.is_unique


@pytest.mark.parametrize('failure', ['absent', 'raw-only', 'ambiguous', 'wrong-output', 'wrong-wave'])
def test_constructed_identity_refuses_missing_or_ambiguous_authority(u, refs, failure):
    path = Path(refs['A'])
    if failure == 'absent':
        refs = None
    elif failure == 'raw-only':
        path.with_suffix('.dta.dvc').unlink()
        path.write_bytes(b'raw bytes must never be hashed for identity')
    elif failure == 'ambiguous':
        path.with_suffix('.dta.dvc').write_text('outs:\n- path: A\n- path: B\n')
    elif failure == 'wrong-output':
        path.with_suffix('.dta.dvc').write_text(f'outs:\n- path: other.dta\n  md5: {"a"*32}\n')
    else:
        refs['A'] = refs['A'].replace('2009-10', '2010-11')
    with pytest.raises(ValueError, match='source|sidecar|wave'):
        u.plot_labor_for_wave('2009-10', raw(), None, u.LABOR_COLMAPS['2009-10'],
                             source_refs=refs)


def test_no_authority_needed_for_ineligible_missing_or_structural_suffix(u):
    data = raw().iloc[[0, 1]].copy()
    cm = {'A': dict(u.LABOR_COLMAPS['2009-10']['A'], plot=None)}
    # Original missing parcel at row1 is a wholly blank, ineligible line.
    result = u.plot_labor_for_wave('2009-10', data, None, cm)
    assert len(result) == 2
    assert set(result.index.get_level_values('plot_id')) == {'1013000101-1-'}
    assert set(result.index.get_level_values('labor_obs')) == {'unresolved_observation'}


def test_reference_is_declared_path_and_sidecar_sha_only(u, refs):
    path = Path(refs['A'])
    before = u._labor_source_reference(path, '2009-10')
    path.write_bytes(b'arbitrary raw contents do not define this coordinate')
    assert u._labor_source_reference(path, '2009-10') == before
    sha = hashlib.sha256(path.with_suffix('.dta.dvc').read_bytes()).hexdigest()
    assert before == f'Uganda/2009-10/Data/AGSEC3A.dta@dvc:AGSEC3A.dta:{sha}'


def test_empty_axis_and_seven_driver_path_bindings(u):
    empty = u.plot_labor_for_wave('2009-10', None, None, {})
    assert empty.index.names[-1] == 'labor_obs'
    scripts = sorted((countries_root() / 'Uganda').glob('*/_/plot_labor.py'))
    assert len(scripts) == 7
    for script in scripts:
        tree = ast.parse(script.read_text())
        loaded = {n.targets[0].id[-1].upper(): ast.literal_eval(n.value.args[0])
                  for n in tree.body if isinstance(n, ast.Assign)
                  and isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name)
                  and n.value.func.id == '_try'}
        call = next(n for n in ast.walk(tree) if isinstance(n, ast.Call)
                    and isinstance(n.func, ast.Name) and n.func.id == 'plot_labor_for_wave')
        bound = ast.literal_eval(next(k.value for k in call.keywords if k.arg == 'source_refs'))
        assert bound == loaded
