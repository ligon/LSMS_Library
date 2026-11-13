"""Execute the actual Uganda compiler, replacing invented IO leaves only."""
import importlib.util
import json
import os
from pathlib import Path
import runpy
import sys

import pandas as pd
import pytest

from lsms_library import Country, local_tools

UGANDA = Path(__file__).resolve().parents[1] / 'lsms_library/countries/Uganda'


@pytest.fixture
def compiler(monkeypatch, tmp_path):
    spec = importlib.util.spec_from_file_location('uganda', UGANDA / '_/uganda.py')
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, 'uganda', module)
    spec.loader.exec_module(module)
    country = Country('Uganda')
    # Scripts, not YAML keys, declare this feature for all eight native waves.
    assert set(country.waves) == set(module.Waves)
    assert all('plot_features' in country[w].data_scheme for w in module.Waves)
    state = {'reads': [], 'writes': [], 'walks': []}
    frames = {}
    for n, wave in enumerate(module.Waves):
        frames[wave] = pd.DataFrame({'Area': [float(n + 1), None]},
            index=pd.MultiIndex.from_tuples([(wave, 'raw-house', '1_A'),
                                             (wave, 'raw-house', '2_B')],
                                            names=['t', 'i', 'plot_id']))
    updated = {w: {'raw-house': 'canonical-house'} for w in module.Waves}
    monkeypatch.chdir(tmp_path)
    (tmp_path / 'updated_ids.json').write_text(json.dumps(updated))
    walk = module.id_walk

    def walking(frame, ids):
        state['walks'].append(True)
        return walk(frame, ids)

    monkeypatch.setattr(module, 'id_walk', walking)
    monkeypatch.setattr(local_tools, 'to_parquet',
                        lambda frame, path: state['writes'].append((frame.copy(), path)))

    def run(failure=None):
        def reader(path):
            wave = Path(path).parts[1]
            assert Path(path).name == 'plot_features.parquet'
            assert wave in frames
            state['reads'].append(wave)
            if failure is not None and wave == '2011-12':
                raise failure
            return frames[wave].copy(deep=True)

        monkeypatch.setattr(local_tools, 'get_dataframe', reader)
        script = Path(os.environ.get('UGANDA1287_CONTROL_SCRIPT', UGANDA / '_/plot_features.py'))
        runpy.run_path(str(script), run_name='__main__')
        return state

    return run, state, frames, module


@pytest.mark.parametrize('error', [FileNotFoundError('invented absent declared output'),
                                  ValueError('invented corrupt output'),
                                  TypeError('invented schema error'),
                                  RuntimeError('invented reader failure')])
def test_declared_plot_feature_wave_failure_refuses_partial_output(compiler, error):
    run, state, _, _ = compiler
    with pytest.raises(RuntimeError, match='Uganda/2011-12: declared plot_features') as exc:
        run(error)
    assert exc.value.__cause__ is error
    assert not state['writes'] and not state['walks']
    assert state['reads'] == ['2005-06', '2009-10', '2010-11', '2011-12']


def test_healthy_eight_wave_compiler_preserves_exact_rows_payload_and_walk(compiler):
    run, state, frames, module = compiler
    run()
    assert state['reads'] == list(module.Waves)
    assert len(state['writes']) == 1 and state['walks'] == [True]
    actual, path = state['writes'][0]
    expected = pd.concat(frames.values()).rename(index={'raw-house': 'canonical-house'}, level='i')
    expected.attrs['id_converted'] = True
    pd.testing.assert_frame_equal(actual, expected)
    assert actual.attrs == expected.attrs
    assert len(actual) == 16 and actual.Area.isna().sum() == 8
    assert path == '../var/plot_features.parquet'


def test_successfully_empty_declared_wave_is_not_a_reader_failure(compiler):
    run, state, frames, _ = compiler
    frames['2011-12'] = frames['2011-12'].iloc[:0]
    run()
    assert '2011-12' in state['reads']
    actual, _ = state['writes'][0]
    expected = pd.concat(frames.values()).rename(index={'raw-house': 'canonical-house'}, level='i')
    expected.attrs['id_converted'] = True
    pd.testing.assert_frame_equal(actual, expected)
    assert state['walks'] == [True]
