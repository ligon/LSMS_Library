"""Ethiopia's country food selector: actual discovery, invented IO only.

Ledger: .coder/ledger/1287-ethiopia-food-country-selector.md §§2–4.
No raw data, crosswalk JSON, decoder, Make, cache or API table builds.
The public country helper and compiler's complete postselection tail run;
only their reader, writer and org-table input leaves are invented.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'lsms_library/countries/Ethiopia/_/food_acquired.py'
HELPER = SCRIPT.with_name('ethiopia.py')
WAVES = ('2011-12', '2013-14', '2015-16', '2018-19', '2021-22')
DECLARED = ('native', 'yaml', 'null', 'make', 'both')
AUTHORITIES = ('module_waves', 'module_Waves', 'yaml', 'SOURCE.org', 'SOURCE')


def _frame(wave, *, empty=False, one_column=False):
    # Deliberately unsorted, repeated food labels after rename, nullable
    # payloads, explicit zero, and unknown/NA units. No survey-derived values.
    idx = pd.MultiIndex.from_tuples([
        (wave, 'hh-b', 'raw-b', 'Unknown', 'inkind'),
        (wave, 'hh-a', 'raw-a', pd.NA, 'produced'),
        (wave, 'hh-a', 'raw-b', 'Kg', 'purchased'),
    ], names=['t', 'i', 'j', 'u', 's'])
    out = pd.DataFrame({
        'Quantity': pd.array([7.25, pd.NA, 0], dtype='Float64'),
        'Expenditure': pd.array([pd.NA, pd.NA, 3], dtype='Int64'),
    }, index=idx)
    out.attrs = {'invented': {'recall': 'unchanged', 'sources': ['p', 'pr', 'ik']}}
    if one_column:
        out = out[['Quantity']]
    return out.iloc[:0] if empty else out


def _equal(actual, expected):
    if isinstance(expected, pd.Series):
        pd.testing.assert_series_equal(actual, expected, check_exact=True)
        columns = [(actual, expected)]
    else:
        pd.testing.assert_frame_equal(actual, expected, check_exact=True)
        columns = [(actual[c], expected[c]) for c in expected]
    # GH #1206: frame/series equality alone can miss nullable numeric changes.
    for x, y in columns:
        pd.testing.assert_extension_array_equal(x.array, y.array, check_exact=True,
                                                rtol=0, atol=0, check_dtype=True)
        np.testing.assert_array_equal(x[x.notna()].to_numpy(),
                                      y[y.notna()].to_numpy(), strict=True)
    assert actual.attrs == expected.attrs


def _renamed(frame, *, converted=True):
    # Both codes share a label intentionally; renaming must never aggregate.
    out = frame.rename(index={'raw-a': 'Food', 'raw-b': 'Food'}, level='j')
    if converted:
        out.attrs['id_converted'] = True
    return out


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    import lsms_library as ll
    from lsms_library import local_tools
    from lsms_library.paths import countries_root, data_root
    C = importlib.import_module('lsms_library.country')

    assert Path(ll.__file__).resolve().is_relative_to(ROOT)
    assert Path(C.__file__).resolve() == ROOT / 'lsms_library/country.py'
    private = tmp_path / 'metadata'
    country_dir = private / 'Ethiopia'
    common = country_dir / '_'
    common.mkdir(parents=True)
    common.joinpath('data_scheme.yml').write_text(
        'Country: Ethiopia\nData Scheme:\n  food_acquired: !make\n', encoding='utf-8')
    monkeypatch.setenv('LSMS_COUNTRIES_ROOT', str(private))
    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path / 'unused-data'))
    monkeypatch.setenv('LSMS_SKIP_AUTH', '1')
    countries_root.cache_clear()
    data_root.cache_clear()
    assert countries_root().resolve() == private.resolve()
    assert data_root().resolve() == (tmp_path / 'unused-data').resolve()

    reads, writes, covers, label_calls, denials = [], [], [], [], []
    frames, maps = {}, {}

    def denied(*args, **kwargs):
        denials.append('real-data/build leaf')
        raise AssertionError('real-data/build leaf is forbidden in selector tests')

    for cls, names in ((C.Country, ('_aggregate_wave_data',
                                  '_read_l2_country_cache', '_read_assumed_fresh_cache')),
                       (C.Wave, ('grab_data',)), (ll.Feature, ('__call__',))):
        for name in names:
            monkeypatch.setattr(cls, name, denied)
    for module, names in ((C, ('get_dataframe', 'df_data_grabber', 'to_parquet')),
                          (local_tools, ('df_data_grabber',))):
        for name in names:
            monkeypatch.setattr(module, name, denied)
    for name in ('read_parquet', 'read_stata', 'read_csv', 'read_spss'):
        monkeypatch.setattr(pd, name, denied)
    monkeypatch.setattr(pd.DataFrame, 'to_parquet', denied)

    def map_leaf(self):
        # The actual Country property is exercised, with only its committed
        # map input leaf invented. No crosswalk file/build may be consulted.
        self._panel_ids_attempted = True
        self._panel_ids_cache = {}
        self._updated_ids_cache = maps

    monkeypatch.setattr(C.Country, '_compute_panel_ids', map_leaf)

    def reader(path, *args, **kwargs):
        assert not args and not kwargs
        reads.append(str(path))
        value = frames[str(path)]
        if isinstance(value, BaseException):
            raise value
        return value.copy(deep=True)

    def writer(frame, path, *args, **kwargs):
        assert not args and not kwargs
        writes.append((frame.copy(deep=True), str(path)))

    monkeypatch.setattr(local_tools, 'get_dataframe', reader)
    monkeypatch.setattr(local_tools, 'to_parquet', writer)
    spec = importlib.util.spec_from_file_location('_gh1287_eth_food_helper', HELPER)
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    assert tuple(helper.Waves) == WAVES

    def cover_reader(path):
        covers.append(path)
        # A source-only invented table, not a real crosswalk or a JSON file.
        return pd.DataFrame({'household_id2': ['unmatched'],
                             'household_id': ['unmatched-before']})

    def label_reader(path):
        label_calls.append(path)
        return pd.DataFrame({**{w: ['raw-a', 'raw-b'] for w in WAVES},
                             'Preferred Label': ['Food', 'Food']})

    monkeypatch.setattr(helper, 'get_dataframe', cover_reader)
    monkeypatch.setattr(helper, '_read_food_label_table', label_reader)
    monkeypatch.setitem(sys.modules, 'ethiopia', helper)
    # The public compiler appends a relative sys.path entry; contain it.
    monkeypatch.setattr(sys, 'path', list(sys.path))
    monkeypatch.chdir(common)

    def install(waves, declarations, *, authority='module_waves', folders=None):
        folders = folders or {}
        if authority.startswith('module_'):
            assignment = (f'waves = {list(waves)!r}\n' if authority == 'module_waves'
                          else f'Waves = {dict.fromkeys(waves, ())!r}\n')
            common.joinpath('ethiopia.py').write_text(
                assignment + f'wave_folder_map = {folders!r}\n', encoding='utf-8')
        elif authority == 'yaml':
            import yaml
            common.joinpath('data_scheme.yml').write_text(yaml.safe_dump({
                'Country': 'Ethiopia', 'Waves': list(waves),
                'wave_folder_map': folders, 'Data Scheme': {'food_acquired': None},
            }), encoding='utf-8')
        else:
            assert not folders
            for w in waves:
                doc = country_dir / w / 'Documentation'
                doc.mkdir(parents=True)
                doc.joinpath(authority).write_text('Invented discovery marker\n', encoding='utf-8')
        for folder, mode in declarations.items():
            if mode == 'missing':
                continue
            config = country_dir / folder / '_'
            config.mkdir(parents=True, exist_ok=True)
            if mode in ('native', 'both'):
                # Discovery must not execute the native builder.
                config.joinpath('food_acquired.py').write_text(
                    "raise AssertionError('native builder must not execute')\n", encoding='utf-8')
            if mode in ('yaml', 'null', 'make', 'both', 'unrelated'):
                body = {'yaml': 'food_acquired: {file: invented-not-readable.dta}',
                        'null': 'food_acquired: null',
                        'make': 'food_acquired: !make',
                        'both': 'food_acquired: null',
                        'unrelated': 'unrelated: null'}[mode]
                config.joinpath('data_info.yml').write_text(body + '\n', encoding='utf-8')
            if mode == 'empty':
                # A formatting module named for the folder is not a table.
                config.joinpath(f'{folder}.py').write_text('# formatting only\n', encoding='utf-8')
        return ll.Country('Ethiopia', preload_panel_ids=False)

    def run():
        namespace = {'__name__': '__main__', '__file__': str(SCRIPT)}
        exec(compile(SCRIPT.read_text(encoding='utf-8'), str(SCRIPT), 'exec'), namespace)
        return namespace

    yield SimpleNamespace(install=install, run=run, frames=frames, reads=reads,
                          writes=writes, covers=covers, label_calls=label_calls,
                          helper=helper, country_dir=country_dir, maps=maps,
                          Country=C.Country, local_tools=local_tools)
    assert not denials
    countries_root.cache_clear()
    data_root.cache_clear()


@pytest.mark.parametrize('authority', AUTHORITIES)
@pytest.mark.parametrize('declaration', DECLARED)
def test_food_selector_actual_discovery(sandbox, authority, declaration):
    wave = WAVES[0]
    country = sandbox.install([wave], {wave: declaration}, authority=authority)
    assert country.waves == [wave]
    assert 'food_acquired' in country[wave].data_scheme
    source = _frame(wave)
    before = source.copy(deep=True)
    path = f'../{wave}/_/food_acquired.parquet'
    sandbox.frames[path] = source
    sandbox.run()
    assert sandbox.reads == [path]
    assert len(sandbox.writes) == 1
    frame, target = sandbox.writes[0]
    assert target == '../var/food_acquired.parquet'
    _equal(frame, _renamed(source))
    _equal(source, before)
    assert len(frame) == 3 and frame['Expenditure'].isna().sum() == 2
    assert len(sandbox.label_calls) == len(WAVES)


@pytest.mark.parametrize('retired_mode', ('native', 'yaml', 'null', 'both'))
def test_food_selector_skips_retired_despite_invented_stale_output(sandbox, retired_mode):
    retired, active = WAVES[:2]
    country = sandbox.install([retired, active], {retired: retired_mode, active: 'native'})
    assert 'food_acquired' in country[retired].data_scheme
    config = sandbox.country_dir / retired / '_'
    for name in ('food_acquired.py', 'data_info.yml'):
        if config.joinpath(name).exists():
            config.joinpath(name).unlink()
    assert 'food_acquired' not in country[retired].data_scheme
    # The invented reader would successfully supply a stale frame if asked.
    sandbox.frames[f'../{retired}/_/food_acquired.parquet'] = _frame(retired)
    sandbox.frames[f'../{active}/_/food_acquired.parquet'] = _frame(active)
    sandbox.run()
    assert sandbox.reads == [f'../{active}/_/food_acquired.parquet']
    _equal(sandbox.writes[0][0], _renamed(_frame(active)))
    # Shared committed maps replace the obsolete raw-cover/j-axis loop.
    assert sandbox.covers == []


@pytest.mark.parametrize('unwired', ('missing', 'empty', 'unrelated'))
def test_food_selector_unwired_contributions_are_not_read(sandbox, unwired):
    retired, active = WAVES[:2]
    sandbox.install([retired, active], {retired: unwired, active: 'null'})
    sandbox.frames[f'../{active}/_/food_acquired.parquet'] = _frame(active)
    sandbox.run()
    assert sandbox.reads == [f'../{active}/_/food_acquired.parquet']
    _equal(sandbox.writes[0][0], _renamed(_frame(active)))


@pytest.mark.parametrize('authority', ('module_waves', 'module_Waves', 'yaml'))
def test_food_selector_physical_alias_read_once_without_t_filter(sandbox, authority):
    first, second = WAVES[:2]
    folder = 'invented-shared-rounds'
    country = sandbox.install([second, first], {folder: 'null'}, authority=authority,
                              folders={first: folder, second: folder})
    assert country.waves == [first, second]
    assert country[first].wave_folder == country[second].wave_folder == folder
    source = pd.concat([_frame(second), _frame(first)])
    sandbox.frames[f'../{folder}/_/food_acquired.parquet'] = source
    sandbox.run()
    assert sandbox.reads == [f'../{folder}/_/food_acquired.parquet']
    assert sandbox.covers == []  # local walker receives first logical wave
    _equal(sandbox.writes[0][0], _renamed(source))


@pytest.mark.parametrize('bad_index', (0, 1, 2))
@pytest.mark.parametrize('error_type', (FileNotFoundError, PermissionError,
                                       OSError, KeyError, ValueError, RuntimeError))
def test_food_selector_declared_failure_chains_before_writer(sandbox, bad_index, error_type):
    selected = WAVES[:3]
    folders = {w: f'physical-{n}' for n, w in enumerate(selected)}
    sandbox.install(selected, {folders[w]: mode for w, mode in zip(selected, DECLARED)},
                    folders=folders)
    paths = [f'../{folders[w]}/_/food_acquired.parquet' for w in selected]
    sandbox.frames.update({p: _frame(w) for w, p in zip(selected, paths)})
    failure = error_type('invented declared-read failure')
    sandbox.frames[paths[bad_index]] = failure
    with pytest.raises(RuntimeError) as caught:
        sandbox.run()
    assert caught.value.__cause__ is failure
    for context in ('Ethiopia', 'food_acquired', selected[bad_index],
                    folders[selected[bad_index]], paths[bad_index]):
        assert context in str(caught.value)
    assert sandbox.reads == paths[:bad_index + 1]
    assert sandbox.writes == [] and sandbox.label_calls == []


@pytest.mark.parametrize('error_type', (KeyboardInterrupt, SystemExit))
def test_food_selector_process_control_is_not_wrapped(sandbox, error_type):
    wave = WAVES[0]
    sandbox.install([wave], {wave: 'native'})
    failure = error_type('invented stop')
    sandbox.frames[f'../{wave}/_/food_acquired.parquet'] = failure
    with pytest.raises(error_type) as caught:
        sandbox.run()
    assert caught.value is failure and caught.value.__cause__ is None
    assert sandbox.writes == []


def test_food_selector_country_authority_excludes_legacy_dictionary_and_stray_dir(sandbox):
    wave = WAVES[0]
    # Neither a legacy helper Waves entry nor a configured but unlisted folder
    # can add a contribution to the authoritative country list.
    sandbox.install([wave], {wave: 'native', WAVES[1]: 'native', 'stray': 'native'})
    sandbox.frames[f'../{wave}/_/food_acquired.parquet'] = _frame(wave)
    sandbox.run()
    assert sandbox.reads == [f'../{wave}/_/food_acquired.parquet']
    _equal(sandbox.writes[0][0], _renamed(_frame(wave)))


@pytest.mark.parametrize('one_column', (False, True))
@pytest.mark.parametrize('empty_first,empty_second', ((True, True), (True, False), (False, True)))
def test_food_selector_empty_and_squeezed_contributions(sandbox, one_column, empty_first, empty_second):
    first, second = WAVES[:2]
    sandbox.install([second, first], {first: 'native', second: 'yaml'})
    frames = [_frame(w, empty=empty, one_column=one_column)
              for w, empty in zip((first, second), (empty_first, empty_second))]
    sandbox.frames.update({f'../{w}/_/food_acquired.parquet': f
                           for w, f in zip((first, second), frames)})
    sandbox.run()
    assert sandbox.reads == [f'../{w}/_/food_acquired.parquet' for w in (first, second)]
    # Preserve the existing squeeze: a one-column contribution is a Series.
    expected = pd.concat([f.squeeze() for f in frames])
    _equal(sandbox.writes[0][0], _renamed(expected))


def test_food_selector_preserves_public_single_row_squeeze_failure(sandbox):
    wave = WAVES[0]
    sandbox.install([wave], {wave: 'native'})
    sandbox.frames[f'../{wave}/_/food_acquired.parquet'] = _frame(wave).iloc[:1]
    # The unchanged squeeze produces a Series without a j index. Removing
    # the obsolete local walker does not authorize emitting that malformed
    # single-row contribution or changing the writer contract.
    with pytest.raises(KeyError, match='j') as caught:
        sandbox.run()
    assert 'j' in str(caught.value)
    assert sandbox.writes == []


def test_food_selector_preserves_public_no_contribution_failure(sandbox):
    sandbox.install([WAVES[0]], {WAVES[0]: 'unrelated'})
    with pytest.raises(ValueError, match='No objects to concatenate'):
        sandbox.run()
    assert sandbox.reads == sandbox.writes == sandbox.label_calls == []


def test_food_selector_shared_walk_changes_household_i_and_keeps_food_j(sandbox):
    wave = WAVES[1]
    sandbox.install([wave], {wave: 'native'})
    source = _frame(wave)
    sandbox.frames[f'../{wave}/_/food_acquired.parquet'] = source
    sandbox.maps[wave] = {'hh-a': 'earlier-a', 'hh-b': 'earlier-b',
                          'raw-a': 'wrong-food-axis', 'raw-b': 'wrong-food-axis'}
    sandbox.run()
    expected = source.rename(index={'hh-a': 'earlier-a', 'hh-b': 'earlier-b'}, level='i')
    _equal(sandbox.writes[0][0], _renamed(expected))
    assert sandbox.writes[0][0].index.get_level_values('j').tolist() == ['Food'] * 3
    assert sandbox.writes[0][0].attrs['id_converted'] is True
    assert sandbox.covers == []


def test_food_selector_walk_leaf_failure_stays_public_and_prevents_write(sandbox, monkeypatch):
    wave = WAVES[1]
    sandbox.install([wave], {wave: 'native'})
    sandbox.frames[f'../{wave}/_/food_acquired.parquet'] = _frame(wave)
    failure = ValueError('invented shared map refusal')

    def refused(*args, **kwargs):
        raise failure

    monkeypatch.setattr(sandbox.local_tools, 'id_walk', refused)
    with pytest.raises(ValueError) as caught:
        sandbox.run()
    assert caught.value is failure and caught.value.__cause__ is None
    assert sandbox.writes == sandbox.label_calls == []


def test_food_selector_raw_chain_is_one_substitution(sandbox):
    wave = WAVES[1]
    sandbox.install([wave], {wave: 'native'})
    source = _frame(wave)
    sandbox.frames[f'../{wave}/_/food_acquired.parquet'] = source
    sandbox.maps[wave] = {'hh-a': 'hh-b', 'hh-b': 'hh-c'}
    sandbox.run()
    expected = source.rename(index={'hh-a': 'hh-b', 'hh-b': 'hh-c'}, level='i')
    _equal(sandbox.writes[0][0], _renamed(expected))
    assert sandbox.writes[0][0].index.get_level_values('i').tolist() == ['hh-c', 'hh-b', 'hh-b']


def test_food_selector_converted_frame_is_not_walked_again(sandbox):
    wave = WAVES[1]
    sandbox.install([wave], {wave: 'native'})
    source = _frame(wave)
    source.attrs['id_converted'] = True
    sandbox.frames[f'../{wave}/_/food_acquired.parquet'] = source
    sandbox.maps[wave] = {'hh-a': 'hh-b', 'hh-b': 'hh-c'}
    sandbox.run()
    _equal(sandbox.writes[0][0], _renamed(source))


def test_food_selector_future_registered_wave_uses_shared_map(sandbox):
    wave = '2030-31'
    sandbox.install([wave], {wave: 'null'})
    source = _frame(wave)
    sandbox.frames[f'../{wave}/_/food_acquired.parquet'] = source
    sandbox.maps[wave] = {'hh-a': 'future-canonical-a', 'hh-b': 'future-canonical-b'}
    sandbox.run()
    expected = source.rename(index=sandbox.maps[wave], level='i')
    _equal(sandbox.writes[0][0], _renamed(expected))
    assert sandbox.covers == []


def test_food_selector_map_load_failure_prevents_payload_or_writer(sandbox, monkeypatch):
    sandbox.install([WAVES[0]], {WAVES[0]: 'native'})
    failure = ValueError('invented committed-map refusal')
    def refused(self):
        raise failure
    monkeypatch.setattr(sandbox.Country, '_compute_panel_ids', refused)
    with pytest.raises(ValueError) as caught:
        sandbox.run()
    assert caught.value is failure
    assert sandbox.reads == sandbox.writes == sandbox.covers == []
