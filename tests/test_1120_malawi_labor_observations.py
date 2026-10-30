"""Invented labor operands only; no historical raw/API cohort is replayed."""
import ast
import hashlib
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root


@pytest.fixture(scope='module')
def mw():
    spec = importlib.util.spec_from_file_location(
        'malawi_1120_labor', countries_root() / 'Malawi/_/malawi.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def block(mw, raw, *, ref='Malawi/release@dvc:source', positions=None,
          suffix='', other=True):
    if positions is None:
        positions = np.arange(len(raw), dtype=np.int64)
    return mw._plot_labor_block(raw, hhid='hh', plotkey='plot', t='2010-11',
        hired_suffix=suffix, include_other=other,
        source_ref=ref, row_positions=positions)


def numbers(actual, expected):
    expected = np.asarray(expected, dtype=np.float64)
    assert actual.dtype == expected.dtype
    np.testing.assert_array_equal(actual.isna().to_numpy(), np.isnan(expected))
    present = ~np.isnan(expected)
    np.testing.assert_array_equal(actual.to_numpy()[present], expected[present], strict=True)


def test_missing_observations_keep_original_positions_values_and_masks(mw):
    raw = pd.DataFrame({'hh': ['h'] * 4, 'plot': [None] * 4,
        'ag_d47a': [2., np.nan, 0., 0.], 'ag_d47b': [7., 0., 0., np.nan]},
        index=[90, 90, 2, 1])
    before = raw.copy(deep=True)
    out = mw.assemble_plot_labor('2010-11', [block(mw, raw, positions=[7, 11, 20, 25])])
    assert out.index.get_level_values('plot_id').tolist() == ['unknown_plot'] * 3
    assert out.index.get_level_values('labor_obs').tolist() == [
        'row:Malawi/release@dvc:source:7', 'row:Malawi/release@dvc:source:11',
        'row:Malawi/release@dvc:source:20']
    numbers(out.PersonDays, [2., np.nan, 0.])
    numbers(out.Wage, [7., 0., 0.])
    pd.testing.assert_frame_equal(raw, before, check_exact=True)


def test_known_plots_keep_existing_sum_and_first_nonmissing_wage(mw):
    raw = pd.DataFrame({'hh': ['h', 'h'], 'plot': ['p', 'p'],
        'ag_d42b': [2., 1.], 'ag_d42c': [3., 4.],
        'ag_d47a': [1., 2.], 'ag_d47b': [np.nan, 9.]})
    out = mw.assemble_plot_labor('2010-11', [block(mw, raw, ref=None)])
    assert out.index.tolist() == [
        ('2010-11', 'h', 'p', 'family', 'unresolved_observation'),
        ('2010-11', 'h', 'p', 'hired', 'unresolved_observation')]
    numbers(out.PersonDays, [10., 3.])
    numbers(out.Wage, [np.nan, 9.])


def test_known_wage_only_legacy_sum_is_not_changed(mw):
    raw = pd.DataFrame({'hh': ['h'], 'plot': ['p'], 'ag_d47b': [0.]})
    out = mw.assemble_plot_labor('2010-11', [block(mw, raw, ref=None)])
    numbers(out.PersonDays, [0.])  # legacy known-plot sum, not the source-observation boundary
    numbers(out.Wage, [0.])


def test_all_blank_unknown_stubs_need_no_fabricated_authority(mw):
    raw = pd.DataFrame({'hh': ['h'], 'plot': [None], 'ag_d47a': [0.]})
    out = mw._plot_labor_block(raw, hhid='hh', plotkey='plot', t='2010-11',
                              hired_suffix='')
    assert out.empty


@pytest.mark.parametrize('positions', [[1, 1], [-1, 2], [None, 2], [True, 2], [1.5, 2], [1]])
def test_missing_observation_refuses_invalid_original_positions(mw, positions):
    raw = pd.DataFrame({'hh': ['h', 'h'], 'plot': [None, None], 'ag_d47a': [1., 2.]})
    with pytest.raises((ValueError, TypeError)):
        block(mw, raw, positions=positions)


def test_missing_observation_requires_authority_and_refuses_duplicate_identity(mw):
    raw = pd.DataFrame({'hh': ['h'], 'plot': [None], 'ag_d47a': [1.]})
    with pytest.raises(ValueError, match='source and original positions'):
        block(mw, raw, ref=None)
    piece = block(mw, raw)
    with pytest.raises(ValueError, match='not unique'):
        mw.assemble_plot_labor('2010-11', [piece, piece.copy()])
    piece['labor_obs'] = 'unresolved_observation'
    with pytest.raises(ValueError, match='require source observations'):
        mw.assemble_plot_labor('2010-11', [piece])


def test_source_halves_and_categories_do_not_borrow_wages(mw):
    raw = pd.DataFrame({'hh': ['h'], 'plot': [None],
                        'ag_d47a1': [2.], 'ag_d47b1': [5.], 'ag_d52a': [3.]})
    xs = block(mw, raw, ref='Malawi/CS@dvc:same-name', suffix='1/2/3', other=False)
    pn = block(mw, raw, ref='Malawi/Panel@dvc:same-name', suffix='1/2/3')
    out = mw.assemble_plot_labor('2010-11', [xs, pn])
    assert out.index.get_level_values('source').tolist() == ['hired', 'hired', 'other']
    assert len(set(out.index.get_level_values('labor_obs'))) == 2
    numbers(out.PersonDays, [2., 2., 3.])
    numbers(out.Wage, [5., 5., np.nan])


def test_tiny_reported_days_remain_exact_per_observation(mw):
    raw = pd.DataFrame({'hh': ['h', 'h'], 'plot': [None, None],
                        'ag_d47a': [1e-12, np.nextafter(1., 2.)]})
    out = mw.assemble_plot_labor('2010-11', [block(mw, raw)])
    numbers(out.PersonDays, [1e-12, np.nextafter(1., 2.)])


def test_sidecar_identity_reuses_driver_and_countries_relative_paths(mw, tmp_path, monkeypatch):
    from lsms_library import paths
    monkeypatch.setattr(paths, 'countries_root', lambda: tmp_path)
    driver = tmp_path / 'Malawi/2016-17/_/plot_labor.py'
    driver.parent.mkdir(parents=True)
    data = driver.parent.parent / 'Data/Cross_Sectional/ag_mod_d.dta'
    data.parent.mkdir(parents=True)
    content = b'outs:\n- md5: abcdef\n  path: ag_mod_d.dta\n'
    data.with_suffix('.dta.dvc').write_bytes(content)
    expected = ('Malawi/2016-17/Data/Cross_Sectional/ag_mod_d.dta@dvc:ag_mod_d.dta:'
                + hashlib.sha256(content).hexdigest())
    assert mw._labor_source_reference('../Data/Cross_Sectional/ag_mod_d.dta', script_path=driver) == expected
    assert mw._labor_source_reference('Malawi/2016-17/Data/Cross_Sectional/ag_mod_d.dta') == expected
    assert not data.exists()  # no payload exists or is used for fallback
    with pytest.raises(ValueError, match='sidecar'):
        mw._labor_source_reference('../Data/absent.dta', script_path=driver)


def test_all_six_driver_refs_equal_their_loader_literals_and_capture_before_fields():
    seen = 0
    for wave in ['2010-11', '2013-14', '2016-17', '2019-20']:
        tree = ast.parse((countries_root() / f'Malawi/{wave}/_/plot_labor.py').read_text())
        reads = {}
        positions = {}
        for n in tree.body:
            if not isinstance(n, ast.Assign) or not isinstance(n.targets[0], ast.Name):
                continue
            name = n.targets[0].id
            if isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Name) and n.value.func.id in ['get_dataframe', '_read_usecols']:
                reads[name] = n
            if name.endswith('_labor_positions'):
                positions[name] = n
        for n in ast.walk(tree):
            if not isinstance(n, ast.Call) or not isinstance(n.func, ast.Name) or n.func.id != '_plot_labor_block':
                continue
            raw = n.args[0].id
            kw = {v.arg: v.value for v in n.keywords}
            assert isinstance(kw['source_ref'], ast.Call)
            assert kw['source_ref'].func.id == '_labor_source_reference'
            assert ast.dump(kw['source_ref'].args[0]) == ast.dump(reads[raw].value.args[0])
            point = positions[kw['row_positions'].id]
            assert reads[raw].end_lineno < point.lineno < n.lineno
            assert ast.dump(point.value) == ast.dump(ast.parse(
                f'np.arange(len({raw}), dtype=np.int64)', mode='eval').body)
            # Capture precedes every caller-added household/plot field.
            fields = [a for a in tree.body if isinstance(a, ast.Assign)
                      and isinstance(a.targets[0], ast.Subscript)
                      and isinstance(a.targets[0].value, ast.Name) and a.targets[0].value.id == raw]
            assert fields and point.lineno < min(a.lineno for a in fields)
            seen += 1
    assert seen == 6


def test_actual_feature_alignment_retains_distinct_labor_observations(mw):
    from lsms_library.feature import (_canonical_index_levels,
        _harmonize_country_frame, _level_aliases, _missing_level_sentinels)
    raw = pd.DataFrame({'hh': ['h', 'h'], 'plot': [None, None],
                        'ag_d47a': [2., 5.], 'ag_d47b': [3., 7.]})
    native = mw.assemble_plot_labor('2010-11', [block(mw, raw)])
    observed = native.assign(v='invented-v').set_index('v', append=True)
    out = _harmonize_country_frame(observed, _canonical_index_levels('plot_labor'),
        'Malawi', 'plot_labor', False, aliases=_level_aliases('plot_labor'),
        sentinels=_missing_level_sentinels('plot_labor'))
    assert list(out.index.names) == _canonical_index_levels('plot_labor')
    assert len(out) == 2 and out.index.is_unique
    assert out.index.get_level_values('labor_obs').tolist() == native.index.get_level_values('labor_obs').tolist()
    assert out.index.get_level_values('worker').tolist() == ['Farm-level'] * 2
    assert out.index.get_level_values('stage').tolist() == ['Farm-level'] * 2
    assert out.index.get_level_values('season').tolist() == ['unknown_season'] * 2
    numbers(out.PersonDays, [2., 5.])
    numbers(out.Wage, [3., 7.])
