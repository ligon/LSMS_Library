"""Source-observation preservation; historical private cohorts are not replayed."""
import importlib.util
from pathlib import Path

import pandas as pd
import pytest

from lsms_library.paths import countries_root


@pytest.fixture(scope='module')
def mw():
    spec = importlib.util.spec_from_file_location('malawi_1298_bounded',
        countries_root() / 'Malawi/_/malawi.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def seed(mw, codes=(1, 1), plots=('p', 'p'), quantities=(2, 3), half='single', positions=(4, 9)):
    raw = pd.DataFrame({'hhid': ['h'] * len(codes), 'plotkey': plots,
                        'code': codes, 'q': quantities, 'u': [1] * len(codes)})
    return mw._seed_block(raw, hhid='hhid', plotkey='plotkey', cropcode='code',
        qty='q', unit='u', source_ref='Malawi/release@dvc:hash',
        source_half=half, row_positions=positions)


def purchase(mw, code=1, half='single'):
    raw = pd.DataFrame({'hhid': ['h'], 'code': [code], 'q': [7], 'v': [5]})
    return mw._seed_purchase_block(raw, hhid='hhid', cropcode='code',
        qty_cols=['q'], unit_cols=['u'], value_cols=['v'], source_half=half)


def test_two_original_records_are_not_one_plot(mw):
    result = mw.assemble_plot_inputs('2013-14', [seed(mw)], [purchase(mw)])
    assert len(result) == 2
    assert sorted(result.Quantity) == [2, 3]
    assert result.Quantity_purchased.isna().all()
    assert set(result.index.get_level_values('input_obs')) == {
        'row:Malawi/release@dvc:hash:4', 'row:Malawi/release@dvc:hash:9'}


def test_unknown_plot_all_na_sibling_vetoes_ownership(mw):
    g = seed(mw, plots=('p', None), quantities=(2, None))
    result = mw.assemble_plot_inputs('2013-14', [g], [purchase(mw)])
    assert len(result) == 2
    assert result.Quantity_purchased.isna().all()
    # Each source observation preserves its own quantity and missingness.
    assert result.Quantity.isna().sum() == 1
    assert result.Quantity.dropna().tolist() == [2]


def test_native_code_not_shared_label_and_source_half(mw):
    g = seed(mw, codes=(1, 2))  # two maize varieties, one public label
    result = mw.assemble_plot_inputs('2013-14', [g], [purchase(mw, code=2)])
    assert result.Quantity_purchased.notna().sum() == 1
    attached = result[result.Quantity_purchased.notna()]
    assert attached.Quantity.iloc[0] == 3
    assert attached.Quantity_purchased.iloc[0] == 7
    other_half = mw.assemble_plot_inputs('2013-14', [g], [purchase(mw, code=2, half='panel')])
    assert other_half.Quantity_purchased.isna().all()


def test_missing_crop_retains_original_position(mw):
    g = seed(mw, codes=(None, 1))
    assert g.input_obs.tolist() == ['row:Malawi/release@dvc:hash:4',
                                  'row:Malawi/release@dvc:hash:9']
    assert g.crop.iloc[0] == 'unknown_crop'


@pytest.mark.parametrize('positions', [(0, 0), (-1, 2), (True, 2), (0,)])
def test_invalid_positions_refuse(mw, positions):
    with pytest.raises(ValueError):
        seed(mw, positions=positions)


def test_duplicate_source_identity_across_pieces_refuses(mw):
    g = seed(mw)
    with pytest.raises(ValueError, match='repeated identity'):
        mw.assemble_plot_inputs('2013-14', [g, g])


def test_source_reference_uses_declared_release(mw):
    path = countries_root() / 'Malawi/2013-14/Data/AG_MOD_G_13.dta'
    ref = mw._seed_source_reference(path)
    assert ref.startswith('Malawi/2013-14/Data/AG_MOD_G_13.dta@dvc:AG_MOD_G_13.dta:')
    with pytest.raises(ValueError, match='sidecar'):
        mw._seed_source_reference(path.with_name('absent.dta'))


def test_feature_preserves_observation_and_legacy_sentinel(mw):
    from lsms_library.feature import (_harmonize_country_frame,
        _canonical_index_levels, _missing_level_sentinels, _level_aliases)
    g = mw.assemble_plot_inputs('2013-14', [seed(mw)])
    flat = g.reset_index(); flat['v'] = 'cluster'; flat['season'] = 'unknown_season'
    frame = flat.set_index(list(g.index.names) + ['v', 'season'])
    result = _harmonize_country_frame(frame, _canonical_index_levels('plot_inputs'),
        'Malawi', 'plot_inputs', aliases=_level_aliases('plot_inputs'),
        sentinels=_missing_level_sentinels('plot_inputs'))
    assert len(result) == 2
    assert set(result.index.get_level_values('input_obs')) == set(g.index.get_level_values('input_obs'))
    legacy = frame.iloc[:1].droplevel('input_obs')
    aligned = _harmonize_country_frame(legacy, _canonical_index_levels('plot_inputs'),
        'Tanzania', 'plot_inputs', aliases=_level_aliases('plot_inputs'),
        sentinels=_missing_level_sentinels('plot_inputs'))
    assert aligned.index.get_level_values('input_obs').tolist() == ['unresolved_observation']
    assert aligned.Quantity.iloc[0] == legacy.Quantity.iloc[0]


@pytest.mark.parametrize('plot', [None, 'unknown_plot'])
def test_single_unknown_plot_never_owns_purchase(mw, plot):
    g = seed(mw, codes=(1,), plots=(plot,), quantities=(2,), positions=(7,))
    result = mw.assemble_plot_inputs('2019-20', [g], [purchase(mw)])
    assert result.Quantity_purchased.isna().all()
    assert result.index.get_level_values('plot_id').tolist() == ['unknown_plot']


def test_unmapped_and_fractional_crop_codes_pass_through(mw):
    g = seed(mw, codes=(999, 1.5))
    assert g.crop.tolist() == ['999', '1.5']
    result = mw.assemble_plot_inputs('2019-20', [g], [purchase(mw, code=1.5)])
    assert result.Quantity_purchased.notna().sum() == 1
    assert result.loc[result.Quantity_purchased.notna()].index.get_level_values('crop').tolist() == ['1.5']


def test_unmapped_label_and_absent_crop_are_distinct(mw):
    g = seed(mw, codes=('unlisted crop', None))
    assert g.crop.tolist() == ['unlisted crop', 'unknown_crop']


def test_module_d_marker_and_reducer_remain_separate(mw):
    d = pd.DataFrame({'i':['h','h'], 'plot_id':['p','p'],
        '_input_code':[mw._MWI_INPUT_ORGANIC]*2, 'crop':[pd.NA]*2,
        'Quantity':[pd.NA]*2, 'u':[pd.NA]*2, 'Improved':[pd.NA]*2,
        'Applied':[True]*2})
    result = mw.assemble_plot_inputs('2019-20', [d]).reset_index()
    assert len(result) == 1
    assert result.crop.tolist() == ['(not crop-specific)']
    assert result.input_obs.tolist() == ['unresolved_observation']
    assert result.Quantity.isna().all()


def test_original_positions_survive_complete_record_repeat_policy(mw):
    raw = pd.DataFrame({'hhid':['h']*3, 'plotkey':['p']*3,
        'code':[1]*3, 'q':[2]*3, 'u':[1]*3, 'other':[0,0,1]}, index=[9,9,9])
    positions = [n for n, repeated in enumerate(raw.duplicated()) if not repeated]
    retained = mw.drop_repeated_records(raw)
    g = mw._seed_block(retained, hhid='hhid', plotkey='plotkey', cropcode='code',
        qty='q', unit='u', source_ref='Malawi/release@dvc:hash',
        source_half='Full_Sample', row_positions=positions)
    assert g.input_obs.tolist() == ['row:Malawi/release@dvc:hash:0',
                                  'row:Malawi/release@dvc:hash:2']


@pytest.mark.parametrize('wave', ['2016-17', '2019-20'])
def test_partial_garden_never_manufactures_plot(wave):
    import ast
    from lsms_library.local_tools import format_id
    source = (countries_root() / 'Malawi' / wave / '_' / 'plot_inputs.py').read_text()
    tree = ast.parse(source)
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_plotkey')
    namespace = {'format_id': format_id}
    exec(compile(ast.Module(body=[function], type_ignores=[]), '<plotkey>', 'exec'), namespace)
    frame = pd.DataFrame({'gardenid':['g', None, 'g'], 'plotid':[None, 'p', 'p']})
    got = namespace['_plotkey'](frame)
    assert got.isna().tolist() == [True, True, False]
    assert got.iloc[2] == 'g_p'


def test_recorded_zero_is_not_missing_crop(mw):
    g = seed(mw, codes=(0,), plots=('p',), quantities=(None,), positions=(0,))
    result = mw.assemble_plot_inputs('2019-20', [g])
    assert result.index.get_level_values('crop').tolist() == ['0']
    assert result.Quantity.isna().all()
