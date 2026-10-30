"""Invented operands for the settled native-code ruling; no raw/API execution.

The first five cases and their helpers are narrowly extracted from retained75041761's
mixed packet; D3 plot/input/labor cases and historical data cohorts are excluded.
"""
import importlib.util
import sys
import warnings

import numpy as np
import pandas as pd
import pytest
import yaml
from lsms_library.paths import countries_root

ROOT = countries_root() / 'Malawi'
WAVES = ['2010-11', '2013-14', '2016-17', '2019-20']
KG, S, UNK = 'Kilogramme', 'shelled', 'unknown_condition'

@pytest.fixture(scope='module')
def mw():
    """The Malawi country module, imported by path (it is not a package)."""
    path = ROOT / '_' / 'malawi.py'
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location('malawi_gh637a', path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        sys.path.remove(str(path.parent))
    return mod


def _scheme():
    with open(ROOT / '_' / 'data_scheme.yml') as f:
        return yaml.safe_load(f)['Data Scheme']


def test_crop_production_grain_carries_the_native_record():
    index = _scheme()['crop_production']['index']
    assert index == '(t, i, plot_id, crop, u, condition, production_obs)', index


def test_wave_scripts_assert_uniqueness_on_the_native_grain():
    for w in WAVES:
        src = (ROOT / w / '_' / 'crop_production.py').read_text()
        assert "'condition', 'production_obs']" in src, w


def _harvest_source(rows):
    """rows: (hhid, plotkey, raw crop code, qty, unit code, condition code)."""
    return pd.DataFrame({'hhid': [r[0] for r in rows], 'pk': [r[1] for r in rows],
                         'code': [r[2] for r in rows], 'q': [r[3] for r in rows],
                         'unit': [r[4] for r in rows], 'cond': [r[5] for r in rows]})


def _block(mw, rows, perennial=False):
    return mw._harvest_block(_harvest_source(rows), hhid='hhid', plotkey='pk',
                             cropcode='code', qty='q', unit='unit',
                             condition=None if perennial else 'cond',
                             perennial=perennial, t='2016-17')


def test_harvest_block_names_the_module_and_the_instruments_own_code(mw):
    g = _block(mw, [('h1', 'R1', 1, 5.0, 1, 1), ('h1', 'R1', 3, 2.0, 1, 1)])
    p = _block(mw, [('h1', 'R1', 100, 4.0, 1, None)], perennial=True)
    assert g['production_obs'].tolist() == ['reported:G:1', 'reported:G:3']
    # IHS4 Module P cassava: raw 100, recoded to the library's 1001 -- the
    # coordinate keeps what the instrument recorded
    assert p['production_obs'].tolist() == ['reported:P:100']
    assert int(p['_crop_code'].iloc[0]) == 1001


def _harvest(rows):
    """Consolidation input rows: (i, plot, crop, code, u, condition, qty)."""
    n = len(rows)
    return pd.DataFrame({
        't': ['2010-11'] * n,
        'i': [r[0] for r in rows], 'plot_id': [r[1] for r in rows],
        'crop': [r[2] for r in rows],
        '_crop_code': pd.array([r[3] for r in rows], dtype='Int64'),
        'production_obs': pd.array([f'reported:G:{r[3]}' for r in rows], dtype='string'),
        'u': pd.array([r[4] for r in rows], dtype='string'),
        'condition': pd.array([r[5] for r in rows], dtype='string'),
        'crop_variety': pd.array([f'v{r[3]}' for r in rows], dtype='string'),
        'Quantity': pd.array([r[6] for r in rows], dtype='Float64'),
        'planting_month': pd.array([11] * n, dtype='Int64'),
        'harvest_month': pd.array([6] * n, dtype='Int64'),
        'intercropped': pd.array([False] * n, dtype='boolean'),
        'perennial': pd.array([False] * n, dtype='boolean'),
    })


def _sales(rows):
    """Sale-block output rows: (i, code, u, condition_sold, qty, value)."""
    return pd.DataFrame({
        'i': [r[0] for r in rows],
        '_crop_code': pd.array([r[1] for r in rows], dtype='Int64'),
        'u': pd.array([r[2] for r in rows], dtype='string'),
        'condition_sold': pd.array([r[3] for r in rows], dtype='string'),
        'Quantity_sold': [r[4] for r in rows], 'Value_sold': [r[5] for r in rows]})


def _assemble(mw, harvest_rows, sale_rows):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return mw.assemble_crop_production(
            '2010-11', [_harvest(harvest_rows)],
            [_sales(sale_rows)] if sale_rows else []).reset_index()


def test_two_codes_of_one_label_keep_their_own_rows_varieties_and_sales(mw):
    """Maize LOCAL (1) and HYBRID (3) on one plot, each sold.  Before: one row,
    Quantity 14, variety v1, the code-1 sale only (code 3's 50 MWK dropped)."""
    out = _assemble(mw, [('h1', 'R1', 'Maize', 1, KG, S, 10.0),
                         ('h1', 'R1', 'Maize', 3, KG, S, 4.0)],
                    [('h1', 1, KG, S, 5.0, 100.0), ('h1', 3, KG, S, 2.0, 50.0)])
    n_rows = len(out)
    assert n_rows == 2, n_rows
    by = out.set_index('production_obs')
    assert by.loc['reported:G:1', 'Quantity'] == 10.0
    assert by.loc['reported:G:3', 'Quantity'] == 4.0
    assert by.loc['reported:G:1', 'crop_variety'] == 'v1'
    assert by.loc['reported:G:3', 'crop_variety'] == 'v3'
    assert by.loc['reported:G:1', 'Value_sold'] == 100.0
    assert by.loc['reported:G:3', 'Value_sold'] == 50.0


def test_a_repeated_native_coordinate_raises(mw):
    """Two rows that survive consolidation with one native coordinate (here:
    two library codes behind one instrument code) are a source defect."""
    rows = _harvest([('h1', 'R1', 'Maize', 1, KG, S, 10.0),
                     ('h1', 'R1', 'Maize', 2, KG, S, 4.0)])
    rows['production_obs'] = pd.array(['reported:G:1'] * 2, dtype='string')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        with pytest.raises(ValueError, match='native'):
            mw.assemble_crop_production('2010-11', [rows], [])


def test_same_code_keeps_existing_reported_sum_and_first_nonmissing_months(mw):
    raw = _harvest([('h', 'p', 'Maize', 1, KG, S, 2.),
                    ('h', 'p', 'Maize', 1, KG, S, None),
                    ('h', 'p', 'Maize', 1, KG, S, 3.)])
    raw['planting_month'] = pd.array([None, 8, 9], dtype='Int64')
    raw['harvest_month'] = pd.array([None, 4, 5], dtype='Int64')
    out = mw._consolidate_harvest([raw])
    assert len(out) == 1
    assert (float(out.Quantity.iloc[0]), int(out.planting_month.iloc[0]),
            int(out.harvest_month.iloc[0])) == (5., 8, 4)


def test_zero_blank_and_tiny_values_stay_with_native_code(mw):
    tiny = np.nextafter(1., 2.)
    out = _assemble(mw, [('h', 'p', 'Rice', 17, KG, S, None),
                         ('h', 'p', 'Rice', 18, KG, S, 0.),
                         ('h', 'p', 'Rice', 19, KG, S, tiny)], [])
    q = out.set_index('production_obs').Quantity
    np.testing.assert_array_equal(q.isna().to_numpy(), [True, False, False])
    np.testing.assert_array_equal(q.iloc[1:].to_numpy(dtype=np.float64),
                                  np.array([0., tiny]), strict=True)


def test_module_identity_is_formed_before_perennial_offset_and_recode(mw):
    g = _block(mw, [('h', 'p', 1., 2., 1, 1)])
    p = _block(mw, [('h', 'p', 1., 3., 1, None)], perennial=True)
    assert g.production_obs.tolist() == ['reported:G:1']
    assert p.production_obs.tolist() == ['reported:P:1']
    assert not set(g.production_obs) & set(p.production_obs)
    assert int(p._crop_code.iloc[0]) == 1001


def test_unmapped_crop_and_missing_plot_eligibility_are_unchanged(mw):
    # This unit does not import the precursor's unrelated D3 plot sentinel.
    out = _block(mw, [('h', None, 1, 2., 1, 1),
                      ('h', 'p', None, 3., 1, 1)])
    assert len(out) == 1 and pd.isna(out.plot_id.iloc[0])
    assert out.production_obs.iloc[0] == 'reported:G:1'


def test_feature_preserves_typed_payload_and_native_axis(mw):
    from lsms_library.feature import (_canonical_index_levels,
        _harmonize_country_frame, _level_aliases, _missing_level_sentinels)
    raw = mw.assemble_crop_production('2010-11', [_harvest([
        ('h', 'p', 'Maize', 1, KG, S, 2.),
        ('h', 'p', 'Maize', 3, KG, S, 5.)])], [])
    # The real framework promotes declared columns before Feature assembly.
    public = raw.assign(v='invented-v').set_index(
        ['u', 'condition', 'production_obs', 'v'], append=True)
    names = _canonical_index_levels('crop_production')
    out = _harmonize_country_frame(public, names, 'Malawi', 'crop_production',
        False, aliases=_level_aliases('crop_production'),
        sentinels=_missing_level_sentinels('crop_production'))
    assert list(out.index.names) == names and len(out) == 2
    assert out.index.get_level_values('production_obs').tolist() == [
        'reported:G:1', 'reported:G:3']
    assert out.Quantity.dtype == public.Quantity.dtype
    np.testing.assert_array_equal(out.Quantity.isna(), public.Quantity.isna())
    np.testing.assert_array_equal(out.Quantity.to_numpy(dtype=np.float64),
                                  np.array([2., 5.]), strict=True)
    assert out.crop_variety.tolist() == ['v1', 'v3']
