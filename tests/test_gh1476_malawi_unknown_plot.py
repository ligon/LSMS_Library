"""GH #1476, Malawi: a blank plot key on a harvest row is ``unknown_plot``.

Modules G and P are plot-grain, so a row with a blank ``gardenid``/``plotid`` has
a plot whose identifier is missing: the canonical EPISTEMIC sentinel, not a NULL
the first ``groupby`` deletes.  ``unknown_plot`` is not a plot, so it does not
make a crop "single-plot" and no household-crop sale attaches because of it.

The source oracle reads the module files through ``get_dataframe``.
"""
import importlib.util
import sys
import warnings

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import format_id, get_dataframe
from lsms_library.paths import countries_root

ROOT = countries_root() / 'Malawi'
SENTINEL = 'unknown_plot'


@pytest.fixture(scope='module')
def mw():
    path = ROOT / '_' / 'malawi.py'
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location('malawi_gh1476', path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        sys.path.remove(str(path.parent))
    return mod


def _block(mw, plotkeys):
    src = pd.DataFrame({'hhid': ['h'] * len(plotkeys), 'pk': plotkeys,
                        'code': [1.0] * len(plotkeys), 'q': [2.0] * len(plotkeys),
                        'unit': [1] * len(plotkeys), 'cond': [1] * len(plotkeys)})
    return mw._harvest_block(src, hhid='hhid', plotkey='pk', cropcode='code', qty='q',
                             unit='unit', condition=None, perennial=True, t='2016-17')


def test_a_blank_key_is_the_sentinel_and_a_recorded_key_is_kept(mw):
    got = _block(mw, ['R1', None, pd.NA])
    assert got['plot_id'].tolist() == ['R1', SENTINEL, SENTINEL]


def _sale_frames(mw, plots):
    harv = pd.DataFrame({
        'i': ['h'] * len(plots), 'plot_id': pd.array(plots, dtype='string'),
        'crop': ['Mango'] * len(plots), '_crop_code': [1005] * len(plots),
        'production_obs': [f'reported:P:{k}' for k in range(len(plots))],
        'Quantity': pd.array([3.0] * len(plots), dtype='Float64'),
        'u': pd.array(['Kilogramme'] * len(plots), dtype='string'),
        'condition': pd.array(['unknown_condition'] * len(plots), dtype='string'),
    })
    sale = pd.DataFrame({'i': ['h'], '_crop_code': [1005],
                         'u': pd.array(['Kilogramme'], dtype='string'),
                         'condition_sold': pd.array(['unknown_condition'], dtype='string'),
                         'Quantity_sold': [1.0], 'Value_sold': [100.0]})
    return harv, sale


def test_a_sale_does_not_attach_to_a_crop_whose_only_row_has_no_plot(mw):
    harv, sale = _sale_frames(mw, [SENTINEL])
    out, _, _ = mw._attach_sales('2016-17', harv, sale)
    assert out['Value_sold'].isna().all()


def test_the_null_and_the_sentinel_decide_the_same_sale(mw):
    """The counting rule is the one the null had: unknown_plot is skipped."""
    for plots in (['R1'], ['R1', SENTINEL], ['R1', 'R2'], ['R1', 'R2', SENTINEL]):
        as_sentinel, _, _ = mw._attach_sales('2016-17', *_sale_frames(mw, plots))
        nulls = [None if p == SENTINEL else p for p in plots]
        as_null, _, _ = mw._attach_sales('2016-17', *_sale_frames(mw, nulls))
        pd.testing.assert_series_equal(as_sentinel['Value_sold'], as_null['Value_sold'])


@pytest.mark.requires_s3
def test_served_sentinel_rows_are_the_source_blank_keys():
    """Module P 2016-17, both halves: blank key and a crop code recorded."""
    wave = ROOT / '2016-17' / 'Data'
    n = 0
    for rel, crop in ((('Cross_Sectional', 'ag_mod_p.dta'), 'ag_p0c'),
                      (('Panel', 'ag_mod_p_16.dta'), 'crop_code')):
        src = get_dataframe(str(wave / rel[0] / rel[1]), convert_categoricals=False)
        blank = src['gardenid'].map(format_id).isna() | src['plotid'].map(format_id).isna()
        n += int((blank & src[crop].notna()).sum())
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        cp = ll.Country('Malawi').crop_production()
    flat = cp.reset_index()
    assert not flat['plot_id'].isna().any()
    unknown = flat[flat['plot_id'].astype('string').eq(SENTINEL)]
    assert set(unknown['t'].astype(str)) == {'2016-17'}
    assert unknown['perennial'].astype('boolean').fillna(False).all()
    # Crop codes harmonize_crop does not name are dropped with their warning.
    assert 0 < len(unknown) <= n
