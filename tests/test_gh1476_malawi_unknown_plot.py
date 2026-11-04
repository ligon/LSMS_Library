"""GH #1476, Malawi: a blank plot key on a harvest row is ``unknown_plot``.

Modules G and P are asked per plot, so a row with a blank ``gardenid``/``plotid``
is a plot-grain answer with no identifier: the canonical EPISTEMIC sentinel, not a
NULL the first ``groupby`` deletes.  The sentinel does not say a plot exists (the 25
households behind today's rows report no tree plot; see ``Malawi/_/CONTENTS.org``).
``unknown_plot`` is not a plot, so no household-crop sale reaches such a row.

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


def _sale_frames(plots, units=None, sale_unit='Kilogramme'):
    """One household and crop: a harvest row per plot, and one sale."""
    units = units or ['Kilogramme'] * len(plots)
    harv = pd.DataFrame({
        'i': ['h'] * len(plots), 'plot_id': pd.array(plots, dtype='string'),
        'crop': ['Mango'] * len(plots), '_crop_code': [1005] * len(plots),
        'production_obs': [f'reported:P:{k}' for k in range(len(plots))],
        'Quantity': pd.array([3.0] * len(plots), dtype='Float64'),
        'u': pd.array(units, dtype='string'),
        'condition': pd.array(['unknown_condition'] * len(plots), dtype='string'),
    })
    sale = pd.DataFrame({'i': ['h'], '_crop_code': [1005],
                         'u': pd.array([sale_unit], dtype='string'),
                         'condition_sold': pd.array(['unknown_condition'], dtype='string'),
                         'Quantity_sold': [1.0], 'Value_sold': [100.0]})
    return harv, sale


def _sold(mw, *frames):
    out, _, _ = mw._attach_sales('2016-17', *frames)
    return out.loc[out['Value_sold'].notna(), 'plot_id'].tolist()


def test_a_sale_attaches_to_a_single_known_plot(mw):
    assert _sold(mw, *_sale_frames(['R1'])) == ['R1']


@pytest.mark.parametrize('case', [
    dict(plots=[SENTINEL]),
    dict(plots=[SENTINEL, SENTINEL], units=['Kilogramme', 'Bag']),
    # A known plot beside a sentinel: the crop is not single-plot, whichever row
    # shares the sale's unit, and the sale never lands on the sentinel.
    dict(plots=['R1', SENTINEL]),
    dict(plots=['R1', SENTINEL], units=['Bag', 'Kilogramme']),
    dict(plots=['R1', SENTINEL], units=['Kilogramme', 'Bag']),
], ids=['only', 'only-two-units', 'beside-a-plot-same-unit',
        'beside-a-plot-in-the-sentinels-unit', 'beside-a-plot-in-the-plots-unit'])
def test_no_sale_reaches_a_sentinel_row(mw, case):
    assert _sold(mw, *_sale_frames(**case)) == []


def test_a_null_key_is_no_more_a_plot_than_the_sentinel(mw):
    """`_harvest_block` no longer emits a null; a caller's frame may."""
    for plots in ([SENTINEL], ['R1', SENTINEL], ['R1', 'R2'], ['R1', 'R2', SENTINEL]):
        as_sentinel = _sold(mw, *_sale_frames(plots))
        as_null = _sold(mw, *_sale_frames([None if p == SENTINEL else p for p in plots]))
        assert as_sentinel == as_null == []


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
    assert len(unknown) == n > 0
