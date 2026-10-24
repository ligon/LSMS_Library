"""Malawi plot tables: native crop records, the missing-plot sentinel and the
"not crop-specific" marker (rulings of @ligon, 2026-10-23).

* **GH #637 defect A -- ``production_obs``.**  ``harmonize_crop`` lumps several
  crop codes under one ``crop`` label (Maize 1-4, the rices, groundnuts,
  citrus).  A collapse at the end of ``malawi.assemble_crop_production``
  summed those codes into one row (127 / 54 / 7 / 98 groups by wave), kept the
  first code's ``crop_variety`` (which keys the shipped kg factor) and the
  first code's sale, dropping 28,900 / 18,200 / 50,000 / 276,500 MWK of
  attached sales.  ``production_obs`` = ``reported:<G|P>:<instrument code>``
  (the #1198 precedent; Tanzania #1263) now keeps each code its own row, and a
  repeat at the full grain raises.
* **GH #1120 D3 -- ``unknown_plot``.**  A record kept at plot grain whose plot
  key is blank in the source (2010-11 Module D labour stubs: 40 rows in 32
  households; 2016-17 Module P harvest lines: 41) is served on the registered
  epistemic sentinel, never NaN.  It is not a plot: two of them are never
  summed (the assemblers raise), and a household-crop sale is never attached
  to one, nor to a known plot whose crop also has an unknown-plot line.
* **GH #1119 -- ``(not crop-specific)``.**  The ``crop`` (``j``) of every
  Module D input row -- fertilizer, organic, agrochemical, asked per PLOT with
  no crop item -- is the canonical marker, never NaN.  A Module G seed line is
  asked per plot-CROP, so a seed line without a crop is a MISSING code: it is
  dropped and counted (GH #869 precedent), never marked; a seed row reaching
  the assembler without a crop raises.

THE REPOSITORY IS PUBLIC.  Data assertions compare scalar locals (counts,
sums) computed on their own line; messages carry counts only -- no household
id, plot id or coordinate can reach a failure report.  Data-gated tests skip
only when a source is UNAVAILABLE (``FileNotFoundError``).

Two tiers: synthetic tests (no microdata; they fail on the base
``1865cf884``) and ``requires_s3`` pins on the built tables.
"""
from __future__ import annotations

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
UNKNOWN_PLOT = 'unknown_plot'
MARKER = '(not crop-specific)'
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


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------

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


def test_the_registered_vocabulary_is_used(mw):
    assert mw.PLOT_UNKNOWN == UNKNOWN_PLOT
    assert mw.NOT_CROP_SPECIFIC == MARKER


# ---------------------------------------------------------------------------
# synthetic: production_obs (GH #637 defect A)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# synthetic: unknown_plot (GH #1120 D3)
# ---------------------------------------------------------------------------

def test_a_blank_plot_key_is_served_unknown_plot(mw):
    g = _block(mw, [('h1', None, 1, 5.0, 1, 1), ('h1', 'R1', 1, 2.0, 1, 1)])
    plots = g['plot_id'].tolist()
    assert plots == [UNKNOWN_PLOT, 'R1'], plots


def test_two_unknown_plot_lines_of_one_household_are_never_summed(mw):
    rows = _harvest([('h1', UNKNOWN_PLOT, 'Maize', 1, KG, S, 10.0),
                     ('h1', UNKNOWN_PLOT, 'Maize', 1, KG, S, 4.0)])
    with pytest.raises(ValueError, match=UNKNOWN_PLOT):
        mw._consolidate_harvest([rows])
    # on a KNOWN plot the same two lines are one record (summed, as always)
    known = rows.assign(plot_id='R1')
    out = mw._consolidate_harvest([known])
    n_rows, qty = len(out), float(out['Quantity'].iloc[0])
    assert (n_rows, qty) == (1, 14.0), (n_rows, qty)


def test_no_sale_is_attached_to_or_beside_an_unknown_plot_line(mw):
    """h2's unknown-plot line is in ANOTHER unit, so it is no candidate for
    the KG sale: only the single-plot gate keeps the sale off h2's known row
    (with a NaN plot, `nunique` saw one plot and attached it there)."""
    out = _assemble(mw, [
        ('h1', UNKNOWN_PLOT, 'Maize', 1, KG, S, 10.0),        # only unknown
        ('h2', 'R1', 'Maize', 1, KG, S, 10.0),                # known + unknown
        ('h2', UNKNOWN_PLOT, 'Maize', 1, '50 kg Bag', S, 3.0),
        ('h3', 'R1', 'Maize', 1, KG, S, 10.0)],               # one known plot
        [('h1', 1, KG, S, 5.0, 100.0), ('h2', 1, KG, S, 5.0, 100.0),
         ('h3', 1, KG, S, 5.0, 100.0)])
    sold = out.groupby('i')['Value_sold'].sum(min_count=1)
    got = (pd.isna(sold['h1']), pd.isna(sold['h2']), float(sold['h3']))
    assert got == (True, True, 100.0), got


def _labor_source(rows):
    """rows: (hhid, plotkey, family days on one slot)."""
    return pd.DataFrame({'hhid': [r[0] for r in rows], 'pk': [r[1] for r in rows],
                         'ag_d42b': [r[2] for r in rows], 'ag_d42c': [1] * len(rows)})


def test_labour_on_a_blank_plot_key_is_kept_on_unknown_plot(mw):
    piece = mw._plot_labor_block(_labor_source([('h1', None, 6.0), ('h1', 'R1', 2.0)]),
                                 hhid='hhid', plotkey='pk', t='2010-11',
                                 hired_suffix='', include_other=False)
    out = mw.assemble_plot_labor('2010-11', [piece]).reset_index()
    n_unknown, n_nan = int(out['plot_id'].eq(UNKNOWN_PLOT).sum()), int(out['plot_id'].isna().sum())
    assert (n_unknown, n_nan) == (1, 0), (n_unknown, n_nan)


def test_two_unknown_plot_labour_stubs_of_one_household_raise(mw):
    piece = mw._plot_labor_block(_labor_source([('h1', None, 6.0), ('h1', None, 2.0)]),
                                 hhid='hhid', plotkey='pk', t='2010-11',
                                 hired_suffix='', include_other=False)
    with pytest.raises(ValueError, match=UNKNOWN_PLOT):
        mw.assemble_plot_labor('2010-11', [piece])


# ---------------------------------------------------------------------------
# synthetic: the "not crop-specific" marker (GH #1119)
# ---------------------------------------------------------------------------

def _inputs_source():
    """One plot: NPK in slot 1 (code 1, 10 kg), seed of crop code 1."""
    return pd.DataFrame({'hhid': ['h1'], 'plotkey': ['R1'], 'f1': [1], 'q1': [10.0],
                         'f2': [np.nan], 'q2': [np.nan], 'code': [1], 'sq': [4.0],
                         'su': [2]})


def test_module_d_rows_carry_the_marker_and_seed_rows_their_crop(mw):
    d = _inputs_source()
    fert = mw._fertilizer_block(d, hhid='hhid', plotkey='plotkey', type1='f1',
                                qty1='q1', unit1=None, type2='f2', qty2='q2',
                                unit2=None, t='2010-11')
    seed = mw._seed_block(d, hhid='hhid', plotkey='plotkey', cropcode='code',
                          qty='sq', unit='su', t='2010-11')
    out = mw.assemble_plot_inputs('2010-11', [fert, seed]).reset_index()
    n_nan = int(out['crop'].isna().sum())
    seed_crops = sorted(out.loc[out['input'] == 'Seed', 'crop'].tolist())
    other_crops = sorted(set(out.loc[out['input'] != 'Seed', 'crop']))
    assert n_nan == 0, n_nan
    assert other_crops == [MARKER], other_crops
    assert MARKER not in seed_crops and seed_crops == ['Maize'], seed_crops


def test_a_seed_row_without_a_crop_is_not_marked_it_raises(mw):
    d = _inputs_source()
    seed = mw._seed_block(d, hhid='hhid', plotkey='plotkey', cropcode='code',
                          qty='sq', unit='su', t='2010-11')
    seed['crop'] = pd.array([pd.NA], dtype='string')
    with pytest.raises(ValueError, match='crop'):
        mw.assemble_plot_inputs('2010-11', [seed])


def test_a_seed_line_with_no_crop_code_is_dropped_and_counted(mw):
    d = pd.DataFrame({'hhid': ['h1', 'h1', 'h1'], 'plotkey': ['R1', 'R2', 'R3'],
                      'code': [1, np.nan, 49], 'sq': [4.0, 3.0, 2.0], 'su': [2, 2, 2]})
    with pytest.warns(UserWarning, match=r'2 seed line\(s\) \(2 with a seed quantity\)'):
        seed = mw._seed_block(d, hhid='hhid', plotkey='plotkey', cropcode='code',
                              qty='sq', unit='su', t='2019-20')
    n = len(seed)
    assert n == 1, n


# ---------------------------------------------------------------------------
# data-gated pins on the built tables (counts and sums only)
# ---------------------------------------------------------------------------

class _Tables(dict):
    """The built tables.  pytest prints a failing test's ARGUMENTS in a long
    traceback, and the tail of a frame's repr shows a row -- with its ids --
    so the repr is withheld."""

    def __repr__(self):
        return f'<Malawi tables {sorted(self)}: repr withheld (the frames hold ids)>'


@pytest.fixture(scope='module')
def built():
    import lsms_library as ll
    from lsms_library.local_tools import get_dataframe
    try:   # a source of every wave: skip ONLY when the data is unavailable
        for rel in ('2010-11/Data/Full_Sample/Agriculture/ag_mod_d.dta',
                    '2013-14/Data/AG_MOD_G_13.dta',
                    '2016-17/Data/Panel/ag_mod_p_16.dta',
                    '2019-20/Data/Cross_Sectional/ag_mod_g.dta'):
            get_dataframe(str(ROOT / rel), convert_categoricals=False)
    except FileNotFoundError as exc:  # pragma: no cover
        pytest.skip(f'Malawi agriculture source unavailable: {exc}')
    c = ll.Country('Malawi')
    # Built with warnings silenced: the build's own audit warnings (Site Q's
    # QuantityImplausibleWarning) NAME the offending households, and pytest
    # would print them in its warnings summary -- a public CI log.  These
    # tests assert counts; the warnings are not what they test.
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return _Tables({t: getattr(c, t)().reset_index()
                        for t in ('crop_production', 'plot_labor', 'plot_inputs')})


def _by_wave(df, f):
    return {w: f(df[df['t'].astype(str) == w]) for w in WAVES}


# rows of the built crop_production: development + the 129 / 55 / 7 / 101 code
# rows the collapse used to merge (256 - 127, 109 - 54, 14 - 7, 199 - 98).
CROP_ROWS = {'2010-11': 33549, '2013-14': 12671, '2016-17': 37177, '2019-20': 51973}
# Quantity is summed nowhere new, so the wave totals do not move.
CROP_QTY = {'2010-11': 970088.8, '2013-14': 257839.0, '2016-17': 1871058.4,
            '2019-20': 1260873.9}
# attached sales: development + the 28,900 / 18,200 / 50,000 / 276,500 MWK the
# collapse's 'first' dropped (GH #637 tail diagnosis, reproduced at base).
CROP_SOLD = {'2010-11': 130679005, '2013-14': 95010945, '2016-17': 410017725,
             '2019-20': 529712448}


@pytest.mark.requires_s3
def test_crop_production_keeps_every_native_code(built):
    cp = built['crop_production']
    rows = _by_wave(cp, len)
    assert rows == CROP_ROWS, rows
    qty = _by_wave(cp, lambda x: round(float(x['Quantity'].sum()), 1))
    assert qty == CROP_QTY, qty
    sold = _by_wave(cp, lambda x: int(round(float(x['Value_sold'].sum()))))
    assert sold == CROP_SOLD, sold


@pytest.mark.requires_s3
def test_crop_production_has_no_null_key_and_no_repeat(built):
    cp = built['crop_production']
    n_obs_null = int(cp['production_obs'].isna().sum())
    n_plot_null = int(cp['plot_id'].isna().sum())
    grain = [c for c in ('t', 'i', 'plot_id', 'crop', 'u', 'condition', 'production_obs')
             if c in cp.columns]
    n_dup = int(cp.duplicated(grain).sum())
    assert (n_obs_null, n_plot_null, n_dup) == (0, 0, 0), (n_obs_null, n_plot_null, n_dup)
    shapes = int((~cp['production_obs'].str.fullmatch(r'reported:[GP]:\d+')).sum())
    assert shapes == 0, f'{shapes} production_obs values off the reported:<G|P>:<code> shape'


@pytest.mark.requires_s3
def test_unknown_plot_where_the_source_has_no_plot_key(built):
    cp, pl, pi = built['crop_production'], built['plot_labor'], built['plot_inputs']
    cp_unk = _by_wave(cp, lambda x: int(x['plot_id'].eq(UNKNOWN_PLOT).sum()))
    pl_unk = _by_wave(pl, lambda x: int(x['plot_id'].eq(UNKNOWN_PLOT).sum()))
    pl_hh = int(pl.loc[pl['plot_id'].eq(UNKNOWN_PLOT), 'i'].nunique())
    pi_unk = int(pi['plot_id'].eq(UNKNOWN_PLOT).sum())
    nulls = (int(pl['plot_id'].isna().sum()), int(pi['plot_id'].isna().sum()))
    assert cp_unk == {'2010-11': 0, '2013-14': 0, '2016-17': 41, '2019-20': 0}, cp_unk
    assert pl_unk == {'2010-11': 40, '2013-14': 0, '2016-17': 0, '2019-20': 0}, pl_unk
    assert (pl_hh, pi_unk, nulls) == (32, 0, (0, 0)), (pl_hh, pi_unk, nulls)
    sold_unk = int(cp.loc[cp['plot_id'].eq(UNKNOWN_PLOT), 'Value_sold'].notna().sum())
    assert sold_unk == 0, f'{sold_unk} sales attached to an unknown_plot line'


@pytest.mark.requires_s3
def test_plot_inputs_marks_every_module_d_row_and_no_seed_row(built):
    pi = built['plot_inputs']
    crop = 'crop' if 'crop' in pi.columns else 'j'
    n_nan = int(pi[crop].isna().sum())
    marked = _by_wave(pi, lambda x: int(x[crop].eq(MARKER).sum()))
    seed_marked = int((pi['input'].eq('Seed') & pi[crop].eq(MARKER)).sum())
    other_unmarked = int((~pi['input'].eq('Seed') & ~pi[crop].eq(MARKER)).sum())
    assert n_nan == 0, n_nan
    assert marked == {'2010-11': 18964, '2013-14': 6225, '2016-17': 20152,
                      '2019-20': 22213}, marked
    assert (seed_marked, other_unmarked) == (0, 0), (seed_marked, other_unmarked)
