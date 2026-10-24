"""GH #637 tail -- three demonstrated defects in Malawi's crop / input builders.

The read-only diagnosis is the 2026-10-23 comment on GH #637; the fix, its
evidence and what it leaves alone are in ``Malawi/_/CONTENTS.org`` ("the GH
#637 tail") and ``.coder/ledger/637-malawi-tail.md``.

* **Defect B -- the seed-purchase merge fanned out.**  ``assemble_plot_inputs``
  reduced Module H purchases to ``(i, _crop_code)``, relabelled them to
  ``(i, crop)`` and merged on ``(i, crop)`` without reducing again.
  ``harmonize_crop`` maps several codes to one label (Maize 1-4, the groundnut
  and rice varieties), so each matching seed row was duplicated once per
  purchased code: the additive grouping doubled seed ``Quantity`` and 'first'
  kept one code's purchase.  Fixed by reducing the purchases to the merge key
  (the #323 site-4 rule).
* **Zeros minted from blanks.**  ``_consolidate_harvest``'s bare ``'sum'``
  served a blank ``ag_g13a`` / ``ag_p09a`` as a harvest of ``0.0``.  Malawi's
  instrument has no "did you harvest at all?" answer to preserve (G07 asks
  only whether the harvest is COMPLETE), so ``min_count=1`` (PR #1263's
  Tanzania precedent) -- also at the defensive collapse, which would re-mint
  the zero when two codes of one label are both blank (1 row, 2013-14).
* **IHS3 2010-11 ships exact repeat records.**  A record equal to an earlier
  one in EVERY column is a duplicate entry (the #840 / #651 rule), dropped on
  the full raw record before any block projects it.

Two tiers: static tests on synthetic frames (no microdata; these fail on
development 9a0ed5f3c), and ``requires_s3`` pins on the delivered tables plus
an independent raw-source oracle.  The pins are NOT index uniqueness -- the
served index was unique with all three defects present.
"""
from __future__ import annotations

import ast
import importlib.util
import re
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root

ROOT = countries_root() / 'Malawi'
WAVES = ['2010-11', '2013-14', '2016-17', '2019-20']


@pytest.fixture(scope='module')
def mw():
    """The Malawi country module, imported by path (it is not a package)."""
    path = ROOT / '_' / 'malawi.py'
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location('malawi_gh637_tail', path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        sys.path.remove(str(path.parent))
    return mod


# ---------------------------------------------------------------------------
# static: defect B on synthetic Module G / Module H frames
# ---------------------------------------------------------------------------

def _seed_source(rows):
    """rows: (hhid, plotkey, crop code, seed qty, seed unit code)."""
    return pd.DataFrame({'hhid': [r[0] for r in rows], 'plotkey': [r[1] for r in rows],
                         'code': [r[2] for r in rows], 'q': [r[3] for r in rows],
                         'unit': [r[4] for r in rows]})


def _purchase_source(rows):
    """rows: (hhid, crop code, purchased qty, value)."""
    return pd.DataFrame({'hhid': [r[0] for r in rows], 'code': [r[1] for r in rows],
                         'qa': [r[2] for r in rows], 'ua': [2] * len(rows),
                         'va': [r[3] for r in rows]})


def _plot_inputs(mw, seed_rows, purchase_rows):
    seed = mw._seed_block(_seed_source(seed_rows), hhid='hhid', plotkey='plotkey',
                          cropcode='code', qty='q', unit='unit', t='2010-11')
    sp = mw._seed_purchase_block(_purchase_source(purchase_rows), hhid='hhid',
                                 cropcode='code', qty_cols=['qa'], unit_cols=['ua'],
                                 value_cols=['va'])
    return mw.assemble_plot_inputs('2010-11', [seed], [sp])


class TestSeedPurchaseFanOut:
    """Maize LOCAL (code 1) and HYBRID (code 3) are both `crop == 'Maize'`."""

    def test_two_purchased_codes_do_not_duplicate_the_seed_rows(self, mw):
        out = _plot_inputs(mw, [('h1', 'R1', 1, 10.0, 2), ('h1', 'R1', 3, 4.0, 2)],
                           [('h1', 1, 5.0, 100.0), ('h1', 3, 2.0, 50.0)])
        assert len(out) == 1
        row = out.iloc[0]
        # development served 28.0: each seed row merged once per purchased code
        assert row['Quantity'] == 14.0
        # development served 5.0: 'first' kept code 1's purchase only
        assert row['Quantity_purchased'] == 7.0
        assert bool(row['Purchased']) is True

    def test_one_purchased_code_is_unchanged(self, mw):
        out = _plot_inputs(mw, [('h1', 'R1', 1, 10.0, 2), ('h1', 'R1', 3, 4.0, 2)],
                           [('h1', 3, 2.0, 50.0)])
        assert out['Quantity'].tolist() == [14.0]
        assert out['Quantity_purchased'].tolist() == [2.0]

    def test_purchase_is_still_attached_only_to_a_single_plot_crop(self, mw):
        out = _plot_inputs(mw, [('h1', 'R1', 1, 10.0, 2), ('h1', 'R2', 3, 4.0, 2)],
                           [('h1', 1, 5.0, 100.0), ('h1', 3, 2.0, 50.0)])
        assert sorted(out['Quantity'].tolist()) == [4.0, 10.0]
        assert out['Quantity_purchased'].isna().all()

    def test_a_purchase_with_no_value_is_still_not_a_purchase(self, mw):
        out = _plot_inputs(mw, [('h1', 'R1', 1, 10.0, 2)],
                           [('h1', 1, 5.0, 0.0), ('h1', 3, 2.0, np.nan),
                            ('h2', 1, 2.0, 50.0)])
        assert out['Quantity_purchased'].isna().all()
        assert out['Purchased'].isna().all()


# ---------------------------------------------------------------------------
# static: blank harvests on synthetic harvest pieces
# ---------------------------------------------------------------------------

def _harvest(rows):
    """rows: (i, plot, crop, code, u, condition, qty or None)."""
    n = len(rows)
    return pd.DataFrame({
        't': ['2010-11'] * n,
        'i': [r[0] for r in rows], 'plot_id': [r[1] for r in rows],
        'crop': [r[2] for r in rows],
        '_crop_code': pd.array([r[3] for r in rows], dtype='Int64'),
        # what `_harvest_block` emits since GH #637 defect A (Module G here)
        'production_obs': pd.array([f'reported:G:{r[3]}' for r in rows],
                                   dtype='string'),
        'u': pd.array([r[4] for r in rows], dtype='string'),
        'condition': pd.array([r[5] for r in rows], dtype='string'),
        'crop_variety': pd.array([f'v{r[3]}' for r in rows], dtype='string'),
        'Quantity': pd.array([r[6] for r in rows], dtype='Float64'),
        'planting_month': pd.array([11] * n, dtype='Int64'),
        'harvest_month': pd.array([6] * n, dtype='Int64'),
        'intercropped': pd.array([False] * n, dtype='boolean'),
        'perennial': pd.array([False] * n, dtype='boolean'),
    })


KG, S, UNK = 'Kilogramme', 'shelled', 'unknown_condition'


class TestBlankHarvest:
    def test_consolidation_keeps_a_blank_blank(self, mw):
        out = mw._consolidate_harvest([_harvest([
            ('h1', 'R1', 'Maize', 1, KG, S, None),
            ('h1', 'R2', 'Maize', 1, KG, S, 0.0),
            ('h1', 'R3', 'Maize', 1, KG, S, None),
            ('h1', 'R3', 'Maize', 1, KG, S, 12.0)])]).set_index('plot_id')
        assert pd.isna(out.loc['R1', 'Quantity'])        # development: 0.0
        assert out.loc['R2', 'Quantity'] == 0.0           # a recorded zero stays
        assert out.loc['R3', 'Quantity'] == 12.0          # a blank adds nothing

    def test_assembled_table_serves_na_not_zero(self, mw):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            out = mw.assemble_crop_production('2010-11', [_harvest([
                ('h1', 'R1', 'Maize', 1, KG, S, None),
                ('h1', 'R2', 'Maize', 1, KG, S, 0.0)])], [])
        q = out.reset_index().set_index('plot_id')['Quantity']
        assert pd.isna(q['R1']) and q['R2'] == 0.0

    def test_two_blank_codes_of_one_label_stay_two_blank_rows(self, mw):
        """Codes 1 and 3 share `crop == 'Maize'`.  Defect A's collapse (GH #637
        tail) summed them into one row -- with `min_count=1` so two blanks were
        not re-minted to 0.0.  Since the `production_obs` ruling (2026-10-23)
        there is no collapse: each code is its own row, a blank stays blank,
        and nothing is summed across codes."""
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            out = mw.assemble_crop_production('2010-11', [_harvest([
                ('h2', 'R1', 'Maize', 1, None, UNK, None),
                ('h2', 'R1', 'Maize', 3, None, UNK, None),
                ('h3', 'R1', 'Maize', 1, KG, S, None),
                ('h3', 'R1', 'Maize', 3, KG, S, 7.0)])], [])
        out = out.reset_index()
        n_rows, n_blank = len(out), int(out['Quantity'].isna().sum())
        assert (n_rows, n_blank) == (4, 3), (n_rows, n_blank)
        q = out.set_index(['i', 'production_obs'])['Quantity']
        assert q[('h3', 'reported:G:3')] == 7.0
        assert pd.isna(q[('h3', 'reported:G:1')])


# ---------------------------------------------------------------------------
# static: the repeat-record rule and its wiring
# ---------------------------------------------------------------------------

class TestRepeatedRecords:
    def test_only_a_record_equal_in_every_column_is_dropped(self, mw):
        raw = pd.DataFrame({'case_id': ['a', 'a', 'a', 'b', 'b'],
                            'visit': [1, 1, 2, 1, 1],
                            'ag_g13a': [3.0, 3.0, 3.0, np.nan, np.nan],
                            'ag_g13b': [1, 1, 1, np.nan, np.nan]})
        out = mw.drop_repeated_records(raw)
        # row 1 repeats row 0; row 2 differs only in `visit` and is KEPT; row
        # 4 repeats row 3 including its blanks
        assert out.index.tolist() == [0, 2, 3]

    @pytest.mark.parametrize('script, frames', [
        ('crop_production.py', {'g', 'p', 'i_mod', 'q'}),
        ('plot_inputs.py', {'d', 'g', 'h'}),
    ])
    def test_2010_11_scripts_drop_repeats_from_every_module_they_read(self, script, frames):
        tree = ast.parse((ROOT / '2010-11' / '_' / script).read_text())
        read = {t.id for node in ast.walk(tree) if isinstance(node, ast.Assign)
                and isinstance(node.value, ast.Call)
                and getattr(node.value.func, 'id', None) == 'get_dataframe'
                for t in node.targets}
        assert read == frames
        deduped = set()
        for node in ast.walk(tree):
            if (isinstance(node, ast.Assign) and isinstance(node.value, ast.GeneratorExp)
                    and isinstance(node.value.elt, ast.Call)
                    and getattr(node.value.elt.func, 'id', None) == 'drop_repeated_records'):
                deduped |= {e.id for e in node.targets[0].elts}
                assert {e.id for e in node.value.generators[0].iter.elts} == deduped
        assert deduped == frames

    def test_the_derivation_inputs_replay_drops_them_too(self, mw):
        import inspect
        assert 'drop_repeated_records' in inspect.getsource(mw._crop_pieces)


# ---------------------------------------------------------------------------
# data-gated: pins on the delivered tables, and a raw-source oracle
# ---------------------------------------------------------------------------

#: Served crop_production.Quantity per wave (cold build, 2026-10-23).  NA was
#: 0 / 0 / 0 / 0 on development; 2013-14 has 284 blank consolidation groups.
#: Until GH #637 defect A's ruling (production_obs, 2026-10-23) two of them --
#: codes of one label -- were merged at the declared grain (283 NA), and a
#: recorded zero summed with a sibling code's harvest vanished into the sum
#: (zeros 1,255 / 310 / 3,723 / 3,343); each code is now its own row.
HARVEST_NA = {'2010-11': 32, '2013-14': 284, '2016-17': 200, '2019-20': 276}
HARVEST_ZERO = {'2010-11': 1258, '2013-14': 310, '2016-17': 3723, '2019-20': 3349}
#: 2010-11 fell by 67 = 42 (Module G repeats) + 25 (the Module P repeat).
HARVEST_SUM = {'2010-11': 970_088.8, '2013-14': 257_839.0,
               '2016-17': 1_871_058.4, '2019-20': 1_260_873.9}
#: Seed rows are unchanged in number; their Quantity fell by 8+548 / 9 / 145 /
#: 123 (defect B; 548 = the 2010-11 Module G repeats' ag_g04a).
SEED_SUM = {'2010-11': 830_067.4, '2013-14': 324_288.5,
            '2016-17': 827_722.99, '2019-20': 1_134_377.2}
#: Quantity_purchased rose by 58 / 4 / 157 / 81: the (i, crop) total, not one code's.
PURCHASED_SUM = {'2010-11': 31_797.0, '2013-14': 50_382.7,
                 '2016-17': 68_627.0, '2019-20': 93_688.8}

#: Exact repeat records per raw module (every column equal).
REPEATS_2010 = {'ag_mod_g.dta': 9, 'ag_mod_p.dta': 1, 'ag_mod_i.dta': 61,
                'ag_mod_q.dta': 15, 'ag_mod_h.dta': 45, 'ag_mod_d.dta': 0}
LATER_MODULES = {
    '2013-14': ['AG_MOD_G_13.dta', 'AG_MOD_P_13.dta', 'AG_MOD_I_13.dta',
                'AG_MOD_Q_13.dta', 'AG_MOD_H_13.dta'],
    '2016-17': [f'{h}/ag_mod_{m}{s}.dta' for h, s in (('Cross_Sectional', ''), ('Panel', '_16'))
                for m in 'gpiqh'],
    '2019-20': [f'{h}/ag_mod_{m}{s}.dta' for h, s in (('Cross_Sectional', ''), ('Panel', '_19'))
                for m in 'gpiqh'],
}


def _country_table(table):
    import lsms_library as ll
    try:   # skip ONLY on an unavailable source; any other error is a failure
        return getattr(ll.Country('Malawi'), table)()
    except FileNotFoundError as e:                  # pragma: no cover - no microdata
        pytest.skip(f'Malawi {table} not buildable here: {e}')


@pytest.fixture(scope='module')
def crop_production():
    return _country_table('crop_production')


@pytest.fixture(scope='module')
def plot_inputs():
    return _country_table('plot_inputs')


def _raw(rel):
    import pyreadstat
    from lsms_library.data_access import get_data_file
    path = get_data_file(f'Malawi/{rel}')
    if path is None:                                 # pragma: no cover
        pytest.skip(f'{rel} not available')
    df, _ = pyreadstat.read_dta(str(path), apply_value_formats=False)
    return df


@pytest.mark.requires_s3
class TestDelivered:
    @pytest.mark.parametrize('wave', WAVES)
    def test_blank_harvests_are_na_and_recorded_zeros_stay(self, crop_production, wave):
        # counts on locals: an assert on the Series would print household ids
        q = crop_production.xs(wave, level='t')['Quantity']
        n_na, n_zero, total = int(q.isna().sum()), int((q == 0).sum()), float(q.sum())
        assert n_na == HARVEST_NA[wave], (n_na, HARVEST_NA[wave])
        assert n_zero == HARVEST_ZERO[wave], (n_zero, HARVEST_ZERO[wave])
        assert total == pytest.approx(HARVEST_SUM[wave], abs=1e-6)

    @pytest.mark.parametrize('wave', WAVES)
    def test_seed_quantity_and_purchases(self, plot_inputs, wave):
        x = plot_inputs.xs(wave, level='t')
        seed = x[x.index.get_level_values('input') == 'Seed']
        assert float(seed['Quantity'].sum()) == pytest.approx(SEED_SUM[wave], abs=1e-6)
        assert float(x['Quantity_purchased'].sum()) == pytest.approx(PURCHASED_SUM[wave], abs=1e-6)
        # a purchase sits only on seed rows
        assert x.loc[x.index.get_level_values('input') != 'Seed', 'Quantity_purchased'].isna().all()


@pytest.mark.requires_s3
@pytest.mark.slow
class TestRawOracle:
    """An independent reader: pyreadstat on the L1 blobs, its own parse of the
    org tables, its own grouping -- none of the builder's functions."""

    def test_2010_11_ships_the_repeats_and_no_later_wave_does(self):
        base = '2010-11/Data/Full_Sample/Agriculture/'
        got = {f: int(_raw(base + f).duplicated().sum()) for f in REPEATS_2010}
        assert got == REPEATS_2010
        for wave, files in LATER_MODULES.items():
            for f in files:
                assert int(_raw(f'{wave}/Data/{f}').duplicated().sum()) == 0, (wave, f)

    @pytest.mark.parametrize('wave', WAVES)
    def test_seed_totals_from_raw_modules(self, plot_inputs, wave):
        from lsms_library.local_tools import format_id
        crop = _org_codes('harmonize_crop')
        seed_rows, purchases = [], []
        for half in _HALVES[wave]:
            g = _raw(half['g'])
            h = _raw(half['h'])
            if wave == '2010-11':
                g, h = g[~g.duplicated()], h[~h.duplicated()]
            plot = half['plot'](g, format_id)
            s = pd.DataFrame({'i': half['hh'](g, format_id), 'plot_id': plot,
                              'crop': pd.to_numeric(g[half['gcrop']], errors='coerce').map(crop),
                              'Quantity': pd.to_numeric(g['ag_g04a'], errors='coerce')})
            seed_rows.append(s[s['crop'].notna()])
            qp = pd.concat([pd.to_numeric(h[q], errors='coerce').where(
                pd.to_numeric(h[v], errors='coerce').fillna(0) != 0)
                for q, v in zip(half['q'], half['v'])], axis=1).sum(axis=1, min_count=1)
            p = pd.DataFrame({'i': half['hh'](h, format_id),
                              'crop': pd.to_numeric(h[half['hcrop']], errors='coerce').map(crop),
                              'qp': qp})
            purchases.append(p[p['crop'].notna()])
        S = pd.concat(seed_rows, ignore_index=True)
        P = pd.concat(purchases, ignore_index=True).groupby(['i', 'crop'])['qp'].sum(min_count=1).dropna()
        nplots = S[S['plot_id'].notna()].groupby(['i', 'crop'])['plot_id'].nunique()
        single = P[P.index.isin(nplots[nplots == 1].index)]
        x = plot_inputs.xs(wave, level='t')
        seed = x[x.index.get_level_values('input') == 'Seed']
        # seed planted: every raw record counted once (the bare sum of the
        # additive grouping, all-blank groups included as 0.0 -- #1174)
        assert float(seed['Quantity'].sum()) == pytest.approx(float(S['Quantity'].sum()), abs=1e-6)
        # each single-plot (i, crop) carries ITS total once per seed row, so
        # the max over its rows is that total; summed, the oracle's total
        served = seed['Quantity_purchased'].groupby(
            [seed.index.get_level_values('i'), seed.index.get_level_values('crop')]).max()
        assert int(served.notna().sum()) == len(single)
        assert float(served.sum()) == pytest.approx(float(single.sum()), abs=1e-6)

    @pytest.mark.parametrize('wave', WAVES)
    def test_harvest_blanks_from_raw_modules(self, crop_production, wave):
        """At the native grain (GH #637 defect A): one row per (household,
        plot, crop, unit, condition, module + INSTRUMENT crop code); a blank
        plot key is `unknown_plot` (GH #1120 D3)."""
        from lsms_library.local_tools import format_id
        crop, unit, cond = (_org_codes(t) for t in
                            ('harmonize_crop', 'harmonize_crop_unit', 'harmonize_crop_condition'))
        parts = []
        for half in _HALVES[wave]:
            for mod, perennial in (('g', False), ('p', True)):
                df = _raw(half[mod])
                if wave == '2010-11':
                    df = df[~df.duplicated()]
                col = half['gcrop'] if mod == 'g' else (
                    half.get('pcrop') or ('ag_p0c' if 'ag_p0c' in df.columns else 'crop_code'))
                code = pd.to_numeric(df[col], errors='coerce')
                obs = ('reported:' + ('P' if perennial else 'G') + ':'
                       + code.astype('Int64').astype('string'))
                if perennial:
                    code = (code + 1000).replace({1100: 1001, 1021: 1018})
                c = (pd.to_numeric(df['ag_g13c'], errors='coerce').map(cond).fillna(UNK)
                     if mod == 'g' else pd.Series(UNK, index=df.index))
                u = pd.to_numeric(df['ag_g13b' if mod == 'g' else 'ag_p09b'], errors='coerce').map(unit)
                parts.append(pd.DataFrame({
                    'i': half['hh'](df, format_id),
                    'plot_id': half['plot' if mod == 'g' else 'pplot'](df, format_id),
                    'crop': code.map(crop), 'u': u.fillna('<NA>'), 'condition': c,
                    'production_obs': obs,
                    'Quantity': pd.to_numeric(df['ag_g13a' if mod == 'g' else 'ag_p09a'],
                                              errors='coerce')}))
        R = pd.concat(parts, ignore_index=True)
        R = R[R['crop'].notna()]
        R['plot_id'] = R['plot_id'].fillna('unknown_plot')
        exp = R.groupby(['i', 'plot_id', 'crop', 'u', 'condition', 'production_obs']
                        )['Quantity'].sum(min_count=1)
        q = crop_production.xs(wave, level='t')['Quantity']
        n_exp, n_q = len(exp), len(q)
        na_exp, na_q = int(exp.isna().sum()), int(q.isna().sum())
        assert n_exp == n_q, (n_exp, n_q)
        assert na_exp == na_q == HARVEST_NA[wave], (na_exp, na_q, HARVEST_NA[wave])
        sum_exp, sum_q = float(exp.sum()), float(q.sum())
        assert sum_exp == pytest.approx(sum_q, abs=1e-6)
        unk_exp = int((exp.index.get_level_values('plot_id') == 'unknown_plot').sum())
        unk_q = int((q.index.get_level_values('plot_id') == 'unknown_plot').sum())
        assert unk_exp == unk_q, (unk_exp, unk_q)


def _org_codes(name):
    """Code -> Preferred Label, parsed from the org file without the library."""
    lines = (ROOT / '_' / 'categorical_mapping.org').read_text().splitlines()
    start = next(k for k, l in enumerate(lines) if l.strip().lower() == f'#+name: {name}')
    out = {}
    for line in lines[start + 2:]:
        if not line.strip().startswith('|'):
            break
        if re.match(r'^\|[-+]+\|?$', line.strip()):
            continue
        code, label = [c.strip() for c in line.strip().strip('|').split('|')][:2]
        out[float(code)] = label or None
    return out


def _fid(col):
    return lambda df, fid: df[col].apply(fid)


def _garden(df, fid):
    return df['gardenid'].apply(fid) + '_' + df['plotid'].apply(fid)


_HALVES = {
    '2010-11': [dict(g='2010-11/Data/Full_Sample/Agriculture/ag_mod_g.dta',
                     p='2010-11/Data/Full_Sample/Agriculture/ag_mod_p.dta',
                     h='2010-11/Data/Full_Sample/Agriculture/ag_mod_h.dta',
                     hh=_fid('case_id'), plot=_fid('ag_g0b'), pplot=_fid('ag_p0b'),
                     gcrop='ag_g0d', pcrop='ag_p0d', hcrop='ag_h0b',
                     q=['ag_h16a', 'ag_h26a'], v=['ag_h19', 'ag_h29'])],
    '2013-14': [dict(g='2013-14/Data/AG_MOD_G_13.dta', p='2013-14/Data/AG_MOD_P_13.dta',
                     h='2013-14/Data/AG_MOD_H_13.dta',
                     hh=_fid('y2_hhid'), plot=_fid('ag_g00'), pplot=_fid('ag_p00'),
                     gcrop='ag_g0b', pcrop='ag_p0c', hcrop='ag_h0c',
                     q=['ag_h16a', 'ag_h26a'], v=['ag_h19', 'ag_h29'])],
    '2016-17': [dict(g='2016-17/Data/Cross_Sectional/ag_mod_g.dta',
                     p='2016-17/Data/Cross_Sectional/ag_mod_p.dta',
                     h='2016-17/Data/Cross_Sectional/ag_mod_h.dta',
                     hh=lambda df, fid: 'cs-17-' + df['case_id'].apply(fid),
                     plot=_garden, pplot=_garden, gcrop='crop_code', hcrop='crop_code',
                     q=['ag_h07a', 'ag_h38a'], v=['ag_h09', 'ag_h40']),
                dict(g='2016-17/Data/Panel/ag_mod_g_16.dta', p='2016-17/Data/Panel/ag_mod_p_16.dta',
                     h='2016-17/Data/Panel/ag_mod_h_16.dta',
                     hh=_fid('y3_hhid'), plot=_garden, pplot=_garden,
                     gcrop='crop_code', hcrop='crop_code',
                     q=['ag_h16a', 'ag_h26a'], v=['ag_h19', 'ag_h29'])],
    '2019-20': [dict(g='2019-20/Data/Cross_Sectional/ag_mod_g.dta',
                     p='2019-20/Data/Cross_Sectional/ag_mod_p.dta',
                     h='2019-20/Data/Cross_Sectional/ag_mod_h.dta',
                     hh=_fid('case_id'), plot=_garden, pplot=_garden,
                     gcrop='crop_code', hcrop='crop_code',
                     q=['ag_h07a', 'ag_h38a'], v=['ag_h09', 'ag_h40']),
                dict(g='2019-20/Data/Panel/ag_mod_g_19.dta', p='2019-20/Data/Panel/ag_mod_p_19.dta',
                     h='2019-20/Data/Panel/ag_mod_h_19.dta',
                     hh=_fid('y4_hhid'), plot=_garden, pplot=_garden,
                     gcrop='crop_code', hcrop='crop_code',
                     q=['ag_h16a', 'ag_h26a'], v=['ag_h19', 'ag_h29'])],
}
