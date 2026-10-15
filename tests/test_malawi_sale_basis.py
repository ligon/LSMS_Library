"""Malawi crop_production: the sale-basis LADDER as registered derivations.

GH #833 made the sale's own shelled/unshelled answer (ag_i02c / ag_q02c) a
condition of attaching a Module I/Q sale to a Module G/P harvest row, and
suppressed 1,112 sales / 64,110,157 MWK whose basis the row did not share.
@ligon's 2026-09-24 ruling on that residue -- "shelled vs unshelled is
really a missing data problem, decipherable by looking at prices", "this
would be a derived value", and not-applicable IS the basis where shelling
does not apply -- is implemented as a ladder of rungs inside
``malawi.assemble_crop_production`` (``_attach_sales``), each rung past an
exact match a registered derivation in ``countries/Malawi/_/derivations.yml``
with its key stamped on the ``Derivation`` column.  Evidence:
``slurm_logs/backlog_fix_2026-09-24/malawi_sale_basis/README.org`` (branch
``fix/malawi-sale-basis-probe``); ``Malawi/_/CONTENTS.org``, "the sale-basis
LADDER".  Two rungs were added on 2026-09-25 on @ligon's rulings on the
review of the ladder: ``sale-basis-from-price-overrule`` (the price decides
on the S/U-sale-vs-NA-harvest side even against the sale's recorded
answer) and ``sale-basis-no-reference`` (rung 3 split: admitted because NO
reference exists, as distinct from a reference that measurably does not
separate).

Two tiers, following ``test_ghanalss_12b.py``:

* **Static** (``TestRegistry``, ``TestScripts``, ``TestFunction``,
  ``TestAssemble``) -- the registry as config, the module as text, the pure
  decision function, and the ladder on synthetic frames.  No microdata.
* **Data-gated** (``TestDelivered``) -- the delivered table's labels, pinned
  counts, and the inputs join; skipped where the microdata are not
  available.
"""
from __future__ import annotations

import importlib.util
import inspect
import re
import sys
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.derivations import derivation_records, resolve_callable
from lsms_library.paths import countries_root

COUNTRY = 'Malawi'
WAVES = ['2010-11', '2013-14', '2016-17', '2019-20']
ROOT = countries_root() / COUNTRY
MALAWI_ = ROOT / '_'
FUNCTION = 'lsms_library.countries.Malawi._.malawi:derive_sale_basis'

W = 'Malawi::crop_production::sale-basis-unknown-wildcard'
P = 'Malawi::crop_production::sale-basis-from-price'
OV = 'Malawi::crop_production::sale-basis-from-price-overrule'
N = 'Malawi::crop_production::sale-basis-not-applicable'
NR = 'Malawi::crop_production::sale-basis-no-reference'
CS = 'Malawi::crop_production::contradiction-price-sale'
CH = 'Malawi::crop_production::contradiction-price-harvest'
DEL = 'Malawi::crop_production::sale-basis-unresolved'
KEYS = (W, P, OV, N, NR, CS, CH)
SINCE = {OV: '2026-09-25', NR: '2026-09-25'}

#: Delivered ROWS carrying each key, per wave -- measured on the cold build
#: of 2026-09-25 (fix/malawi-derivations @ 95449ffb9 -> this branch).  Rows
#: are BELOW the attached sales on the wildcard (949 / 357 / 450 / 1,046
#: sales) for TWO reasons, neither of them this ladder: the GH #869
#: unmapped-crop drop (13 rows, all 2013-14) and the defensive collapse at
#: the declared grain in `assemble_crop_production` (`_grain`, `Value_sold:
#: 'first'`), which merges two same-label VARIETIES of one plot-crop
#: (Citrus 1005/1010/1011, Groundnut 11/12/16, Maize 1/3) into one row and
#: keeps one variety's sale -- 2 / 3 / 1 / 5 keyed wildcard sales, and 27
#: sales / 373,600 MWK in all (6 / 3 / 1 / 17 per wave), a pre-existing
#: composition the ladder does not touch (issue text in
#: slurm_logs/backlog_fix_2026-09-24/issues_to_file.org).  Unsold Module I/Q
#: lines summed to 0/0 by `_sale_block` (901 / 225 / 2,433 / 2,185) attach on
#: the wildcard as 0.0, as they always did, and carry NO key.
#: Before 2026-09-25 the not-applicable key carried N + NR (122 / 11 / 79 /
#: 198) and the from-price key's harvest side refused what OV now carries.
ROWS = {
    '2010-11': {W: 947, P: 1, OV: 0, N: 5, NR: 117, CS: 67, CH: 19},
    '2013-14': {W: 341, P: 0, OV: 0, N: 0, NR: 11, CS: 18, CH: 9},
    '2016-17': {W: 449, P: 8, OV: 2, N: 42, NR: 37, CS: 47, CH: 23},
    '2019-20': {W: 1041, P: 29, OV: 2, N: 67, NR: 131, CS: 85, CH: 51},
}
#: Total Value_sold per wave, before (the GH #833 bundle) -> after.  The
#: overrule rung of 2026-09-25 moved 2016-17 by +37,000 (409,053,925 ->
#: 409,090,925) and 2019-20 by +120,000 (527,653,548 -> 527,773,548).
VALUE_SOLD_BEFORE = {'2010-11': 126_816_048.0, '2013-14': 94_338_845.0,
                     '2016-17': 394_826_755.0, '2019-20': 496_063_698.0}
VALUE_SOLD_AFTER = {'2010-11': 130_650_105.0, '2013-14': 94_992_745.0,
                    '2016-17': 409_090_925.0, '2019-20': 527_773_548.0}
#: Sales the ladder still suppresses (attrs['sale_basis_mismatch']), per
#: wave: before 303 / 59 / 247 / 503 (64,110,157 MWK); after the ladder of
#: 2026-09-24, 94 / 21 / 90 / 140 (13,805,180 MWK); the overrule rung took 2
#: / 2 sales (37,000 / 120,000 MWK) out of 2016-17 / 2019-20.
UNRESOLVED = {'2010-11': (94, 1_088_250.0), '2013-14': (21, 315_300.0),
              '2016-17': (88, 5_972_600.0), '2019-20': (138, 6_272_030.0)}
#: attrs['sale_basis_ladder'][DEL]['grounds'], summed over the four waves.
UNRESOLVED_GROUNDS = {'contradiction_no_separation': 271,
                      'contradiction_price_undecided': 57,
                      'na_sale_price_undecided': 6,
                      'na_harvest_price_undecided': 7,
                      'claimed_by_tier_a': 0}
TOTAL_ROWS = {'2010-11': 33_420, '2013-14': 12_616, '2016-17': 37_170, '2019-20': 51_872}


@pytest.fixture(scope='module')
def malawi_mod():
    """The Malawi country module, imported by path (it is not a package)."""
    path = MALAWI_ / 'malawi.py'
    if not path.exists():                                   # pragma: no cover
        pytest.skip(f'{path} not present')
    sys.path.insert(0, str(MALAWI_))
    try:
        spec = importlib.util.spec_from_file_location('malawi_sale_basis', path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        sys.path.remove(str(MALAWI_))
    return mod


def _source():
    return (MALAWI_ / 'malawi.py').read_text()


# ---------------------------------------------------------------------------
# static: the registry
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_eight_entries_one_per_rung(self):
        r = derivation_records(COUNTRY)
        assert set(r) == set(KEYS) | {DEL}
        for key in KEYS:
            rec = r[key]
            assert rec.waves == WAVES, key
            assert rec.columns == ['Quantity_sold', 'Value_sold'], key
            assert rec.rows is None, key
            assert rec.function == FUNCTION, key
            assert rec.inputs.startswith('lsms_library.countries.Malawi._.malawi:inputs_sale_basis_'), key
            assert rec.validated_against and rec.contents, key
            assert rec.since == SINCE.get(key, '2026-09-24'), key

    def test_the_deletion_entry(self):
        rec = derivation_records(COUNTRY)[DEL]
        assert rec.rows == 0
        assert rec.inputs.endswith(':inputs_sale_basis_unresolved')
        assert 'sale_basis_mismatch' in rec.rule

    def test_bases_name_the_evidence(self):
        r = derivation_records(COUNTRY)
        assert set(r[W].bases) == {'questionnaire', 'modelling-choice'}
        assert set(r[P].bases) == {'questionnaire', 'modelling-choice'}
        assert set(r[N].bases) == {'questionnaire', 'modelling-choice'}
        assert set(r[NR].bases) == {'questionnaire', 'modelling-choice'}
        for key in (OV, CS, CH, DEL):
            assert 'modelling-choice' in r[key].bases
        # the overrule of a RECORDED answer names no document: modelling-choice only
        assert set(r[OV].bases) == {'modelling-choice'}

    def test_the_two_grounds_of_rung_three_are_separate_keys(self):
        """@ligon, 2026-09-25, ruling 2: measured non-separation vs. no
        reference are exposed per row, and each entry names the other."""
        r = derivation_records(COUNTRY)
        assert 'sale-basis-no-reference' in r[N].rule
        assert 'ABSENCE of evidence' in r[NR].rule
        assert 'NOTHING' in r[NR].validated_against
        assert 'sale-basis-from-price-overrule' in r[P].rule
        assert 'overrule' in r[DEL].rule

    def test_the_margin_prose_matches_the_gate(self):
        """Review item 1: the gate admits the OUTER quarter between the
        medians; the prose used to say any price between them resolves
        nothing."""
        r = derivation_records(COUNTRY)
        assert 'OUTER' in r[P].rule and 'MIDDLE HALF' in r[P].rule
        assert 'sits between' not in r[P].rule.lower().replace('sits between the medians resolves', 'X')

    def test_every_callable_resolves(self):
        r = derivation_records(COUNTRY)
        fns = set()
        for key, rec in r.items():
            assert callable(resolve_callable(rec.function)), key
            fn = resolve_callable(rec.inputs)
            assert callable(fn) and 'wave' in inspect.signature(fn).parameters, key
            fns.add(fn)
        assert len(fns) == 8, 'each key has its own inputs callable'

    def test_the_contents_pointer_resolves(self):
        text = (MALAWI_ / 'CONTENTS.org').read_text()
        assert 'the sale-basis LADDER' in text
        for key in (*KEYS, DEL):
            assert key.split('::')[-1] in text, key


# ---------------------------------------------------------------------------
# static: the module and the wave scripts
# ---------------------------------------------------------------------------

class TestScripts:
    def test_keys_are_literal_in_the_module(self):
        src = _source()
        for key in (*KEYS, DEL):
            assert f"'{key}'" in src, key

    def test_label_is_stamped_after_the_collapse(self):
        """The registry key is written AFTER the declared-grain groupby."""
        src = _source()
        i_collapse = src.index("harv.groupby(_grain, as_index=False, dropna=False)")
        i_stamp = src.index("harv['Derivation'] = pd.array(")
        assert i_stamp > i_collapse
        # and the collapse carries the branch through, so the stamp cannot
        # land on a row the collapse assembled from two
        assert "'_branch':        'first'" in src

    def test_the_unresolved_warning_names_the_three_grounds(self):
        """Review item 6: the deletion's warning used to say the sale
        CONTRADICTS every row; the class holds three grounds."""
        src = _source()
        i = src.index('registered as the deletion "')      # the f-string, not the docstring
        head = src[i - 1500:i]
        assert 'three' in head and 'grounds' in head
        assert 'CONTRADICTS' in head and 'SEPARATING' in head and 'claimed' in head

    @pytest.mark.parametrize('wave', WAVES)
    def test_wave_script_asserts_uniqueness_on_the_declared_grain(self, wave):
        src = (ROOT / wave / '_' / 'crop_production.py').read_text()
        assert "_GRAIN = ['t', 'i', 'plot_id', 'crop', 'u', 'condition']" in src
        assert 'assert not df.reset_index().duplicated(_GRAIN).any()' in src

    @pytest.mark.parametrize('wave', ['2010-11', '2013-14'])
    def test_early_wave_docstrings_name_the_sale_side_su(self, wave):
        """The reviewer's follow-up on the GH #833 bundle."""
        src = (ROOT / wave / '_' / 'crop_production.py').read_text()
        head = src.split('"""')[1]
        assert 'ag_i02c' in head and 'ag_q02c' in head and W in head

    @pytest.mark.parametrize('wave', WAVES)
    def test_inputs_source_table_matches_the_wave_script(self, malawi_mod, wave):
        """`_CROP_SALE_SOURCES` transcribes the scripts; a drift fails here."""
        src = (ROOT / wave / '_' / 'crop_production.py').read_text()
        for half in malawi_mod._CROP_SALE_SOURCES[wave]:
            for k in ('g', 'p', 'i', 'q'):
                assert half[k] in src, (wave, half[k])
            assert half['hhid'] in src, (wave, half['hhid'])
            for k in ('gplot', 'gcrop', 'pplot', 'icrop', 'qcrop'):
                assert half[k] in src, (wave, k, half[k])


# ---------------------------------------------------------------------------
# static: the decision function
# ---------------------------------------------------------------------------

class TestFunction:
    def test_all_null_unit_reference_below_floor(self, malawi_mod):
        ref = pd.DataFrame({'crop': ['Groundnut'], 'u': [None],
                            'condition': ['shelled'], 'logp': [np.log(10.0)]})
        fine, mid, cell = malawi_mod._su_reference(ref)
        assert fine.empty and mid.empty
        assert len(cell) == 1 and cell.index.get_level_values('u').isna().all()
        assert cell['cell_n'].iloc[0] == 1
        assert cell['cell_med'].iloc[0] == pytest.approx(np.log(10.0))

    def test_all_null_unit_reference_is_available_to_lookup(self, malawi_mod):
        ref = pd.DataFrame({
            'crop': ['Groundnut'] * 20, 'u': [None] * 20,
            'condition': ['shelled'] * 10 + ['unshelled'] * 10,
            'logp': [np.log(100.0)] * 10 + [np.log(30.0)] * 10,
        })
        fine, mid, cell = malawi_mod._su_reference(ref)
        assert len(fine) == len(mid) == len(cell) == 1
        assert fine.index.get_level_values('u').isna().all()
        assert fine['n_S'].iloc[0] == fine['n_U'].iloc[0] == 10
        assert cell['cell_n'].iloc[0] == 20
        median = (np.log(100.0) + np.log(30.0)) / 2
        assert cell['cell_med'].iloc[0] == pytest.approx(median)
        assert mid['m_S'].iloc[0] == pytest.approx(np.log(100.0) - median)
        assert mid['m_U'].iloc[0] == pytest.approx(np.log(30.0) - median)
        pairs = pd.DataFrame({'crop': ['Groundnut'] * 2,
                              'u': [None, 'Kilogramme']})
        found = malawi_mod._lookup_reference(pairs, fine, mid, cell)
        assert found.loc[0, 'rung'] == 'fine'
        assert found.loc[0, 'm_S'] == pytest.approx(np.log(100.0))
        assert found.loc[0, 'm_U'] == pytest.approx(np.log(30.0))
        assert found.loc[0, 'P'] == 1.0
        assert pd.isna(found.loc[1, 'rung'])
        assert found.loc[1, ['m_S', 'm_U', 'P']].isna().all()

    def test_no_options(self):
        sig = inspect.signature(resolve_callable(FUNCTION))
        assert list(sig.parameters) == ['condition', 'condition_sold', 'log_price',
                                        'median_shelled', 'median_unshelled',
                                        'p_shelled_over_unshelled']
        assert all(p.default is inspect.Parameter.empty for p in sig.parameters.values())

    def test_thresholds_are_the_probes(self, malawi_mod):
        assert malawi_mod.SALE_BASIS_MIN_REFERENCE == 10
        assert malawi_mod.SALE_BASIS_SEPARATION_P == 0.75
        assert malawi_mod.SALE_BASIS_MARGIN == 0.5
        assert malawi_mod.SALE_BASIS_MIN_CELL == 3

    def test_prob_greater_is_the_mann_whitney_auc(self, malawi_mod):
        rng = np.random.default_rng(0)
        a = rng.integers(0, 6, 40).astype(float)      # ties on purpose
        b = rng.integers(0, 6, 25).astype(float)
        brute = np.mean([(x > y) + 0.5 * (x == y) for x in a for y in b])
        assert malawi_mod._prob_greater(a, b) == pytest.approx(brute)
        assert malawi_mod._prob_greater([5, 6], [1, 2]) == 1.0
        assert np.isnan(malawi_mod._prob_greater([], [1]))

    def test_the_rungs(self, malawi_mod):
        fn = resolve_callable(FUNCTION)
        S, U, NA, UNK = 'shelled', 'unshelled', 'shell_not_applicable', 'unknown_condition'
        mS, mU = np.log(100.0), np.log(30.0)
        hi, lo, mid = np.log(100.0), np.log(30.0), (mS + mU) / 2
        cases = [
            # condition, condition_sold, logp, mS, mU, P        -> expected
            ((S, S, hi, mS, mU, 0.9), ''),                   # exact
            ((NA, NA, hi, mS, mU, 0.9), ''),                 # exact (both stated)
            ((UNK, S, hi, mS, mU, 0.9), W),                  # wildcard
            ((S, UNK, hi, np.nan, np.nan, np.nan), W),
            ((UNK, UNK, np.nan, np.nan, np.nan, np.nan), W), # not an exact match
            ((S, NA, hi, np.nan, np.nan, np.nan), NR),       # no reference: its own key
            ((NA, U, hi, np.nan, np.nan, np.nan), NR),
            ((NA, U, lo, mS, mU, 0.55), N),                  # MEASURED: does not separate
            ((S, NA, hi, mS, mU, 0.3), N),                   # 0.3 is inside (0.25, 0.75)
            ((S, NA, hi, mS, mU, 0.9), P),                   # NA sale, price says S, row S
            ((U, NA, hi, mS, mU, 0.9), None),                # ... but the row is U
            ((U, NA, lo, mS, mU, 0.9), P),
            ((NA, S, hi, mS, mU, 0.9), P),                   # S sale vs NA row, corroborated
            ((NA, S, lo, mS, mU, 0.9), OV),                  # ... contradicted: price OVERRULES
            ((NA, U, hi, mS, mU, 0.9), OV),
            ((NA, U, lo, mS, mU, 0.2), P),                   # P <= 0.25 separates too
            ((S, NA, mid, mS, mU, 0.9), None),               # separates, margin fails
            ((NA, S, mid, mS, mU, 0.9), None),               # ... on the harvest-NA side too
            ((S, NA, np.nan, mS, mU, 0.9), None),            # separates, no price
            ((NA, S, np.nan, mS, mU, 0.9), None),
            ((U, S, hi, mS, mU, 0.9), CS),                   # contradiction, price with the sale
            ((U, S, lo, mS, mU, 0.9), CH),                   # ... with the harvest row
            ((S, U, hi, mS, mU, 0.2), CH),                   # P <= 0.25 separates too
            ((S, U, hi, mS, mU, 0.6), None),                 # contradiction, no separation
            ((S, U, hi, np.nan, np.nan, np.nan), None),      # contradiction, no reference
        ]
        cols = list(zip(*[c for c, _ in cases]))
        got = fn(*[np.array(c, dtype=object if i < 2 else float) for i, c in enumerate(cols)])
        assert list(got) == [e for _, e in cases]

    def test_the_margin_is_the_outer_quarter(self, malawi_mod):
        """margin > 0.5 x gap admits a price strictly between the medians
        when it sits in the outer quarter of the interval; the middle half
        is undecided (review item 1; the gate is unchanged, the prose was
        wrong)."""
        fn = resolve_callable(FUNCTION)
        S, NA = 'shelled', 'shell_not_applicable'
        mS, mU = np.log(100.0), np.log(30.0)
        gap = mS - mU
        inside_outer = mS - 0.2 * gap        # 20% of the way down from m_S
        inside_middle = mS - 0.3 * gap       # 30%: inside the middle half
        got = fn(np.array([S, S], dtype=object), np.array([NA, NA], dtype=object),
                 np.array([inside_outer, inside_middle]), np.array([mS, mS]),
                 np.array([mU, mU]), np.array([0.9, 0.9]))
        assert list(got) == [P, None]


# ---------------------------------------------------------------------------
# static: the ladder on synthetic frames
# ---------------------------------------------------------------------------

def _harv(rows):
    """rows: (i, plot, crop, code, u, condition, qty)."""
    n = len(rows)
    return pd.DataFrame({
        't': ['2010-11'] * n,
        'i': [r[0] for r in rows], 'plot_id': [r[1] for r in rows],
        'crop': [r[2] for r in rows],
        '_crop_code': pd.array([r[3] for r in rows], dtype='Int64'),
        'u': pd.array([r[4] for r in rows], dtype='string'),
        'condition': pd.array([r[5] for r in rows], dtype='string'),
        'crop_variety': pd.array(['x'] * n, dtype='string'),
        'Quantity': pd.array([float(r[6]) for r in rows], dtype='Float64'),
        'planting_month': pd.array([11] * n, dtype='Int64'),
        'harvest_month': pd.array([6] * n, dtype='Int64'),
        'intercropped': pd.array([False] * n, dtype='boolean'),
        'perennial': pd.array([False] * n, dtype='boolean'),
    })


def _sale(rows):
    """rows: (i, code, u, condition_sold, qty, value)."""
    return pd.DataFrame({
        'i': [r[0] for r in rows],
        '_crop_code': pd.array([r[1] for r in rows], dtype='Int64'),
        'u': pd.array([r[2] for r in rows], dtype='string'),
        'condition_sold': pd.array([r[3] for r in rows], dtype='string'),
        'Quantity_sold': pd.array([r[4] for r in rows], dtype='Float64'),
        'Value_sold': pd.array([r[5] for r in rows], dtype='Float64'),
    })


S, U, NA, UNK = 'shelled', 'unshelled', 'shell_not_applicable', 'unknown_condition'
KG = 'Kilogramme'


class TestAssemble:
    def test_exact_null_unit_pair_keeps_amounts_and_rejects_other_unit(self, malawi_mod):
        harv = malawi_mod._consolidate_harvest([
            _harv([('h1', 'R1', 'Groundnut', 11, None, S, 10)])])
        sale = _sale([('h1', 11, None, S, 3, 30),
                      ('h1', 11, '50 kg Bag', S, 3, 300)])
        out, decisions, tallies = malawi_mod._attach_sales('2010-11', harv, sale)
        pd.testing.assert_frame_equal(harv, out[harv.columns])
        assert out['u'].isna().all()
        assert out['Quantity_sold'].iloc[0] == 3.0
        assert out['Value_sold'].iloc[0] == 30.0
        assert out['_branch'].iloc[0] == ''
        assert decisions.loc[decisions['u'].isna(), 'outcome'].tolist() == ['exact']
        assert decisions.loc[decisions['u'].notna(), 'outcome'].isna().all()
        assert tallies['sale_basis_ladder']['reference']['sales'] == 1
        assert tallies['sale_suppressed']['sales'] == 0
        assert tallies['sale_basis_mismatch']['sales'] == 0

    def test_exact_match_carries_no_key_and_column_exists_without_sales(self, malawi_mod):
        out = malawi_mod.assemble_crop_production(
            '2010-11', [_harv([('h1', 'R1', 'Groundnut', 11, KG, S, 10)])],
            [_sale([('h1', 11, KG, S, 4, 2500)])])
        assert out['Value_sold'].iloc[0] == 2500.0
        assert str(out['Derivation'].dtype) == 'string'
        assert out['Derivation'].isna().all()
        assert out.attrs['sale_basis_ladder']['exact']['sales'] == 1
        empty = malawi_mod.assemble_crop_production(
            '2010-11', [_harv([('h1', 'R1', 'Groundnut', 11, KG, S, 10)])], [])
        assert 'Derivation' in empty.columns and empty['Derivation'].isna().all()

    def test_wildcard_is_keyed_and_unknown_unknown_is_not_exact(self, malawi_mod):
        with warnings.catch_warnings():
            warnings.simplefilter('error', malawi_mod.SaleAttachmentWarning)
            out = malawi_mod.assemble_crop_production(
                '2010-11',
                [_harv([('h1', 'R1', 'Mango', 1004, KG, UNK, 10),
                        ('h2', 'R1', 'Groundnut', 11, KG, S, 10)])],
                [_sale([('h1', 1004, KG, UNK, 4, 2500),
                        ('h2', 11, KG, UNK, 4, 2500)])])
        assert (out['Derivation'] == W).all()
        assert out.attrs['sale_basis_ladder'][W]['sales'] == 2

    def test_an_unsold_zero_line_carries_no_key(self, malawi_mod):
        """`_sale_block` sums an unsold line to 0/0 under unknown_condition."""
        out = malawi_mod.assemble_crop_production(
            '2010-11', [_harv([('h1', 'R1', 'Groundnut', 11, KG, S, 10)])],
            [_sale([('h1', 11, KG, UNK, 0, 0)])])
        assert out['Value_sold'].iloc[0] == 0.0          # served as before
        assert out['Derivation'].isna().all()
        assert out.attrs['sale_basis_ladder'][W] == {
            'sales': 1, 'value': 0.0, 'zero_quantity': 1, 'rungs': {}}

    def test_not_applicable_with_no_reference_is_its_own_key(self, malawi_mod):
        """No reference cell at all: admitted on the ABSENCE of evidence,
        under `sale-basis-no-reference` (ruling 2), not the measured key."""
        with warnings.catch_warnings():
            warnings.simplefilter('error', malawi_mod.SaleAttachmentWarning)
            out = malawi_mod.assemble_crop_production(
                '2010-11',
                [_harv([('h1', 'R1', 'Tobacco', 5, KG, S, 10),
                        ('h2', 'R1', 'Tobacco', 5, KG, NA, 10)])],
                [_sale([('h1', 5, KG, NA, 4, 2500),
                        ('h2', 5, KG, S, 4, 2500)])])
        assert (out['Derivation'] == NR).all()
        assert out.attrs['sale_basis_ladder'][NR]['not_applicable_side'] == {'sale': 1, 'harvest': 1}
        assert out.attrs['sale_basis_ladder'][N]['sales'] == 0
        assert out.attrs['sale_basis_mismatch']['sales'] == 0

    def test_not_applicable_with_a_non_separating_reference_is_the_measured_key(self, malawi_mod):
        """12 shelled and 12 unshelled Maize kg sales at ONE price: the
        reference exists and measurably does not separate (P ~ 0.5), so the
        NA pair lands on `sale-basis-not-applicable`, not no-reference."""
        harv, sale = [], []
        for k in range(12):
            harv += [(f's{k}', 'R1', 'Maize', 1, KG, S, 10), (f'u{k}', 'R1', 'Maize', 1, KG, U, 10)]
            sale += [(f's{k}', 1, KG, S, 1, 50 + (k % 3)), (f'u{k}', 1, KG, U, 1, 50 + ((k + 1) % 3))]
        harv += [('x', 'R1', 'Maize', 1, KG, NA, 10), ('y', 'R1', 'Maize', 1, KG, S, 10)]
        sale += [('x', 1, KG, S, 1, 51), ('y', 1, KG, NA, 1, 51)]
        with warnings.catch_warnings():
            warnings.simplefilter('error', malawi_mod.SaleAttachmentWarning)
            out = malawi_mod.assemble_crop_production('2010-11', [_harv(harv)], [_sale(sale)])
        d = out.reset_index().set_index('i')['Derivation']
        assert d.loc['x'] == N and d.loc['y'] == N
        assert d.drop(['x', 'y']).isna().all()
        L = out.attrs['sale_basis_ladder']
        assert L['reference']['separating_fine'] == 0 and L['reference']['cells_fine'] == 1
        assert L[N]['sales'] == 2 and L[N]['rungs'] == {'fine': 2}
        assert L[N]['not_applicable_side'] == {'sale': 1, 'harvest': 1}
        assert L[NR]['sales'] == 0

    def test_contradiction_with_no_reference_is_unresolved(self, malawi_mod):
        with pytest.warns(malawi_mod.SaleAttachmentWarning, match='CONTRADICTS'):
            out = malawi_mod.assemble_crop_production(
                '2010-11', [_harv([('h1', 'R1', 'Groundnut', 11, KG, S, 10)])],
                [_sale([('h1', 11, KG, U, 4, 2500)])])
        assert out['Value_sold'].isna().all() and out['Derivation'].isna().all()
        assert out.attrs['sale_basis_mismatch'] == {
            'wave': '2010-11', 'sales': 1, 'rows': 1, 'value': 2500.0}
        assert out.attrs['sale_basis_ladder'][DEL]['sales'] == 1

    @pytest.fixture
    def priced(self):
        """A reference of 12 shelled Groundnut kg sales at ~100 and 12
        unshelled at ~30 (exact matches), plus the cases under test."""
        harv, sale = [], []
        for k in range(12):
            harv += [(f's{k}', 'R1', 'Groundnut', 11, KG, S, 10),
                     (f'u{k}', 'R1', 'Groundnut', 11, KG, U, 10)]
            sale += [(f's{k}', 11, KG, S, 1, 100 + k),
                     (f'u{k}', 11, KG, U, 1, 30 + k)]
        harv += [
            ('x', 'R1', 'Groundnut', 11, KG, S, 10),     # split plot-crop, NA sale priced S
            ('x', 'R1', 'Groundnut', 11, KG, U, 4),
            ('y', 'R1', 'Groundnut', 11, KG, S, 10),     # row S, sale U priced S -> harvest wins
            ('z', 'R1', 'Groundnut', 11, KG, U, 10),     # row U, sale S priced S -> sale wins
            ('m', 'R1', 'Groundnut', 11, KG, S, 10),     # NA sale priced between -> unresolved
            ('n', 'R1', 'Groundnut', 11, KG, NA, 10),    # NA row, sale S priced S -> corroborated
            ('o', 'R1', 'Groundnut', 11, KG, NA, 10),    # NA row, sale S priced U -> OVERRULED
            ('q', 'R1', 'Groundnut', 11, KG, S, 10),     # exact S sale + NA sale priced S -> the
                                                         # second finds its row claimed: unresolved
        ]
        sale += [('x', 11, KG, NA, 1, 100), ('y', 11, KG, U, 1, 100),
                 ('z', 11, KG, S, 1, 100), ('m', 11, KG, NA, 1, 55),
                 ('n', 11, KG, S, 1, 100), ('o', 11, KG, S, 1, 30),
                 ('q', 11, KG, S, 1, 100), ('q', 11, KG, NA, 1, 100)]
        return _harv(harv), _sale(sale)

    def test_price_rungs_on_a_separating_reference(self, malawi_mod, priced):
        harv, sale = priced
        with pytest.warns(malawi_mod.SaleAttachmentWarning, match='CONTRADICTS'):
            out = malawi_mod.assemble_crop_production('2010-11', [harv], [sale])
        out = out.reset_index().set_index(['i', 'condition'])
        d, v = out['Derivation'], out['Value_sold']
        assert d.loc[('x', S)] == P and v.loc[('x', S)] == 100.0
        assert pd.isna(v.loc[('x', U)]) and pd.isna(d.loc[('x', U)])
        assert d.loc[('y', S)] == CH and v.loc[('y', S)] == 100.0
        assert d.loc[('z', U)] == CS and v.loc[('z', U)] == 100.0
        assert pd.isna(v.loc[('m', S)])
        assert d.loc[('n', NA)] == P
        assert d.loc[('o', NA)] == OV and v.loc[('o', NA)] == 30.0      # ruling 1
        assert pd.isna(d.loc[('q', S)]) and v.loc[('q', S)] == 100.0   # the exact one
        for k in range(12):                                             # reference untouched
            assert pd.isna(d.loc[(f's{k}', S)]) and pd.isna(d.loc[(f'u{k}', U)])
        L = out.attrs['sale_basis_ladder']
        assert L['reference'] == {'sales': 25, 'cells_fine': 1, 'cells_mid': 1, 'separating_fine': 1}
        assert L[P]['sales'] == 2 and L[P]['not_applicable_side'] == {'sale': 1, 'harvest': 1}
        assert L[OV]['sales'] == 1 and L[OV]['not_applicable_side'] == {'sale': 0, 'harvest': 1}
        assert L[OV]['value'] == 30.0 and L[OV]['rungs'] == {'fine': 1}
        assert L[N]['sales'] == 0 and L[NR]['sales'] == 0
        assert L[CS]['sales'] == 1 and L[CH]['sales'] == 1
        assert L[P]['rungs'] == {'fine': 2}
        assert out.attrs['sale_basis_mismatch']['sales'] == 2          # m, q's NA sale
        assert L[DEL]['grounds'] == {'contradiction_no_separation': 0,
                                     'contradiction_price_undecided': 0,
                                     'na_sale_price_undecided': 1,        # m
                                     'na_harvest_price_undecided': 0,
                                     'claimed_by_tier_a': 1}              # q
        assert out.attrs['sale_suppressed']['sales'] == 0

    def test_the_overruled_basis_is_on_the_decision_frame(self, malawi_mod, priced):
        """The served row keeps its recorded `condition` (NA); the basis the
        price chose lives on the decision frame, as on every price rung."""
        harv, sale = priced
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', malawi_mod.SaleAttachmentWarning)
            _, dec, _ = malawi_mod._attach_sales(
                '2010-11', malawi_mod._consolidate_harvest([harv]), sale)
        o = dec[dec['i'] == 'o'].iloc[0]
        assert o['outcome'] == OV and o['basis'] == U and o['condition_sold'] == S
        n = dec[dec['i'] == 'n'].iloc[0]
        assert n['outcome'] == P and n['basis'] == S

    def test_monotone_over_the_bundle_rule(self, malawi_mod, priced):
        """Nothing tier A attaches is taken away by tier B: the exact and
        wildcard attachments are identical with and without a reference."""
        harv, sale = priced
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', malawi_mod.SaleAttachmentWarning)
            full = malawi_mod.assemble_crop_production('2010-11', [harv], [sale])
            tier_a_only = malawi_mod.assemble_crop_production(
                '2010-11', [harv[~harv['i'].isin(['x', 'y', 'z', 'm', 'n', 'o'])]],
                [sale[~sale['i'].isin(['x', 'y', 'z', 'm', 'n', 'o'])]])
        a = tier_a_only.reset_index().set_index(['i', 'condition'])['Value_sold'].dropna()
        f = full.reset_index().set_index(['i', 'condition'])['Value_sold']
        assert (f.reindex(a.index) == a).all()


# ---------------------------------------------------------------------------
# data-gated
# ---------------------------------------------------------------------------

@pytest.fixture(scope='module')
def country():
    import lsms_library as ll
    return ll.Country(COUNTRY)


@pytest.fixture(scope='module')
def delivered(country):
    try:
        cp = country.crop_production()
    except Exception as e:                    # pragma: no cover - no microdata
        pytest.skip(f'{COUNTRY} crop_production not buildable here: {e}')
    return cp


@pytest.mark.requires_s3
@pytest.mark.slow
class TestDelivered:
    @pytest.mark.parametrize('wave', WAVES)
    def test_rows_per_key_per_wave(self, delivered, wave):
        x = delivered.xs(wave, level='t', drop_level=False)
        assert len(x) == TOTAL_ROWS[wave]
        got = x['Derivation'].value_counts().to_dict()
        assert {k: got.get(k, 0) for k in KEYS} == ROWS[wave]
        assert set(got) <= set(KEYS)

    @pytest.mark.parametrize('wave', WAVES)
    def test_value_sold_moved_by_the_re_attached_sales(self, delivered, wave):
        x = delivered.xs(wave, level='t', drop_level=False)
        assert x['Value_sold'].sum() == pytest.approx(VALUE_SOLD_AFTER[wave], rel=1e-9)
        assert VALUE_SOLD_AFTER[wave] > VALUE_SOLD_BEFORE[wave]

    def test_a_key_marks_a_sale_and_only_a_sale(self, delivered):
        keyed = delivered[delivered['Derivation'].notna()]
        assert ((keyed['Quantity_sold'] > 0) | (keyed['Value_sold'] > 0)).all()
        # and the unkeyed side still carries the exact matches AND the 0.0
        # "did not sell" lines -- Derivation is NA on both
        unkeyed = delivered[delivered['Derivation'].isna()]
        assert (unkeyed['Value_sold'] > 0).sum() > 10_000
        assert (unkeyed['Value_sold'] == 0).sum() > 1_000

    def test_attrs_summary_counts_the_labelled_rows_and_lists_the_deletion(self, delivered):
        summ = delivered.attrs['derivations'][COUNTRY]
        assert set(summ) == set(KEYS) | {DEL}
        for key in KEYS:
            assert summ[key]['rows'] == int((delivered['Derivation'] == key).sum())
            assert summ[key]['in'] == 'crop_production'
        assert summ[DEL]['rows'] == 0

    def test_the_two_new_keys_stamp_exactly_their_rows(self, delivered):
        """The overrule rung is Groundnut on a not-applicable harvest row
        with an S/U sale; the no-reference rung never lands on a Groundnut
        row (every wave has a Groundnut reference)."""
        ov = delivered[delivered['Derivation'] == OV]
        assert len(ov) == sum(r[OV] for r in ROWS.values()) == 4
        assert set(ov.index.get_level_values('crop')) == {'Groundnut'}
        assert set(ov.index.get_level_values('condition')) == {'shell_not_applicable'}
        assert (ov['Value_sold'] > 0).all()
        assert ov['Value_sold'].sum() == pytest.approx(157_000.0)
        nr = delivered[delivered['Derivation'] == NR]
        assert len(nr) == sum(r[NR] for r in ROWS.values()) == 296
        assert 'Groundnut' not in set(nr.index.get_level_values('crop'))
        assert nr['Value_sold'].sum() == pytest.approx(13_270_507.0)
        n = delivered[delivered['Derivation'] == N]
        assert len(n) == sum(r[N] for r in ROWS.values()) == 114
        assert n['Value_sold'].sum() == pytest.approx(25_416_200.0)
        # Cotton is entirely no-reference; Tobacco is split across the two
        crops = nr.index.get_level_values('crop').value_counts()
        assert crops['Cotton'] == 57 and crops['Tobacco'] == 37
        assert n.index.get_level_values('crop').value_counts()['Tobacco'] == 72

    def test_registry_lists_every_key(self, country):
        import lsms_library as ll
        assert set(country.derivations('crop_production')) == set(KEYS) | {DEL}
        tbl = ll.derivations()
        assert set(KEYS) | {DEL} <= set(tbl.index)

    def test_derived_food_tables_do_not_carry_the_column(self, country):
        fe = country.food_expenditures()
        assert 'Derivation' not in fe.columns and len(fe) > 0

    def test_inputs_join_to_the_served_rows(self, country, delivered):
        """2013-14, the smallest wave: the contradiction-price-sale inputs."""
        wave = '2013-14'
        try:
            raw = country.derivation_inputs(CS, wave=wave)
        except Exception as e:                # pragma: no cover - no microdata
            pytest.skip(f'{COUNTRY} {wave} sources not available here: {e}')
        assert list(raw.index.names) == ['t', 'i', 'crop', 'u']
        assert {'ag_i02a', 'ag_i02b', 'ag_i02c', 'ag_i03'} <= set(raw.columns)
        assert (raw['outcome'] == CS).all()
        served = delivered.xs(wave, level='t', drop_level=False)
        served = served[served['Derivation'] == CS]
        served_keys = set(zip(served.index.get_level_values('i'),
                              served.index.get_level_values('crop'),
                              served.index.get_level_values('u')))
        raw_keys = set(zip(raw.index.get_level_values('i'),
                           raw.index.get_level_values('crop'),
                           raw.index.get_level_values('u')))
        assert served_keys <= raw_keys, sorted(served_keys - raw_keys)[:5]
        assert len(served) == ROWS[wave][CS]
        # (i, crop, u) is NOT unique on the served side: a household growing
        # two Groundnut VARIETIES (codes 12 and 13, both `crop` 'Groundnut')
        # on two plots is single-plot for each code and gets a sale on each
        # (2013-14 206023630354, R02 / R03).  The raw frame keeps
        # `_crop_code`, so the pair is separable through `crop_variety`.
        assert '_crop_code' in raw.columns
        assert len(served) - len(served_keys) == 1        # exactly that household

    def test_the_deletion_inputs_are_the_suppressed_sales(self, country):
        wave = '2013-14'
        try:
            raw = country.derivation_inputs(DEL, wave=wave)
        except Exception as e:                # pragma: no cover - no microdata
            pytest.skip(f'{COUNTRY} {wave} sources not available here: {e}')
        keys = raw.reset_index()[['i', '_crop_code', 'u', 'condition_sold']].drop_duplicates()
        assert len(keys) == UNRESOLVED[wave][0]
        assert (raw['outcome'] == 'unresolved').all()

    @pytest.mark.parametrize('wave', ['2016-17', '2019-20'])
    def test_the_overrule_inputs_are_the_contradicted_sales(self, country, wave):
        try:
            raw = country.derivation_inputs(OV, wave=wave)
        except Exception as e:                # pragma: no cover - no microdata
            pytest.skip(f'{COUNTRY} {wave} sources not available here: {e}')
        keys = raw.reset_index()[['i', '_crop_code', 'u', 'condition_sold']].drop_duplicates()
        assert len(keys) == 2
        assert (raw['outcome'] == OV).all()
        assert set(raw['condition_sold']) <= {'shelled', 'unshelled'}
        # the price-chosen basis is the OTHER one from the recorded answer
        assert (raw['basis'] != raw['condition_sold']).all()
        assert set(raw.index.get_level_values('crop')) == {'Groundnut'}

    def test_unresolved_grounds_sum_to_the_tally(self, malawi_mod):
        """Replayed from sources for the smallest wave: the by-ground count
        partitions the deletion."""
        wave = '2013-14'
        try:
            harvest, sale, _ = malawi_mod._crop_pieces(wave)
        except Exception as e:                # pragma: no cover - no microdata
            pytest.skip(f'{COUNTRY} {wave} sources not available here: {e}')
        harv = malawi_mod._consolidate_harvest(harvest)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            _, dec, tallies = malawi_mod._attach_sales(wave, harv, pd.concat(sale, ignore_index=True))
        g = tallies['sale_basis_ladder'][DEL]['grounds']
        assert set(g) == set(UNRESOLVED_GROUNDS)
        assert sum(g.values()) == tallies['sale_basis_mismatch']['sales'] == UNRESOLVED[wave][0]
        assert g == {'contradiction_no_separation': 16, 'contradiction_price_undecided': 5,
                     'na_sale_price_undecided': 0, 'na_harvest_price_undecided': 0,
                     'claimed_by_tier_a': 0}
