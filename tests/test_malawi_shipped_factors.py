"""Malawi's two SHIPPED IHS5 crop conversion tables, and the `condition` level.

GH #854.  Malawi's IHS5 (2019-20) v04 release supplemented the survey with two
crop-side conversion files that sat DVC-tracked and unwired; separately, all
four wave scripts passed `condition='ag_g13c'` into `_harvest_block`, which
accepted the argument and discarded it.  The two are one problem: the shipped
tables key their kg factors ON the shelled/unshelled axis, so without a
`condition` level on `crop_production` the seasonal table is AMBIGUOUS and
`harvest_kg(shipped_factors=...)` refuses it outright.

What is pinned here, and why each:

* the loader's SHAPE and KEYS -- the frame must be joinable by
  `transformations._shipped_factor_lookup` without the caller doing anything
  but resolve `region`, so every key it declares must be in
  `SHIPPED_FACTOR_JOIN_LEVELS`;
* the code->label MATCH RATES, by exact number.  The files key on a crop
  VARIETY ('MAIZE HYBRID') and a wider unit namespace than the harvest module
  asks; a silent drop in either match rate is the failure mode that makes a
  shipped table match nothing while looking fine;
* the VARIETY COLLAPSE refusal -- 23 seasonal + 8 tree keys whose varieties
  disagree are DROPPED, never averaged;
* the CONDITION vocabulary: bare form tokens, declared in `data_info.yml`,
  and `shell_not_applicable` distinct from `unknown_condition`;
* the tree table's `condition` is the SENTINEL STRING, not NA -- an NA there
  would match only a NULL condition, which no crop_production row has, and the
  table would serve zero perennial rows.

The pure-vocabulary and pure-mapping tests run anywhere.  Anything that reads
a `.dta` or builds a table is data-gated and skips cleanly.
"""
from __future__ import annotations

import importlib.util
import sys
import warnings

import numpy as np
import pandas as pd
import pytest
import yaml

from lsms_library.country import Country
from lsms_library.paths import countries_root
from lsms_library.transformations import (
    SHIPPED_FACTOR_JOIN_LEVELS,
    harvest_kg,
    harvest_kg_factors,
)

MALAWI_ = countries_root() / 'Malawi' / '_'

# Row counts of the two shipped files, and the match rates they produce.
# Measured cold 2026-09-09 against the v04 release.
SEASONAL_ROWS, TREE_ROWS = 857, 153
# Keyed on `crop` (by_variety=False): the varieties must be reduced.
SEASONAL_KEYS, TREE_KEYS = 291, 84
SEASONAL_AMBIGUOUS, TREE_AMBIGUOUS = 23, 8
# Keyed on `crop_variety` (the default): no reduction is needed at all, so the
# key count is just the unit-matched row count and NOTHING is refused.
SEASONAL_VARIETY_KEYS, TREE_VARIETY_KEYS = 703, 110
SEASONAL_UNIT_MATCHED, TREE_UNIT_MATCHED = 703, 110
# Unit codes the conversion files carry that the harvest module never asks.
SEASONAL_UNIT_MISSES = ['14', '31A', '31B', '31C', '98']
TREE_UNIT_MISSES = ['14', '8A', '8B', '8C']

CONDITIONS = ('shelled', 'unshelled', 'shell_not_applicable',
              'unknown_condition')


@pytest.fixture(scope='module')
def malawi_mod():
    """The Malawi country module, imported by path.

    It is not a package -- the wave scripts reach it with a `sys.path` append
    -- so it is loaded from its file rather than imported by name.
    """
    path = MALAWI_ / 'malawi.py'
    if not path.exists():                                   # pragma: no cover
        pytest.skip(f'{path} not present')
    sys.path.insert(0, str(MALAWI_))
    try:
        spec = importlib.util.spec_from_file_location('malawi_gh854', path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    except Exception as exc:                                # pragma: no cover
        pytest.skip(f'Malawi module unavailable: {type(exc).__name__}: {exc}')
    finally:
        sys.path.remove(str(MALAWI_))
    return mod


@pytest.fixture(scope='module')
def shipped(malawi_mod):
    try:
        return malawi_mod.crop_conversion_factors()
    except Exception as exc:                                # pragma: no cover
        pytest.skip(f'IHS5 conversion files unavailable: '
                    f'{type(exc).__name__}: {exc}')


@pytest.fixture(scope='module')
def cp():
    try:
        df = Country('Malawi').crop_production()
    except Exception as exc:                                # pragma: no cover
        pytest.skip(f'Malawi crop_production unavailable: '
                    f'{type(exc).__name__}: {exc}')
    if df is None or df.empty:                              # pragma: no cover
        pytest.skip('Malawi crop_production empty')
    return df


def _strip_condition(cp):
    """`crop_production` with the `condition` level removed."""
    flat = cp.reset_index().drop(columns=['condition'])
    return flat.set_index([n for n in cp.index.names if n != 'condition'])


# --------------------------------------------------------------------------
# The canonical vocabulary (no data needed)
# --------------------------------------------------------------------------

class TestConditionVocabulary:
    def test_the_three_malawi_tokens_are_declared_cross_country(self):
        """`data_info.yml` is the single source of truth, not `malawi.py`."""
        info = yaml.safe_load(
            (countries_root().parent / 'data_info.yml').read_text())
        spell = info['Columns']['crop_production']['condition']['spellings']
        for token in CONDITIONS:
            assert token in spell, (
                f'{token!r} is written onto crop_production.condition by '
                'Malawi but is not in the canonical vocabulary; '
                '_enforce_canonical_spellings reads that list.')

    def test_shell_not_applicable_is_not_the_missing_sentinel(self):
        """Two different facts, and conflating them loses one.

        Code 3 is the instrument's OWN answer ('this crop has no shell').
        `unknown_condition` is what malawi.py mints for a NULL or off-scheme
        code and for every perennial row, where the question is never asked.
        """
        assert 'shell_not_applicable' != 'unknown_condition'

    def test_no_dryness_family_is_asserted(self, malawi_mod):
        """Malawi's 13c asks the FORM only; a dryness token would be invented.

        Uganda's scheme crosses dryness with form; borrowing `dried_grain`
        for Malawi's 'S: SHELLED' would claim the survey recorded a dryness
        it never asked about.
        """
        table = malawi_mod._malawi_code_map('harmonize_crop_condition')
        assert set(table.values()) == {'shelled', 'unshelled',
                                       'shell_not_applicable'}
        assert not any(str(v).startswith(('dried', 'dry_', 'fresh', 'green'))
                       for v in table.values())

    def test_off_scheme_and_null_codes_become_the_sentinel(self, malawi_mod):
        """Including the NON-INTEGRAL 4.5 the 2016-17 panel carries.

        `.astype('Int64')`, which every other Malawi code map uses, raises on
        it under pandas 3.0 -- which is why `_crop_conditions` does not.
        """
        got = malawi_mod._crop_conditions(
            pd.Series([1, 2, 3, np.nan, 5, 15, 4.5, 0]))
        assert list(got) == ['shelled', 'unshelled', 'shell_not_applicable',
                             'unknown_condition', 'unknown_condition',
                             'unknown_condition', 'unknown_condition',
                             'unknown_condition']

    def test_condition_is_a_declared_index_level(self):
        scheme = yaml.safe_load(
            (MALAWI_ / 'data_scheme.yml').read_text())
        idx = scheme['Data Scheme']['crop_production']['index']
        assert 'condition' in idx, (
            f'crop_production index is {idx!r}; #854 made `condition` a level '
            'and the shipped-factor join depends on it.')


# --------------------------------------------------------------------------
# The loader
# --------------------------------------------------------------------------

class TestLoaderShape:
    def test_every_key_is_joinable(self, shipped):
        """A key outside SHIPPED_FACTOR_JOIN_LEVELS is silently dropped."""
        for name in shipped.index.names:
            assert name in SHIPPED_FACTOR_JOIN_LEVELS, (
                f'{name!r} is not a join level, so the transform would ignore '
                f'it: {list(SHIPPED_FACTOR_JOIN_LEVELS)}')

    def test_it_keys_on_the_unit(self, shipped):
        """Enforced by the transform: a kg-per-unit factor needs a unit."""
        assert 'u' in shipped.index.names

    def test_the_canonical_columns(self, shipped):
        assert list(shipped.columns) == ['KgFactor', 'Source']
        assert set(shipped['Source']) == {'IHS5 seasonal', 'IHS5 tree'}

    def test_no_t_level_by_default(self, shipped):
        """The 2019-20 vintage is applied to every wave -- deliberately."""
        assert 't' not in shipped.index.names

    def test_waves_argument_fences_the_extrapolation(self, malawi_mod):
        one = malawi_mod.crop_conversion_factors(waves=['2019-20'])
        assert 't' in one.index.names
        assert set(one.index.get_level_values('t')) == {'2019-20'}

    def test_the_vintage_is_stated(self, malawi_mod, shipped):
        """A 2019-20 artefact served to 2010-11 must SAY it is one."""
        assert '2019-20' in shipped.attrs['vintage']
        doc = malawi_mod.crop_conversion_factors.__doc__
        assert 'VINTAGE' in doc and '2019-20' in doc

    def test_the_index_is_unique(self, shipped):
        """`_shipped_factor_lookup` refuses a duplicate -- even a lossless one."""
        assert shipped.index.is_unique

    def test_factors_are_positive_and_finite(self, shipped):
        v = shipped['KgFactor'].to_numpy(dtype=float)
        assert np.isfinite(v).all() and (v > 0).all()

    def test_refuses_to_return_nothing(self, malawi_mod):
        with pytest.raises(ValueError, match='neither the seasonal nor'):
            malawi_mod.crop_conversion_factors(seasonal=False, tree=False)


class TestMatchRates:
    """Pinned by exact number: a silent drop here is the whole failure mode."""

    def test_every_crop_variety_decodes(self, shipped):
        d = shipped.attrs['loader']
        assert d['seasonal']['rows'] == SEASONAL_ROWS
        assert d['tree']['rows'] == TREE_ROWS
        assert d['seasonal']['crop_matched'] == SEASONAL_ROWS
        assert d['tree']['crop_matched'] == TREE_ROWS
        assert d['seasonal']['crop_unmatched_labels'] == []
        assert d['tree']['crop_unmatched_labels'] == []

    def test_every_condition_and_region_decodes(self, shipped):
        d = shipped.attrs['loader']
        assert d['seasonal']['condition_matched'] == SEASONAL_ROWS
        assert d['seasonal']['region_matched'] == SEASONAL_ROWS
        assert d['tree']['region_matched'] == TREE_ROWS

    def test_the_unit_namespace_is_wider_than_the_harvest_module(self, shipped):
        """PAIL MEDIUM / BUNDLE / HEAP / sized BUNCH have no harvest code.

        They are dropped and counted, never folded onto a coarser label:
        three bunch sizes with three weights sharing one 'Bunch' label would
        be exactly the disagreeing collapse this loader refuses elsewhere.
        """
        d = shipped.attrs['loader']
        assert d['seasonal']['unit_matched'] == SEASONAL_UNIT_MATCHED
        assert d['tree']['unit_matched'] == TREE_UNIT_MATCHED
        assert d['seasonal']['unit_unmatched_codes'] == SEASONAL_UNIT_MISSES
        assert d['tree']['unit_unmatched_codes'] == TREE_UNIT_MISSES

    def test_region_south_is_translated_to_the_served_spelling(self, shipped):
        """`cluster_features.Region` says 'Southern'; the file says 'South'.

        Untranslated it matches nothing, silently, for a third of the country.
        """
        assert set(shipped.index.get_level_values('region')) == {
            'North', 'Central', 'Southern'}


class TestVarietyKey:
    """The file's crop key is FINER than ours -- so we key on the variety.

    Keying on the collapsed `crop` forces a reduction and refuses 31 keys
    (6,890 served rows on Groundnut, Rice and Citrus).  Keying on
    `crop_variety` -- what EPAR does with `crop_code_long` -- needs no
    reduction at all.
    """

    def test_the_default_is_the_variety_key_and_refuses_nothing(self, shipped):
        assert shipped.attrs['keyed_on'] == 'crop_variety'
        assert 'crop_variety' in shipped.index.names
        assert 'crop' not in shipped.index.names
        d = shipped.attrs['loader']
        assert d['seasonal']['keys_kept'] == SEASONAL_VARIETY_KEYS
        assert d['tree']['keys_kept'] == TREE_VARIETY_KEYS
        assert d['seasonal']['keys_dropped_ambiguous'] == []
        assert d['tree']['keys_dropped_ambiguous'] == []
        assert len(shipped) == SEASONAL_VARIETY_KEYS + TREE_VARIETY_KEYS

    def test_the_collapsed_key_still_works_and_still_refuses(self, malawi_mod):
        """`by_variety=False` is the path for a pre-#854 frame."""
        old = malawi_mod.crop_conversion_factors(by_variety=False)
        assert old.attrs['keyed_on'] == 'crop'
        d = old.attrs['loader']
        assert d['seasonal']['keys_kept'] == SEASONAL_KEYS
        assert d['tree']['keys_kept'] == TREE_KEYS
        assert len(d['seasonal']['keys_dropped_ambiguous']) == SEASONAL_AMBIGUOUS
        assert len(d['tree']['keys_dropped_ambiguous']) == TREE_AMBIGUOUS
        assert len(old) == SEASONAL_KEYS + TREE_KEYS

    def test_the_variety_key_never_contradicts_the_collapsed_one(
            self, malawi_mod, cp, shipped):
        """Where both can serve a row they must return the SAME factor.

        This is what makes the switch safe: it fills cells the collapsed key
        had to leave empty and moves no number the collapsed key produced.
        Measured 2026-09-10: 81,886 rows in common, identical on every one.
        """
        old = malawi_mod.crop_conversion_factors(by_variety=False)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            cpr = malawi_mod.with_region(cp)
            fv = harvest_kg_factors(cpr, shipped_factors=shipped)
            fc = harvest_kg_factors(cpr, shipped_factors=old)
        both = np.isfinite(fv['kg_shipped']) & np.isfinite(fc['kg_shipped'])
        assert both.sum() > 50_000
        assert (fv.loc[both, 'kg_shipped'].to_numpy()
                == fc.loc[both, 'kg_shipped'].to_numpy()).all()
        gained = int((np.isfinite(fv['kg_shipped'])
                      & ~np.isfinite(fc['kg_shipped'])).sum())
        assert gained > 1_000, (
            f'the variety key gained only {gained} rows; it was adopted '
            'because it gains ~9,200 (all 31 refused keys)')

    def test_the_withdrawals_are_corrections_not_losses(self, malawi_mod, cp,
                                                        shipped):
        """The collapsed rule cannot tell "all agree" from "only one is here".

        The file carries ONE tobacco variety (Burley), so `nunique == 1` kept
        the key and handed Burley's factor to flue-cured, NNDF, SDF and
        oriental tobacco.  The variety key withdraws exactly those.
        """
        old = malawi_mod.crop_conversion_factors(by_variety=False)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            cpr = malawi_mod.with_region(cp)
            fv = harvest_kg_factors(cpr, shipped_factors=shipped)
            fc = harvest_kg_factors(cpr, shipped_factors=old)
        lost = (~np.isfinite(fv['kg_shipped'])
                & np.isfinite(fc['kg_shipped'])).to_numpy()
        assert 0 < lost.sum() < 1_000
        crops = set(fc.index.get_level_values('crop')[lost])
        assert 'Tobacco' in crops, (
            f'expected the tobacco over-reach among the withdrawals; got {crops}')
        # The file really does carry only Burley.
        varieties = set(shipped.index.get_level_values('crop_variety'))
        tob = {v for v in varieties if str(v).startswith('Tobacco')}
        assert tob == {'Tobacco Burley'}, tob

    def test_nothing_is_averaged(self, malawi_mod):
        """The rule is agreement-or-drop, on a hand-built conflict."""
        df = pd.DataFrame({
            'crop': ['Maize'] * 3 + ['Rice'] * 2,
            'u': ['50 kg Bag'] * 5,
            'condition': ['shelled'] * 5,
            'region': ['North'] * 5,
            'KgFactor': [50.0, 50.0, 50.0, 40.0, 60.0],
        })
        keys = ['crop', 'u', 'condition', 'region']
        out = malawi_mod._collapse_varieties(df, keys, source='test')
        assert list(out['crop']) == ['Maize']          # Rice disagreed -> gone
        assert out['KgFactor'].iloc[0] == 50.0
        assert len(out.attrs['dropped_ambiguous']) == 1
        assert out.attrs['dropped_ambiguous'][0]['crop'] == 'Rice'
        # NOT the mean (50.0), NOT the median, NOT the first (40.0).
        assert 'Rice' not in set(out['crop'])


class TestTreeConditionIsTheSentinelNotNA:
    def test_tree_rows_carry_unknown_condition(self, shipped):
        """NA would normalise to the join's private NA sentinel and match
        only a NULL condition -- which no crop_production row has, because
        `_harvest_block` sentinel-fills every one.  The table would serve
        zero perennial rows, and only a ShippedFactorWarning would say so.
        """
        tree = shipped[shipped['Source'] == 'IHS5 tree']
        conds = set(tree.index.get_level_values('condition'))
        assert conds == {'unknown_condition'}
        assert not any(pd.isna(c) for c in conds)


# --------------------------------------------------------------------------
# End to end, on the built table
# --------------------------------------------------------------------------

class TestAgainstCropProduction:
    def test_condition_is_served_and_never_null(self, cp):
        assert 'condition' in cp.index.names
        vals = cp.index.get_level_values('condition')
        assert not pd.isna(vals).any(), (
            'a NULL index key is deleted outright by a duplicate collapse '
            '(GH #323 3b); condition is sentinel-filled for exactly that '
            'reason')
        assert set(vals) <= set(CONDITIONS)

    def test_perennial_rows_are_all_unknown_condition(self, cp):
        """Module P asks no such question, so it must not claim one."""
        f = cp.reset_index()
        per = f[f['perennial'].fillna(False).astype(bool)]
        assert set(per['condition']) == {'unknown_condition'}

    def test_the_declared_grain_is_unique(self, cp):
        f = cp.reset_index()
        keys = [k for k in ['t', 'i', 'plot_id', 'crop', 'u', 'condition']
                if k in f.columns]
        assert not f.duplicated(keys).any()

    def test_with_region_resolves_the_lower_case_key(self, malawi_mod, cp):
        out = malawi_mod.with_region(cp)
        assert 'region' in out.columns
        assert 'Region' not in out.columns, (
            'the join matches the key name EXACTLY and refuses both '
            'spellings, so the rename is load-bearing')
        assert len(out) == len(cp)
        assert set(out['region'].dropna()) <= {'North', 'Central', 'Southern'}

    def test_with_region_refuses_a_frame_with_no_cluster(self, malawi_mod, cp):
        flat = cp.reset_index().drop(columns=['v'])
        no_v = flat.set_index([n for n in cp.index.names if n != 'v'])
        with pytest.raises(ValueError, match='cluster level'):
            malawi_mod.with_region(no_v)

    def test_the_shipped_layer_actually_serves_rows(self, malawi_mod, cp,
                                                    shipped):
        """`shipped_matched` is the JOIN diagnostic -- pre-screen, pre-rank.

        `counts['shipped']` reads 0 for a perfectly-keyed table that
        `reported` outranks, so it cannot answer "did my table key
        correctly?".  Malawi reports no KgFactor at all, so here they nearly
        coincide; the assertion is on the diagnostic regardless.
        """
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            fac = harvest_kg_factors(malawi_mod.with_region(cp),
                                     shipped_factors=shipped)
        counts = fac.attrs['kg_factor_sources']
        assert counts['shipped_matched'] > 0.4 * len(cp), counts
        assert counts['shipped'] > 0
        assert set(fac.loc[np.isfinite(fac['kg_shipped']),
                           'kg_shipped_source']) == {'IHS5 seasonal',
                                                     'IHS5 tree'}

    def test_without_condition_the_table_is_refused(self, malawi_mod, cp,
                                                    shipped):
        """Strip `condition` and the remaining keys go ambiguous.

        Shelled and unshelled are two different answers to one question, so
        the transform REFUSES rather than averaging them.  That refusal IS the
        honest "without condition" outcome: not a smaller match, no match at
        all, until a loader throws the distinction away.
        """
        no_cond = _strip_condition(malawi_mod.with_region(cp))
        with pytest.raises(ValueError, match='ambiguous'):
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                harvest_kg_factors(no_cond, shipped_factors=shipped)

    def test_condition_more_than_doubles_the_shipped_reach(self, malawi_mod,
                                                           cp, shipped):
        """The measurement that settles the schema decision.

        Build the best condition-BLIND table a loader could honestly produce
        -- keep a (crop, u, region) key only where its conditions AGREE -- and
        compare what it reaches against the real thing.  Asserted as a ratio,
        not as two frozen totals, so a future build that moves row counts does
        not fail this for the wrong reason.
        """
        flat = shipped.reset_index()
        K = ['crop_variety', 'u', 'region']
        n = flat.groupby(K)['KgFactor'].nunique()
        blind = (flat.set_index(K).loc[n[n == 1].index]
                 .reset_index().drop_duplicates(K)
                 .set_index(K)[['KgFactor', 'Source']])
        assert (n > 1).sum() > 0, (
            'if no key disagreed across conditions the axis would be inert '
            'and this whole measurement would be moot')
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            blind_matched = harvest_kg_factors(
                _strip_condition(malawi_mod.with_region(cp)),
                shipped_factors=blind
            ).attrs['kg_factor_sources']['shipped_matched']
            full_matched = harvest_kg_factors(
                malawi_mod.with_region(cp), shipped_factors=shipped
            ).attrs['kg_factor_sources']['shipped_matched']
        assert full_matched > 2 * blind_matched, (
            f'condition-keyed match {full_matched:,} vs condition-blind '
            f'{blind_matched:,}; the level is justified by that ratio '
            '(86,657 vs 36,915 on the collapsed crop key, 2026-09-09)')

    def test_crop_variety_is_a_column_not_a_level(self, cp):
        """A function of the crop code splits no key, so it is not a level.

        Putting it in the grain would only widen the index -- and widen the
        index-shape divergence that already costs Malawi its place in
        `Feature('crop_production')` (see the class below).
        """
        assert 'crop_variety' in cp.columns
        assert 'crop_variety' not in cp.index.names
        assert not pd.isna(cp['crop_variety']).any()

    def test_the_kilogram_unit_is_a_no_op(self, malawi_mod, cp, shipped):
        """A shipped 'KILOGRAM' factor must be 1, or the screen rejects it."""
        kg = shipped[shipped.index.get_level_values('u') == 'Kilogramme']
        assert len(kg) and (kg['KgFactor'] == 1.0).all()

    def test_harvest_kg_rises_overall_and_nothing_is_clipped(
            self, malawi_mod, cp, shipped):
        """Overall, not per wave -- and the exception is the point.

        The shipped layer outranks `inferred` and reaches units the inferred
        parser cannot, so the corpus total must rise.  It does NOT rise in
        every wave: 2010-11 falls ~0.14% because the variety key WITHDRAWS the
        tobacco rows the collapsed key was serving Burley's factor to, and the
        parser cannot read "Bale", so those rows now get nothing rather than a
        borrowed number.  A per-wave monotone assertion would be asserting the
        bug.
        """
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            base = harvest_kg(cp)
            ship = harvest_kg(malawi_mod.with_region(cp),
                              shipped_factors=shipped)
        b = base.reset_index().groupby('t')['Harvest_kg'].sum()
        s = ship.reset_index().groupby('t')['Harvest_kg'].sum()
        assert float(s.sum()) > float(b.sum())
        # and no wave may move by an implausible amount in either direction
        assert ((s / b > 0.95) & (s / b < 1.5)).all(), (s / b).to_dict()
        # The ox-cart factors (388-682 kg) exceed KG_FACTOR_MAX and are
        # REJECTED, not clipped -- counted, and the rows fall through.
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            fac = harvest_kg_factors(malawi_mod.with_region(cp),
                                     shipped_factors=shipped)
        counts = fac.attrs['kg_factor_sources']
        assert counts['shipped_implausible'] > 0
        rejected = fac[fac['kg_shipped_rejected']]
        assert pd.isna(rejected['kg_shipped']).all(), (
            'a rejected shipped factor must read NaN here, never a clipped '
            'value')


class TestSaleSuppressionIsCounted:
    """"Attach nothing and COUNT IT" has to be code, not prose.

    A number that lives only in `CONTENTS.org` is frozen at the day it was
    measured and will not move when the data does (GH #854 red-team item 6).
    """

    def test_the_tally_rides_on_attrs(self, malawi_mod):
        import pandas as _pd
        harv = _pd.DataFrame({
            't': ['2010-11'] * 2,
            'i': ['h1'] * 2,
            'plot_id': ['R1'] * 2,
            'crop': ['Maize'] * 2,
            '_crop_code': _pd.array([1, 1], dtype='Int64'),
            'u': _pd.array(['50 kg Bag'] * 2, dtype='string'),
            'condition': _pd.array(['shelled', 'unshelled'], dtype='string'),
            'crop_variety': _pd.array(['Maize Local'] * 2, dtype='string'),
            'Quantity': _pd.array([10.0, 4.0], dtype='Float64'),
            'planting_month': _pd.array([11, 11], dtype='Int64'),
            'harvest_month': _pd.array([6, 6], dtype='Int64'),
            'intercropped': _pd.array([False, False], dtype='boolean'),
            'perennial': _pd.array([False, False], dtype='boolean'),
        })
        sale = _pd.DataFrame({
            'i': ['h1'],
            '_crop_code': _pd.array([1], dtype='Int64'),
            'u': _pd.array(['50 kg Bag'], dtype='string'),
            'Quantity_sold': _pd.array([1.5], dtype='Float64'),
            'Value_sold': _pd.array([2500.0], dtype='Float64'),
        })
        with pytest.warns(malawi_mod.SaleAttachmentWarning, match='2,500 MWK'):
            out = malawi_mod.assemble_crop_production('2010-11', [harv], [sale])
        tally = out.attrs['sale_suppressed']
        assert tally == {'wave': '2010-11', 'sales': 1, 'rows': 2,
                         'value': 2500.0}
        # and the sale really is on neither row -- not on one of them
        assert pd.isna(out['Value_sold']).all()

    def test_a_single_condition_plot_crop_still_gets_its_sale(self, malawi_mod):
        """The gate must not fire on the ordinary case."""
        import pandas as _pd
        harv = _pd.DataFrame({
            't': ['2010-11'], 'i': ['h1'], 'plot_id': ['R1'], 'crop': ['Maize'],
            '_crop_code': _pd.array([1], dtype='Int64'),
            'u': _pd.array(['50 kg Bag'], dtype='string'),
            'condition': _pd.array(['shelled'], dtype='string'),
            'crop_variety': _pd.array(['Maize Local'], dtype='string'),
            'Quantity': _pd.array([10.0], dtype='Float64'),
            'planting_month': _pd.array([11], dtype='Int64'),
            'harvest_month': _pd.array([6], dtype='Int64'),
            'intercropped': _pd.array([False], dtype='boolean'),
            'perennial': _pd.array([False], dtype='boolean'),
        })
        sale = _pd.DataFrame({
            'i': ['h1'], '_crop_code': _pd.array([1], dtype='Int64'),
            'u': _pd.array(['50 kg Bag'], dtype='string'),
            'Quantity_sold': _pd.array([1.5], dtype='Float64'),
            'Value_sold': _pd.array([2500.0], dtype='Float64'),
        })
        with warnings.catch_warnings():
            warnings.simplefilter('error', malawi_mod.SaleAttachmentWarning)
            out = malawi_mod.assemble_crop_production('2010-11', [harv], [sale])
        assert out['Value_sold'].iloc[0] == 2500.0
        assert out.attrs['sale_suppressed']['sales'] == 0


class TestFeatureExclusionIsRecordedNotAccidental:
    """`condition` costs Malawi its place in `Feature('crop_production')`.

    That is a FRAMEWORK defect (`crop_production` is not in `index_info`, so
    `Feature` falls through to the modal-index-shape filter), fixed separately
    on `fix/feature-canonical-index-crop-production` -- GH #775 / #569.  The
    level is kept ON PURPOSE: without it the shipped table is refused outright,
    and Uganda already carries `condition` and is already excluded, so the rule
    has to be fixed for Uganda regardless.

    This test does not assert the exclusion (that would pin the defect); it
    asserts the consequence is WRITTEN DOWN, so the next person auditing
    `Feature('crop_production')` does not read Malawi's absence as a build
    failure.
    """

    def test_contents_and_ledger_name_it(self):
        contents = (MALAWI_ / 'CONTENTS.org').read_text()
        assert "Feature('crop_production')" in contents
        assert '131,548' in contents
        assert 'fix/feature-canonical-index-crop-production' in contents
        ledger = (countries_root().parent.parent / '.coder' / 'ledger'
                  / '854-malawi-shipped-factors.md')
        if ledger.exists():
            text = ledger.read_text()
            assert 'modal' in text and '131,548' in text
