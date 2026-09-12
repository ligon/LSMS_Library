"""EHCVM 2018+ ``food_acquired.Expenditure``: the 7-day purchase value (GH #876).

Section 7B is headed "Consommation alimentaire des 7 derniers jours et achat
des 30 derniers jours", and one row carries both clocks.  All twelve EHCVM
waves of eight countries used to wire ``Expenditure: s07bq08``, "Valeur (en
FCFA) du [PRODUIT] achete la derniere fois" -- the value of ONE purchase of an
unrecorded size -- beside a 7-day consumption ``Quantity``.  The served value
is now DERIVED (:func:`lsms_library.ehcvm.derive_ehcvm_purchase_value`: the
row's own purchased quantity -- the survey's accounting identity
``(s07bq03a - s07bq04 - s07bq05).clip(0)`` -- times the unit value of that last
purchase, NA where the units differ), registered once per country and stamped
on every purchased row.  The gift quantity ``s07bq05`` it subtracts is served
as ``s = 'inkind'`` rows with ``Expenditure`` NA: the survey asks no value for
a gift, and UEMOA's ``depan(Don)`` is an imputation at this same unit value
(measured: it is reproduced within 1% for 99.7% / 100% of rows), so it is
checked and deliberately not served.  ``SkunkWorks/derived_values.org``; each country's
``_/CONTENTS.org``, "food_acquired: one row, two windows (GH #876)".

Two tiers, following ``tests/test_ghanalss_12b.py``:

* **Static** (``TestFunction``, ``TestConfig``, ``TestRegistry``) -- the pure
  function, the twelve YAML/mapping wirings read as text, and the eight
  registry entries.  No cache, no microdata.
* **Data-gated** (``TestDelivered``) -- the delivered table's label, the
  ``Expenditure / Quantity`` identity, and that the raw inputs join to the
  served rows; skipped where the microdata are not available.
"""
from __future__ import annotations

import inspect
import re

import numpy as np
import pandas as pd
import pytest

from lsms_library.derivations import derivation_records, resolve_callable
from lsms_library.ehcvm import (
    EHCVM_COUNTRIES, MYVARS, RAW_VARIABLES, derivation_key,
    derive_ehcvm_purchase_value, unit_match,
)
from lsms_library.paths import countries_root

#: The twelve EHCVM 2018+ waves, country -> waves.
WAVES = {
    'Togo': ['2018'],
    'Benin': ['2018-19'],
    'Guinea-Bissau': ['2018-19'],
    'Senegal': ['2018-19', '2021-22'],
    'Mali': ['2018-19', '2021-22'],
    'Niger': ['2018-19', '2021-22'],
    'Burkina_Faso': ['2018-19', '2021-22'],
    'CotedIvoire': ['2018-19'],
}
CELLS = [(c, w) for c, ws in WAVES.items() for w in ws]
FUNCTION = 'lsms_library.ehcvm:derive_ehcvm_purchase_value'

#: Served purchased-side ``Expenditure`` sum BEFORE this landed -- i.e. the raw
#: ``s07bq08`` total -- measured warm on development, 2026-09-12.  Kept as the
#: scale the change is against.
BEFORE_SUM = {
    ('Togo', '2018'): 44_452_024.0,
    ('Benin', '2018-19'): 87_287_926.96,
    ('Guinea-Bissau', '2018-19'): 91_353_379.75,
    ('Senegal', '2018-19'): 171_946_367.74,
    ('Senegal', '2021-22'): 185_854_824.98,
    ('Mali', '2018-19'): 134_798_354.29,
    ('Mali', '2021-22'): 134_405_775.0,
    ('Niger', '2018-19'): 84_069_921.87,
    ('Niger', '2021-22'): 99_448_402.27,
    ('Burkina_Faso', '2018-19'): 65_315_307.5,
    ('Burkina_Faso', '2021-22'): 39_905_158.0,
    ('CotedIvoire', '2018-19'): 119_107_505.0,
}

#: Served purchased-side ``Expenditure`` AFTER, under construction C, and the
#: number of ``s='inkind'`` rows ``s07bq05`` now supplies.  Measured warm in the
#: worktree, 2026-09-12.  The three candidate quantities per wave are in each
#: country's ``_/derivations.yml`` and ``_/CONTENTS.org``.
AFTER = {
    ('Togo', '2018'): (31_676_794.87, 10_212),
    ('Benin', '2018-19'): (68_929_996.70, 18_632),
    ('Guinea-Bissau', '2018-19'): (57_552_942.85, 13_818),
    ('Senegal', '2018-19'): (147_345_570.63, 10_802),
    ('Senegal', '2021-22'): (182_628_314.17, 10_917),
    ('Mali', '2018-19'): (89_614_678.64, 16_459),
    ('Mali', '2021-22'): (97_178_691.52, 16_869),
    ('Niger', '2018-19'): (53_351_991.10, 8_141),
    ('Niger', '2021-22'): (67_358_383.42, 7_112),
    ('Burkina_Faso', '2018-19'): (52_466_280.38, 9_386),
    ('Burkina_Faso', '2021-22'): (27_888_221.36, 6_148),
    ('CotedIvoire', '2018-19'): (105_011_084.65, 35_289),
}

#: Waves of these countries that are NOT EHCVM 2018+ and must carry no key.
NON_EHCVM_WAVES = {
    'Mali': ['2014-15'],
    'Burkina_Faso': ['2014'],
}


def _yaml(country, wave):
    return (countries_root() / country / wave / '_' / 'data_info.yml').read_text(encoding='utf-8')


def _mapping(country, wave):
    return (countries_root() / country / wave / '_' / 'mapping.py').read_text(encoding='utf-8')


# ---------------------------------------------------------------------------
# static: the pure function
# ---------------------------------------------------------------------------

class TestFunction:
    def test_worked_example(self):
        """2 kg purchased, 5 kg bought last time for 1000 -> 2 * 200 = 400."""
        assert derive_ehcvm_purchase_value(2, 'Kg', 'Taille unique',
                                           5, 'Kg', 'Taille unique',
                                           1000) == pytest.approx(400.0)

    def test_a_different_unit_is_NA_not_a_conversion(self):
        out = derive_ehcvm_purchase_value(2, 'Kg', 'Taille unique',
                                          1, 'Sac (50 Kg)', 'Taille unique', 15000)
        assert np.isnan(out)

    def test_the_same_unit_in_a_different_SIZE_is_NA(self):
        out = derive_ehcvm_purchase_value(2, 'Bol', 'Petit',
                                          1, 'Bol', 'Grand', 500)
        assert np.isnan(out)

    @pytest.mark.parametrize('last_q', [0, -1, np.nan, None])
    def test_no_positive_last_quantity_is_NA(self, last_q):
        out = derive_ehcvm_purchase_value(2, 'Kg', 'Petit', last_q, 'Kg', 'Petit', 1000)
        assert np.isnan(out)

    def test_a_missing_unit_never_matches(self):
        for a, b in ((None, 'Kg'), ('Kg', None), (None, None), (np.nan, np.nan)):
            assert np.isnan(derive_ehcvm_purchase_value(2, a, 'Petit', 1, b, 'Petit', 100))

    def test_unit_identity_uses_the_ehcvm_code_where_the_label_carries_one(self):
        """Senegal 2021-22 spells one unit two ways; the numeric prefix settles it."""
        assert unit_match(['139. Sachet'], ['1. Petit'],
                          ['139. sachet industriel'], ['1. Petit']).iloc[0]
        assert not unit_match(['100. kg'], ['0. Taille unique'],
                              ['138. Sac (50kg)'], ['0. Taille unique']).iloc[0]

    def test_unit_identity_folds_case_padding_accents_and_mojibake(self):
        """Mali 2018-19 pads s07bq03b; CotedIvoire's 'Unite' arrives as mojibake."""
        assert unit_match(['       kg'], ['       Unite de taille unique'],
                          ['kg'], ['Taille unique']).iloc[0]
        assert unit_match(['Unité'], ['Petit'],
                          ['UnitÃ©'], ['Petit']).iloc[0]

    def test_no_options(self):
        """One construction per parquet: seven positional inputs, no defaults."""
        sig = inspect.signature(derive_ehcvm_purchase_value)
        assert list(sig.parameters) == [
            'quantity', 'quantity_unit', 'quantity_size', 'last_quantity',
            'last_unit', 'last_size', 'last_value']
        assert all(p.default is inspect.Parameter.empty for p in sig.parameters.values())

    def test_vectorised(self):
        out = derive_ehcvm_purchase_value(
            pd.Series([2.0, 3.0, 4.0]), pd.Series(['Kg', 'Kg', 'Bol']),
            pd.Series(['Petit'] * 3), pd.Series([5.0, 0.0, 2.0]),
            pd.Series(['Kg', 'Kg', 'Sachet']), pd.Series(['Petit'] * 3),
            pd.Series([1000.0, 500.0, 300.0]))
        assert isinstance(out, np.ndarray)
        assert out[0] == pytest.approx(400.0)
        assert np.isnan(out[1]) and np.isnan(out[2])


# ---------------------------------------------------------------------------
# static: the twelve wirings
# ---------------------------------------------------------------------------

class TestConfig:
    @pytest.mark.parametrize('country,wave', CELLS)
    def test_expenditure_is_no_longer_wired_to_s07bq08(self, country, wave):
        """The defect itself: `Expenditure: s07bq08` must not reappear."""
        assert not re.search(r'^\s*Expenditure:\s*s07bq08\s*$',
                             _yaml(country, wave), re.MULTILINE)

    @pytest.mark.parametrize('country,wave', CELLS)
    def test_every_raw_input_is_bound(self, country, wave):
        y = _yaml(country, wave)
        for name, var in MYVARS.items():
            assert re.search(rf'^\s*{name}:\s*{var}\s*$', y, re.MULTILINE), (name, var)

    @pytest.mark.parametrize('country,wave', CELLS)
    def test_the_hook_calls_the_shared_derivation(self, country, wave):
        m = _mapping(country, wave)
        assert '_food_acquired_ehcvm(df, FOOD_ACQUIRED_DERIVATION)' in m
        assert f"_derivation_key('{country}')" in m
        assert 'def inputs_food_acquired(wave):' in m

    def test_the_shared_function_is_not_copied_into_a_country(self):
        """One function, eight registrations -- not eight copies."""
        offenders = [str(p) for p in countries_root().glob('*/**/_/*.py')
                     if 'def derive_ehcvm_purchase_value' in p.read_text(encoding='utf-8')]
        assert not offenders, offenders


# ---------------------------------------------------------------------------
# static: the eight registry entries
# ---------------------------------------------------------------------------

class TestRegistry:
    @pytest.mark.parametrize('country', sorted(WAVES))
    def test_the_entry_is_complete(self, country):
        r = derivation_records(country)[derivation_key(country)]
        assert r.waves == WAVES[country]
        assert r.columns == ['Expenditure']
        assert r.rows == {'s': 'purchased'}
        assert r.function == FUNCTION
        assert r.raw_variables == RAW_VARIABLES
        assert 'modelling-choice' in r.bases          # the gift term is named
        assert r.validated_against and 'conso' in r.validated_against
        assert r.contents and 'one row, two windows' in r.contents

    @pytest.mark.parametrize('country', sorted(WAVES))
    def test_both_callables_resolve(self, country):
        r = derivation_records(country)[derivation_key(country)]
        assert callable(resolve_callable(r.function))
        assert callable(resolve_callable(r.inputs))

    def test_eight_entries_share_one_function(self):
        fns = {derivation_records(c)[derivation_key(c)].function for c in WAVES}
        assert fns == {FUNCTION}

    def test_the_key_is_per_country_not_framework_level(self):
        """An empty country slot would claim all 40 countries; only 8 are EHCVM."""
        from lsms_library.derivations import framework_records
        assert not any('7day-purchase' in k for k in framework_records())
        assert set(WAVES) == set(EHCVM_COUNTRIES)

    @pytest.mark.parametrize('country', sorted(WAVES))
    def test_the_contents_pointer_resolves(self, country):
        text = (countries_root() / country / '_' / 'CONTENTS.org').read_text(encoding='utf-8')
        assert 'one row, two windows' in text
        assert '#876' in text


# ---------------------------------------------------------------------------
# data-gated
# ---------------------------------------------------------------------------

@pytest.fixture(scope='module')
def built():
    import lsms_library as ll
    out = {}

    def get(country):
        if country not in out:
            try:
                out[country] = ll.Country(country).food_acquired()
            except Exception as e:            # pragma: no cover - no microdata
                pytest.skip(f'{country} food_acquired not buildable here: {e}')
        return out[country]
    return get


@pytest.mark.slow
class TestDelivered:
    @pytest.mark.parametrize('country,wave', CELLS)
    def test_the_key_is_on_every_purchased_row_of_the_wave_and_nowhere_else(
            self, built, country, wave):
        fa = built(country)
        assert 'Derivation' in fa.columns
        x = fa.xs(wave, level='t', drop_level=False)
        s = x.index.get_level_values('s').astype(str)
        got = x['Derivation'].notna().to_numpy()
        assert (got == (s == 'purchased')).all()
        assert set(x.loc[got, 'Derivation'].unique()) == {derivation_key(country)}

    @pytest.mark.parametrize('country', sorted(NON_EHCVM_WAVES))
    def test_a_non_ehcvm_wave_of_the_same_country_carries_no_key(self, built, country):
        fa = built(country)
        for wave in NON_EHCVM_WAVES[country]:
            x = fa.xs(wave, level='t', drop_level=False)
            assert x['Derivation'].isna().all(), wave

    @pytest.mark.parametrize('country,wave', CELLS)
    def test_expenditure_over_quantity_is_a_unit_value(self, built, country, wave):
        """The point of the fix: the two numbers on the row refer to the same
        thing, so E/Q is the last purchase's price per unit -- finite and
        positive on every row that carries a value."""
        x = built(country).xs(wave, level='t', drop_level=False)
        p = x.xs('purchased', level='s')
        v = p[p['Expenditure'].notna() & (p['Expenditure'] > 0)]
        assert len(v) > 0
        ratio = v['Expenditure'] / v['Quantity']
        assert np.isfinite(ratio).all() and (ratio > 0).all()

    @pytest.mark.parametrize('country,wave', CELLS)
    def test_some_rows_are_NA_because_the_units_differ(self, built, country, wave):
        """The NA arm is live in every wave -- 4% to 21% of purchase rows."""
        x = built(country).xs(wave, level='t', drop_level=False)
        p = x.xs('purchased', level='s')
        assert p['Expenditure'].isna().any()

    @pytest.mark.parametrize('country,wave', CELLS)
    def test_the_served_total_is_no_longer_the_raw_s07bq08_total(self, built, country, wave):
        x = built(country).xs(wave, level='t', drop_level=False)
        total = float(x.xs('purchased', level='s')['Expenditure'].sum())
        before = BEFORE_SUM[(country, wave)]
        after, _ = AFTER[(country, wave)]
        assert total != pytest.approx(before, rel=1e-4)
        assert 0.4 * before < total < before      # a strict subset, revalued
        assert total == pytest.approx(after, rel=1e-6)

    @pytest.mark.parametrize('country,wave', CELLS)
    def test_gifts_are_served_as_inkind_rows_with_no_value(self, built, country, wave):
        """`s07bq05` used to be read nowhere.  It is now its own `s` row, in the
        consumption unit, and it carries NO Expenditure: the survey asks no
        value for a gift, and UEMOA's depan(Don) is an imputation at the
        purchase unit value (reproduced within 1% for 99.7% of 1,255 Togo rows
        and 100% of 149 Guinea-Bissau rows), which we check and do not serve."""
        x = built(country).xs(wave, level='t', drop_level=False)
        inkind = x.xs('inkind', level='s')
        _, n = AFTER[(country, wave)]
        assert len(inkind) == n
        assert inkind['Expenditure'].isna().all()
        assert (inkind['Quantity'] > 0).all()
        assert inkind['Derivation'].isna().all()   # a reported quantity, not a derivation

    @pytest.mark.parametrize('country,wave', CELLS)
    def test_own_production_is_untouched_and_valueless(self, built, country, wave):
        """`s='produced'` already carried s07bq04 with no Expenditure; the gift
        rows get the identical treatment, and this side must not have moved."""
        produced = built(country).xs(wave, level='t', drop_level=False) \
                                 .xs('produced', level='s')
        assert len(produced) > 0
        assert produced['Expenditure'].isna().all()
        assert produced['Derivation'].isna().all()

    @pytest.mark.parametrize('country,wave', CELLS)
    def test_inputs_join_to_the_served_rows(self, built, country, wave):
        """Every served purchased row has its raw 7B answers under the same
        (t, i, j).  All twelve cells, not just Togo: `derivation_inputs`
        re-keys `i` through `updated_ids`, and `inputs_last_purchase` has to
        reproduce the served path's INDEX rewrites as well -- three waves sweep
        value-label mojibake out of `j` after the reshape, and skipping that
        left 80,563 of CotedIvoire's 218,224 keys (37%) and 164 of
        Guinea-Bissau's unjoinable.  A wave that adds a fourth kind of index
        rewrite must fail here, not unjoin quietly."""
        import lsms_library as ll
        c = ll.Country(country)
        key = derivation_key(country)
        try:
            raw = c.derivation_inputs(key, wave=wave)
        except Exception as e:                # pragma: no cover - no microdata
            pytest.skip(f'{country} {wave} sources not available here: {e}')
        assert list(raw.columns) == RAW_VARIABLES
        assert list(raw.index.names) == ['t', 'i', 'j']
        served = built(country).xs(wave, level='t', drop_level=False)
        served = served[served['Derivation'].notna()]
        served_k = set(served.reset_index().set_index(['t', 'i', 'j']).index)
        missing = served_k - set(raw.index)
        assert not missing, (
            f'{len(missing)} of {len(served_k)} served purchased keys have no '
            f'raw answers: {sorted(missing)[:3]}')

    def test_attrs_summary_counts_the_labelled_rows(self, built):
        fa = built('Togo')
        summ = fa.attrs['derivations']['Togo'][derivation_key('Togo')]
        assert summ['in'] == 'food_acquired'
        assert summ['rows'] == int(fa['Derivation'].notna().sum()) > 0
        assert summ['columns'] == ['Expenditure']

    def test_derived_tables_survive_the_string_column(self, built):
        """The derived food tables must not be asked to reduce `Derivation`."""
        import lsms_library as ll
        c = ll.Country('Togo')
        fe = c.food_expenditures()
        fq = c.food_quantities(units='units')
        assert 'Derivation' not in fe.columns and 'Derivation' not in fq.columns
        assert len(fe) > 0 and len(fq) > 0
