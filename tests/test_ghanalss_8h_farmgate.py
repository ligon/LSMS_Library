"""GhanaLSS section 8H own production: valued at the farmgate price.

GLSS4-GLSS7 (1998-99, 2005-06, 2012-13, 2016-17) record, for each home-produced
food consumed, a real ``Quantity`` in a native unit and the household's own
reported farmgate ``Price`` -- but never a total.  Until 2026-09-13 the wave
scripts left ``Expenditure`` NaN and said so ("no produced value is recorded"),
which is true about the *survey* and was never a claim that the value could not
be constructed; ``Country('GhanaLSS').food_expenditures()`` therefore returned
``<NA>`` for own production in every wave but the two 1980s ones.

It is now DERIVED -- ``Expenditure = Quantity * Price``, registered as
``GhanaLSS::food_acquired::8h-farmgate``.  A **policy change**, made policy by
the derived-values machinery (@ligon, 2026-09-13): the library now has somewhere
honest to put a construction, so it is served with the key on the row rather
than withheld.

Not comparable with ``12b-fortnight``: 8H asks a *producer* price, 12B asks what
it would *cost to buy*.  See ``GhanaLSS/_/CONTENTS.org``, "Section 8H own
production is valued at the farmgate price", and
``.coder/ledger/ghanalss-produced-qp.md``.

Two tiers, following ``test_ghanalss_12b.py``: static (scripts, registry, the
pure function) and data-gated (the delivered table).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import yaml

from lsms_library.derivations import BASES, REQUIRED_FIELDS, resolve_callable
from lsms_library.paths import countries_root

COUNTRY = 'GhanaLSS'
KEY = 'GhanaLSS::food_acquired::8h-farmgate'
WAVES = ('1998-99', '2005-06', '2012-13', '2016-17')
EXCLUDED = '1991-92'
FUNCTION = 'lsms_library.countries.GhanaLSS._.ghanalss:derive_produced_farmgate_value'
INPUTS = 'lsms_library.countries.GhanaLSS._.ghanalss:inputs_produced_farmgate'

#: Measured on the warm corpus, 2026-09-13, after the rebuild.
STAMPED_ROWS = 605_333
STAMPED_BY_WAVE = {'1998-99': 98_403, '2005-06': 170_275,
                   '2012-13': 197_906, '2016-17': 138_749}


def _script(wave):
    return (countries_root() / COUNTRY / wave / '_' / 'food_acquired.py').read_text()


def _registry():
    path = countries_root() / COUNTRY / '_' / 'derivations.yml'
    with open(path, encoding='utf-8') as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# static -- no cache, no microdata
# ---------------------------------------------------------------------------

class TestScripts:
    @pytest.mark.parametrize('wave', WAVES)
    def test_script_stamps_the_registry_key(self, wave):
        assert KEY in _script(wave), f'{wave} does not stamp {KEY}'

    @pytest.mark.parametrize('wave', WAVES)
    def test_script_calls_the_registered_function(self, wave):
        assert 'derive_produced_farmgate_value' in _script(wave), (
            f'{wave} computes the value without calling the registered '
            f'function, so the registry would name code nothing runs')

    @staticmethod
    def _positions(wave):
        """Anchor on the ASSIGNMENT sites, not on any mention.

        The key and ``to_parquet`` both appear earlier in these files -- in the
        module docstring (which now documents the derivation) and in the import
        line respectively -- so ``str.index`` on the bare strings finds the
        wrong occurrence. That cost five spurious failures on first run.
        """
        src = _script(wave)
        stamp = src.index("['Derivation'] = pd.Series(np.where(_both")
        # The guard takes one of two forms: most waves ASSERT uniqueness,
        # 2016-17 COLLAPSES with groupby(...).sum(min_count=1), which
        # guarantees it just as well. Either satisfies the invariant that
        # matters -- no string reaches core's additive .sum().
        uniq = max(src.rfind('index.duplicated()', 0, stamp),
                   src.rfind('index.is_unique', 0, stamp),
                   src.rfind('.sum(min_count=1)', 0, stamp))
        write = src.rindex('to_parquet(')
        return stamp, uniq, write

    @pytest.mark.parametrize('wave', WAVES)
    def test_stamp_is_after_the_uniqueness_assert(self, wave):
        """A string must not reach core's additive ``.sum()`` (CLAUDE.md)."""
        stamp, uniq, _ = self._positions(wave)
        assert uniq != -1 and uniq < stamp, (
            f'{wave} stamps Derivation before the wave collapses/asserts its '
            f'index; a string would then reach core\'s additive .sum()')

    @pytest.mark.parametrize('wave', WAVES)
    def test_stamp_is_before_the_write(self, wave):
        stamp, _, write = self._positions(wave)
        assert stamp < write, f'{wave} stamps after the write'

    def test_the_excluded_wave_is_not_stamped(self):
        assert KEY not in _script(EXCLUDED), (
            f'{EXCLUDED} is stamped, but only 28% of its produced rows carry '
            f'both factors -- serving it would look complete and would not be')


class TestFunction:
    def test_it_is_the_product(self):
        fn = resolve_callable(FUNCTION)
        assert float(np.ravel(fn(3.0, 4.0))[0]) == 12.0

    def test_missing_either_factor_yields_nan(self):
        fn = resolve_callable(FUNCTION)
        out = fn(pd.Series([2.0, np.nan, 5.0]), pd.Series([10.0, 3.0, np.nan]))
        assert float(out[0]) == 20.0
        assert np.isnan(out[1]) and np.isnan(out[2]), (
            'a row missing a factor must yield NaN -- it is then served '
            'unlabelled, which is what makes the coverage claim honest')

    def test_it_takes_no_options(self):
        """A cached parquet must be ONE identifiable construction."""
        import inspect
        sig = inspect.signature(resolve_callable(FUNCTION))
        assert list(sig.parameters) == ['quantity', 'price'], sig


class TestRegistry:
    def test_the_entry_is_complete(self):
        e = _registry()[KEY]
        assert [f for f in REQUIRED_FIELDS if f not in e] == []
        assert e['waves'] == list(WAVES)
        assert e['rows'] == {'s': 'produced'}
        assert all(a['basis'] in BASES for a in e['assumptions'])

    def test_the_modelling_choice_is_declared_as_one(self):
        """Valuing own consumption at the household's own farmgate price is a
        choice no document supplies; it must not be dressed as questionnaire."""
        e = _registry()[KEY]
        bases = {a['basis'] for a in e['assumptions']}
        assert 'modelling-choice' in bases, (
            'the valuation basis is a modelling choice and must say so')

    def test_the_exclusion_is_recorded(self):
        e = _registry()[KEY]
        assert EXCLUDED not in e['waves']
        assert any(EXCLUDED in a['text'] for a in e['assumptions']), (
            f'{EXCLUDED} is excluded but the entry does not say why -- an '
            f'unevidenced omission is the Albania mistake')

    def test_both_callables_resolve(self):
        assert callable(resolve_callable(FUNCTION))
        assert callable(resolve_callable(INPUTS))


# ---------------------------------------------------------------------------
# data-gated
# ---------------------------------------------------------------------------

@pytest.fixture(scope='module')
def delivered():
    import lsms_library as ll
    try:
        return ll.Country(COUNTRY).food_acquired()
    except Exception as e:                     # pragma: no cover - no microdata
        pytest.skip(f'{COUNTRY} food_acquired not buildable here: {e}')


@pytest.mark.requires_s3
@pytest.mark.slow
class TestDelivered:
    def test_key_lands_on_exactly_the_computed_rows(self, delivered):
        r = delivered.reset_index()
        q = pd.to_numeric(r['Quantity'], errors='coerce')
        p = pd.to_numeric(r['Price'], errors='coerce')
        expect = ((r['s'] == 'produced') & q.notna() & p.notna()
                  & r['t'].isin(WAVES))
        assert bool(((r['Derivation'] == KEY) == expect).all()), (
            'the key is on rows the derivation did not compute, or missing '
            'from rows it did')

    def test_no_key_on_purchased_or_on_the_1980s_or_on_the_excluded_wave(self, delivered):
        r = delivered.reset_index()
        keyed = r['Derivation'] == KEY
        assert int((keyed & (r['s'] == 'purchased')).sum()) == 0
        assert int((keyed & r['t'].isin(['1987-88', '1988-89'])).sum()) == 0
        assert int((keyed & (r['t'] == EXCLUDED)).sum()) == 0

    def test_expenditure_is_exactly_quantity_times_price(self, delivered):
        r = delivered.reset_index()
        k = r['Derivation'] == KEY
        e = pd.to_numeric(r.loc[k, 'Expenditure'], errors='coerce')
        q = pd.to_numeric(r.loc[k, 'Quantity'], errors='coerce')
        p = pd.to_numeric(r.loc[k, 'Price'], errors='coerce')
        assert float((e - q * p).abs().max()) == 0.0

    def test_unstamped_produced_rows_keep_expenditure_null(self, delivered):
        """The 8.5% of 2016-17 with no Quantity must stay NaN AND unlabelled."""
        r = delivered.reset_index()
        unstamped = ((r['s'] == 'produced') & (r['Derivation'] != KEY)
                     & r['t'].isin(WAVES))
        assert int(pd.to_numeric(r.loc[unstamped, 'Expenditure'],
                                 errors='coerce').notna().sum()) == 0

    def test_the_excluded_wave_still_serves_no_produced_value(self, delivered):
        r = delivered.reset_index()
        m = (r['t'] == EXCLUDED) & (r['s'] == 'produced')
        assert int(pd.to_numeric(r.loc[m, 'Expenditure'],
                                 errors='coerce').notna().sum()) == 0

    def test_the_12b_derivation_is_untouched(self, delivered):
        r = delivered.reset_index()
        n = int((r['Derivation'] == 'GhanaLSS::food_acquired::12b-fortnight').sum())
        assert n == 44_112, f'12B rows moved: {n} (44,112 measured)'

    @pytest.mark.parametrize('wave', WAVES)
    def test_inputs_join_to_the_served_rows(self, wave):
        """Each wave composes ``i`` differently; a generic id guess does not join."""
        fn = resolve_callable(INPUTS)
        import lsms_library as ll
        try:
            fa = ll.Country(COUNTRY).food_acquired()
            inp = fn(wave)
        except Exception as e:                 # pragma: no cover
            pytest.skip(f'not buildable here: {e}')
        r = fa.reset_index()
        served = set(r[(r['Derivation'] == KEY) & (r['t'] == wave)]['i'].astype(str))
        have = set(inp.reset_index()['i'].astype(str))
        assert served <= have, (
            f'{wave}: {len(served - have)} served household(s) absent from '
            f'derivation_inputs -- the inputs frame does not join')

    def test_derived_food_tables_do_not_carry_the_column(self):
        """`Derivation` is a food_acquired column; the derived tables summarise
        it in ``attrs`` instead (CLAUDE.md, "Carriers")."""
        import lsms_library as ll
        try:
            fe = ll.Country(COUNTRY).food_expenditures()
        except Exception as e:                 # pragma: no cover
            pytest.skip(f'not buildable here: {e}')
        assert 'Derivation' not in fe.columns
