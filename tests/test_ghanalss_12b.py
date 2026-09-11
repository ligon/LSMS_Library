"""GhanaLSS 1987-88 / 1988-89 own production: the first registered derivation.

Section 12B of GLSS1/GLSS2 asks no recall question.  ``VFOODCPD`` is "How much
would it cost to buy the amount they ate each time?" -- the value of ONE
EATING OCCASION -- and until 2026-09-11 both wave scripts served it as the
produced-side ``Expenditure`` beside a fortnight of purchases.  The served
value is now DERIVED (``ghanalss.derive_12b_fortnight_value``: the
year-average fortnight, GSS's annual ``EXPEND.HPFOOD`` / 26), registered as
``GhanaLSS::food_acquired::12b-fortnight`` and stamped on every produced row.
``SkunkWorks/derived_values.org``, "First instance"; ``GhanaLSS/_/CONTENTS.org``,
"12B is a value per eating occasion".

Two tiers, following ``test_ghanalss_visit_alignment.py``:

* **Static** (``TestScripts``, ``TestFunction``, ``TestRegistry``) -- reads
  the scripts and the registry as text/config and exercises the pure
  function.  No cache, no microdata.
* **Data-gated** (``TestDelivered``) -- the HPFOOD reproduction from the raw
  inputs, and the delivered table's label; skipped where the microdata are
  not available.
"""
from __future__ import annotations

import inspect
import re

import numpy as np
import pandas as pd
import pytest

from lsms_library.derivations import derivation_records, resolve_callable
from lsms_library.paths import countries_root

COUNTRY = 'GhanaLSS'
WAVES = ['1987-88', '1988-89']
KEY = 'GhanaLSS::food_acquired::12b-fortnight'
FUNCTION = 'lsms_library.countries.GhanaLSS._.ghanalss:derive_12b_fortnight_value'
INPUTS = 'lsms_library.countries.GhanaLSS._.ghanalss:inputs_12b'

#: Measured cold in a private LSMS_DATA_DIR, 2026-09-11 (baseline
#: development @ e6f8acb27 -> this branch).  Row counts are UNCHANGED by the
#: derivation: rows are kept or dropped on the recorded VFOODCPD, so the three
#: 1987-88 rows with MFOODCLY == 0 are served as 0 under the key.
ROWS = {'1987-88': 71_605, '1988-89': 72_649}
PRODUCED_SUM_BEFORE = {'1987-88': 3_157_354.0, '1988-89': 3_677_833.0}
PRODUCED_SUM_AFTER = {'1987-88': 9_484_484.0, '1988-89': 8_334_683.5}
PURCHASED_SUM = {'1987-88': 14_969_244.0, '1988-89': 16_625_321.0}


def _script(wave):
    return (countries_root() / COUNTRY / wave / '_' / 'food_acquired.py').read_text()


def _fn():
    return resolve_callable(FUNCTION)


# ---------------------------------------------------------------------------
# static
# ---------------------------------------------------------------------------

class TestScripts:
    @pytest.mark.parametrize('wave', WAVES)
    def test_script_stamps_the_registry_key(self, wave):
        src = _script(wave)
        assert KEY in src, f'{wave}: the literal key {KEY!r} is not stamped'
        assert re.search(r"\['Derivation'\]\s*=", src), f'{wave}: no Derivation column written'

    @pytest.mark.parametrize('wave', WAVES)
    def test_script_calls_the_registered_function(self, wave):
        src = _script(wave)
        assert 'derive_12b_fortnight_value(' in src
        assert re.search(r'from ghanalss import .*derive_12b_fortnight_value', src)

    @pytest.mark.parametrize('wave', WAVES)
    def test_script_no_longer_calls_vfoodcpd_a_daily_or_recall_value(self, wave):
        src = _script(wave)
        bad = re.findall(r'^.*VFOODCPD.*(?:per day|daily|per-recall|per recall).*$',
                         src, re.MULTILINE | re.IGNORECASE)
        assert not bad, f'{wave}: {bad}'

    @pytest.mark.parametrize('wave', WAVES)
    def test_label_is_stamped_after_the_collapse(self, wave):
        """A string column must never reach the wave script's .sum()."""
        src = _script(wave)
        i_sum = src.index('.sum(min_count=1)')
        i_stamp = src.index("f['Derivation'] =")
        assert i_stamp > i_sum
        assert 'assert f.index.is_unique' in src


class TestFunction:
    def test_worked_example(self):
        """6 months of 12, 12 times a month, 10 cedis each time -> 27.6 (design note)."""
        fn = _fn()
        assert fn(6, 12, 5, 10) == pytest.approx(27.6, abs=0.05)
        # in-season fortnight (drop months/12) and GSS's annual (x 26)
        assert fn(12, 12, 5, 10) == pytest.approx(55.3, abs=0.05)
        assert fn(6, 12, 5, 10) * 26 == pytest.approx(720, rel=0.005)

    def test_unit_legend_and_window(self):
        import sys
        mod = sys.modules[_fn().__module__]
        assert set(mod.DAYS_PER_UNIT) == {3, 4, 5, 6, 7, 8}
        assert mod.DAYS_PER_UNIT[3] == 1 and mod.DAYS_PER_UNIT[4] == 7
        assert mod.DAYS_PER_UNIT[8] == 365
        assert mod.WINDOW_DAYS == 14

    def test_no_options(self):
        """One construction per parquet: four positional inputs, no defaults."""
        sig = inspect.signature(_fn())
        assert list(sig.parameters) == ['months', 'times', 'unit_code', 'value_each_time']
        assert all(p.default is inspect.Parameter.empty for p in sig.parameters.values())

    def test_vectorised_and_missing_unit(self):
        fn = _fn()
        out = fn(pd.Series([6, 0, 12]), pd.Series([12, 1, 1]),
                 pd.Series([5, 3, 99]), pd.Series([10, 5, 5]))
        assert isinstance(out, np.ndarray)
        assert out[0] == pytest.approx(27.6, abs=0.05)
        assert out[1] == 0.0                     # months == 0 -> 0, not NaN
        assert np.isnan(out[2])                  # unknown unit code


class TestRegistry:
    def test_the_entry_is_complete(self):
        r = derivation_records(COUNTRY)[KEY]
        assert r.waves == WAVES
        assert r.columns == ['Expenditure', 'Quantity']
        assert r.rows == {'s': 'produced'}
        assert r.function == FUNCTION and r.inputs == INPUTS
        assert r.raw_variables == ['Y12B.MFOODCLY', 'Y12B.TFOODC',
                                   'Y12B.UTFOODC', 'Y12B.VFOODCPD']
        assert {'questionnaire', 'modelling-choice'} <= set(r.bases)
        assert 'HPFOOD' in r.validated_against
        assert r.contents and '12B is a value per eating occasion' in r.contents

    def test_the_contents_pointer_resolves(self):
        text = (countries_root() / COUNTRY / '_' / 'CONTENTS.org').read_text()
        assert '12B is a value per eating occasion' in text
        # the two corrections the fix demanded
        assert '12B: no recall (derived)' in text
        assert 'only 12A is' in text

    def test_both_callables_resolve(self):
        assert callable(resolve_callable(FUNCTION))
        assert callable(resolve_callable(INPUTS))


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
        fa = country.food_acquired()
    except Exception as e:                    # pragma: no cover - no microdata
        pytest.skip(f'{COUNTRY} food_acquired not buildable here: {e}')
    return fa


@pytest.mark.slow
class TestDelivered:
    @pytest.mark.parametrize('wave', WAVES)
    def test_function_x26_reproduces_gss_hpfood(self, country, wave):
        """Sum over items of derive(...) x 26 per household == EXPEND.HPFOOD,
        median ratio in [0.99, 1.05] (measured 1.018 / 1.018)."""
        from lsms_library.local_tools import get_dataframe
        try:
            raw = country.derivation_inputs(KEY, wave=wave)
            expend = get_dataframe(str(countries_root() / COUNTRY / wave / 'Data' / 'EXPEND.DAT'))
        except Exception as e:                # pragma: no cover - no microdata
            pytest.skip(f'{COUNTRY} {wave} sources not available here: {e}')
        fn = _fn()
        fort = fn(raw['MFOODCLY'], raw['TFOODC'], raw['UTFOODC'], raw['VFOODCPD'])
        per_hh = pd.Series(fort * 26, index=raw.index).groupby(level='i').sum()
        i_of = resolve_callable(f'lsms_library.countries.{COUNTRY}.{wave}._.mapping:i')
        hp = expend.assign(i=expend['HID'].apply(i_of)).set_index('i')['HPFOOD']
        hp = pd.to_numeric(hp, errors='coerce')
        both = pd.concat([per_hh.rename('ours'), hp.rename('hpfood')], axis=1, join='inner')
        both = both[both['hpfood'] > 0]
        assert len(both) > 2000
        ratio = (both['ours'] / both['hpfood']).median()
        assert 0.99 <= ratio <= 1.05, ratio

    def test_label_on_every_produced_row_of_the_two_waves_and_nowhere_else(self, delivered):
        fa = delivered
        assert 'Derivation' in fa.columns
        t = fa.index.get_level_values('t').astype(str)
        s = fa.index.get_level_values('s').astype(str)
        expected = pd.Series(np.isin(t, WAVES) & (s == 'produced'), index=fa.index)
        got = fa['Derivation'].notna()
        assert (got == expected).all()
        assert set(fa.loc[got, 'Derivation'].unique()) == {KEY}

    @pytest.mark.parametrize('wave', WAVES)
    def test_rows_unchanged_and_produced_side_rescaled(self, delivered, wave):
        x = delivered.xs(wave, level='t', drop_level=False)
        assert len(x) == ROWS[wave]
        assert x.index.is_unique
        by_s = x.groupby(level='s')['Expenditure'].sum()
        assert by_s['purchased'] == pytest.approx(PURCHASED_SUM[wave], rel=1e-6)
        assert by_s['produced'] == pytest.approx(PRODUCED_SUM_AFTER[wave], rel=1e-6)
        assert by_s['produced'] > 2 * PRODUCED_SUM_BEFORE[wave]
        prod = x.xs('produced', level='s')
        assert (prod['Quantity'] == prod['Expenditure']).all()     # u = 'Value'

    def test_attrs_summary_counts_the_labelled_rows(self, delivered):
        summ = delivered.attrs['derivations'][COUNTRY][KEY]
        assert summ['in'] == 'food_acquired'
        assert summ['rows'] == int(delivered['Derivation'].notna().sum())
        assert summ['columns'] == ['Expenditure', 'Quantity']

    def test_derived_tables_survive_the_string_column(self, country):
        """The derived food tables must not be asked to reduce `Derivation`."""
        fe = country.food_expenditures()
        fq = country.food_quantities(units='units')
        assert 'Derivation' not in fe.columns and 'Derivation' not in fq.columns
        assert len(fe) > 0 and len(fq) > 0
