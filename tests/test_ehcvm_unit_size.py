"""EHCVM ``food_acquired.u`` carries the unit SIZE (GH #1224, @ligon 2026-11-03).

Where the consumption size ``s07bq03c`` names a size, ``u`` is served as
``<unit> (<Size>)`` -- ``Tas (Petit)``, Uganda's ``Heap (Small)`` form.  The
size-free code and a missing size keep the bare unit.  The base is the label
the country serves, mapped BEFORE the suffix, because every read-time mapper is
keyed on the bare label and would pass a sized one through unmapped.

Config-only: real ``u`` tables from the tree, synthetic frames, no microdata.
The rule on delivered data, all twelve cells, is
``tests/test_ehcvm_7day.py::TestDelivered::test_every_size_suffix_is_the_countrys_own``;
what a cold build does to the counts is measured in the PR, not pinned.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lsms_library import ehcvm
from lsms_library.ehcvm import (
    UnitSizeLabelError, carry_unit_size, derivation_key, food_acquired_ehcvm,
    served_unit_labels, size_label, sized_unit, unit_match,
)
from lsms_library.paths import countries_root


def _decode(s):
    """The wave hooks' scalar mojibake repair, in miniature."""
    if not isinstance(s, str):
        return s
    try:
        return s.encode('latin-1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s


def _frame(pairs):
    units = [u for u, _ in pairs]
    idx = pd.MultiIndex.from_arrays(
        [['2018-19'] * len(units), [f'h{n}' for n in range(len(units))],
         ['Riz'] * len(units), pd.Index(units, dtype=object)],
        names=['t', 'i', 'j', 'u'])
    return pd.DataFrame({'Quantity': np.arange(1.0, len(units) + 1)}, index=idx)


def _carry(country, pairs, decode=None):
    frame = _frame(pairs)
    out = carry_unit_size(frame, [s for _, s in pairs], country, decode=decode)
    assert out['Quantity'].tolist() == frame['Quantity'].tolist()
    assert list(out.index.names) == list(frame.index.names)
    return list(out.index.get_level_values('u'))


class TestSizeLabel:
    @pytest.mark.parametrize('raw', [
        'Taille unique', '0. Taille unique', 'Unite de taille unique',
        'Unité de taille unique', '       Unité de taille unique',
        'Tamanho único', 'Tamanho Ãºnico',
    ])
    def test_the_size_free_code_names_no_size(self, raw):
        assert size_label(raw) is None

    @pytest.mark.parametrize('raw', [None, np.nan, pd.NA])
    def test_a_missing_size_names_no_size(self, raw):
        assert size_label(raw) is None

    def test_the_documented_synonym_is_the_only_one(self):
        """'Unite de taille unique' equals 'Taille unique'; 'Entier' does not."""
        assert unit_match(['Tas'], ['Unite de taille unique'],
                          ['Tas'], ['Taille unique']).iloc[0]
        assert not unit_match(['Tas'], ['Entier'],
                              ['Tas'], ['Taille unique']).iloc[0]
        assert size_label('Entier') == 'Entier'

    @pytest.mark.parametrize('raw,want', [
        ('Petit', 'Petit'),
        ('       Moyen', 'Moyen'),            # Mali 2018-19 pads its labels
        ('1. Petit', 'Petit'),                # Niger / Senegal 2021-22 number theirs
        ('6. Entier', 'Entier'),
        ('Quart', 'Quart'),
        ('Demi', 'Demi'),
        ('TrÃ¨s petit', 'Très petit'),        # Senegal 2018-19, mojibake repaired
        ('7. Très Petite', 'Très petite'),    # Senegal 2021-22, its own word
        ('Sans Brassée', 'Sans brassée'),     # Benin, accents kept, case only
        ('avec 2 Brassées', 'Avec 2 brassées'),
        ('A 3 doigts', 'A 3 doigts'),
        ('Autre taille', 'Autre taille'),
        ('Pequeno', 'Pequeno'),               # Guinea-Bissau, verbatim
        ('Medio', 'Medio'),
        ('Grande', 'Grande'),
    ])
    def test_the_instruments_own_label_with_only_case_normalised(self, raw, want):
        assert size_label(raw) == want

    @pytest.mark.parametrize('raw', ['98', 98, 98.0])
    def test_a_code_with_no_value_label_stays_a_code(self, raw):
        """Togo 2018 code 98 has no label in the file; none is invented."""
        assert size_label(raw) == '98'


class TestSizedUnit:
    def test_sized(self):
        assert sized_unit('Tas', 'Petit') == 'Tas (Petit)'
        assert sized_unit('Tiya', '6. Entier') == 'Tiya (Entier)'
        assert sized_unit('Bol', 'Grand') == 'Bol (Grand)'

    @pytest.mark.parametrize('size', ['Taille unique', '0. Taille unique', None, np.nan])
    def test_size_free_and_missing_stay_bare(self, size):
        assert sized_unit('Tas', size) == 'Tas'

    def test_no_unit_is_not_a_sized_unit(self):
        assert pd.isna(sized_unit(np.nan, 'Petit'))


class TestCarryUnitSize:
    def test_the_base_is_mapped_before_the_suffix(self):
        """Niger 2021-22's raw label is code-prefixed; the read-time table is
        keyed on it and would pass '139. Sachet (Petit)' through unmapped."""
        got = _carry('Niger', [('139. Sachet', '1. Petit'), ('512. Tiya', '6. Entier')])
        assert got == ['Sachet (Petit)', 'Tiya (Entier)']

    def test_a_fold_in_the_u_table_is_composed(self):
        """Mali's table folds a heap of meat and a heap into one served `Tas`."""
        got = _carry('Mali', [('Avec os en tas', 'Petit'), ('Tas', 'Petit'),
                              ('sachet industriel', 'Moyen')])
        assert got == ['Tas (Petit)', 'Tas (Petit)', 'Sachet (Moyen)']

    def test_a_bare_row_keeps_its_label_byte_for_byte(self):
        """Size-free and missing-size rows reach the read-time table as today."""
        got = _carry('Niger', [('100. Kg', '0. Taille unique'), ('143. Tas', np.nan),
                               ('139. Sachet', None)])
        assert got == ['100. Kg', '143. Tas', '139. Sachet']

    def test_no_unit_stays_no_unit(self):
        got = _carry('Niger', [(np.nan, '1. Petit')])
        assert len(got) == 1 and pd.isna(got[0])

    def test_the_missing_unit_sentinel_is_never_sized(self):
        """`Unknown (Petit)` would escape the closed _U_SENTINELS vocabulary."""
        assert _carry('Niger', [('Manquant', 'Petit')]) == ['Manquant']

    @pytest.mark.parametrize('country,raw,size', [
        ('Senegal', 'Kg', 'Moyen'),
        ('Burkina_Faso', 'Kg', 'Grand'),
        ('Burkina_Faso', 'Litre', 'Grand'),
        ('Benin', 'Litre', 'Quart'),
        ('Mali', 'kg', 'Grand'),              # raw spelling, served `Kg`
    ])
    def test_a_size_does_not_cost_an_exact_metric_label_its_reading(self, country, raw, size):
        assert _carry(country, [(raw, size)]) == [raw]

    def test_a_stated_quantity_keeps_its_size_and_its_reading(self):
        from lsms_library.transformations import _parse_explicit_metric as parse
        got = _carry('Niger', [('Sac (50 Kg)', 'Moyen')])
        assert got == ['Sac (50 Kg) (Moyen)']
        assert parse(got[0]) == 50.0
        got = _carry('Benin', [('Bidon de 1 litre et démi', 'Autre taille')])
        assert got == ['Bidon de 1,5 L (Autre taille)']
        assert parse(got[0]) == 1.5

    @pytest.mark.parametrize('size', [
        'Petit', 'Moyen', 'Grand', 'Quart', 'Demi', 'Entier', 'Très petit',
        'Très petite', 'Sans brassée', 'Avec 1 brassée', 'Avec 2 brassées',
        'A 2 doigts', 'A 3 doigts', 'A 5 doigts', 'Autre taille', 'Pequeno',
        'Medio', 'Grande', '98',
    ])
    def test_no_size_word_fakes_a_metric_reading(self, size):
        from lsms_library.transformations import _parse_explicit_metric as parse
        assert parse(f'Tas ({size})') is None

    def test_guinea_bissau_is_served_in_its_own_language(self):
        got = _carry('Guinea-Bissau', [('Unidade', 'Pequeno'), ('Monte', 'Medio'),
                                       ('PedaÃ§o', 'Grande'), ('Monte', 'Tamanho Ãºnico')],
                     decode=_decode)
        assert got == ['Unidade (Pequeno)', 'Monte (Medio)', 'Pedaço (Grande)', 'Monte']

    def test_decode_is_applied_to_the_base_of_a_sized_row_only(self):
        """The wave's own sweep repairs the bare rows after the reshape."""
        got = _carry('CotedIvoire', [('UnitÃ©', 'Grand'), ('UnitÃ©', 'Taille unique'),
                                     ('Boite', 'Petit')], decode=_decode)
        assert got == ['Unité (Grand)', 'UnitÃ©', 'Boîte (Petit)']

    @pytest.mark.parametrize('country,pairs', [
        ('Niger', [('139. Sachet', '1. Petit'), ('143. Tas', '3. Grand')]),
        ('Mali', [('Avec os en tas', 'Petit'), ('boite', 'Grand')]),
        ('Togo', [('Tas', 'Petit'), ('Kpogban', 'Entier')]),
        ('Senegal', [('tas', 'Petit'), ('Morceau', 'Grand')]),
    ])
    def test_stripping_the_suffix_gives_the_label_served_without_it(self, country, pairs):
        got = _carry(country, pairs)
        served = list(served_unit_labels(country, [u for u, _ in pairs]))
        sizes = [size_label(s) for _, s in pairs]
        assert [g[: -len(f' ({z})')] for g, z in zip(got, sizes)] == served
        # and the read-time table leaves every sized label alone
        assert list(served_unit_labels(country, got)) == got

    def test_a_sized_label_the_read_time_table_would_move_is_refused(self, monkeypatch):
        real = ehcvm.served_unit_labels

        def moved(country, values):
            return np.asarray([{'Tas (Petit)': 'Tas'}.get(v, v)
                               for v in real(country, values)], dtype=object)
        monkeypatch.setattr(ehcvm, 'served_unit_labels', moved)
        with pytest.raises(UnitSizeLabelError, match='would relabel'):
            _carry('Togo', [('Tas', 'Petit')])

    def test_a_decode_that_rewrites_a_sized_label_is_refused(self):
        with pytest.raises(UnitSizeLabelError, match='would rewrite'):
            _carry('Togo', [('Tas', 'Petit')], decode=str.upper)

    def test_a_base_the_u_table_serves_no_label_for_is_refused(self, monkeypatch):
        monkeypatch.setattr(ehcvm, 'served_unit_labels',
                            lambda country, values: np.asarray([None] * len(values), dtype=object))
        with pytest.raises(UnitSizeLabelError, match='serves no label'):
            _carry('Togo', [('Tas', 'Petit')])


class TestGuardReachesTheCaller:
    """The derived-table paths catch ValueError / KeyError / RuntimeError and fall
    back to another build, which turns the guard's instruction into "Could not
    materialize".  The delivered path is tested in test_ehcvm_7day.py."""

    def test_it_is_none_of_what_those_paths_catch(self):
        caught = (FileNotFoundError, KeyError, ValueError, RuntimeError)
        assert issubclass(UnitSizeLabelError, Exception)
        assert not issubclass(UnitSizeLabelError, caught)

    @pytest.mark.parametrize('table', ['food_prices', 'food_quantities', 'food_expenditures'])
    def test_the_derived_path_does_not_catch_it(self, monkeypatch, table):
        from lsms_library.country import Country
        calls = []

        def boom(self, waves=None, method_name=None, **kw):
            calls.append(method_name)
            raise UnitSizeLabelError('the instruction')
        monkeypatch.setattr(Country, '_aggregate_wave_data', boom)
        with pytest.raises(UnitSizeLabelError, match='the instruction'):
            getattr(Country('Togo'), table)()
        assert calls == ['food_acquired'], calls   # no fallback build was tried

    @pytest.mark.parametrize('table', ['food_prices', 'food_quantities', 'food_expenditures'])
    def test_a_valueerror_there_would_have_been_caught(self, monkeypatch, table):
        """The control: this is the behaviour the subclass exists to avoid."""
        from lsms_library.country import Country
        calls = []

        def boom(self, waves=None, method_name=None, **kw):
            calls.append(method_name)
            if method_name == 'food_acquired':
                raise ValueError('the instruction')
            raise RuntimeError('fallback reached')
        monkeypatch.setattr(Country, '_aggregate_wave_data', boom)
        with pytest.raises(RuntimeError, match='fallback reached'):
            getattr(Country('Togo'), table)()
        assert calls[0] == 'food_acquired' and len(calls) > 1, calls


class TestHook:
    """The whole hook on a wide frame: three acquisition sources, one label."""

    @staticmethod
    def _wide():
        idx = pd.MultiIndex.from_arrays(
            [['2018'] * 3, ['v1'] * 3, [1, 1, 1], ['h1', 'h2', 'h3'],
             ['Riz'] * 3, ['Tas', 'Tas', 'Kg']],
            names=['t', 'v', 'visit', 'i', 'j', 'u'])
        return pd.DataFrame({
            'Quantity': [10.0, 10.0, 5.0],
            'Produced': [2.0, 0.0, 0.0],
            'GiftQuantity': [3.0, 0.0, 0.0],
            'ConsumptionUnitCode': ['Tas', 'Tas', 'Kg'],
            'ConsumptionUnitSize': ['Petit', 'Grand', 'Unité de taille unique'],
            'LastPurchaseQuantity': [1.0, 1.0, 1.0],
            'LastPurchaseUnitCode': ['Tas', 'Tas', 'Kg'],
            'LastPurchaseUnitSize': ['Petit', 'Moyen', 'Unité de taille unique'],
            'LastPurchaseValue': [100.0, 100.0, 500.0],
        }, index=idx)

    def test_every_source_carries_the_size_and_nothing_else_moves(self):
        out = food_acquired_ehcvm(self._wide(), derivation_key('Togo')).sort_index()
        flat = out.reset_index().set_index(['i', 's'])
        assert flat.loc[('h1', 'purchased'), 'u'] == 'Tas (Petit)'
        assert flat.loc[('h1', 'produced'), 'u'] == 'Tas (Petit)'
        assert flat.loc[('h1', 'inkind'), 'u'] == 'Tas (Petit)'
        assert flat.loc[('h2', 'purchased'), 'u'] == 'Tas (Grand)'
        assert flat.loc[('h3', 'purchased'), 'u'] == 'Kg'
        # The #876 identity and its NA arm read the size COLUMNS, as before.
        assert flat.loc[('h1', 'purchased'), 'Quantity'] == 5.0
        assert flat.loc[('h1', 'purchased'), 'Expenditure'] == pytest.approx(500.0)
        assert pd.isna(flat.loc[('h2', 'purchased'), 'Expenditure'])
        assert flat.loc[('h3', 'purchased'), 'Expenditure'] == pytest.approx(2500.0)
        assert flat.loc[('h1', 'produced'), 'Quantity'] == 2.0
        assert flat.loc[('h1', 'inkind'), 'Quantity'] == 3.0

    def test_a_key_outside_the_eight_countries_is_refused(self):
        with pytest.raises(ValueError, match='EHCVM'):
            food_acquired_ehcvm(self._wide(), 'Uganda::food_acquired::x')


class TestWiring:
    @pytest.mark.parametrize('country', ['CotedIvoire', 'Guinea-Bissau', 'Niger'])
    def test_a_hook_that_sweeps_mojibake_passes_its_repair(self, country):
        text = (countries_root() / country / '2018-19' / '_' / 'mapping.py').read_text(encoding='utf-8')
        assert 'decode=_decode_mojibake' in text

    def test_the_metric_copy_has_not_drifted(self):
        from lsms_library.transformations import KNOWN_METRIC
        assert ehcvm._EXACT_METRIC_LABELS == frozenset(KNOWN_METRIC)

    def test_the_sentinel_copy_has_not_drifted(self):
        from lsms_library.transformations import _U_SENTINELS
        assert ehcvm._MISSING_UNIT_LABELS == frozenset(_U_SENTINELS)

    def test_no_kg_inference_symbol_reaches_the_ehcvm_closure(self):
        """Why the two sets above are copies: GH #850 shipped a corpus-wide kg
        change with no re-warm, and an import here would end that for 8 countries."""
        from lsms_library import _build_registry as R
        names = {p.split('=', 1)[0] for p in R._closure_parts(food_acquired_ehcvm, set())}
        assert not {n for n in names if n.startswith('lsms_library.transformations.')}, names
