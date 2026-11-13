"""EHCVM 2018-19 and 2021-22 ``Quantity_kg`` from the survey's own unit-weight table (GH #1452).

Two tiers.  The rule on synthetic frames and the tree's own config needs no data.
``TestDelivered`` reads the tables and the source files directly, as the oracle
for what the build served, and carries ``requires_s3``.  Measured totals are in
the PR, not pinned.
"""
from __future__ import annotations

import re
import unicodedata
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lsms_library import ehcvm
from lsms_library.ehcvm import (
    NSU_COUNTRIES, StatedLabelOverruledWarning, UnitWeightJoinError, derivation_key,
    derive_nsu_kg_factor, food_acquired_ehcvm, food_acquired_with_unit_weights,
    nsu_derivation_key, nsu_product_crosswalk, nsu_unit_weights,
)
from lsms_library.paths import countries_root

WAVE = '2021-22'
OLD = '2018-19'
WAVES = (OLD, WAVE)
SPEC = ehcvm.NSU_TABLES
CELLS = [(c, w) for c in NSU_COUNTRIES for w in WAVES]
def _table(rows, year=False):
    cols = ['p', 'u', 'z', 'w'] + (['y'] if year else [])
    return pd.DataFrame(rows, columns=cols)


def _weights(rows, year=False):
    return nsu_unit_weights(_table(rows, year), product='p', unit='u', size='z', weight='w',
                            year='y' if year else None)



def _withheld(country, wave):
    """``(i, j)`` of the records a registered ``rows: 0`` entry withholds or serves NA.

    Since 2026-11-12 (GH #1707, #1519, and Senegal's salt): those records' served rows
    are gone, or carry Quantity NA, so the raw quantity no longer describes them.
    """
    import lsms_library as ll
    from lsms_library.derivations import records_for
    out = set()
    for key, rec in records_for(country, 'food_acquired').items():
        waves = rec.waves if isinstance(rec.waves, (list, tuple)) else None
        if rec.rows == 0 and (waves is None or wave in waves):
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                raw = ll.Country(country).derivation_inputs(key, wave=wave).reset_index()
            out |= set(zip(raw['i'].astype(str), raw['j'].astype(str)))
    return out


class TestNationalWeights:
    def test_a_key_is_the_median_of_its_rows_in_kilograms(self):
        w = _weights([(1, 143, 1, 100.0), (1, 143, 1, 200.0), (1, 143, 1, 900.0),
                      (1, 143, 2, 400.0)])
        assert w.loc[(1, 143, 1)] == pytest.approx(0.2)
        assert w.loc[(1, 143, 2)] == pytest.approx(0.4)
        assert list(w.index.names) == ['product', 'unit', 'size']

    def test_a_duplicated_cell_contributes_both_rows(self):
        """Burkina Faso holds 34 such cells, and each pair disagrees."""
        assert _weights([(1, 143, 1, 100.0), (1, 143, 1, 300.0)]).loc[(1, 143, 1)] == pytest.approx(0.2)

    def test_a_row_with_no_positive_weight_or_no_code_is_not_a_weighing(self):
        w = _weights([(1, 143, 1, 0.0), (1, 143, 1, np.nan), (1, 143, 1, 300.0),
                      (np.nan, 143, 1, 999.0)])
        assert w.loc[(1, 143, 1)] == pytest.approx(0.3)
        assert len(w) == 1

    def test_a_key_comes_from_its_latest_year_only(self):
        """Niger's table holds two rounds' weighings of mostly different keys."""
        w = _weights([(1, 143, 1, 100.0, 2018), (1, 143, 1, 200.0, 2021), (1, 143, 1, 300.0, 2021),
                      (2, 143, 1, 700.0, 2018)], year=True)
        assert w.loc[(1, 143, 1)] == pytest.approx(0.25)   # 2021 only
        assert w.loc[(2, 143, 1)] == pytest.approx(0.7)    # 2018 is all there is

    def test_float_codes_are_codes(self):
        """Burkina Faso's codpr is float32."""
        t = _table([(131.0, 143.0, 1.0, 100.0)]).astype({'p': 'float32'})
        w = nsu_unit_weights(t, product='p', unit='u', size='z', weight='w')
        assert w.loc[(131, 143, 1)] == pytest.approx(0.1)


class TestFactor:
    W = _weights([(1, 143, 1, 250.0), (1, 100, 0, 985.6), (1, 101, 0, 4371.0),
                  (1, 136, 2, 15000.0)])

    def _f(self, *rows):
        p, u, z, served = zip(*rows)
        return derive_nsu_kg_factor(p, u, z, served, self.W)

    def test_the_row_reads_its_own_key(self):
        assert self._f((1, 143, 1, 'Tas'))[0] == pytest.approx(0.25)

    def test_a_key_the_table_lacks_is_nan(self):
        assert np.isnan(self._f((1, 143, 2, 'Tas'))).all()
        assert np.isnan(self._f((9, 143, 1, 'Tas'))).all()

    @pytest.mark.parametrize('p,u,z', [(np.nan, 143, 1), (1, np.nan, 1), (1, 143, np.nan)])
    def test_a_missing_code_is_nan_not_a_match(self, p, u, z):
        assert np.isnan(self._f((p, u, z, 'Tas'))).all()

    @pytest.mark.parametrize('u,served', [(100, 'Kg'), (101, 'Litre')])
    def test_an_exact_metric_label_keeps_what_it_states(self, u, served):
        assert np.isnan(self._f((1, u, 0, served))).all()

    def test_a_container_that_states_a_size_reads_the_table(self):
        """Senegal's Paquet (250 g) is why: the table and households agree against it."""
        assert self._f((1, 136, 2, 'Sac (25 Kg)'))[0] == pytest.approx(15.0)


class TestStatedLabelReport:
    """survey_kg outranks metric, so nothing downstream reports a disagreement."""

    def test_the_copy_reads_every_served_label_as_the_ladder_does(self):
        """Served labels only: a raw key like '101. Litre' never reaches either reader."""
        import lsms_library as ll
        from lsms_library.transformations import _parse_explicit_metric as parse
        for country in NSU_COUNTRIES:
            table = ll.Country(country).categorical_mapping['u']
            for label in set(table['Preferred Label'].dropna().astype(str)):
                got, want = ehcvm._stated_kg(label), parse(label)
                if want is None:
                    assert np.isnan(got), (country, label, got)
                else:
                    assert got == pytest.approx(want), (country, label, got, want)

    @pytest.mark.parametrize('label,want', [
        ('Paquet (250 g)', 0.25), ('Sac (25 Kg)', 25.0), ('Bouteille de 1,5 L', 1.5),
        ('Bouteille de 25 cl', 0.25), ('Sac(5kg)', 5.0), ('Filet (2,5 kg)', 2.5),
    ])
    def test_a_stated_size_is_read(self, label, want):
        assert ehcvm._stated_kg(label) == pytest.approx(want)

    @pytest.mark.parametrize('label', ['Tas', 'Kg', 'Tablette (30)', 'Motte (boule)', np.nan])
    def test_no_stated_size_is_invented(self, label):
        assert np.isnan(ehcvm._stated_kg(label))

    def test_a_wide_gap_is_reported_with_its_label_and_counts(self):
        with pytest.warns(StatedLabelOverruledWarning, match=r"'Paquet \(250 g\)' 2 of 3 rows"):
            ehcvm._report_overruled_labels('Senegal', WAVE, ['Paquet (250 g)'] * 3 + ['Tas'],
                                           [0.025, 0.025, 0.25, 0.1])

    def test_agreement_within_the_tolerance_is_silent(self):
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            ehcvm._report_overruled_labels('Mali', WAVE, ['Sac (25 Kg)', 'Tas', 'Kg'],
                                           [15.0, 0.1, np.nan])

    def test_it_never_changes_the_factor(self, monkeypatch):
        wide, raw, table = _wide()
        table = table.assign(W=[5000.0, 900.0, 1000.0])
        monkeypatch.setattr(ehcvm, 'served_unit_labels',
                            lambda country, values: np.asarray(
                                ['Seau (10 Kg)' if v == 'Tas' else v for v in values], dtype=object))
        _fake_spec(monkeypatch, raw, table)
        with pytest.warns(StatedLabelOverruledWarning):
            factor = ehcvm._nsu_factor(wide, 'Mali', WAVE)
        assert factor[0] == pytest.approx(5.0)


def _wide():
    """A wide frame as the hook receives it, with its source file beside it."""
    idx = pd.MultiIndex.from_arrays(
        [[WAVE] * 5, ['v1'] * 5, [1] * 5, ['h1', 'h1', 'h2', 'h2', 'h3'],
         ['Riz', 'Gombo', 'Riz', 'Riz', 'Riz'],
         ['Tas', 'Kg', 'Tas', 'Tas', 'Tas']],
        names=['t', 'v', 'visit', 'i', 'j', 'u'])
    wide = pd.DataFrame({
        'Quantity': [10.0, 3.0, 2.0, 4.0, 5.0],
        'Produced': [2.0, 0.0, 0.0, 0.0, 0.0],
        'GiftQuantity': [3.0, 0.0, 0.0, 0.0, 0.0],
        'ConsumptionUnitCode': ['Tas', 'Kg', 'Tas', 'Tas', 'Tas'],
        'ConsumptionUnitSize': ['Petit', 'Taille unique', 'Petit', 'Petit', 'Moyen'],
        'LastPurchaseQuantity': [1.0] * 5,
        'LastPurchaseUnitCode': ['Tas', 'Kg', 'Tas', 'Tas', 'Tas'],
        'LastPurchaseUnitSize': ['Petit', 'Taille unique', 'Petit', 'Petit', 'Moyen'],
        'LastPurchaseValue': [100.0, 500.0, 100.0, 100.0, 100.0],
    }, index=idx)
    raw = pd.DataFrame({
        's07bq01': [1, 2, 1, 3, 1],             # rows 2 and 3 fold onto one `j`
        's07bq03a': wide['Quantity'].to_numpy(),
        's07bq03b': [143, 100, 143, 143, 143],
        's07bq03c': [1, 0, 1, 1, 2],
        's07bq08': wide['LastPurchaseValue'].to_numpy(),
    })
    table = pd.DataFrame({'P': [1, 3, 2], 'U': [143, 143, 100], 'Z': [1, 1, 0],
                          'W': [250.0, 900.0, 1000.0]})
    return wide, raw, table


def _fake_spec(monkeypatch, raw, table, wave_labels=None, own_labels=None, country='Mali'):
    from lsms_library import local_tools

    def fake(fn, convert_categoricals=True, **kw):
        assert convert_categoricals is False, 'the join reads codes, never labels'
        return raw if Path(fn).name.startswith('source') else table

    def labels(fn, *a, **kw):
        name = Path(fn).name
        if name == 'source18.dta':
            return {'s07bq01': wave_labels}
        if name == 'source.dta':
            return {'s07bq01': own_labels}
        return {}
    monkeypatch.setattr(local_tools, 'get_dataframe', fake)
    monkeypatch.setattr(local_tools, 'get_categorical_mapping', labels)
    monkeypatch.setitem(SPEC, country, dict(
        table='table.dta', product='P', unit='U', size='Z', weight='W', year=None, names=None,
        sources={OLD: 'source18.dta', WAVE: 'source.dta'}))


def _build(monkeypatch, wide, raw, table, country='Mali'):
    _fake_spec(monkeypatch, raw, table, country=country)
    return food_acquired_with_unit_weights(wide, derivation_key(country), wave=WAVE)


def _walk(wave, table, harmonize=None):
    out = nsu_product_crosswalk(wave, table, harmonize)
    return {w: (sorted(g['table_code']), g['tier'].iloc[0]) for w, g in out.groupby('wave_code')}


class TestProductCrosswalk:
    """Most product codes the two waves share carry different labels, in all four countries
    (Burkina Faso 124 of 135, Mali 123 of 134, Niger 126 of 138, Senegal 128 of 140)."""

    def test_a_shared_code_means_nothing_the_trap(self):
        got = _walk({118: 'Sel', 119: 'Piment'},
                    {118: ["Pâte d'arachide"], 139: ['Sel'], 119: ['Fromage à base de soja']})
        assert got == {118: ([139], 'label')}

    def test_a_label_with_no_partner_is_not_rescued_by_its_code(self):
        assert _walk({118: 'Sel'}, {118: ["Pâte d'arachide"]}) == {}

    @pytest.mark.parametrize('wave_label,table_label', [
        ('       Canne à sucre', 'Canne à sucre'),       # Mali pads 2018-19's labels
        ('118. Sel', 'Sel'),                             # Niger and Senegal number theirs
        ('UnitÃ©', 'Unité'),                             # mojibake
        ('Cuillère à&nbsp; soupe', 'cuillere a soupe'),  # Senegal's html
        ('Viande de bÅ__uf', 'Viande de bœuf'),          # Niger's lost bytes
        ('Viande de boeuf', 'Viande de bœuf'),
    ])
    def test_the_label_tier_is_spelling_only(self, wave_label, table_label):
        assert _walk({1: wave_label}, {9: [table_label]}) == {1: ([9], 'label')}

    def test_the_harmonized_tier_reads_the_countrys_own_table(self):
        got = _walk({1: 'Poisson fumé type 1'}, {9: ['Poisson fumé']},
                    {'poisson fume type 1': 'Poisson fumé', 'poisson fume': 'Poisson fumé'})
        assert got == {1: ([9], 'harmonized')}

    def test_a_bundle_reads_every_part_the_instrument_later_split(self):
        assert _walk({1: 'Aubergine, Courge/Courgette'},
                     {8: ['Aubergine,'], 9: ['Courge/Courgette']}) == {1: ([8, 9], 'parts')}
        assert _walk({1: 'Sucre (poudre ou morceaux)'},
                     {8: ['Sucre poudre'], 9: ['Sucre morceaux'], 7: ['Sel']}) == {1: ([8, 9], 'parts')}

    def test_the_stem_tier_is_for_a_truncated_label(self):
        assert _walk({1: '       Cube alimentaire (Maggi, Ju'},
                     {9: ['Cube alimentaire (Maggi, Jumbo, etc.)']}) == {1: ([9], 'stem')}

    def test_the_first_tier_with_a_match_wins(self):
        got = _walk({1: 'Café, thé'}, {8: ['Café, thé'], 9: ['Café'], 7: ['Thé']})
        assert got == {1: ([8], 'label')}

    def test_a_sibling_word_is_not_a_match(self):
        """A leading word is not a label: `Lait` must not read `Lait caillé`."""
        assert _walk({1: 'Lait'}, {9: ['Lait caillé, yaourt'], 8: ['Lait en poudre']}) == {}


class TestOlderWaveHook:
    @staticmethod
    def _frames():
        wide, raw, _ = _wide()
        # Rows 0, 2, 3, 4 are code 1 in this wave, `Riz` in its labels.  The table numbers
        # `Riz` 7, and its code 1 is another food, which is the trap.
        table = pd.DataFrame({'P': [1, 7, 7, 2], 'U': [143, 143, 143, 100], 'Z': [1, 1, 2, 0],
                              'W': [9000.0, 250.0, 400.0, 1000.0]})
        return wide, raw.assign(s07bq01=[1, 2, 1, 1, 1]), table

    def test_the_row_reads_its_labels_weight_not_its_codes(self, monkeypatch):
        wide, raw, table = self._frames()
        _fake_spec(monkeypatch, raw, table, wave_labels={1: 'Riz', 2: 'Gombo'},
                   own_labels={1: 'Autre chose', 7: 'Riz', 2: 'Gombo'})
        factor = ehcvm._nsu_factor(wide, 'Mali', OLD)
        assert factor[0] == pytest.approx(0.25)           # (Riz, Tas, Petit), not 9 kg
        assert factor[4] == pytest.approx(0.4)            # (Riz, Tas, Moyen)
        assert np.isnan(factor[1])                        # Kg keeps its label

    def test_a_product_with_no_label_stays_on_the_ladder(self, monkeypatch):
        wide, raw, table = self._frames()
        _fake_spec(monkeypatch, raw, table, wave_labels={1: 'Sorgho', 2: 'Gombo'},
                   own_labels={1: 'Autre chose', 7: 'Riz', 2: 'Gombo'})
        factor = ehcvm._nsu_factor(wide, 'Mali', OLD)
        assert np.isnan(factor[0]) and np.isnan(factor[4])

    def test_a_bundle_takes_the_median_of_its_parts(self, monkeypatch):
        wide, raw, _ = self._frames()
        table = pd.DataFrame({'P': [7, 8], 'U': [143, 143], 'Z': [1, 1], 'W': [100.0, 300.0]})
        _fake_spec(monkeypatch, raw, table, wave_labels={1: 'Aubergine, Courge'},
                   own_labels={7: 'Aubergine', 8: 'Courge'})
        assert ehcvm._nsu_factor(wide, 'Mali', OLD)[0] == pytest.approx(0.2)


def _h1_riz(out, source, column):
    flat = out.reset_index()
    hit = flat[(flat['i'] == 'h1') & (flat['j'] == 'Riz') & (flat['s'] == source)]
    assert len(hit) == 1
    return hit[column].iloc[0]


class TestHook:
    def test_each_source_gets_its_own_quantity_times_the_factor(self, monkeypatch):
        out = _build(monkeypatch, *_wide())
        assert _h1_riz(out, 'purchased', 'u') == 'Tas (Petit)'
        assert _h1_riz(out, 'purchased', 'Quantity') == 5.0
        assert _h1_riz(out, 'purchased', 'Quantity_kg') == pytest.approx(1.25)
        assert _h1_riz(out, 'produced', 'Quantity_kg') == pytest.approx(0.5)
        assert _h1_riz(out, 'inkind', 'Quantity_kg') == pytest.approx(0.75)

    def test_nothing_else_about_the_frame_moves(self, monkeypatch):
        wide, raw, table = _wide()
        got = _build(monkeypatch, wide.copy(), raw, table)
        base = food_acquired_ehcvm(wide.copy(), derivation_key('Mali'))
        pd.testing.assert_frame_equal(got[['Quantity', 'Expenditure']],
                                      base[['Quantity', 'Expenditure']])
        assert list(got.index) == list(base.index)

    def test_a_metric_row_stays_on_its_label(self, monkeypatch):
        out = _build(monkeypatch, *_wide()).reset_index()
        kg = out[out['u'] == 'Kg']
        assert len(kg) and kg['Quantity_kg'].isna().all()
        assert (kg['Derivation'] == derivation_key('Mali')).all()

    def test_a_duplicated_key_gets_no_kilogram_because_core_would_select_it(self, monkeypatch):
        """h2's two products fold onto one (j, u); core sums Quantity and selects
        any other column, so a kilogram here would describe one of the two rows."""
        out = _build(monkeypatch, *_wide()).reset_index()
        dup = out[out['i'] == 'h2']
        assert len(dup) == 2 and dup['Quantity_kg'].isna().all()
        assert (dup['Derivation'] == derivation_key('Mali')).all()

    def test_the_key_is_on_exactly_the_rows_with_a_kilogram(self, monkeypatch):
        out = _build(monkeypatch, *_wide())
        weight_key = nsu_derivation_key('Mali')
        has_key = out['Derivation'].fillna('').str.contains(weight_key, regex=False)
        assert (has_key.to_numpy() == out['Quantity_kg'].notna().to_numpy()).all()
        assert has_key.any()

    def test_a_purchased_row_keeps_both_keys_sorted(self, monkeypatch):
        out = _build(monkeypatch, *_wide())
        keys = sorted([derivation_key('Mali'), nsu_derivation_key('Mali')])
        assert _h1_riz(out, 'purchased', 'Derivation') == '+'.join(keys)
        assert _h1_riz(out, 'produced', 'Derivation') == nsu_derivation_key('Mali')
        assert _h1_riz(out, 'inkind', 'Derivation') == nsu_derivation_key('Mali')

    def test_a_row_with_no_table_weight_stays_on_the_ladder(self, monkeypatch):
        out = _build(monkeypatch, *_wide()).reset_index()
        h3 = out[out['i'] == 'h3']     # (1, 143, 2) is not in the table
        assert len(h3) and h3['Quantity_kg'].isna().all()

    @pytest.mark.parametrize('column', ['s07bq03a', 's07bq08'])
    def test_a_frame_that_is_not_the_source_row_for_row_is_refused(self, monkeypatch, column):
        wide, raw, table = _wide()
        raw = raw.assign(**{column: raw[column].to_numpy()[::-1]})
        with pytest.raises(UnitWeightJoinError, match='row for row'):
            _build(monkeypatch, wide, raw, table)

    def test_a_country_with_no_table_is_refused(self):
        with pytest.raises(ValueError, match='no EHCVM unit-weight table'):
            nsu_derivation_key('Togo')

    def test_the_guard_is_none_of_what_the_derived_paths_catch(self):
        assert not issubclass(UnitWeightJoinError, (FileNotFoundError, KeyError, ValueError, RuntimeError))


class TestWiring:
    def test_the_spec_is_the_one_the_tables_were_measured_on(self):
        """Why each: lsms_library/ehcvm.py and each country's CONTENTS.org."""
        got = {c: (SPEC[c]['table'], SPEC[c]['weight'], SPEC[c]['year']) for c in NSU_COUNTRIES}
        assert got == {'Burkina_Faso': ('ehcvm_nsu_bfa2021.dta', 'poids_moyen', None),
                       'Mali': ('ehcvm_nsu_mli2021.dta', 'poids', None),
                       'Niger': ('ehcvm_nsu_ner2021.dta', 'mu', 'annee'),
                       'Senegal': ('ehcvm_nsu_sen2021_nat.dta', 'poids', None)}

    @pytest.mark.parametrize('country,wave', CELLS)
    def test_what_the_hook_reads_is_a_declared_source(self, country, wave):
        import os
        from lsms_library.local_tools import _collect_file_paths_from_block
        from lsms_library.yaml_utils import load_yaml
        root = countries_root() / country
        info = load_yaml(root / wave / '_' / 'data_info.yml')
        declared = {os.path.normpath(root / wave / 'Data' / f)
                    for f in _collect_file_paths_from_block(info['food_acquired'])}
        data = root / WAVE / 'Data'
        assert os.path.normpath(data / SPEC[country]['table']) in declared
        assert os.path.normpath(root / wave / 'Data' / SPEC[country]['sources'][wave]) in declared
        assert info['food_acquired']['file'] == SPEC[country]['sources'][wave]
        if wave == OLD:   # the crosswalk reads 2021-22's product labels too
            assert os.path.normpath(data / SPEC[country]['sources'][WAVE]) in declared

    @pytest.mark.parametrize('country,wave', CELLS)
    def test_the_hook_names_its_wave(self, country, wave):
        text = (countries_root() / country / wave / '_' / 'mapping.py').read_text(encoding='utf-8')
        assert re.search(rf"_food_acquired_with_unit_weights\(\s*df, FOOD_ACQUIRED_DERIVATION, "
                         rf"wave='{wave}'", text)

    @pytest.mark.parametrize('country', NSU_COUNTRIES)
    def test_the_derivation_is_registered_with_the_shared_function(self, country):
        from lsms_library.derivations import derivation_records
        rec = derivation_records(country)[nsu_derivation_key(country)]
        assert rec['function'] == 'lsms_library.ehcvm:derive_nsu_kg_factor'
        assert rec['columns'] == ['Quantity_kg']
        assert rec['waves'] == list(WAVES)
        assert f'countries.{country}.{WAVE}._.mapping:inputs_food_acquired_unit_weight' in rec['inputs']

    def test_no_kg_inference_symbol_reaches_the_new_hook(self):
        """As tests/test_ehcvm_unit_size.py pins for food_acquired_ehcvm: an edit to
        the kg ladder must not re-warm these four countries."""
        from lsms_library import _build_registry as R
        names = {p.split('=', 1)[0] for p in R._closure_parts(food_acquired_with_unit_weights, set())}
        assert not {n for n in names if n.startswith('lsms_library.transformations.')}, names

    def test_the_eight_country_hook_does_not_reach_the_new_code(self):
        """Which is what leaves the other waves' hashes where they are."""
        from lsms_library import _build_registry as R
        names = {p.split('=', 1)[0] for p in R._closure_parts(food_acquired_ehcvm, set())}
        assert not {n for n in names if 'unit_weight' in n or 'nsu' in n}, names


class TestInputsReadTheServedUnit:
    """`inputs_unit_weight` on synthetic frames.  The raw answer's unit is deliberately
    not the served one, as in Mali's padded and Burkina Faso's bare-number 2018-19 labels."""

    def _inputs(self, monkeypatch, extracted):
        wide, raw, table = _wide()
        _fake_spec(monkeypatch, raw, table)
        answers = pd.DataFrame(
            {'s07bq03a': raw['s07bq03a'].to_numpy(), 's07bq03b': [143.0, 100.0, 143.0, 143.0, 143.0],
             's07bq03c': raw['s07bq03c'].to_numpy(), 's07bq04': wide['Produced'].to_numpy(),
             's07bq05': wide['GiftQuantity'].to_numpy()},
            index=pd.MultiIndex.from_arrays(
                [[WAVE] * 5, wide.index.get_level_values('i'), wide.index.get_level_values('j')],
                names=['t', 'i', 'j']))
        monkeypatch.setattr(ehcvm, 'inputs_last_purchase', lambda c, w: answers)
        monkeypatch.setattr(ehcvm, '_extracted_units', lambda c, w, q: np.array(extracted, dtype=object))
        return ehcvm.inputs_unit_weight('Mali', WAVE)

    def test_a_served_kilogram_reports_no_table_weight(self, monkeypatch):
        """The table holds a weight for this row's codes; the hook declines it because
        the extraction serves `Kg`, whatever the raw answer says."""
        out = self._inputs(monkeypatch, ['Tas', 'Kg', 'Tas', 'Tas', 'Tas'])
        assert out['nsu_kg_per_unit'].isna().tolist() == [False, True, False, False, True]
        assert out['nsu_join'].notna().tolist() == [True, False, True, True, False]

    def test_the_same_codes_on_a_container_read_the_table(self, monkeypatch):
        out = self._inputs(monkeypatch, ['Tas', 'Tas', 'Tas', 'Tas', 'Tas'])
        assert out['nsu_kg_per_unit'].iloc[1] == pytest.approx(1.0)

    def test_what_a_label_states_comes_from_the_served_label(self, monkeypatch):
        out = self._inputs(monkeypatch, ['Sac (50 kg)', 'Kg', 'Tas', 'Tas', 'Tas'])
        assert out['stated_kg'].iloc[0] == pytest.approx(50.0)
        assert out['stated_kg'].iloc[1:].isna().all()

    def test_it_agrees_with_the_hook_on_every_row(self, monkeypatch):
        wide, raw, table = _wide()
        labels = wide.index.get_level_values('u').to_numpy(dtype=object)
        _fake_spec(monkeypatch, raw, table)
        hook = ehcvm._nsu_factor(wide, 'Mali', WAVE)
        out = self._inputs(monkeypatch, labels)
        assert np.array_equal(out['nsu_kg_per_unit'].to_numpy(dtype=float), hook, equal_nan=True)


def _source(country, fn, wave=WAVE, **kw):
    from lsms_library.local_tools import get_dataframe
    return get_dataframe(countries_root() / country / wave / 'Data' / fn, **kw)


def _exact_metric_unit_codes(country, wave=WAVE):
    """The s07bq03b codes the country serves as ``Kg`` or ``Litre``, by value label."""
    from lsms_library.local_tools import get_categorical_mapping
    labels = get_categorical_mapping(str(countries_root() / country / wave / 'Data'
                                         / SPEC[country]['sources'][wave]))['s07bq03b']
    codes = list(labels)
    served = ehcvm.served_unit_labels(country, [labels[c].strip() for c in codes])
    found = [c for c, s in zip(codes, served)
             if isinstance(s, str) and s.strip().lower() in ehcvm._EXACT_METRIC_LABELS]
    # Burkina Faso's 2018-19 file labels no Litre; its u comes from the shared codebook.
    return sorted(set(found) | {100, 101})


def _table_weights(country):
    """The national weight read directly from the file, as (product, unit, size)."""
    spec = SPEC[country]
    pc, uc, zc, wc = spec['product'], spec['unit'], spec['size'], spec['weight']
    t = _source(country, spec['table'], convert_categoricals=False)
    t = t[pd.to_numeric(t[wc], errors='coerce') > 0].dropna(subset=[pc, uc, zc]).astype(
        {pc: 'float64', uc: 'float64', zc: 'float64', wc: 'float64'}).astype(
        {pc: 'int64', uc: 'int64', zc: 'int64'})
    if spec['year']:
        t = t[t[spec['year']] == t.groupby([pc, uc, zc])[spec['year']].transform('max')]
    return t.groupby([pc, uc, zc])[wc].median() / 1000


@pytest.mark.requires_s3
class TestTheTableAgainstMarketWeighings:
    """ehcvm_prix owes nothing to the table or to household prices.  Mali's file holds
    no usable unit weights and none is held for Burkina Faso."""

    def test_niger_tiya_weighs_at_market_what_the_table_says(self):
        prix = _source('Niger', 'ehcvm_prix_ner2021.dta', convert_categoricals=False)
        tiya = prix[prix['variete__lab'].astype(str).str.contains('Tiya', case=False)
                    & (pd.to_numeric(prix['unite'], errors='coerce') == 100)
                    & (pd.to_numeric(prix['quantite1'], errors='coerce') > 0)]
        market = tiya.groupby('produit__id')['quantite1'].agg(['median', 'size'])
        market = market[market['size'] >= 30]
        table = _table_weights('Niger').xs((512, 6), level=[1, 2]) * 1000
        both = pd.concat([market['median'].astype(float).rename('market'),
                          table.rename('table')], axis=1).dropna()
        assert len(both) >= 15
        error = np.log(both['market'] / both['table'])
        assert error.abs().median() < 0.2
        assert (error.abs() < np.log(2)).mean() > 0.9
        assert abs(error.median()) < 0.15

    def test_senegal_market_weighs_the_tea_packet_at_the_tables_weight_not_the_labels(self):
        prix = _source('Senegal', 'ehcvm_prix_sen2021.dta', convert_categoricals=False)
        tea = prix[(pd.to_numeric(prix['codpr'], errors='coerce') == 157)
                   & prix['variete_desc2'].astype(str).str.contains('paquet de 25g')]
        assert len(tea) > 100
        market = float(pd.to_numeric(tea['poids'], errors='coerce').median())
        assert market == pytest.approx(0.025)
        assert _table_weights('Senegal').loc[(157, 129, 1)] == pytest.approx(market)
        assert ehcvm._stated_kg('Paquet (250 g)') / market == pytest.approx(10.0)


#: Every 2018-19 product that finds the table other than by its own label, as
#: ``wave code: (tier, table codes)``.  A DISCLOSURE, not an approval: most of these are
#: spelling, but Mali's 98 reads five peanut codes, in-shell cells included, because
#: ``harmonize_food`` folds them to ``Arachide``; Mali's 126 reads a differently described
#: catch-all, about 5x heavy; Niger's 77 reads four codes that disagree by 30x at one key.
#: These rows depend on the country's ``harmonize_food``, so a later fold there changes
#: kilograms with no other signal than the rebuild.  If this moves, say which kilograms
#: moved and why before re-pinning it.
NON_LABEL_CROSSWALK = {
    'Burkina_Faso': {
        49: ('harmonized', (57,)),  # Fromage
        56: ('harmonized', (64, 65)),  # Huile d'arachide
        67: ('harmonized', (78,)),  # Pasteque, Melon
        77: ('parts', (93, 94)),  # Aubergine, Courge/Courgette
        90: ('harmonized', (107,)),  # Autre legumes frais n.d.a (Kapok, Voaga)
        92: ('harmonized', (109, 110)),  # Petits pois
        98: ('harmonized', (115,)),  # Arachides decortiquees ou pilees
        106: ('harmonized', (125,)),  # Plantain
        114: ('parts', (134,)),  # Sucre (poudre ou morceaux)
        120: ('harmonized', (142,)),  # Gingembre
        121: ('harmonized', (144,)),  # Cube alimentaire (Maggi, Jumbo, )
        125: ('harmonized', (148,)),  # Vinaigre /moutarde
        129: ('harmonized', (155, 156)),  # Cafe
        135: ('harmonized', (162,)),  # Boissons gazeuses (coca, etc.)
        137: ('harmonized', (164,)),  # Bieres et vins traditionnels (dolo, vin de p
    },
    'Mali': {
        11: ('harmonized', (11,)),  # Autres cereales
        34: ('harmonized', (39,)),  # Autres viandes n.d.a.
        39: ('harmonized', (44, 45)),  # Poisson fume type 1
        40: ('harmonized', (44, 45)),  # Poisson fume type 2
        56: ('harmonized', (64, 65)),  # Huile d'arachide
        59: ('harmonized', (70,)),  # Autres huiles n.d.a. (mais, soja, huile palm
        65: ('harmonized', (75,)),  # Autres agrumes
        67: ('harmonized', (78,)),  # Pasteque, Melon
        71: ('harmonized', (87,)),  # Autres fruits (pommes, raisin, etc.)
        77: ('parts', (93, 94)),  # Aubergine, Courge/Courgette
        89: ('harmonized', (106,)),  # Autres legumes en feuilles
        98: ('harmonized', (113, 114, 115, 116, 117)),  # Arachides decortiquees ou pilees
        114: ('parts', (134, 135)),  # Sucre (poudre ou morceaux)
        119: ('harmonized', (140, 141)),  # Piment
        120: ('harmonized', (142,)),  # Gingembre
        125: ('parts', (150,)),  # Vinaigre /moutarde
        126: ('harmonized', (152,)),  # Autres condiments (poivre etc.)
        128: ('harmonized', (154,)),  # Autres produits alimentaires
        129: ('harmonized', (155, 156)),  # Cafe
        137: ('harmonized', (164,)),  # Bieres et vins traditionnels (dolo, vin de p
    },
    'Niger': {
        42: ('parts', (47,)),  # Crabes, crevettes et autres fruits de mer
        56: ('stem', (65,)),  # Huile d'arachide
        65: ('stem', (75,)),  # Autres agrumes
        67: ('parts', (78, 79)),  # Pasteque, Melon
        71: ('stem', (87,)),  # Autres fruits (pommes, raisin, etc.)
        73: ('harmonized', (89,)),  # Chou
        77: ('parts', (93, 94, 9400, 10022)),  # Aubergine, Courge/Courgette
        125: ('parts', (148, 150)),  # Vinaigre /moutarde
        137: ('stem', (164,)),  # Bieres et vins traditionnels (Bourkoutou, vi
    },
    'Senegal': {
        35: ('harmonized', (40,)),  # Poisson frais yaboy ou obo (sardinelle)
        36: ('harmonized', (41,)),  # Poisson frais thiof/seudd (baracouda)
        38: ('harmonized', (43,)),  # Autres poissons frais (dorade, youfoufe, rou
        39: ('harmonized', (44,)),  # Poisson fume ketiakh (sardinelle)
        40: ('harmonized', (45,)),  # Autres poissons fumes (kong fume, yaboy ou o
        41: ('harmonized', (46,)),  # Poisson seche (tambadiang, kong, etc.)
        42: ('harmonized', (47, 48)),  # Crabes, crevettes et autres fruits de mer
        58: ('harmonized', (66,)),  # Huile de soja / vegetale (e.g., Ninaal, Jaar
        59: ('harmonized', (70,)),  # Autres huiles n.d.a. (mais, huile palmiste
        71: ('harmonized', (87,)),  # Autres fruits (pommes, raisin, etc.)
        77: ('parts', (93, 94)),  # Aubergine, Courge/Courgette
        86: ('harmonized', (103,)),  # Feuilles de baobab (lalo)
        88: ('harmonized', (105,)),  # Feuilles de nebeday (moringa)
        92: ('harmonized', (109,)),  # Petits pois conserve
        98: ('harmonized', (115, 116)),  # Arachides decortiquees ou pilees (noflaye)
        126: ('harmonized', (152,)),  # Autres condiments (poivre etc.)
        137: ('harmonized', (164,)),  # Bieres et vins traditionnels (soum soum, sun
        140: ('harmonized', (65,)),  # Huile daarachide "sAgal"
    },
}


@pytest.mark.requires_s3
class TestTheTablesAreWhatTheWiringSays:
    """The choices the hooks make, read from the files themselves."""

    def test_senegal_three_files_are_one_table(self):
        strat = _source('Senegal', 'ehcvm_nsu_sen2021.dta', convert_categoricals=False)
        nat = _source('Senegal', 'ehcvm_nsu_sen2021_nat.dta', convert_categoricals=False)
        nat23 = _source('Senegal', 'ehcvm_nsu_sen2021_nat2023.dta', convert_categoricals=False)
        keys = ['produitID', 'uniteID', 'tailleID']
        assert strat.groupby(keys)['poids'].nunique().max() == 1
        assert not nat.duplicated(keys).any()
        m = strat.merge(nat, on=keys, suffixes=('', '_nat'))
        assert len(m) == len(strat) and (m['poids'] == m['poids_nat']).all()
        m = nat.merge(nat23.rename(columns={'codpr': 'produitID'}), on=keys, suffixes=('', '_23'))
        assert len(m) == len(nat) and (m['poids'] == m['poids_23']).all()

    def test_burkina_two_columns_are_labelled_the_wrong_way_round(self):
        """A median of gram weighings is an integer or a half; a mean is rarely either."""
        t = _source('Burkina_Faso', 'ehcvm_nsu_bfa2021.dta', convert_categoricals=False)
        spread = t['poids_sd'].astype(float) > 0

        def half_integer(col):
            x = t.loc[spread, col].astype(float)
            return (np.abs(2 * x - np.round(2 * x)) < 1e-3).mean()
        assert half_integer('poids_moyen') > 0.95 > 0.5 > half_integer('poids_median')

    def test_niger_p50_is_scaled_and_mu_is_not(self):
        t = _source('Niger', 'ehcvm_nsu_ner2021.dta', convert_categoricals=False)
        kg = t[t['unite'] == 100]
        assert kg['p50'].median() == pytest.approx(985.6)
        assert kg['mu'].median() == pytest.approx(1000.0)

    @pytest.mark.parametrize('country', NSU_COUNTRIES)
    def test_product_codes_do_not_carry_across_waves_the_trap(self, country):
        """Widened from Niger: the review of #1452 measured the trap in all four."""
        from lsms_library.local_tools import get_categorical_mapping as labels
        root = countries_root() / country
        old = labels(str(root / OLD / 'Data' / SPEC[country]['sources'][OLD]))['s07bq01']
        new = labels(str(root / WAVE / 'Data' / SPEC[country]['sources'][WAVE]))['s07bq01']
        shared = set(old) & set(new)
        assert len(shared) > 100
        differ = sum(ehcvm._label_key(old[k]) != ehcvm._label_key(new[k]) for k in shared)
        assert differ > 0.8 * len(shared)

    @pytest.mark.parametrize('country', NSU_COUNTRIES)
    def test_the_non_label_crosswalk_is_the_one_on_record(self, country):
        tab = _source(country, SPEC[country]['table'], convert_categoricals=False)
        walk = ehcvm._nsu_walk(country, OLD, tab)
        got = {int(code): (g['tier'].iloc[0], tuple(sorted(int(v) for v in g['table_code'])))
               for code, g in walk[walk['tier'] != 'label'].groupby('wave_code')}
        assert got == NON_LABEL_CROSSWALK[country]

    @pytest.mark.parametrize('country,variable', [
        (c, v) for c in NSU_COUNTRIES for v in ('s07bq03b', 's07bq03c')])
    def test_unit_and_size_codes_do_carry_across_waves(self, country, variable):
        """Which is what lets the older wave read them as codes."""
        from lsms_library.local_tools import get_categorical_mapping as labels
        root = countries_root() / country
        old = labels(str(root / OLD / 'Data' / SPEC[country]['sources'][OLD]))[variable]
        new = labels(str(root / WAVE / 'Data' / SPEC[country]['sources'][WAVE]))[variable]
        s = _source(country, SPEC[country]['sources'][OLD], wave=OLD, convert_categoricals=False)
        s = s[pd.to_numeric(s['s07bq03a'], errors='coerce') > 0]
        counts = pd.to_numeric(s[variable], errors='coerce').value_counts()
        same = differ = 0
        for code, n in counts.items():
            a, b = ehcvm._unit_key(old.get(int(code))), ehcvm._unit_key(new.get(int(code)))
            if a is None or b is None:
                continue
            a, b = ehcvm._label_key(old[int(code)]), ehcvm._label_key(new[int(code)])
            same, differ = (same + n, differ) if (a == b or a in b or b in a) else (same, differ + n)
        assert same > 0 and differ / (same + differ) < 0.02, (same, differ)

    def test_niger_annee_is_not_a_filter(self):
        """The commonest consumed keys exist only in the 2018 rows."""
        t = _source('Niger', 'ehcvm_nsu_ner2021.dta', convert_categoricals=False)
        s = _source('Niger', 's07b_me_ner2021.dta', convert_categoricals=False)
        s = s[pd.to_numeric(s['s07bq03a'], errors='coerce') > 0].dropna(subset=['s07bq01', 's07bq03b', 's07bq03c'])
        codes = list(zip(s['s07bq01'].astype(int), s['s07bq03b'].astype(int), s['s07bq03c'].astype(int)))
        keys = lambda x: set(zip(x['produit'].astype(int), x['unite'].astype(int), x['taille'].astype(int)))
        k21, kall = keys(t[t['annee'] == 2021]), keys(t)
        assert np.mean([c in kall for c in codes]) > 0.99 > 0.6 > np.mean([c in k21 for c in codes])

    @pytest.mark.parametrize('country', NSU_COUNTRIES)
    def test_a_kilogram_weighs_a_kilogram_on_the_national_rule(self, country):
        """The check that is not the table against itself: the label states the mass."""
        spec = SPEC[country]
        kw = {k: spec[k] for k in ('product', 'unit', 'size', 'weight')}
        t = _source(country, spec['table'], convert_categoricals=False)
        w = nsu_unit_weights(t, year=spec['year'], **kw)
        kg = w.xs(100, level='unit')
        assert kg.median() == pytest.approx(1.0)
        assert (np.abs(np.log(kg)) < np.log(1.5)).mean() > 0.9


@pytest.mark.requires_s3
@pytest.mark.slow
class TestDelivered:
    @pytest.fixture(scope='class')
    def built(self):
        import lsms_library as ll
        cache = {}

        def get(country):
            if country not in cache:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    cache[country] = ll.Country(country).food_acquired()
            return cache[country]
        return get

    @pytest.mark.parametrize('country', NSU_COUNTRIES)
    def test_only_these_waves_have_kilograms(self, built, country):
        fa = built(country)
        t = fa.index.get_level_values('t').astype(str)
        assert fa.loc[~t.isin(WAVES), 'Quantity_kg'].isna().all()
        for wave in WAVES:
            assert fa.loc[t == wave, 'Quantity_kg'].notna().any()

    @pytest.mark.parametrize('country,wave', CELLS)
    def test_no_exact_metric_row_has_one_and_every_one_carries_the_key(self, built, country, wave):
        fa = built(country)
        fa = fa[fa.index.get_level_values('t').astype(str) == wave]
        kg = fa['Quantity_kg'].notna().to_numpy()
        u = fa.index.get_level_values('u').astype(str).str.strip().str.lower()
        assert not (kg & u.isin(sorted(ehcvm._EXACT_METRIC_LABELS))).any()
        key = fa['Derivation'].fillna('').str.contains(nsu_derivation_key(country), regex=False)
        assert (key.to_numpy() == kg).all()
        assert not fa[kg].index.duplicated(keep=False).any()

    def _long(self, country, wave, weight, mask_metric=True):
        """One raw row per acquisition source, keyed as served.  `i` and `j` come through
        the library, from the raw-answer frame, checked against the file by position.
        ``mask_metric=False`` leaves ``weight`` as it came, for a test of the rows it blanks."""
        import lsms_library as ll
        s = _source(country, SPEC[country]['sources'][wave], wave=wave, convert_categoricals=False)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            inp = ll.Country(country).derivation_inputs(
                nsu_derivation_key(country), wave=wave).reset_index()
        assert len(inp) == len(s)
        q = pd.to_numeric(s['s07bq03a'], errors='coerce')
        assert np.array_equal(pd.to_numeric(inp['s07bq03a'], errors='coerce').to_numpy(dtype=float),
                              q.to_numpy(dtype=float), equal_nan=True)
        unit = pd.to_numeric(s['s07bq03b'], errors='coerce').to_numpy(dtype=float)
        w = weight(s, inp)
        if mask_metric:
            w = np.where(np.isin(unit, _exact_metric_unit_codes(country, wave)), np.nan, w)
        produced = pd.to_numeric(s['s07bq04'], errors='coerce').fillna(0).to_numpy(dtype=float)
        gift = pd.to_numeric(s['s07bq05'], errors='coerce').fillna(0).to_numpy(dtype=float)
        total = q.fillna(0).to_numpy(dtype=float)
        long = pd.concat([
            pd.DataFrame({'i': inp['i'].astype(str), 'j': inp['j'].astype(str), 's': name,
                          'q': qty, 'w': w})
            for name, qty in (('purchased', np.clip(total - produced - gift, 0, None)),
                              ('produced', produced), ('inkind', gift))])
        long = long[long['q'] > 0]
        withheld = _withheld(country, wave)
        if withheld:
            long = long[~pd.Index(list(zip(long['i'], long['j']))).isin(withheld)]
        return long[~long.duplicated(['i', 'j', 's'], keep=False)]

    def _served(self, built, country, wave):
        fa = built(country).reset_index()
        fa = fa[fa['t'].astype(str) == wave].assign(i=lambda d: d['i'].astype(str),
                                                    j=lambda d: d['j'].astype(str),
                                                    s=lambda d: d['s'].astype(str))
        return fa, fa[~fa.duplicated(['i', 'j', 's'], keep=False)]

    @pytest.mark.parametrize('country', NSU_COUNTRIES)
    def test_each_served_row_reads_its_own_keys_weight(self, built, country):
        """Row by row on (i, j, s).  The expected factor comes from the two files by
        code and owes nothing to the hook."""
        oracle = _table_weights(country)

        def weight(s, inp):
            codes = pd.MultiIndex.from_arrays(
                [pd.to_numeric(s[c], errors='coerce').fillna(-1).astype('int64')
                 for c in ('s07bq01', 's07bq03b', 's07bq03c')])
            return oracle.reindex(codes).to_numpy(dtype=float)
        long = self._long(country, WAVE, weight)
        fa, unique = self._served(built, country, WAVE)
        m = unique.merge(long, on=['i', 'j', 's'], how='inner')
        assert np.isclose(m['Quantity'].astype(float), m['q']).all()
        served = m[m['Quantity_kg'].notna()]
        assert len(served) > 0.95 * int(fa['Quantity_kg'].notna().sum())
        got = served['Quantity_kg'].astype(float) / served['Quantity'].astype(float)
        assert np.isclose(got, served['w'].astype(float), rtol=1e-9).all()
        # No row the table covers is left out, off a duplicated key.
        missed = m[m['Quantity_kg'].isna() & m['w'].notna()]
        assert missed.empty, f'{len(missed)} covered rows lack kilograms'
        assert served['Quantity_kg'].astype(float).sum() == pytest.approx(
            float((served['q'] * served['w']).sum()), rel=1e-9)

    @pytest.mark.parametrize('country', NSU_COUNTRIES)
    def test_an_older_row_reads_its_labels_weight_row_by_row(self, built, country):
        """The oracle for 2018-19: a plain label match, written here, with unit and size by
        code.  It covers the `label` tier, the bulk of the rows, and owes nothing to the
        crosswalk.  A row on that tier must read exactly this, whatever its code means."""
        from lsms_library.local_tools import get_categorical_mapping as labels

        def plain(text):
            if not isinstance(text, str):
                return None
            text = text.replace('&nbsp;', ' ').replace('\u00c5__', 'oe').strip()
            try:
                text = text.encode('latin-1').decode('utf-8')
            except (UnicodeEncodeError, UnicodeDecodeError):
                pass
            for mark in ('\u0153', '\u0152', '\x9c', '\x8c'):
                text = text.replace(mark, 'oe')
            text = re.sub(r'^\d+\s*[.\-]\s*', '', text)
            text = ''.join(c for c in unicodedata.normalize('NFKD', text) if not unicodedata.combining(c))
            return re.sub(r'[^a-z0-9]+', ' ', text.lower()).strip() or None

        spec = SPEC[country]
        root = countries_root() / country
        oracle = _table_weights(country)
        own = labels(str(root / WAVE / 'Data' / spec['sources'][WAVE]))['s07bq01']
        names = labels(str(root / WAVE / 'Data' / spec['table'])).get(spec['product'], {})
        table = _source(country, spec['table'], convert_categoricals=False)
        codes_of = {}
        for code in table[spec['product']].dropna().astype(float).astype('int64').unique():
            found = {plain(own.get(int(code))), plain(names.get(int(code)))}
            if spec['names']:
                found |= {plain(v) for v in table.loc[
                    table[spec['product']].astype(float) == code, spec['names']].unique()}
            for key in found - {None}:
                codes_of.setdefault(key, set()).add(int(code))
        by_key = oracle.reset_index()
        by_key.columns = ['p', 'u', 'z', 'w']
        old = labels(str(root / OLD / 'Data' / spec['sources'][OLD]))['s07bq01']
        lookup = {}
        for code, label in old.items():
            for table_code in codes_of.get(plain(label), ()):
                lookup.setdefault(int(code), set()).add(table_code)
        pairs = pd.DataFrame([(a, b) for a, bs in lookup.items() for b in bs], columns=['code', 'p'])
        w = pairs.merge(by_key, on='p').groupby(['code', 'u', 'z'])['w'].median()

        def weight(s, inp):
            idx = pd.MultiIndex.from_arrays(
                [pd.to_numeric(s[c], errors='coerce').fillna(-1).astype('int64')
                 for c in ('s07bq01', 's07bq03b', 's07bq03c')])
            return w.reindex(idx).to_numpy(dtype=float)
        long = self._long(country, OLD, weight)
        fa, unique = self._served(built, country, OLD)
        m = unique.merge(long, on=['i', 'j', 's'], how='inner')
        assert np.isclose(m['Quantity'].astype(float), m['q']).all()
        # Burkina Faso's 2018-19 file labels too few units to name its metric codes, so the
        # served label decides here, as it does in the hook.
        metric = m['u'].astype(str).str.strip().str.lower().isin(sorted(ehcvm._EXACT_METRIC_LABELS))
        expected = m[m['w'].notna() & ~metric]
        assert len(expected) > 0.5 * int(fa['Quantity_kg'].notna().sum())
        assert expected['Quantity_kg'].notna().all()
        got = expected['Quantity_kg'].astype(float) / expected['Quantity'].astype(float)
        assert np.isclose(got, expected['w'].astype(float), rtol=1e-9).all()

    def test_niger_sel_in_2018_19_reads_sel_not_the_code_the_trap(self, built):
        """Code 118 is Sel in this wave and Pate d'arachide in the table."""
        raw, weights, _, tiers = ehcvm._nsu_read('Niger', OLD)
        assert tiers[118] == 'label'
        table = _table_weights('Niger')
        assert weights.loc[(118, 139, 1)] == pytest.approx(table.loc[(139, 139, 1)])
        assert table.loc[(139, 139, 1)] != pytest.approx(table.loc[(118, 139, 1)])

    @pytest.mark.parametrize('country,wave', CELLS)
    def test_the_ladder_counts_these_rows_as_the_surveys(self, built, country, wave):
        from lsms_library.transformations import food_kg_factors
        fa = built(country)
        fa = fa[fa.index.get_level_values('t').astype(str) == wave]
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            parts = food_kg_factors(fa).attrs['kg_factor_sources']
        assert parts['survey_kg'] == int(fa['Quantity_kg'].notna().sum())
        assert sum(parts.values()) == len(fa)

    @pytest.mark.parametrize('country,wave', CELLS)
    def test_the_inputs_weight_is_the_served_factor_or_nan_on_both_sides(self, built, country, wave):
        """What `derivation_inputs` says a row read is what the row was served, on the rows
        the hook declines too: a padded or bare-number raw label must not make a served
        `Kg` look like a table row (Mali and Burkina Faso 2018-19, review of #1452).  Not
        masked by unit, because the mask is where that hid.  A repeating (i, j, s) is left
        out: `inputs_unit_weight` says what it reports there."""
        long = self._long(country, wave, lambda s, inp: pd.to_numeric(
            inp['nsu_kg_per_unit'], errors='coerce').to_numpy(dtype=float), mask_metric=False)
        _, unique = self._served(built, country, wave)
        m = unique.merge(long, on=['i', 'j', 's'], how='inner')
        assert len(m) > 0.9 * len(unique)
        assert np.isclose(m['Quantity'].astype(float), m['q']).all()
        got = pd.to_numeric(m['Quantity_kg'], errors='coerce') / m['Quantity'].astype(float)
        want = m['w'].astype(float)
        both_nan = got.isna() & want.isna()
        same = np.isclose(got, want, rtol=1e-9)
        wrong = m[~(both_nan | same)]
        assert wrong.empty, f'{len(wrong)} input factors disagree with served kilograms'
        assert got.notna().any() and both_nan.any()

    @pytest.mark.parametrize('country,wave', CELLS)
    def test_the_inputs_callable_joins_and_counts_the_branches(self, built, country, wave):
        import lsms_library as ll
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            raw = ll.Country(country).derivation_inputs(nsu_derivation_key(country), wave=wave)
        assert list(raw.index.names) == ['t', 'i', 'j']
        assert {'s07bq01', 's07bq03a', 'unit_code', 'size_code', 'nsu_kg_per_unit',
                'stated_kg', 'nsu_join'} <= set(raw.columns)
        found = raw['nsu_kg_per_unit'].notna()
        assert (raw['nsu_join'].notna() == found).all()
        assert set(raw['nsu_join'].dropna().unique()) <= set(ehcvm.NSU_JOIN_TIERS)
        assert (raw['nsu_join'].dropna() == 'code').all() == (wave == WAVE)
        # Niger's two rounds of weighing are one key: the year a weight came from
        # is here, so the branch stays countable.
        assert ('nsu_year' in raw.columns) == (country == 'Niger')
        if country == 'Niger':
            assert set(raw['nsu_year'].dropna().unique()) == {2018.0, 2021.0}
            assert raw.loc[~found, 'nsu_year'].isna().all()
        fa = built(country)
        fa = fa[(fa.index.get_level_values('t').astype(str) == wave) & fa['Quantity_kg'].notna()]
        served = set(fa.reset_index().set_index(['t', 'i', 'j']).index)
        missing = served - set(raw.index)
        assert not missing, f'{len(missing)} served keys lack derivation inputs'


    @pytest.mark.parametrize('country', NSU_COUNTRIES)
    def test_public_derivation_summary_counts_every_stamped_row(self, built, country):
        fa = built(country)
        key = nsu_derivation_key(country)
        stamped = fa['Derivation'].fillna('').str.split('+', regex=False).map(
            lambda keys: key in keys)
        record = fa.attrs['derivations'][country][key]
        assert record['rows'] == int(stamped.sum()) > 0
        assert record['columns'] == ['Quantity_kg']
        # Public summaries expose rows/columns/in; coverage is carried by
        # the stamped observations, not a nonexistent summary `waves` key.
        by_wave = fa.loc[stamped].groupby(level='t').size()
        assert by_wave.index.astype(str).tolist() == list(WAVES)
        assert by_wave.gt(0).all()
        assert int(by_wave.sum()) == record['rows']
