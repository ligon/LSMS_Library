"""Senegal 2018-19 community_prices from s05_co_sen2018.dta (GH #1226).

The Niger template (rulings D1-D6) with the household's sized `u` and no folding
(#1453 ruling).  Senegal's `harmonize_food` and `u` tables are keyed on the
household file's mojibake, so `j` and `u` go by code through the household's raw
labels and nothing is decoded.  Synthetic tests run the real wave code against
the real tables, with no microdata.  The `requires_s3` tests rebuild the expected
numbers from the raw file by arithmetic of their own: they import nothing from
`countries/`.
"""
import importlib.util
import runpy
import sys
import warnings

import pandas as pd
import pytest

from lsms_library import local_tools as tools
from lsms_library.ehcvm import UnitSizeLabelError, carry_unit_size, served_unit_labels, sized_unit
from lsms_library.paths import countries_root

COUNTRY = 'Senegal'
KEY = 'Senegal::community_prices::one-item-per-price'
INDEX = ['t', 'v', 'j', 'u', 'variety', 'visit', 'obs']

COMMUNITY = {
    's05q01': {1: 'Riz local brisé', 3: 'Riz importé brisé', 7: 'Mil'},
    's05q02': {1000: 'Kg - taille unique', 1071: 'boÃ®te - petit', 1431: 'tas - petit',
               1437: 'tas - Tres petit', 10000: 'Bouteille de 20l - taille unique',
               10010: '12,5cl(demi walatt) - taille unique', 1471: 'unitÃ© (gousse, carcasse etc.) - Petit'},
}
HOUSEHOLD = {
    's07bq01': {1: 'Riz local brisÃ©', 3: 'Riz importÃ© brisÃ©', 7: 'Mil'},
    's07bq03b': {100: 'Kg', 107: 'BoÃ®te', 111: 'boule (dankk)', 143: 'tas', 147: 'unitÃ©',
                 1000: 'Bouteille de 19L', 1001: '12,5cl(demi walatt)'},
    's07bq03c': {0: 'Taille unique', 1: 'Petit', 2: 'Moyen', 3: 'Grand', 7: 'TrÃ¨s petit',
                 98: 'Autre taille 1'},
}


@pytest.fixture
def mapping(monkeypatch):
    path = countries_root() / COUNTRY / '2018-19' / '_'
    spec = importlib.util.spec_from_file_location('mapping', path / 'mapping.py')
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, 'mapping', module)
    spec.loader.exec_module(module)
    monkeypatch.chdir(path)
    return module


def source(rows=None):
    rows = rows or [(12, 1, 1, 1000, 500., 500.),
                    (12, 1, 7, 1431, 100., 125.),
                    (13, 2, 3, 10000, 15000., 15500.)]
    return pd.DataFrame(rows, columns=['grappe', 'vague', 's05q01', 's05q02', 's05q03', 's05q04']
                        ).assign(Existe=1)


class TestUnit:
    @pytest.mark.parametrize('code,label', [
        (1000, 'Kg'),                      # size-free: bare
        (1431, 'Tas (Petit)'),             # lower-case raw unit, through the u table
        (1437, 'Tas (Très petit)'),        # the size label's mojibake repaired
        (1071, 'Boîte (Petit)'),           # keyed on the household's mojibake
        (1471, 'Unité (Petit)'),           # the household's word, not the form's long one
        (10010, 'Demi walatt'),            # 10 * a four-digit unit, not 100 * unit + 10
        (1111, 'Boule (Petit)'),           # no s05 label: the code alone is enough
        (10000, 'Bouteille de 20l'),       # the form's 20 L, not the household's 19 L
    ])
    def test_label_by_code(self, mapping, code, label):
        assert mapping._s05_u_by_code([code], COMMUNITY, HOUSEHOLD) == {code: label}

    @pytest.mark.parametrize('code,unit,size', [(1431, 'tas', 'Petit'),
                                                (1437, 'tas', 'TrÃ¨s petit'),
                                                (1071, 'BoÃ®te', 'Petit')])
    def test_label_is_what_the_household_side_serves(self, mapping, code, unit, size):
        """The rule of the #1453 ruling: same composition as `food_acquired.u`."""
        frame = pd.DataFrame({'Quantity': [1.0]},
                             index=pd.MultiIndex.from_tuples([('i', unit)], names=['i', 'u']))
        household = carry_unit_size(frame, [size], COUNTRY).index.get_level_values('u')[0]
        assert mapping._s05_u_by_code([code], COMMUNITY, HOUSEHOLD)[code] == household

    def test_no_two_units_may_share_a_label(self, mapping, monkeypatch):
        monkeypatch.setattr(mapping, '_served_unit_labels', lambda c, v: ['Tas' for _ in v])
        with pytest.raises(UnitSizeLabelError, match='would both be served'):
            mapping._s05_u_by_code([1431, 1071], COMMUNITY, HOUSEHOLD)

    def test_unknown_unit_raises(self, mapping):
        with pytest.raises(UnitSizeLabelError, match='names no household unit'):
            mapping._s05_u_by_code([9991], COMMUNITY, HOUSEHOLD)


class TestRows:
    def test_both_readings_as_recorded(self, mapping):
        out = mapping.community_prices_s05(source(), COMMUNITY, HOUSEHOLD)
        assert list(out.index.names) == INDEX and out.index.is_unique and len(out) == 6
        got = out.xs('Tas (Petit)', level='u')
        assert got.index.get_level_values('obs').tolist() == [1, 2]
        assert got['Price'].tolist() == [100., 125.]
        assert set(out.index.get_level_values('variety')) == {'unknown_variety'}
        assert 'Quantity' not in out

    def test_j_goes_through_the_mojibake_keys_and_description_stays_recorded(self, mapping):
        out = mapping.community_prices_s05(source(), COMMUNITY, HOUSEHOLD).reset_index()
        row = out[out['Description'] == 'Riz local brisé'].iloc[0]
        assert 'Ã' not in row['j']

    def test_repeated_key_is_obs_3_4_and_nothing_is_dropped(self, mapping):
        rows = [(12, 1, 7, 1431, 100., 125.), (12, 1, 7, 1431, 100., 130.)]
        out = mapping.community_prices_s05(source(rows), COMMUNITY, HOUSEHOLD)
        assert out.index.get_level_values('obs').tolist() == [1, 2, 3, 4]
        assert out['Price'].tolist() == [100., 125., 100., 130.]


class TestDerivation:
    def test_registered_and_stamped_without_touching_price(self, mapping):
        from lsms_library.derivations import derivation_records, resolve_callable
        base = mapping.community_prices_s05(source(), COMMUNITY, HOUSEHOLD)
        record = derivation_records(COUNTRY)[KEY]
        out = resolve_callable(record.function)(base)
        pd.testing.assert_frame_equal(out.drop(columns=['Quantity', 'Derivation']), base)
        assert (out['Quantity'] == 1.0).all() and str(out['Quantity'].dtype) == 'Float64'
        assert set(out['Derivation']) == {KEY}
        assert record.waves == ['2018-19'] and record.columns == ['Quantity']
        assert {a['basis'] for a in record.assumptions} == {'questionnaire', 'modelling-choice'}
        assert record.inputs.endswith('2018-19._.mapping:inputs_community_prices')

    def test_inputs_are_raw_answers_by_their_own_names(self, mapping, monkeypatch):
        codes = source()

        def read(path, **kwargs):
            if not kwargs.get('categories_only'):
                return codes
            return COMMUNITY if str(path).endswith('s05_co_sen2018.dta') else HOUSEHOLD
        monkeypatch.setattr(tools, 'get_dataframe', read)
        out = mapping.inputs_community_prices()
        pd.testing.assert_frame_equal(out.reset_index()[list(codes)], codes)
        assert 'Quantity' not in out and 'obs' not in out.index.names
        with pytest.raises(ValueError, match='only for 2018-19'):
            mapping.inputs_community_prices('2021-22')

    def test_script_reads_only_its_sources_and_writes_every_reading(self, mapping, monkeypatch):
        calls, writes = [], []

        def read(path, **kwargs):
            calls.append(path)
            if not kwargs.get('categories_only'):
                return source()
            return COMMUNITY if path.endswith('s05_co_sen2018.dta') else HOUSEHOLD
        monkeypatch.setattr(tools, 'get_dataframe', read)
        monkeypatch.setattr(tools, 'to_parquet', lambda df, path: writes.append((df, path)))
        runpy.run_path('community_prices.py')
        assert set(calls) == {'../Data/s05_co_sen2018.dta', '../Data/s07b_me_sen2018.dta'}
        assert writes[0][1] == 'community_prices.parquet' and len(writes[0][0]) == 6
        assert set(writes[0][0]['Derivation']) == {KEY}


# ---------------------------------------------------------------------------
# Against the held file.  The oracle reads the raw source and imports nothing
# from countries/.
# ---------------------------------------------------------------------------
#: What the printed form says for the code whose digits disagree with it.
FORM_BASE = {10000: 'Bouteille de 20l'}


def _labels(categories, name):
    return {int(k): str(v).strip() for k, v in categories[name].items()
            if pd.notna(v) and str(v).strip()}


@pytest.mark.requires_s3
class TestHeldFile:
    @pytest.fixture(scope='class')
    def raw(self):
        data = countries_root() / COUNTRY / '2018-19' / 'Data'
        codes = tools.get_dataframe(data / 's05_co_sen2018.dta', convert_categoricals=False)
        household = tools.get_dataframe(data / 's07b_me_sen2018.dta', categories_only=True)
        return codes, household

    @pytest.fixture(scope='class')
    def served(self):
        import lsms_library as ll
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            return ll.Country(COUNTRY).community_prices()

    def _expected_u(self, code, units, sizes):
        unit, size = divmod(code, 10)
        if unit not in units:
            unit, size = divmod(code, 100)
        base = FORM_BASE.get(code) or served_unit_labels(COUNTRY, [units[unit]])[0]
        return sized_unit(base, sizes[size])

    def test_every_recorded_price_is_served_under_its_unit(self, raw, served):
        codes, household = raw
        units, sizes = _labels(household, 's07bq03b'), _labels(household, 's07bq03c')
        long = pd.concat([codes[['s05q02', 's05q03']].rename(columns={'s05q03': 'p'}),
                          codes[['s05q02', 's05q04']].rename(columns={'s05q04': 'p'})])
        long['p'] = long['p'].astype('float64')
        long['s05q02'] = long['s05q02'].astype(int)
        labels = {c: self._expected_u(c, units, sizes) for c in long['s05q02'].unique()}
        long['u'] = long['s05q02'].map(labels)
        expected = long.groupby('u')['p'].agg(['size', 'sum'])
        got = served.reset_index().astype({'Price': 'float64'}).groupby('u')['Price'].agg(['size', 'sum'])
        assert len(served) == 2 * len(codes) and served.index.is_unique
        pd.testing.assert_frame_equal(got.sort_index(), expected.sort_index(), check_names=False,
                                      check_exact=False, rtol=1e-12, check_index_type=False)

    def test_cluster_and_visit_are_the_source_s(self, raw, served):
        codes, _ = raw
        got = served.reset_index()[['v', 'visit']].drop_duplicates().astype({'v': str})
        want = codes[['grappe', 'vague']].drop_duplicates().astype(int)
        want = pd.DataFrame({'v': want['grappe'].astype(str), 'visit': want['vague']})
        assert set(map(tuple, got.values)) == set(map(tuple, want.values))

    def test_quantity_is_the_stamped_construction(self, served):
        assert (served['Quantity'] == 1.0).all() and (served['Derivation'] == KEY).all()

    def test_a_unit_size_the_household_uses_joins_its_u(self, raw, served):
        import lsms_library as ll
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            hh_u = set(ll.Country(COUNTRY).food_acquired(waves=['2018-19']).index.get_level_values('u'))
        codes, household = raw
        data = countries_root() / COUNTRY / '2018-19' / 'Data'
        hh = tools.get_dataframe(data / 's07b_me_sen2018.dta', convert_categoricals=False)
        used = set(map(tuple, hh[['s07bq03b', 's07bq03c']].dropna().astype(int).values))
        units, sizes = _labels(household, 's07bq03b'), _labels(household, 's07bq03c')
        for code in codes['s05q02'].astype(int).unique():
            if code in FORM_BASE:
                continue
            unit, size = divmod(code, 10)
            if unit not in units:
                unit, size = divmod(code, 100)
            if (unit, size) in used:
                assert self._expected_u(code, units, sizes) in hh_u, code

    def test_inputs_join_the_served_rows(self, served):
        import lsms_library as ll
        raw = ll.Country(COUNTRY).derivation_inputs(KEY, wave='2018-19')
        keys = ['t', 'v', 'j', 'u', 'variety', 'visit']
        s = pd.MultiIndex.from_frame(served.reset_index()[keys].drop_duplicates())
        r = pd.MultiIndex.from_frame(raw.reset_index()[keys].drop_duplicates())
        assert s.isin(r).all() and r.isin(s).all()
