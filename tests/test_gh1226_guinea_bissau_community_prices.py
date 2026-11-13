"""Guinea-Bissau 2018-19 community_prices from s05_co_gnb2018.dta (GH #1226).

The Niger template (rulings D1-D6) with the household's sized `u` and no folding
(#1453 ruling).  Every label in the file is double-encoded and in Portuguese.
Synthetic tests run the real wave code against the real `u` and `harmonize_food`
tables, with no microdata.  The `requires_s3` tests rebuild the expected numbers
from the raw file by arithmetic of their own: they import nothing from
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

COUNTRY = 'Guinea-Bissau'
KEY = 'Guinea-Bissau::community_prices::one-item-per-price'
INDEX = ['t', 'v', 'j', 'u', 'variety', 'visit', 'obs']

COMMUNITY = {
    's05q01': {1: 'Arroz local de bolanha', 17: 'PÃ£o moderno', 55: 'Ã__leo de palma',
               94: 'FeijÃ£o seco'},
    's05q02': {1000: 'Kg - Unidade de tamanho Ãºnico', 1262: 'PedaÃ§o - MÃ©dio',
               4131: 'Monte - Pequeno', 4206: 'Corda - Inteiro',
               4160: 'PÃ© - Unidade de tamanho Ãºnico',
               41398: 'Monte - Outro tamanho 1', 41399: 'Monte - Outro tamanho 2'},
}
HOUSEHOLD = {
    's07bq01': {1: 'Arroz local de bolanha', 17: 'PÃ£o moderno', 55: 'Ã__leo de palma'},
    's07bq03b': {100: 'Kg', 126: 'PedaÃ§o', 413: 'Monte', 416: 'PÃ©', 420: 'Corda'},
    's07bq03c': {0: 'Tamanho Ãºnico', 1: 'Pequeno', 2: 'Medio', 3: 'Grande', 6: 'Integer'},
}


def _decode(s):
    if not isinstance(s, str):
        return s
    try:
        return s.encode('latin-1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s


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
                    (12, 1, 17, 4131, 100., 125.),
                    (13, 2, 94, 1262, 300., 310.)]
    return pd.DataFrame(rows, columns=['grappe', 'vague', 's05q01', 's05q02', 's05q03', 's05q04']
                        ).assign(Existe=1)


class TestUnit:
    @pytest.mark.parametrize('code,label', [
        (1000, 'Kg'),                         # size-free: bare, Tamanho único is the free code
        (4160, 'Pé'),                         # the household's repaired label
        (4131, 'Monte (Pequeno)'),            # the instrument's Portuguese, no translation
        (1262, 'Pedaço (Medio)'),             # the household's word, not the form's Médio
        (4206, 'Corda (Integer)'),            # the household's word, not the form's Inteiro
        (41398, 'Monte (Outro tamanho 1)'),   # 100 * unit + size; the household lists no 98
        (41399, 'Monte (Outro tamanho 2)'),
    ])
    def test_label_by_code(self, mapping, code, label):
        assert mapping._s05_u_by_code([code], COMMUNITY, HOUSEHOLD) == {code: label}

    @pytest.mark.parametrize('code,unit,size', [(4131, 'Monte', 'Pequeno'),
                                                (1262, 'PedaÃ§o', 'Medio')])
    def test_label_is_what_the_household_side_serves(self, mapping, code, unit, size):
        """The rule of the #1453 ruling: same composition as `food_acquired.u`."""
        frame = pd.DataFrame({'Quantity': [1.0]},
                             index=pd.MultiIndex.from_tuples([('i', unit)], names=['i', 'u']))
        household = carry_unit_size(frame, [size], COUNTRY,
                                    decode=mapping._decode_mojibake).index.get_level_values('u')[0]
        assert mapping._s05_u_by_code([code], COMMUNITY, HOUSEHOLD)[code] == household

    def test_no_two_units_may_share_a_label(self, mapping, monkeypatch):
        monkeypatch.setattr(mapping, '_served_unit_labels', lambda c, v: ['Monte' for _ in v])
        with pytest.raises(UnitSizeLabelError, match='would both be served'):
            mapping._s05_u_by_code([4131, 1261], {**COMMUNITY, 's05q02': {4131: 'x', 1261: 'y'}},
                                   {**HOUSEHOLD})

    def test_unknown_unit_raises(self, mapping):
        with pytest.raises(UnitSizeLabelError, match='names no household unit'):
            mapping._s05_u_by_code([9991], COMMUNITY, HOUSEHOLD)


class TestRows:
    def test_both_readings_as_recorded(self, mapping):
        out = mapping.community_prices_s05(source(), COMMUNITY, HOUSEHOLD)
        assert list(out.index.names) == INDEX and out.index.is_unique and len(out) == 6
        got = out.xs('Monte (Pequeno)', level='u')
        assert got.index.get_level_values('obs').tolist() == [1, 2]
        assert got['Price'].tolist() == [100., 125.]
        assert set(out.index.get_level_values('variety')) == {'unknown_variety'}
        assert 'Quantity' not in out

    def test_labels_are_repaired_and_a_code_the_household_lacks_takes_its_own(self, mapping):
        out = mapping.community_prices_s05(source(), COMMUNITY, HOUSEHOLD).reset_index()
        assert 'Pão moderno' in set(out['Description'])
        row = out[out['Description'] == 'Feijão seco'].iloc[0]
        assert row['j'] == 'Feijão seco'
        assert not out['Description'].str.contains('Ã').any()

    def test_product_94_is_served_under_the_households_95(self, mapping):
        """GH #1226, ruled 2026-11-04: 94 (`Feijão seco`, price form only) takes the
        household label of 95.  95 is not priced, so nothing is lumped."""
        household = {**HOUSEHOLD, 's07bq01': {**HOUSEHOLD['s07bq01'],
                                              95: 'FeijÃ£o-frade, feijÃ£o seco'}}
        out = mapping.community_prices_s05(source(), COMMUNITY, household).reset_index()
        row = out[out['Description'] == 'Feijão seco'].iloc[0]
        assert row['j'] == 'Niébé/Haricots secs'
        assert set(out['variety']) == {'unknown_variety'}
        assert mapping._S05_PRODUCT_BRIDGE == {94: 95}

    def test_repeated_key_is_obs_3_4_and_nothing_is_dropped(self, mapping):
        rows = [(12, 1, 17, 4131, 100., 125.), (12, 1, 17, 4131, 100., 130.)]
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
            return COMMUNITY if str(path).endswith('s05_co_gnb2018.dta') else HOUSEHOLD
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
            return COMMUNITY if path.endswith('s05_co_gnb2018.dta') else HOUSEHOLD
        monkeypatch.setattr(tools, 'get_dataframe', read)
        monkeypatch.setattr(tools, 'to_parquet', lambda df, path: writes.append((df, path)))
        runpy.run_path('community_prices.py')
        assert set(calls) == {'../Data/s05_co_gnb2018.dta', '../Data/s07b_me_gnb2018.dta'}
        assert writes[0][1] == 'community_prices.parquet' and len(writes[0][0]) == 6
        assert set(writes[0][0]['Derivation']) == {KEY}


# ---------------------------------------------------------------------------
# Against the held file.  The oracle reads the raw source and imports nothing
# from countries/.
# ---------------------------------------------------------------------------
def _labels(categories, name):
    return {int(k): str(v).strip() for k, v in categories[name].items()
            if pd.notna(v) and str(v).strip()}


@pytest.mark.requires_s3
class TestHeldFile:
    @pytest.fixture(scope='class')
    def raw(self):
        data = countries_root() / COUNTRY / '2018-19' / 'Data'
        codes = tools.get_dataframe(data / 's05_co_gnb2018.dta', convert_categoricals=False)
        community = tools.get_dataframe(data / 's05_co_gnb2018.dta', categories_only=True)
        household = tools.get_dataframe(data / 's07b_me_gnb2018.dta', categories_only=True)
        return codes, community, household

    @pytest.fixture(scope='class')
    def served(self):
        import lsms_library as ll
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            return ll.Country(COUNTRY).community_prices()

    def _expected_u(self, code, units, sizes, community_units):
        unit, size = divmod(code, 10)
        if unit not in units:
            unit, size = divmod(code, 100)
        base = served_unit_labels(COUNTRY, [_decode(units[unit])])[0]
        word = sizes.get(size) or community_units[code].rsplit('-', 1)[-1].strip()
        return sized_unit(base, _decode(word))

    def test_every_recorded_price_is_served_under_its_unit(self, raw, served):
        codes, community, household = raw
        units, sizes = _labels(household, 's07bq03b'), _labels(household, 's07bq03c')
        s05_units = _labels(community, 's05q02')
        long = pd.concat([codes[['s05q02', 's05q03']].rename(columns={'s05q03': 'p'}),
                          codes[['s05q02', 's05q04']].rename(columns={'s05q04': 'p'})])
        long['p'] = long['p'].astype('float64')
        long['s05q02'] = long['s05q02'].astype(int)
        labels = {c: self._expected_u(c, units, sizes, s05_units) for c in long['s05q02'].unique()}
        long['u'] = long['s05q02'].map(labels)
        expected = long.groupby('u')['p'].agg(['size', 'sum'])
        got = served.reset_index().astype({'Price': 'float64'}).groupby('u')['Price'].agg(['size', 'sum'])
        assert len(served) == 2 * len(codes) and served.index.is_unique
        pd.testing.assert_frame_equal(got.sort_index(), expected.sort_index(), check_names=False,
                                      check_exact=False, rtol=1e-12, check_index_type=False)

    def test_cluster_and_visit_are_the_source_s(self, raw, served):
        codes, _, _ = raw
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
            hh_u = set(ll.Country(COUNTRY).food_acquired().index.get_level_values('u'))
        codes, community, household = raw
        data = countries_root() / COUNTRY / '2018-19' / 'Data'
        hh = tools.get_dataframe(data / 's07b_me_gnb2018.dta', convert_categoricals=False)
        used = set(map(tuple, hh[['s07bq03b', 's07bq03c']].dropna().astype(int).values))
        units, sizes = _labels(household, 's07bq03b'), _labels(household, 's07bq03c')
        s05_units = _labels(community, 's05q02')
        for code in codes['s05q02'].astype(int).unique():
            unit, size = divmod(code, 10)
            if unit not in units:
                unit, size = divmod(code, 100)
            if (unit, size) in used:
                assert self._expected_u(code, units, sizes, s05_units) in hh_u, code

    def test_inputs_join_the_served_rows(self, served):
        import lsms_library as ll
        raw = ll.Country(COUNTRY).derivation_inputs(KEY, wave='2018-19')
        keys = ['t', 'v', 'j', 'u', 'variety', 'visit']
        s = pd.MultiIndex.from_frame(served.reset_index()[keys].drop_duplicates())
        r = pd.MultiIndex.from_frame(raw.reset_index()[keys].drop_duplicates())
        assert s.isin(r).all() and r.isin(s).all()
