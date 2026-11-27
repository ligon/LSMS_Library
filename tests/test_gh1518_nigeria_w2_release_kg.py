"""W2 A3 production and provenance on invented inputs, no survey/cache reads."""

import runpy
import sys

import numpy as np
import pandas as pd
import pytest

from lsms_library import Country, local_tools
from lsms_library.countries.Nigeria._ import nigeria as ng
from lsms_library.paths import countries_root
from lsms_library.transformations import (
    _sold_kg_factors, harvest_kg_factors, median_price_valuation,
)

COUNTRY = countries_root() / 'Nigeria'
SCRIPT = COUNTRY / '_' / 'crop_production.py'
A3 = COUNTRY / '2012-13/Data/Post Harvest Wave 2/Agriculture/secta3_harvestw2.dta'
LOOKUP = COUNTRY / '2012-13/Data/w2agnsconversion.csv'
KEY = 'Nigeria::crop_production::w2-release-kg'
LABELS = {1121: 'Yam--roots', 1123: 'Yam--roots'}
INDEX = ['t', 'i', 'plot_id', 'crop', 'production_obs']


@pytest.fixture
def a3():
    # Two crop codes and two sized unit codes collapse onto identical public
    # labels. The same native pair also occurs on a second plot.
    # (plot, cropid, native crop, native unit, Q6 amount, Q3)
    raw = pd.DataFrame([
        (1, 1, 1121, 21, 3, 1), (1, 2, 1123, 21, 3, 1),
        (1, 3, 1121, 22, 3, 1), (2, 1, 1121, 21, .25, 1),
        (1, 4, 1121, 25, 3, 1),  # kg=0, same rate as a kg match
        (1, 5, 1121, 26, 3, 1),  # unmatched
        (1, 6, 1121, 21, 3, 2), (1, 7, 1121, 21, 3, pd.NA),  # off-route
        (1, 8, 1121, 21, 0, 1), (1, 9, 1121, 21, -2, 1),
        (1, 10, 1121, 21, pd.NA, 1), (1, 11, 1121, 21, np.inf, 1),
        (1, 12, 1121, 95, 3, 1),  # own code but no decoded label on this fixture
        (1, 13, 1121, 999, 3, 1), (1, 14, 1121, pd.NA, 3, 1),
        (1, 15, 1121, 21, 1e308, 1),  # product overflows
        (1, 16, 1121, 22, 3, 7),  # unsupported Q3
        (1, 17, 1121, 1, 3, 1), (1, 18, 1121, 2, 3, 1),
        (1, 19, 1121, 3, 3, 1), (1, 20, 1121, 4, 3, 1),
        (1, 21, 1121, 27, 3, 1), (1, 22, 1121, 28, 3, 1),  # missing, unsupported basis
        (1, 23, 1121, 22, 3, 1),  # label prefix contradicts native code
        (1, 24, 1121, 21, 3, 1),  # missing household (not retained)
        (1, 25, 7777, 21, 3, 1),  # no served crop label (not retained)
        (1, 26, 1121, 31, 1e-300, 1),  # product underflows to zero
    ], columns=['plotid', 'cropid', 'cropcode', 'sa3q6a2', 'sa3q6a1', 'sa3q3'])
    raw['sa3q6a1'] = raw['sa3q6a1'].astype('Float64')
    raw['sa3q6a2'] = raw['sa3q6a2'].astype('Int64')
    raw['sa3q3'] = raw['sa3q3'].astype('Int64')
    raw['hhid'] = 'fixture'
    raw.loc[raw.cropid.eq(24), 'hhid'] = pd.NA
    raw['sa3q11a'] = 11 + raw.index
    raw['sa3q11b'] = np.where(raw.cropid.eq(1), 21, 1)
    raw['sa3q12'] = 1200 + 13 * raw.index
    raw['sa3q18'] = 1500 + 17 * raw.index
    raw.index = 900 - 7 * raw.index
    raw = raw.sample(frac=1, random_state=13)
    unit_names = {
        1: 'KILOGRAM (KG)', 2: 'GRAMS (G)', 3: 'LITRE (L)', 4: 'CENTILITRE (CL)',
        21: 'BAG/SACK: SMALL', 22: '22. BAG/SACK: MEDIUM', 25: 'BASKET: SMALL',
        26: 'BASKET: MEDIUM', 27: 'BASKET: LARGE', 28: 'BASKET: EXTRA LARGE',
        31: 'BASIN/BOWL: SMALL', 95: '95', 999: '999',
    }
    dec = raw.copy()
    labels = raw.sa3q6a2.map(unit_names)
    labels.loc[raw.cropid.eq(23)] = '21. BAG/SACK: SMALL'
    dec['sa3q6a2'] = labels.astype('category')
    dec['sa3q11b'] = raw.sa3q11b.map(unit_names).astype('category')
    return raw, dec


@pytest.fixture
def lookup():
    data = [
        (1121, 21, 2., 1), (1123, 21, 7., 1), (1121, 22, 5., 1),
        (1121, 25, 2., 0), (1121, 27, 2., None), (1121, 28, 2., 2),
        (1121, 95, 2., 1), (1121, 999, 2., 1), (7777, 21, 2., 1),
        (1121, 31, 1e-300, 1),
        *((1121, code, 10., 1) for code in (1, 2, 3, 4)),
    ]
    return (pd.DataFrame(data, columns=['cropcode', 'nscode', 'conversion', 'kg'])
            .assign(cropname='irrelevant to matching').sample(frac=1, random_state=4))


def _reader_guard(monkeypatch, raw, dec, lookup):
    files_read = []

    def reader(path, *, convert_categoricals):
        path = (SCRIPT.parent / path).resolve()
        files_read.append((path, convert_categoricals))
        if path == A3:
            return (dec if convert_categoricals else raw).copy()
        if path == LOOKUP and not convert_categoricals:
            if isinstance(lookup, Exception):
                raise lookup
            return lookup.copy()
        raise AssertionError(f'Unexpected reader: {path}')

    monkeypatch.setattr(local_tools, 'get_dataframe', reader)
    monkeypatch.setattr(ng, '_crop_labels', lambda **kwargs: LABELS)
    return reader, files_read


def _run_country_script(monkeypatch, raw, dec, lookup, *, script=SCRIPT):
    reader, files_read = _reader_guard(monkeypatch, raw, dec, lookup)
    # Inject other rounds while running the actual country script from start
    # to finish. No builder, subprocess, cache or getter reaches survey data.
    def one_plot(**overrides):
        return pd.DataFrame({
            'hhid': ['other_round'], 'plotid': [3], 'cropcode': [1121], 'cropid': [7],
            **{col: [val] for col, val in overrides.items()},
        })

    w3 = one_plot(sa3iq6i=3, sa3iq6ii=130, zone=1)
    w3_dec = w3.assign(sa3iq6ii='130. SACK/BAG SMALL')
    w3_table = pd.DataFrame({
        'crop_cd': [1121], 'unit_cd': [130], 'unit_name': ['SACK/BAG SMALL'],
        'note': [None], **{name: [11.] for name in ng.W3_CONVERSION_ZONES.values()},
    })
    pp = pd.DataFrame({'hhid': ['fixture', 'fixture'], 'plotid': [1, 1],
                       'cropcode': [1123, 1121], 's11fq2': [1, 2]})
    invented = {
        '../2010-11/Data/Post Harvest Wave 1/Agriculture/secta3_harvestw1.dta':
            one_plot(sa3q2=1121, sa3q6a=3, sa3q6b=1, sa3q11a=7, sa3q11b=1, sa3q12=10),
        '../2010-11/Data/Post Planting Wave 1/Agriculture/sect11f_plantingw1.dta':
            pd.DataFrame(),
        '../2012-13/Data/Post Planting Wave 2/Agriculture/sect11f_plantingw2.dta': pp,
        '../2015-16/Data/secta3i_harvestw3.dta': w3,
        '../2015-16/Data/ag_conv_w3.dta': w3_table,
        '../2015-16/Data/sect11f_plantingw3.dta': pd.DataFrame(),
    }
    invented.update({
        '../2018-19/Data/secta3i_harvestw4.dta':
            one_plot(sa3iq6i=3, sa3iq6ii='SACK/BAG', sa3iq6_conv=9., sa3iq6a=50.),
        '../2018-19/Data/secta3iii_harvestw4.dta': one_plot().iloc[:0],
        '../2018-19/Data/sect11f_plantingw4.dta': pd.DataFrame(),
        '../2023-24/Data/Post Harvest Wave 5/Agriculture/secta3i_harvestw5.dta':
            one_plot().iloc[:0],
        '../2023-24/Data/Post Harvest Wave 5/Agriculture/secta3iii_harvestw5.dta':
            one_plot(sa3iiiq23a=4, sa3iiiq23b='SACK/BAG', sa3iiiq23_conv=8., sa3iiiq24=12.),
        '../2023-24/Data/sect11f_plantingw5.dta': pd.DataFrame(),
    })

    def script_reader(path, *, convert_categoricals):
        if path in invented:
            if path == '../2015-16/Data/secta3i_harvestw3.dta' and convert_categoricals:
                return w3_dec.copy()
            return invented[path].copy()
        return reader(path, convert_categoricals=convert_categoricals)

    written = []
    monkeypatch.setitem(sys.modules, 'nigeria', ng)
    monkeypatch.setattr(sys, 'path', sys.path.copy())
    monkeypatch.setattr(local_tools, 'get_dataframe', script_reader)
    monkeypatch.setattr(local_tools, 'source_fingerprint', lambda fp: 'dvc:synthetic:test')
    monkeypatch.setattr(local_tools, 'to_parquet',
                        lambda frame, path: written.append((frame.copy(), path)))
    monkeypatch.setattr(ng, 'hh_crop_sale_records', lambda *args: pd.DataFrame({
        'i': ['sale_only'], 'crop': ['Yam--roots'], 'Quantity_sold': [4.],
        'Value_sold': [20.], 'Unit_sold': ['Kg'], 'KgFactor_sold': [np.nan],
        '_unit_key': ['kg'],
    }) if args[0] == '2015-16' else None)
    runpy.run_path(str(script))
    [(out, path)] = written
    assert path == '../var/crop_production.parquet'
    return out, files_read


def _key(plot, line):
    return ('2013Q1', 'fixture', str(plot), 'Yam--roots', f'reported:crop_id:{line}')


ESTIMATES = {_key(1, 1): 6., _key(1, 2): 21., _key(1, 3): 15., _key(2, 1): .5}


def test_actual_script_preserves_every_old_w2_field_and_only_adds_estimates(monkeypatch, a3, lookup):
    raw, dec = a3
    all_rows, reads = _run_country_script(monkeypatch, raw, dec, lookup)
    assert reads == [(A3, False), (A3, True), (LOOKUP, False)]
    got = all_rows.loc[all_rows.index.get_level_values('t') == '2013Q1']
    # Pre-assignment frame with independently constructed native PP attachment.
    inter = pd.Series(pd.NA, index=raw.index, dtype='boolean')
    inter.loc[raw.plotid.eq(1) & raw.cropcode.eq(1121) & raw.hhid.notna()] = True
    inter.loc[raw.plotid.eq(1) & raw.cropcode.eq(1123)] = False
    baseline = ng.crop_production_for_wave('2013Q1', [dict(
        df=raw, dec=dec, hhid='hhid', plot='plotid', crop='cropcode', line='cropid',
        qty='sa3q6a1', unit='sa3q6a2', qty_sold='sa3q11a', unit_sold='sa3q11b',
        value_sold='sa3q12', sold_on='plot', value_harvested='sa3q18',
        intercropped=inter, perennial=False)], LABELS)
    # The country concat widens the fixture's nullable native Quantity and
    # pins the sale amounts. The exact-base whole-script gate checks dtypes.
    baseline = baseline.astype({'Quantity': object, 'Quantity_sold': float, 'Value_sold': float})
    expected = baseline.copy()
    lookup_series = pd.Series(ESTIMATES).rename_axis(INDEX)
    expected['Quantity_kg'] = lookup_series.reindex(expected.index)
    expected['Derivation'] = expected['Derivation'].mask(expected.Quantity_kg.notna(), KEY)
    pd.testing.assert_frame_equal(got, expected, check_exact=True, check_index_type=False)
    assert got.index.is_unique and len(got) == len(raw) - 2
    assert got.loc[_key(1, 1), 'Quantity_sold'] == 11.
    assert got.loc[_key(1, 1), 'Value_sold'] == 1200.
    assert got.loc[_key(1, 1), 'Value_harvested'] == 1500.
    assert (got.loc[list(ESTIMATES), 'u'] == 'Sack/Bag').all()

    w3 = all_rows.xs('2016Q1', level='t')
    assert w3.loc[('other_round', '3', 'Yam--roots', 'reported:crop_id:7'), 'Derivation'] == ng.CROP_CONVERSION_KEY
    assert w3.Quantity_kg.dropna().tolist() == [33.]
    assert w3.xs('Farm-level', level='plot_id').Value_sold.tolist() == [20.]
    assert all_rows.xs('2011Q1', level='t').Quantity_kg.isna().all()
    assert all_rows.xs('2019Q1', level='t').KgFactor.tolist() == [9.]
    assert all_rows.xs('2024Q1', level='t').KgFactor.tolist() == [8.]

    for volume in (True, False):
        factors = harvest_kg_factors(got, volume_as_mass=volume)
        old_factors = harvest_kg_factors(baseline, volume_as_mass=volume)
        assert set(factors.index[factors.KgFactorSource.eq('survey_kg')]) == set(ESTIMATES)
        pd.testing.assert_frame_equal(factors.drop(index=list(ESTIMATES)),
                                      old_factors.drop(index=list(ESTIMATES)),
                                      check_index_type=False)
        assert got.Derivation.dropna().eq(KEY).all()

        old_sold_factor, old_diagnostics = _sold_kg_factors(baseline, volume_as_mass=volume)
        sold_factor, diagnostics = _sold_kg_factors(got, volume_as_mass=volume)
        pd.testing.assert_series_equal(sold_factor, old_sold_factor, check_index_type=False)
        assert diagnostics == old_diagnostics
        assert pd.isna(sold_factor.loc[_key(1, 1)])  # converted harvest still no sold kg
        assert sold_factor.loc[_key(1, 2)] == 1.  # an independently stated kg sale
        pd.testing.assert_frame_equal(
            median_price_valuation(got, [], item_keys=('crop',), volume_as_mass=volume),
            median_price_valuation(baseline, [], item_keys=('crop',), volume_as_mass=volume),
            check_index_type=False,
        )


def test_missing_screener_keeps_rows_without_stamping(monkeypatch, a3, lookup):
    raw, dec = a3
    out, _ = _run_country_script(monkeypatch, raw.drop(columns='sa3q3'), dec, lookup)
    w2 = out.xs('2013Q1')
    assert len(w2) == len(raw) - 2
    assert w2[['Quantity_kg', 'Derivation']].isna().all().all()


@pytest.mark.parametrize('change, message', [
    (lambda d: d.iloc[:0], 'nonempty'),
    (lambda d: d.rename(columns={'kg': 'basis'}), 'columns'),
    (lambda d: pd.concat([d, d.iloc[[0]]]), 'repeated native'),
    (lambda d: pd.concat([d, d.iloc[[0]].assign(kg=0, conversion=99.)]), 'repeated native'),
    (lambda d: d.assign(cropcode=np.nan), 'native cropcode'),
    (lambda d: d.assign(nscode=21.5), 'native nscode'),
    (lambda d: d.assign(kg='kilograms'), 'kg basis'),
    (lambda d: d.assign(conversion=pd.NA), 'strictly positive'),
    (lambda d: d.assign(conversion=0.), 'strictly positive'),
    (lambda d: d.assign(conversion=-1.), 'strictly positive'),
    (lambda d: d.assign(conversion=np.inf), 'strictly positive'),
])
def test_invalid_lookup_fails_the_actual_script(monkeypatch, a3, lookup, change, message):
    with pytest.raises(ValueError, match=rf'w2agnsconversion\.csv: .*{message}'):
        _run_country_script(monkeypatch, *a3, change(lookup))


def test_unavailable_lookup_is_not_zero_coverage(monkeypatch, a3):
    with pytest.raises(FileNotFoundError, match='w2agnsconversion.csv'):
        _run_country_script(monkeypatch, *a3, FileNotFoundError(LOOKUP.name))


def test_registry_inputs_are_exact_native_answers_and_can_be_reproduced(monkeypatch, a3, lookup):
    raw, dec = a3
    _, reads = _reader_guard(monkeypatch, raw, dec, lookup)
    c = Country('Nigeria', preload_panel_ids=False)
    # Exercise Country's existing ID walker with an invented one-to-one map.
    monkeypatch.setattr(Country, 'updated_ids',
                        property(lambda self: {'2013Q1': {'fixture': 'panel-fixture'}}))
    rec = c.derivations()[KEY]
    assert tuple(rec.columns) == ('Quantity_kg',)
    receipt = c.derivation_inputs(KEY)
    assert reads == [(A3, False), (A3, True), (LOOKUP, False)]
    assert receipt.index.names == INDEX and receipt.index.is_unique
    estimates = {tuple('panel-fixture' if j == 'fixture' else j for j in k): v
                 for k, v in ESTIMATES.items()}
    expected = pd.Series(estimates).rename_axis(INDEX).sort_index()
    got = pd.Series(rec.resolve()(receipt.sa3q6a1, receipt.conversion), index=receipt.index)
    pd.testing.assert_series_equal(got, expected, check_exact=True, check_index_type=False)
    assert receipt.sa3q3.eq(1).all() and receipt.kg.eq(1).all()
    assert receipt.sa3q6a2.eq(receipt.nscode).all()
    assert set(receipt.cropcode) == {1121, 1123}
    assert set(receipt.sa3q6a2) == {21, 22}
    assert receipt.hhid.eq('fixture').all()
    with pytest.raises(ValueError, match='only to W2 post-harvest'):
        c.derivation_inputs(KEY, wave='2012Q3')


def test_actual_managed_make_manifest_fingerprints_the_literal_sidecar():
    c = Country('Nigeria', preload_panel_ids=False)
    manifest, paths = c._make_declared_inputs('crop_production', _with_sources=True)
    assert LOOKUP in paths
    assert LOOKUP.with_suffix('.csv.dvc').is_file()
    assert manifest['sources'][str(LOOKUP)] == local_tools.source_fingerprint(LOOKUP)
    assert manifest['sources'][str(LOOKUP)].startswith('dvc:w2agnsconversion.csv:')
    assert A3 in paths
