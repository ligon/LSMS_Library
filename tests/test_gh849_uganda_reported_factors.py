"""GH849: actual source fields stay paired at the GH1198 observation grain.

The literal q6d source columns are independent inputs, confirmed against held
metadata/rows in ledger849 sections2-4. These synthetic rows assert behavior,
not a copy of CROP_COLMAPS; no real source record or cache fixture is embedded.
"""
import importlib.util

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root
from lsms_library.transformations import harvest_kg, harvest_kg_factors


@pytest.fixture
def ug(monkeypatch):
    spec = importlib.util.spec_from_file_location(
        'uganda_gh849', countries_root() / 'Uganda' / '_' / 'uganda.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, '_crop_label_map', lambda: {1: 'Maize'})
    monkeypatch.setattr(module, '_harvest_unit_map', lambda: {22: 'sack', 1: 'Kg'})
    monkeypatch.setattr(module, '_harvest_condition_map', lambda: {20: 'fresh'})
    return module


def source(ug, wave, season, slot=1, factors=(10., 20.), unit=22):
    """Make raw-format columns; q6d is supplied independently of the map."""
    cm = ug.CROP_COLMAPS[wave][season]
    n = len(factors)
    fields = {cm['hhid']: ['H'] * n, cm['parcel']: [1] * n,
              cm['plot']: [1] * n, cm['crop']: [1] * n}
    if cm.get('production_id'):
        fields[cm['production_id']] = list(range(1, n + 1))
    for number, cond in enumerate(cm['conditions'], 1):
        active = number == slot
        for key, value in [('qty', [2., 5.] if n == 2 else [2.] * n),
                           ('unit', [unit] * n), ('condition', [20] * n),
                           ('qty_sold', [3.] * n), ('value_sold', [90.] * n),
                           ('unit_sold', [unit] * n),
                           ('condition_sold', [20] * n), ('month', [6] * n)]:
            if cond.get(key):
                fields[cond[key]] = value if active else [np.nan] * n
    prefix = ('s5' + season.lower() + 'q06d_') if wave == '2019-20' else None
    factor_field = prefix + str(slot) if prefix else 'a5' + season.lower() + 'q6d'
    if prefix:
        for number in (1, 2):
            fields[prefix + str(number)] = list(factors) if number == slot else [np.nan] * n
    else:
        fields[factor_field] = list(factors)
    # A neighboring column with plausible but different numbers must not be read.
    fields[factor_field + '_distractor'] = [200.] * n
    return pd.DataFrame(fields), factor_field


def build(ug, wave, season, raw):
    return ug.crop_production_for_wave(
        wave, raw if season == 'A' else None,
        raw if season == 'B' else None, None, ug.CROP_COLMAPS[wave],
        df4b=None, source_refs={'A': 'synthetic:GH849-A', 'B': 'synthetic:GH849-B'})


CASES = [(wave, season, 1)
         for wave in ('2010-11', '2011-12', '2013-14', '2015-16')
         for season in ('A', 'B')]
CASES += [('2019-20', season, slot)
          for season in ('A', 'B') for slot in (1, 2)]


@pytest.mark.parametrize('wave,season,slot', CASES)
def test_source_report_stays_with_its_own_native_quantity(ug, wave, season, slot):
    raw, _ = source(ug, wave, season, slot)
    out = build(ug, wave, season, raw)
    assert len(out) == 2 and out.index.is_unique
    pd.testing.assert_series_equal(
        out.KgFactor, pd.Series(pd.array([10., 20.], dtype='Float64'),
                               index=out.index, name='KgFactor'),
        check_exact=True, rtol=0, atol=0)
    pd.testing.assert_series_equal(
        out.Quantity, pd.Series(pd.array([2., 5.], dtype='Float64'),
                               index=out.index, name='Quantity'),
        check_exact=True, rtol=0, atol=0)
    # 2*10 + 5*20, never a first-factor multiplied by the summed quantity.
    assert float(harvest_kg(out).Harvest_kg.iloc[0]) == 120.


@pytest.mark.parametrize('season', ['A', 'B'])
def test_actual_wide_slots_keep_equal_and_partial_reports(ug, season):
    first, _ = source(ug, '2019-20', season, 1, factors=(10., np.nan))
    second, _ = source(ug, '2019-20', season, 2, factors=(10., 30.))
    raw = first.copy(deep=True)
    for field in second:
        if field.startswith('s5' + season.lower()) and field.endswith('_2'):
            raw[field] = second[field]
    out = build(ug, '2019-20', season, raw)
    assert len(out) == 4 and out.index.is_unique
    assert list(out.index.get_level_values('production_obs')) == [
        'slot:1/row:synthetic:GH849-' + season + ':0',
        'slot:1/row:synthetic:GH849-' + season + ':1',
        'slot:2/row:synthetic:GH849-' + season + ':0',
        'slot:2/row:synthetic:GH849-' + season + ':1']
    np.testing.assert_array_equal(out.KgFactor.to_numpy(dtype=float, na_value=np.nan),
                                  [10., np.nan, 10., 30.])


@pytest.mark.parametrize('value', [0., -3., 'not numeric', np.nan])
def test_existing_builder_nonpositive_or_nonnumeric_normalization(ug, value):
    raw, _ = source(ug, '2010-11', 'A', factors=(value,))
    out = build(ug, '2010-11', 'A', raw)
    assert len(out) == 1 and pd.isna(out.KgFactor.iloc[0])
    assert float(out.Quantity.iloc[0]) == 2.


@pytest.mark.parametrize('value', [251., 2018., np.inf])
def test_suspect_positive_reports_are_not_repaired_in_builder(ug, value):
    raw, _ = source(ug, '2010-11', 'A', factors=(value,))
    out = build(ug, '2010-11', 'A', raw)
    assert out.KgFactor.iloc[0] == value
    factors = harvest_kg_factors(out)
    assert pd.isna(factors.kg_reported.iloc[0])
    assert factors.attrs['kg_factor_sources']['reported_implausible'] == int(np.isfinite(value))


def test_kg_not_one_is_kept_then_screened_by_existing_ladder(ug):
    raw, _ = source(ug, '2010-11', 'A', factors=(2.,), unit=1)
    out = build(ug, '2010-11', 'A', raw)
    assert out.KgFactor.iloc[0] == 2.
    factors = harvest_kg_factors(out)
    assert pd.isna(factors.kg_reported.iloc[0])
    assert factors.kg_per_unit.iloc[0] == 1.
    assert factors.attrs['kg_factor_sources']['reported_implausible'] == 1


@pytest.mark.parametrize('wave,season,slot', [('2010-11', 'A', 1),
                                             ('2019-20', 'B', 2)])
def test_declared_report_column_missing_is_loud(ug, wave, season, slot):
    raw, field = source(ug, wave, season, slot)
    with pytest.raises(ug.CropColmapError, match='kg conversion factor'):
        build(ug, wave, season, raw.drop(columns=field))
