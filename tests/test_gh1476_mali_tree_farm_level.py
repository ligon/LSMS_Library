"""GH #1476, Mali: a tree row is ``plot_id='Farm-level'``, never null.

The EAC-I perennial rosters (2014-15 ``EACIS3B_p2``, 2017-18 ``eaci17_s11fp1``)
have one row per household and species and no field grid, so the record is
household-grain by design: the canonical structural sentinel.  A NULL on the
declared ``plot_id`` level was deleted by the first ``groupby``, which is how
``harvest_kg`` lost these rows in silence.

The oracle reads the roster through ``get_dataframe`` and imports nothing from
``Mali/_/``.
"""
import warnings
from pathlib import Path

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library import transformations as T
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

COUNTRY = 'Mali'
ROSTERS = {'2014-15': ('EACIS3B_p2.dta', 's3bq03'),
           '2017-18': ('eaci17_s11fp1.dta', 's11fq03')}


def _scripts():
    return {w: (Path(countries_root()) / COUNTRY / w / '_' / 'crop_production.py').read_text()
            for w in ROSTERS}


def test_no_wave_script_writes_a_null_plot():
    for wave, text in _scripts().items():
        assert "'plot_id': pd.NA" not in text, wave
        assert "'plot_id': FARM_LEVEL" in text, wave


@pytest.fixture(scope='module')
def built():
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return ll.Country(COUNTRY).crop_production()


@pytest.mark.requires_s3
def test_no_row_is_null_on_plot_id(built):
    assert not built.index.get_level_values('plot_id').isna().any()


@pytest.mark.requires_s3
@pytest.mark.parametrize('wave', list(ROSTERS))
def test_farm_level_rows_are_the_producing_roster(built, wave):
    """One way, from the source: each species a household reports as producing
    has a Farm-level row, and a Farm-level row in these waves is a tree row."""
    fn, flag = ROSTERS[wave]
    src = get_dataframe(str(Path(countries_root()) / COUNTRY / wave / 'Data' / fn))
    producing = src[flag].astype('string').str.strip().eq('Oui')
    n = int(producing.sum())
    flat = built.reset_index()
    flat = flat[flat['t'].astype(str) == wave]
    farm = flat[flat['plot_id'].astype('string').eq('Farm-level')]
    assert 0 < len(farm) <= n                     # the crop-label fold only reduces
    assert n - len(farm) <= 1                     # 2014-15: one all-zero fold
    assert farm['perennial'].astype('boolean').fillna(False).all()
    # and a seasonal row is never on the sentinel
    seasonal = flat[~flat['perennial'].astype('boolean').fillna(False)]
    assert not seasonal['plot_id'].astype('string').eq('Farm-level').any()


@pytest.mark.requires_s3
def test_harvest_kg_loses_no_converted_kilogram(built):
    """The reduction's total equals the rows' own kilograms: no key deleted one."""
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        got = T.harvest_kg(built)
        factors = T.harvest_kg_factors(built)
    kg = (pd.to_numeric(built['Quantity'], errors='coerce').to_numpy(dtype='float64')
          * pd.to_numeric(factors['kg_per_unit'], errors='coerce').to_numpy(dtype='float64'))
    want = pd.Series(kg).where(lambda s: s > 0).sum()
    assert got['Harvest_kg'].sum() == pytest.approx(want, rel=1e-9)
