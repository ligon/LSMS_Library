"""GH #637 defect C -- Nigeria W3 community_prices: two C8 locations, one row each.

The W3 (2015-16) post-harvest community questionnaire fields the food-price
module at TWO locations per community, over the same item list:
``sectc8a_harvestw3.dta`` is "C8a: FOOD PRICES - 1ST LOCATION" and
``sectc8b_harvestw3.dta`` "C8b: FOOD PRICES - 2ND LOCATION"
(PH_W3_Community_Quest.pdf, SECTIONS list "8 Food Prices / a. First location
18 / b. Second location 24").  ``nigeria.community_prices_for_wave`` collapsed
the two on ``(t, v, j, u)`` with ``Price: 'first'``, so location 1 always won:
983 of the 2,591 cells both locations priced held a different location-2
price that was dropped, and 3,218 location-2-only rows were served
indistinguishable from location 1.

The fix is the GH #840 ``obs`` level (``lsms_library/data_info.yml``, Index
Info): ``obs`` is the form's reading slot, 1 for C8a and 2 for C8b, 1 in every
other wave.  Niger's duplicate-entry drop (a SECOND source record of the same
slot that repeats a served row) has nothing to act on -- each C8 file is
``(ea, item_cd)``-unique -- so a location-2 price equal to location 1's is a
second slot and is served.  W1/W2's own ``first`` collapse (size variants and
lumped items) is out of scope and pinned unchanged here.

Every test here fails on development ``9a0ed5f3c``: the builder ignores
``obs`` and the declared index is ``(t, v, j, u)``.
"""
import importlib.util
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml


@pytest.fixture(scope='module')
def nigeria_module():
    path = countries_root() / 'Nigeria' / '_' / 'nigeria.py'
    spec = importlib.util.spec_from_file_location('_nigeria_637', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _frame(rows, obs=None, unit='Heap Small'):
    """A W3-shaped C8 frame: (ea, item_cd, unit label, price) rows."""
    raw = pd.DataFrame({
        'state': [1] * len(rows), 'lga': [101] * len(rows),
        'ea': [r[0] for r in rows], 'item_cd': [r[1] for r in rows],
        'c8q2': [1] * len(rows), 'c8q3': [r[2] for r in rows],
        'c8q2b': pd.Series([pd.NA] * len(rows), dtype='string'),
    })
    dec = raw.copy()
    dec['c8q2'] = pd.Series([unit] * len(rows), dtype='string')
    fr = dict(df=raw, dec=dec, ea='ea', item='item_cd', price='c8q3',
              unit='c8q2')
    if obs is not None:
        fr['obs'] = obs
    return fr


def _w3(nigeria_module, frames):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        return nigeria_module.community_prices_for_wave(
            '2016Q1', frames, 'codes', {11: 'Millet', 12: 'Sorghum'})


class TestDeclaration:
    def test_nigeria_declares_obs(self):
        scheme = load_yaml(countries_root() / 'Nigeria' / '_' / 'data_scheme.yml')
        spec = scheme['Data Scheme']['community_prices']['index']
        assert [t.strip() for t in spec.strip('() ').split(',')] == \
            ['t', 'v', 'j', 'u', 'obs']

    def test_w3_script_passes_one_slot_per_location(self):
        src = (countries_root() / 'Nigeria' / '2015-16' / '_'
               / 'community_prices.py').read_text(encoding='utf-8')
        assert "(1, '../Data/sectc8a_harvestw3.dta')" in src
        assert "(2, '../Data/sectc8b_harvestw3.dta')" in src
        assert 'obs=obs' in src
        # The comment the GH #637 diagnosis found false.
        assert 'different item ranges)' not in src


class TestBuilder:
    """``community_prices_for_wave`` on synthetic C8 frames (data-free)."""

    def test_two_locations_are_two_rows(self, nigeria_module):
        loc1 = _frame([(1, 11, 100.0), (1, 12, 80.0)], obs=1)
        loc2 = _frame([(1, 11, 125.0), (1, 12, 80.0)], obs=2)
        out = _w3(nigeria_module, [loc1, loc2])
        assert list(out.index.names) == ['t', 'v', 'i', 'j', 'u', 'obs']
        got = out.reset_index().set_index(['j', 'obs'])['Price']
        # A differing location-2 price is served, not dropped ...
        assert got[('Millet', 1)] == 100.0 and got[('Millet', 2)] == 125.0
        # ... and an EQUAL one is a second slot, not a duplicate entry.
        assert got[('Sorghum', 1)] == 80.0 and got[('Sorghum', 2)] == 80.0
        assert len(out) == 4

    def test_location_two_only_row_is_obs_two(self, nigeria_module):
        out = _w3(nigeria_module, [_frame([(1, 11, 100.0)], obs=1),
                                   _frame([(1, 12, 70.0)], obs=2)])
        got = out.reset_index().set_index('j')['obs']
        assert got['Millet'] == 1 and got['Sorghum'] == 2

    def test_obs_defaults_to_one_and_is_integer(self, nigeria_module):
        out = _w3(nigeria_module, [_frame([(1, 11, 100.0)])])
        obs = out.index.get_level_values('obs')
        assert obs.tolist() == [1]
        assert pd.api.types.is_integer_dtype(obs)

    def test_codes_mode_collision_is_an_error(self, nigeria_module):
        # Two records of one slot for one (cluster, item, unit): a broken key
        # in W3-W5, never a cell to select from (GH #840).
        with pytest.raises(AssertionError, match='broken key'):
            _w3(nigeria_module, [_frame([(1, 11, 100.0), (1, 11, 90.0)], obs=1)])

    def test_w1_w2_names_mode_still_collapses_to_first(self, nigeria_module):
        # OUT OF SCOPE for #637 defect C: W1/W2's size variants of one base
        # item (base*10+size) still collapse to the first price at obs 1.
        # Pinned so a change there is a deliberate one.
        raw = pd.DataFrame({'state': [1, 1], 'lga': [101, 101], 'ea': [1, 1],
                            'item_cd': [1511, 1512], 'sc8q2': [50, 90]})
        fr = dict(df=raw, dec=raw, ea='ea', item='item_cd', price='sc8q2')
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            out = nigeria_module.community_prices_for_wave('2011Q1', [fr], 'names')
        # 1511 / 1512 both resolve to base item 151 ('Milk powder', 'tin').
        assert len(out) == 1
        assert out.index.get_level_values('obs').tolist() == [1]
        assert out['Price'].iloc[0] == 50


def _aws_creds_available() -> bool:
    spec = importlib.util.spec_from_file_location(
        '_shared_conftest', Path(__file__).with_name('conftest.py'))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.aws_creds_available()


# Served rows per post-harvest round, measured on cold builds 2026-10-23.
# 2016Q1: 17,344 location-1 + 5,809 location-2 rows (development served
# 20,562: every location-1 row plus the 3,218 location-2-only rows).
EXPECTED = {'2011Q1': {1: 27104}, '2013Q1': {1: 27079},
            '2016Q1': {1: 17344, 2: 5809},
            '2019Q1': {1: 31757}, '2024Q1': {1: 30064}}


@pytest.mark.slow
@pytest.mark.requires_s3
@pytest.mark.skipif(not _aws_creds_available(),
                    reason='needs S3 credentials to build Nigeria')
class TestBuilt:
    @pytest.fixture(scope='class')
    def cp(self):
        from lsms_library.country import Country
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            return Country('Nigeria').community_prices()

    def test_index_carries_obs(self, cp):
        assert list(cp.index.names) == ['t', 'v', 'j', 'u', 'obs']
        assert cp.index.is_unique
        assert pd.api.types.is_integer_dtype(cp.index.get_level_values('obs'))

    def test_rows_per_round_and_slot(self, cp):
        got = {}
        for (t, o), n in cp.reset_index().groupby(['t', 'obs']).size().items():
            got.setdefault(t, {})[int(o)] = int(n)
        assert got == EXPECTED

    def test_location_pairs_are_both_served(self, cp):
        w3 = cp.xs('2016Q1', level='t')['Price'].unstack('obs')
        both = w3.dropna()
        # the 2,591 cells both locations priced -- 983 at a different price
        assert len(both) == 2591
        assert int((both[1] != both[2]).sum()) == 983

    def test_derivation_inputs_join_the_served_grain(self):
        path = countries_root() / 'Nigeria' / '2015-16' / '_' / 'mapping.py'
        spec = importlib.util.spec_from_file_location('_ng_w3_mapping_637', path)
        mp = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mp)
        for frame in (mp.inputs_community_prices('2015-16'),
                      mp.inputs_c8_unrecorded_size_drop('2015-16')):
            assert 'obs' in frame.columns
            assert set(frame['obs']) <= {1, 2}
            assert not frame.duplicated(['v', 'obs', 'item_cd']).any()
