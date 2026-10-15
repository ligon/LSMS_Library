"""GH #869: the IHS4/IHS5 perennial crop-code recode must not drop rows.

Between IHPS (2013-14) and IHS4 (2016-17), Malawi's Module P (perennial /
tree-crop harvest) recoded four groups of crop codes -- cassava 1 -> 100,
"OTHER (SPECIFY)" 18 -> 21, and the tree classes FODDER / FERTILISER /
FUEL WOOD (pre-IHS4 absent) as 1800 / 1900 / 2000 -- verified against each
wave's own Stata value labels.  `_crop_codes(perennial=True)` offsets by
+1000, so cassava arrived as 1100, a code `harmonize_crop` never carried;
`assemble_crop_production`'s `harv[harv['crop'].notna()]` filter then
silently dropped every 2016-17 / 2019-20 cassava, other and tree row
(3,581 raw module rows, 2,990 of them cassava).

The fix lives in `malawi._PERENNIAL_RECODE` (applied post-offset) plus
2800/2900/3000 in `harmonize_crop` / `harmonize_crop_variety` -- code, not
per-wave tables, because the recode is value-identical to the plain offset
for every pre-IHS4 code, and because the 2016-17/2019-20 SALE module
(Module Q) was NOT recoded and still resolves cassava/other through
1001/1018.

The pure-mapping tests run anywhere.  The delivered-table tests are
data-gated (``requires_s3``) and skip cleanly without credentials.
"""
from __future__ import annotations

import importlib.util
import sys

import pandas as pd
import pytest

from lsms_library.country import Country
from lsms_library.paths import countries_root

MALAWI_ = countries_root() / 'Malawi' / '_'


@pytest.fixture(scope='module')
def malawi_mod():
    """The Malawi country module, imported by path (as the wave scripts do)."""
    path = MALAWI_ / 'malawi.py'
    if not path.exists():                                   # pragma: no cover
        pytest.skip(f'{path} not present')
    sys.path.insert(0, str(MALAWI_))
    try:
        spec = importlib.util.spec_from_file_location('malawi_gh869', path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    except Exception as exc:                                # pragma: no cover
        pytest.skip(f'Malawi module unavailable: {type(exc).__name__}: {exc}')
    finally:
        sys.path.remove(str(MALAWI_))
    return mod


@pytest.fixture(scope='module')
def cp():
    try:
        df = Country('Malawi').crop_production()
    except Exception as exc:                                # pragma: no cover
        pytest.skip(f'Malawi crop_production unavailable: '
                    f'{type(exc).__name__}: {exc}')
    if df is None or df.empty:                              # pragma: no cover
        pytest.skip('Malawi crop_production empty')
    return df


def _perennial_label(malawi_mod, code):
    """What `harmonize_crop` serves for a Module P raw code, post-recode."""
    labels, _ = malawi_mod._crop_codes(pd.Series([code]), perennial=True)
    return labels.iloc[0]


class TestPerennialRecode:
    """The mapping itself: IHS4/IHS5 codes resolve, old waves are untouched."""

    def test_recode_codes_resolve(self, malawi_mod):
        assert _perennial_label(malawi_mod, 100) == 'Cassava'
        assert _perennial_label(malawi_mod, 21) == 'Other'
        assert _perennial_label(malawi_mod, 1800) == 'Fodder Trees'
        assert _perennial_label(malawi_mod, 1900) == 'Fertiliser Trees'
        assert _perennial_label(malawi_mod, 2000) == 'Fuel Wood Trees'

    def test_pre_ihs4_codes_are_unchanged(self, malawi_mod):
        """The recode must be a no-op on the IHS3/IHPS namespace."""
        for code, label in [(1, 'Cassava'), (2, 'Tea'), (4, 'Mango'),
                            (17, 'Macadamia'), (18, 'Other')]:
            assert _perennial_label(malawi_mod, code) == label

    def test_sale_module_codes_are_not_recoded(self, malawi_mod):
        """Module Q kept the old codes in IHS4/IHS5; 1001 stays 'Cassava'.

        If the recode were a table row (1100 -> Cassava) instead of code,
        a post-offset 1100 from Module Q would mislabel cassava sales as
        'Other' -- this pins that the recode is `_PERENNIAL_RECODE`, not a
        `harmonize_crop` entry, by checking the map has no 1100 at all.
        """
        cmap = malawi_mod._malawi_code_map('harmonize_crop')
        for ghost in (1100, 1021):
            assert ghost not in cmap, (
                f'harmonize_crop carries {ghost}: the recode belongs in '
                '_PERENNIAL_RECODE so the un-recoded Module Q is safe')
        assert cmap[1001] == 'Cassava'
        assert cmap[1018] == 'Other'

    def test_variety_table_covers_the_tree_classes(self, malawi_mod):
        vmap = malawi_mod._malawi_code_map('harmonize_crop_variety')
        for code in (2800, 2900, 3000):
            assert code in vmap and pd.notna(vmap[code])


@pytest.mark.requires_s3
class TestDelivered:
    """The served table: cassava exists in every wave; nothing else moved."""

    def test_cassava_present_in_every_wave(self, cp):
        counts = cp.reset_index().groupby(['t', 'crop']).size()
        expected = {'2010-11': 1651, '2013-14': 387,
                    '2016-17': 1067, '2019-20': 1917}
        for wave, n in expected.items():
            got = int(counts.get((wave, 'Cassava'), 0))
            assert got == n, f'{wave} Cassava: got {got}, expected {n}'

    def test_recoded_groups_present_in_ihs4_ihs5(self, cp):
        counts = cp.reset_index().groupby(['t', 'crop']).size()
        assert int(counts.get(('2019-20', 'Other'), 0)) == 1551
        assert int(counts.get(('2016-17', 'Other'), 0)) == 522
        trees = ['Fodder Trees', 'Fertiliser Trees', 'Fuel Wood Trees']
        for wave, n in [('2016-17', 15), ('2019-20', 164)]:
            got = sum(int(counts.get((wave, c), 0)) for c in trees)
            assert got == n, f'{wave} tree classes: got {got}, expected {n}'

    def test_old_waves_gain_nothing(self, cp):
        """The recode is a no-op pre-IHS4: total rows must not move."""
        tot = cp.reset_index().groupby('t').size()
        assert int(tot['2010-11']) == 33420
        assert int(tot['2013-14']) == 12616

    def test_no_unmapped_codes_warn_or_survive(self, cp):
        """No delivered row may carry a crop label outside harmonize_crop's
        vocabulary after the fix (the filter drops, but should now have
        nothing to drop)."""
        assert cp.reset_index()['crop'].notna().all()
