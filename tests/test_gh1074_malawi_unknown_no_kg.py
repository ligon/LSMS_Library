"""Malawi 2019-20 food_acquired: 'no unit recorded' keeps no code-23 kg (GH #1074).

Ruling (@ligon, 2026-11-03): a row served ``u = 'Unknown'`` does not keep the
IHS5 conversion table's code-23 OTHER (SPECIFY) factor.  The factor is merged
on the RAW unit code before ``handling_unusual_units`` turns a code-23 row with
no usable specify text into ``Unknown``, so the driver masks ``cfactor_*``
afterwards -- for code 23 only, so a code-23 text that names a unit keeps its
factor.  Measured: 49 rows (Mango 29 at 10.99 kg per mango), 7,143.49 kg, all
2019-20.  The data-gated pin is in tests/test_gh1042_missing_unit_spellings.py.
"""
import ast

from lsms_library.paths import countries_root

DRIVER = countries_root() / 'Malawi' / '2019-20' / '_' / 'food_acquired.py'


def test_driver_masks_the_factor_after_the_unit_is_resolved():
    src = DRIVER.read_text()
    tree = ast.parse(src)
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)]
    names = [getattr(c.func, 'id', getattr(c.func, 'attr', None)) for c in calls]
    assert 'handling_unusual_units' in names and 'is_other_specify_code' in names
    # the mask comes after the unit is resolved and before the canonical melt
    pos_units = src.index('handling_unusual_units(df')
    pos_mask = src.index("df[f'cfactor_{s}'] = df[f'cfactor_{s}'].mask(no_unit)")
    pos_melt = src.index('food_acquired_to_canonical(df')
    assert pos_units < pos_mask < pos_melt
    assert "eq(U_UNKNOWN)" in src
    assert "for s in ['consumed', 'bought', 'produced', 'gifted']:" in src
