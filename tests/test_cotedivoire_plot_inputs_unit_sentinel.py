"""GH #1118 (Cote d'Ivoire): a blank used unit is ``Unknown``, never a minted ``Manquant``.

Both wave scripts used to fill a blank ``s16bq03b`` with ``'Manquant'``.  2018-19
has no such line; 2021-22 has 4.  The first test reads only the script sources;
the second builds the real table (``requires_s3``) and counts against the
``.dta`` through ``get_dataframe``.
"""
import ast
import warnings

import pytest

import lsms_library as ll
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

ROOT = countries_root() / 'CotedIvoire'
CELLS = {'2018-19': 'Data/Menage/s16b_me_CIV2018.dta', '2021-22': 'Data/s16b_me_civ2021.dta'}


@pytest.mark.parametrize('wave', list(CELLS))
def test_no_script_mints_manquant(wave):
    tree = ast.parse((ROOT / wave / '_' / 'plot_inputs.py').read_text())
    literals = {n.value for n in ast.walk(tree) if isinstance(n, ast.Constant)}
    assert 'Manquant' not in literals
    assert 'Unknown' in literals


@pytest.mark.requires_s3
def test_served_missing_units_are_unknown():
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        served = ll.Country('CotedIvoire').plot_inputs()
    u = served.index.get_level_values('u')
    t = served.index.get_level_values('t')
    assert (u == 'Manquant').sum() == 0
    for wave, rel in CELLS.items():
        codes = get_dataframe(ROOT / wave / rel, convert_categoricals=False)
        blank = int(codes.loc[codes['s16bq02'] == 1, 's16bq03b'].isna().sum())
        assert blank == {'2018-19': 0, '2021-22': 4}[wave]
        assert int(((u == 'Unknown') & (t == wave)).sum()) == blank
