"""EthiopiaRHS food units (GH #1044): float-stringified codes, and 1989.

Two leaks in the unit axis ``u`` of ``EthiopiaRHS.food_acquired``:

* **1994a-1997.**  The q36 unit columns are MYVARS, so ``format_id`` is never
  auto-applied (CLAUDE.md: "format_id is auto-applied to idxvars but NOT to
  myvars").  ``harmonize_unit`` decodes a listed code and passes an unlisted
  one through as the raw float, which was stringified ``'31.0'``.  Four codes
  are unlisted -- 31, 37, 83, 95 -- and they are unlisted in the SOURCE too:
  31 is ``OTHER`` in the 1994a/1994b/1995 value labels, 95 is ``Other`` in
  1997's, and 37 / 83 carry no label in any wave's file.  So they are served
  as integer codes (``'31'``), the table's pass-through convention, and keep
  the #962 accepted-residual kg rung.

* **1989.**  ``food89.dta`` carries no unit value labels and its scheme is NOT
  the 1994+ one: the 1989 instrument (Debre Berhan, section E3) numbers units
  per questionnaire page -- E3.1 ``1 = Quintal ... 9 = Other``, E3.2/E3.3
  ``1 = Packets ... 7 = Kilogrammes ... 13 = Ensira``.  Code 0 is on no list
  and is the missing-unit sentinel -> ``U_UNKNOWN`` (#1041).  Code 1 stays
  raw: it is a QUINTAL, not a kilogram (its 9 purchases price at a median 52
  Birr/unit; 1994a barley is 1.10 Birr/kg), so decoding it through
  ``harmonize_unit`` (``| 1 | kg |``) would mint kilograms onto quintal rows.

The first block is config-only; ``TestBuilt`` reads the built table.
"""
import importlib.util
import re

import numpy as np
import pandas as pd
import pytest
import yaml

from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root
from lsms_library.transformations import U_UNKNOWN, food_kg_factors

ERHS = countries_root() / 'EthiopiaRHS'
UNLISTED = ('31', '37', '83', '95')
FLOAT_CODE = re.compile(r'^-?\d+\.\d+$')


def _erhs():
    spec = importlib.util.spec_from_file_location(
        'erhs_1044', ERHS / '_' / 'ethiopiarhs.py')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _wide(units):
    """A 1994+-shaped hook input: one purchased row per decoded unit value,
    exactly as ``df_data_grabber`` hands it over (labels for listed codes,
    raw floats for unlisted ones)."""
    n = len(units)
    return pd.DataFrame({
        'i': [f'v_{k}' for k in range(n)], 'j': ['Teff'] * n,
        'q_purch': [1.0] * n, 'u_purch': units, 'e_purch': [2.0] * n,
        'q_prod': [np.nan] * n, 'u_prod': [np.nan] * n,
        'q_inkind': [np.nan] * n, 'u_inkind': [np.nan] * n,
    }).set_index(['i', 'j'])


def test_unlisted_codes_are_served_as_integer_codes():
    labels = ['kg', '100 kg', 'Value', 'Kettle/Jebena/Pot', "Bere'lei"]
    out = _erhs().food_acquired(
        _wide([31.0, 37.0, 83.0, 95.0] + labels + ['', np.nan]))
    u = out.index.get_level_values('u')
    assert sorted(u) == sorted(list(UNLISTED) + labels)
    assert not [x for x in u if FLOAT_CODE.match(x)], list(u)


def test_the_four_codes_really_are_unlisted():
    """The premise of serving them as codes.  If a maintainer adds a row for
    one of these (31 and 95 are ``Other`` in the source), the label reaches
    ``u`` without any code change -- revisit CONTENTS.org's #1044 entry and
    ``tests/test_u_code_leak.py``'s ledger when that happens."""
    table = df_from_orgfile(str(ERHS / '_' / 'categorical_mapping.org'),
                            name='harmonize_unit')
    listed = {str(int(c)) for c in table['Code']}
    assert not listed & set(UNLISTED)


def test_1989_code_0_is_unknown_and_code_1_stays_raw():
    flat = pd.DataFrame({
        'i': ['1', '2', '3', '4'], 'j': ['Barley'] * 4,
        'u': ['0', '1', '5', '0'], 's': ['purchased', 'produced', 'purchased',
                                         'produced'],
        'Quantity': [0.0, 0.25, 1.0, 2.0], 'Expenditure': [1.0, 13.0, 0.5, 0.0],
    }).set_index(['i', 'j', 'u', 's'])
    out = _erhs().food_acquired(flat)
    u = out.index.get_level_values('u').tolist()
    assert u == [U_UNKNOWN, '1', '5', U_UNKNOWN]
    assert len(out) == len(flat), 'the missing-unit rows must be kept, not dropped'


def test_1989_does_not_decode_through_the_1994_table():
    """Applying ``harmonize_unit`` to 1989's ``unitcode`` would turn the
    quintal (code 1) into ``kg`` and every other code into a 1994+ name the
    1989 instrument contradicts (its 5 is Ensira or Bars, not Kunna)."""
    info = yaml.safe_load((ERHS / '1989' / '_' / 'data_info.yml').read_text())
    assert info['food_acquired']['idxvars']['u'] == 'unitcode'


@pytest.mark.requires_s3
class TestBuilt:
    @pytest.fixture(scope='class')
    def fa(self):
        import warnings
        import lsms_library as ll
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            return ll.Country('EthiopiaRHS', preload_panel_ids=False).food_acquired()

    @pytest.fixture(scope='class')
    def idx(self, fa):
        return fa.index.to_frame(index=False).astype({'u': str, 't': str})

    def test_no_float_stringified_unit_in_any_wave(self, idx):
        bad = idx.loc[idx['u'].str.match(FLOAT_CODE.pattern), ['t', 'u']]
        assert bad.empty, bad.value_counts().to_dict()

    def test_unlisted_codes_keep_their_rows(self, idx):
        counts = idx.loc[idx['u'].isin(UNLISTED), 'u'].value_counts().to_dict()
        assert counts == {'31': 98, '37': 3, '83': 21, '95': 57}

    def test_1989_missing_unit_is_unknown(self, idx):
        w89 = idx[idx['t'] == '1989']
        assert (w89['u'] == '0').sum() == 0
        assert (w89['u'] == U_UNKNOWN).sum() == 14
        # code 1 is the quintal: still raw, and never kilograms
        assert (w89['u'] == '1').sum() == 32
        assert not w89['u'].str.lower().isin(['kg', '100 kg']).any()

    def test_unknown_rows_get_no_kg_factor_by_rule(self, fa, idx):
        src = food_kg_factors(fa)['KgFactorSource'].to_numpy()
        assert set(src[(idx['u'] == U_UNKNOWN).to_numpy()]) == {'none'}
