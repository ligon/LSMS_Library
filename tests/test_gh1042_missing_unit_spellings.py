"""GH #1042: two served unit spellings that mean "no unit recorded".

* GhanaLSS ``u='None'`` (GLSS4/5/6 Section 8H, own production).  Code 0 heads
  the GLSS5/GLSS6 unit lists as ``None ....00`` -- the same "nothing" code that
  heads the crop list -- and is the zero-fill on every non-consumer row; GLSS4's
  printed list has no code 0 at all.  ``_/unit_labels.org`` now maps it to the
  missing-unit sentinel.
* Malawi ``u='N/A'`` (IHS4/IHS5 Module G).  Every such row is unit code 23
  ``OTHER (SPECIFY)`` with the specify text ``N/A``.  ``_clean_freetext_unit``
  now emits the sentinel instead of the free text.

The remap has to happen at BUILD time: the kg inference runs on the raw ``u``,
so the delivered tier checks the property (no inferred kilograms on these
rows), not just the label.  Evidence: both countries' ``_/CONTENTS.org``.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest

from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root, data_root
from lsms_library.transformations import U_UNKNOWN, food_kg_factors

_MALAWI = countries_root() / 'Malawi' / '_' / 'malawi.py'

#: Rows the remap moves, measured on the build at 3e3bd26ee (GH #1042).
_EXPECTED = {
    'GhanaLSS': ('None', {'1998-99': 88, '2005-06': 1450, '2012-13': 970}),
    'Malawi': ('N/A', {'2016-17': 24, '2019-20': 19}),
}

#: Rungs a missing-unit row may be served by: its own shipped factor, a metric
#: label, or nothing.  Never an inferred (pooled) rung.
_NON_INFERRED = {'survey_kg', 'metric', 'none'}


def _malawi():
    spec = importlib.util.spec_from_file_location('malawi_1042', _MALAWI)
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
    except Exception as exc:  # pragma: no cover - environment-dependent imports
        pytest.skip(f'malawi.py not importable here: {exc}')
    return mod


# --------------------------------------------------------------- config tier
def test_ghanalss_unit_labels_map_none_to_the_sentinel():
    ul = df_from_orgfile(countries_root() / 'GhanaLSS' / '_' / 'unit_labels.org',
                         name='unit_label').dropna()
    umap = dict(zip(ul['u'].astype(str).str.strip(),
                    ul['Preferred Label'].astype(str).str.strip()))
    assert umap['None'] == U_UNKNOWN
    assert umap['none'] == U_UNKNOWN
    assert 'None' not in set(umap.values())


@pytest.mark.parametrize('text', ['N/A', 'n/a', ' N/A '])
def test_malawi_na_specify_text_is_the_sentinel(text):
    assert _malawi()._clean_freetext_unit(text) == U_UNKNOWN


def test_malawi_na_does_not_fall_back_to_other_specify():
    """Blanking 'N/A' would fall back to the code-23 label, which gets kg."""
    df = pd.DataFrame({'unitsdetail_bought': ['N/A', 'Basket', None],
                       'units_bought': ['OTHER (SPECIFY)'] * 3})
    out = _malawi().handling_unusual_units(df, suffixes=['bought'])
    assert list(out['u_bought'][:2]) == [U_UNKNOWN, 'Basket']
    assert out['u_bought'].iloc[2] != U_UNKNOWN


# ------------------------------------------------------------ delivered tier
@pytest.mark.parametrize('country', sorted(_EXPECTED))
def test_delivered_rows_carry_the_sentinel_and_no_inferred_kg(country):
    if not (data_root() / country / 'var' / 'food_acquired.parquet').exists():
        pytest.skip(f'{country} food_acquired not cached')
    import lsms_library as ll

    spelling, by_wave = _EXPECTED[country]
    fa = ll.Country(country).food_acquired()
    u = fa.index.get_level_values('u').astype(str)
    assert not (u == spelling).any(), f'{country} still serves u={spelling!r}'

    t = fa.index.get_level_values('t')
    for wave, n in by_wave.items():
        assert int(((u == U_UNKNOWN) & (t == wave)).sum()) == n, (country, wave)

    sources = food_kg_factors(fa).loc[(u == U_UNKNOWN), 'KgFactorSource']
    assert set(sources.astype(str)) <= _NON_INFERRED, sources.value_counts()
