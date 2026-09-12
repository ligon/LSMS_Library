"""GH #874: Ethiopia's food_acquired sources are READ, not derived.

Before this landed, the five wave scripts read only Q2 (total consumed),
Q3 (purchased) and Q4 (spend); ``ethiopia.food_acquired`` derived
``Produced = (Q2 - Q3).clip(lower=0)`` and
``build_transforms.food_acquired_to_canonical`` inverted that into
``purchased = Quantity - Produced = min(Q2, Q3)``.  So ``s='produced'``
was a residual that folded gifts and food aid into own production, and
``s='inkind'`` did not exist in any wave.

The ESS asks all three directly (SECTION 5A / 6A):

    Q3/Q4  how much came from purchases, and what did you spend
    Q5     how much came from own production
    Q6     how much came from gifts and other sources

each with its own unit code.  These tests pin that the config reads those
variables and that the served numbers are those answers.

Two tiers, deliberately:

* config / source pins -- no data access, always run.  They are what catch
  a wave script quietly losing a variable.
* a ``slow`` served-value check -- builds the country table and compares a
  wave's ``produced`` rows against raw ``hh_s5aq05_a``.  A config pin alone
  cannot see a regression in the melt itself.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from lsms_library.paths import countries_root

ETH = Path(countries_root()) / 'Ethiopia'

#: wave -> (own-production stem, gifts stem).  W1-W3 are ESS section 5A,
#: W4-W5 section 6A -- the variable names change, the questions do not.
WAVE_VARS = {
    '2011-12': ('hh_s5aq05_a', 'hh_s5aq05_b', 'hh_s5aq06_a', 'hh_s5aq06_b'),
    '2013-14': ('hh_s5aq05_a', 'hh_s5aq05_b', 'hh_s5aq06_a', 'hh_s5aq06_b'),
    '2015-16': ('hh_s5aq05_a', 'hh_s5aq05_b', 'hh_s5aq06_a', 'hh_s5aq06_b'),
    '2018-19': ('s6aq05a', 's6aq05b', 's6aq06a', 's6aq06b'),
    '2021-22': ('s6aq05a', 's6aq05b', 's6aq06a', 's6aq06b'),
}


def _code_of_food_acquired():
    """``ethiopia.food_acquired``'s body with its docstring removed.

    The docstring QUOTES the construction this test forbids (that is the
    point of the docstring), so matching against the whole function would
    make the prose, not the code, decide whether the test passes.
    """
    src = (ETH / '_' / 'ethiopia.py').read_text()
    body = src[src.index('def food_acquired(fn, myvars):'):]
    body = body[:body.index('\ndef ')]
    head, _, rest = body.partition('"""')
    _doc, _, code = rest.partition('"""')
    return head + code

@pytest.mark.parametrize('wave', sorted(WAVE_VARS))
def test_wave_script_reads_q5_and_q6(wave):
    """Each wave's myvars must name the own-production and gifts variables."""
    src = (ETH / wave / '_' / 'food_acquired.py').read_text()
    q5a, q5b, q6a, q6b = WAVE_VARS[wave]
    assert re.search(rf"quantity_produced\s*=\s*'{q5a}'", src), wave
    assert re.search(rf"units_produced\s*=\s*'{q5b}'", src), wave
    assert re.search(rf"quantity_inkind\s*=\s*'{q6a}'", src), wave
    assert re.search(rf"units_inkind\s*=\s*'{q6b}'", src), wave


def test_country_module_does_not_derive_produced():
    """No residual, and no route through the residual-splitting helper.

    ``food_acquired_to_canonical``'s documented contract IS
    ``purchased = Quantity - Produced``; calling it would reintroduce the
    defect however the wave scripts are wired.
    """
    body = _code_of_food_acquired()
    assert 'food_acquired_to_canonical' not in body
    assert 'clip(lower=0)' not in body
    # The three sources are built from the reported quantities, by name.
    for name in ('quantity_purchased', 'quantity_produced', 'quantity_inkind'):
        assert name in body, name
    # Each source keeps its OWN unit.
    for name in ('units_purchased', 'units_produced', 'units_inkind'):
        assert name in body, name


def test_screen_counts_and_does_not_clip():
    """Q5 > Q2 is impossible, and is COUNTED -- never clipped/dropped/NaN'd."""
    body = _code_of_food_acquired()
    assert '_exceeds_total' in body
    assert 'warnings.warn' in body
    # The screen must not modify any served quantity.
    assert not re.search(r'Q[356]\s*=\s*Q[356]\.(clip|where|mask)', body)


@pytest.mark.slow
def test_served_produced_is_q5_and_inkind_is_q6():
    """The served numbers are the survey's answers, per wave."""
    import os
    import numpy as np
    import pandas as pd
    import lsms_library as ll
    from lsms_library.local_tools import get_dataframe

    fa = ll.Country('Ethiopia').food_acquired()
    served = set(fa.index.get_level_values('s'))
    assert {'purchased', 'produced', 'inkind'} <= served

    wave, fn = '2018-19', 'sect6a_hh_w4.dta'
    cwd = os.getcwd()
    try:
        os.chdir(ETH / wave / '_')
        raw = get_dataframe(f'../Data/{fn}', convert_categoricals=True)
    finally:
        os.chdir(cwd)

    for s, col in (('produced', 's6aq05a'), ('inkind', 's6aq06a')):
        want = pd.to_numeric(raw[col], errors='coerce')
        want = float(want[want > 0].sum())
        got = float(fa.xs(wave, level='t').xs(s, level='s')['Quantity'].sum())
        # id_walk and the canonical dedupe can merge duplicate keys, so the
        # total is the invariant, not the row count.
        assert np.isclose(got, want, rtol=1e-9), (s, got, want)

    # The residual is gone: produced is no longer bounded by Q2 - Q3.
    q2 = pd.to_numeric(raw['s6aq02a'], errors='coerce').fillna(0)
    q3 = pd.to_numeric(raw['s6aq03a'], errors='coerce').fillna(0)
    residual = float((q2 - q3).clip(lower=0).sum())
    got = float(fa.xs(wave, level='t').xs('produced', level='s')['Quantity'].sum())
    assert not np.isclose(got, residual, rtol=1e-6)
