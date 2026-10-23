"""GH #1240: Burkina Faso's panel crosswalk must name ids the 2018-19 tables serve.

Each EHCVM wave builds the household id ``i`` from ``(grappe, menage)`` with a
DIFFERENT formatter (GH #460):

- 2018-19: ``2018-19/_/mapping.py:i`` -- ``grappe + '0' + menage.zfill(2)``;
- 2021-22: no wave override, so the country-level ``burkina_faso.i`` --
  ``grappe + menage.zfill(3)``, byte-identical to the survey's ``hhid``.

The two agree for ``menage <= 99``.  For a three-digit 2018-19 menage they do
not (``(2, 126)`` is ``'20126'`` in 2018-19, ``'2126'`` in 2021-22), and the
survey's own ``hhid_EHCVM1`` is in the 2021-22 form.  ``panel_ids.py`` used to
build its 2018-19 targets in that form, so 1,088 of 3,132 targets were ids no
2018-19 table serves and 3 more were the served id of a DIFFERENT 2018-19
household (``(90, 102)`` -> ``'90102'``, which is ``(9, 102)``).
``_finalize_result`` runs ``id_walk`` with these ids on every read.

Two tiers:

- **hermetic** -- reads only the committed JSON and the wave's own formatter.
  It leans on one survey-design fact: EHCVM-2 tracked households within their
  EA of origin (``bid_bfa_ehcvm2.pdf`` p. 4, quoted in ``CONTENTS.org``), so a
  linked household keeps its ``grappe`` (measured: 3,227 / 3,227 cover rows
  have ``grappe == grappe_EHCVM1``).  The target must therefore be the 2018-19
  formatter applied to the source's grappe and some menage.  On development
  ``9a0ed5f3c`` this fails for 1,091 of 3,132 entries -- the 1,088 unserved
  targets AND the 3 wrong-household ones.
- **requires_s3** -- reconstructs both waves' ids from the raw cover sheets and
  checks each target is the 2018-19 id of the source's OWN
  ``(grappe_EHCVM1, menage_EHCVM1)``.
"""
from __future__ import annotations

import importlib.util
import json
from functools import lru_cache

import pandas as pd
import pytest

from lsms_library.paths import countries_root

COUNTRY = 'Burkina_Faso'
PREV, CUR = '2018-19', '2021-22'


def _root():
    root = countries_root() / COUNTRY
    if not root.is_dir():
        pytest.skip('Burkina_Faso config tree unavailable')
    return root


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@lru_cache(maxsize=1)
def _formatters():
    """(2018-19 wave ``i``, country ``i``) -- the formatters the tables use."""
    root = _root()
    wave18 = _load(root / PREV / '_' / 'mapping.py', '_bf_2018_19_mapping_1240')
    country = _load(root / '_' / 'burkina_faso.py', '_bf_country_1240')
    return (lambda g, m: wave18.i(pd.Series([int(g), int(m)])),
            lambda g, m: country.i(pd.Series([int(g), int(m)])))


def _updated_ids():
    with open(_root() / '_' / 'updated_ids.json') as f:
        return json.load(f)


def _crosswalk():
    return _updated_ids()[CUR]


# --------------------------------------------------------------------------
# Hermetic: the committed JSON against the wave's own formatter
# --------------------------------------------------------------------------

def test_every_target_is_a_2018_19_served_format_id():
    i18, _ = _formatters()
    bad = []
    for src, tgt in _crosswalk().items():
        grappe = src[:-3]                     # 2021-22 id = grappe + 3-digit menage
        prefix = grappe + '0'
        ok = (tgt.startswith(prefix) and tgt[len(prefix):].isdigit()
              and i18(grappe, tgt[len(prefix):]) == tgt)
        if not ok:
            bad.append((src, tgt))
    assert not bad, (
        f'{len(bad)} of {len(_crosswalk())} 2018-19 targets are not '
        "2018-19/_/mapping.py:i(grappe, menage) for the source's grappe "
        f'(GH #1240); first: {bad[:5]}')


def test_every_source_is_a_2021_22_country_format_id():
    _, i21 = _formatters()
    bad = [s for s in _crosswalk()
           if not (s.isdigit() and len(s) >= 4 and i21(s[:-3], s[-3:]) == s)]
    assert not bad, f'{len(bad)} sources are not country i(grappe, menage): {bad[:5]}'


def test_panel_ids_json_agrees_with_updated_ids():
    with open(_root() / '_' / 'panel_ids.json') as f:
        panel = json.load(f)
    as_map = {}
    for key, val in panel.items():
        wave, cur = key.split(',')
        prev_wave, prev = val.split(',')
        assert (wave, prev_wave) == (CUR, PREV), (key, val)
        as_map[cur] = prev
    assert as_map == _crosswalk()
    assert _updated_ids().get(PREV, {}) == {}


def test_crosswalk_is_injective_and_chain_free():
    """id_walk closure-resolves a wave's map (``_close_id_map``), so a target
    that is also a key would chain two households onto one id."""
    xw = _crosswalk()
    targets = pd.Series(list(xw.values()))
    assert not targets.duplicated().any()
    assert not [t for t in xw.values() if t in xw and xw[t] != t]


# --------------------------------------------------------------------------
# Data: each target is the source household's OWN 2018-19 id
# --------------------------------------------------------------------------

@pytest.mark.requires_s3
def test_every_target_is_the_source_households_own_2018_19_id():
    from lsms_library.local_tools import get_dataframe

    root = _root()
    i18, i21 = _formatters()
    try:
        c18 = get_dataframe(str(root / PREV / 'Data' / 's00_me_bfa2018.dta'))
        c21 = get_dataframe(str(root / CUR / 'Data' / 's00_me_bfa2021.dta'))
    except Exception as exc:                      # noqa: BLE001 - data absent
        pytest.skip(f'Burkina_Faso EHCVM cover sheets unavailable: {exc}')

    keys18 = c18[['grappe', 'menage']].dropna().drop_duplicates()
    ids18 = {i18(g, m) for g, m in keys18.itertuples(index=False)}
    assert len(ids18) == len(keys18), '2018-19 formatter is not injective'

    by_src = {}
    for r in c21[['hhid', 'grappe', 'menage',
                  'grappe_EHCVM1', 'menage_EHCVM1']].itertuples(index=False):
        src = i21(r.grappe, r.menage)
        assert src == str(int(r.hhid))            # country i == survey hhid
        by_src[src] = i18(r.grappe_EHCVM1, r.menage_EHCVM1)

    xw = _crosswalk()
    assert set(xw) <= set(by_src), 'a source is not a 2021-22 household'
    assert {t for t in xw.values()} <= ids18, 'a target is not a 2018-19 household'
    wrong = {s: t for s, t in xw.items() if by_src[s] != t}
    assert not wrong, (f'{len(wrong)} targets name a different 2018-19 '
                       f'household; first: {list(wrong.items())[:5]}')
