#!/usr/bin/env python3
"""Build Benin's panel crosswalk: EHCVM-2 2021-22 -> EHCVM-1 2018-19 (GH #1705).

Rulings (@ligon): 2026-11-12, "Make Benin a panel country": build and commit a
Benin panel crosswalk (``panel_ids.json`` / ``updated_ids.json``, design A)
from ``PanelHH``.  2026-11-13, "Unlink the 63": PanelHH stays the rule, with
one registered and disclosed exception (below).  Regenerate with
``make panel-ids`` and review the diff; the two JSON files are committed
sources, never build products (GH #914).

THE RULE.  A 2021-22 household is linked to the 2018-19 household with the same
``(grappe, menage)`` when, and only when,

  1. the 2021-22 cover ``s00_me_ben2021.dta`` flags it ``PanelHH == 'Panel'``
     ("Ménage panel"),
  2. that ``(grappe, menage)`` is a household of the 2018-19 cover
     ``s00_me_ben2018.dta``, and
  3. THE EXCEPTION: its 2021-22 roster ``s01_me_ben2021.dta`` does not record its
     WHOLE pre-loaded 2018-19 roster as gone -- a household with at least one
     pre-loaded member (``s01q00aa`` answered) and every pre-loaded member
     ``s01q00aa`` = 2 (Non, "no longer a member") is not linked.  Registered as
     ``Benin::panel_ids::all-preloaded-left-not-linked`` (``_/derivations.yml``):
     its 63 households' current members match the same key's 2018-19 roster at
     control level, so they read as replacement households carrying a Panel flag.

The 2021-22 files record NO 2018-19 identifier of their own: the cover has no
``s00q07f`` (Mali / Niger) or ``grappe_EHCVM1`` / ``menage_EHCVM1`` (Burkina
Faso), so a panel household's 2018-19 id is its own ``(grappe, menage)``.  The
BID (section 7.1) says exactly that: "only households marked as panel in the
"PanelHH" variable will have valid values for merging".  ``s00q07d`` ("was the
household interviewed in 2018/2019?") is not the rule: it is Oui on every
linked household and also on the 116 recurring keys flagged Non panel.

Measured on the release held at f138a3d61 (_/CONTENTS.org, "2026-11-13 --
Benin is a panel country"); ``__main__`` refuses a release whose counts differ:

  PanelHH == Panel                        6,889
    with a 2018-19 household               6,878
      whole pre-loaded roster gone            63   -> not linked (the exception)
      the rest                             6,815   -> linked
    without one                               11   -> not linked (no target)
  PanelHH == Non panel, key recurs           116   -> not linked
  PanelHH == Non panel, new key            1,027
  duplicate targets                            0

THE IDS.  2018-19's ``i`` is ``benin.i`` = ``format_id(grappe) +
format_id(menage, zeropadding=3)``; 2021-22's is ``2021-22/_/mapping.i`` = the
same digits behind ``mapping.ID_PREFIX`` (``'E2_'``).  Both formatters are
imported here, not re-implemented, so the crosswalk cannot drift from the ids
the tables serve.  ``updated_ids['2021-22']`` maps a linked household's
``E2_`` id onto its bare 2018-19 id; every other 2021-22 household keeps its
``E2_`` id, which is what stops the 116 recurring Non-panel keys and the 63
excepted households from meeting their 2018-19 namesakes.

THE SINGLE-PASS HAZARDS (CLAUDE.md, "Panel ID Transitive Chains").  ``id_walk``
renames each id once.  It cannot survive a CHAIN (X -> T while the household
whose raw id is T moves on to U) or a RENAME-ONTO-OCCUPIED (a target held by a
2021-22 household that keeps its id).  Here both are impossible by
construction -- every key carries the prefix and no target does -- and the
script asserts it rather than assuming it, as Burkina Faso's does, so a future
release or formatter that breaks the construction fails here, at regeneration.
"""

import importlib.util
import json
from pathlib import Path as _Path
import sys

import pandas as pd

# GH #914: everything is resolved off THIS file, never the process's cwd, so a
# maintainer running the script from anywhere updates the tracked files.
_HERE = _Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from lsms_library.local_tools import RecursiveDict, get_dataframe  # noqa: E402
from benin import i as benin_i  # noqa: E402


def _wave_mapping(wave):
    """The wave's own ``mapping.py``, loaded by path (it is not a package)."""
    path = _HERE.parent / wave / '_' / 'mapping.py'
    spec = importlib.util.spec_from_file_location(f'benin_{wave.replace("-", "_")}_mapping', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_m21 = _wave_mapping('2021-22')
PREFIX = _m21.ID_PREFIX


def _pair(frame):
    return [(int(g), int(m)) for g, m in frame[['grappe', 'menage']].itertuples(index=False)]


def _id_2018_19(g, m):
    return benin_i(pd.Series([g, m]))


def _id_2021_22(g, m):
    return _m21.i(pd.Series([g, m]))


def _require(condition, message):
    """An assertion that survives ``python -O``: these guards are the design."""
    if not condition:
        raise AssertionError(message)




#: ``s01q00aa`` ("[NOM] est-il toujours un membre de ce ménage?"), by code: 1 Oui, 2 Non.
STILL_MEMBER, GONE = 1, 2

#: The counts of the release this crosswalk was ruled on (``__main__`` refuses another).
EXPECTED = {'linked': 6815, 'all_preloaded_left': 63, 'panel_no_target': 11,
            'recurring_non_panel': 116, 'own_id': 1217}


def all_preloaded_left(roster_21):
    """The 2021-22 ``(grappe, menage)`` keys whose WHOLE pre-loaded 2018-19 roster is gone.

    Registered function of ``Benin::panel_ids::all-preloaded-left-not-linked``.  A
    household qualifies when at least one ``s01`` row is pre-loaded (``s01q00aa``
    answered) and every pre-loaded row answers ``s01q00aa`` = 2 (Non).  ``roster_21``
    is ``s01_me_ben2021.dta`` read by CODE (``convert_categoricals=False``).
    """
    answered = roster_21[roster_21['s01q00aa'].notna()]
    codes = set(pd.to_numeric(answered['s01q00aa']).astype(int))
    _require(codes <= {STILL_MEMBER, GONE},
             f's01q00aa codes changed: {sorted(codes)}; review the exception')
    gone = answered['s01q00aa'].astype(int).eq(GONE)
    by_key = gone.groupby([answered['grappe'].astype(int), answered['menage'].astype(int)]).all()
    return {key for key, every in by_key.items() if every}


def link(cover_18, cover_21, roster_21):
    """The crosswalk from the two covers and the 2021-22 roster, by the rule above.

    Returns ``(links, baseline, current, counts)``: ``{2021-22 id: 2018-19 id}``, the
    2018-19 ids, every 2021-22 id (the FULL cover), and the counts the PR reports.
    """
    _require(cover_18[['grappe', 'menage']].notna().all(axis=None), '2018-19 cover: blank grappe/menage')
    _require(cover_21[['grappe', 'menage']].notna().all(axis=None), '2021-22 cover: blank grappe/menage')
    ids_18 = [_id_2018_19(g, m) for g, m in _pair(cover_18)]
    ids_21 = [_id_2021_22(g, m) for g, m in _pair(cover_21)]
    baseline, current = set(ids_18), set(ids_21)
    _require(len(baseline) == len(cover_18), '2018-19 cover: (grappe, menage) is not a household key')
    _require(len(current) == len(cover_21), '2021-22 cover: (grappe, menage) is not a household key')
    # The two namespaces must be disjoint for the construction below to hold ...
    _require(all(i.startswith(PREFIX) for i in current), 'a 2021-22 id lacks the wave prefix')
    _require(not any(i.startswith(PREFIX) for i in baseline), 'a 2018-19 id carries the 2021-22 prefix')
    # ... and the 2021-22 id must be the prefixed 2018-19 spelling of the same key.
    _require(all(c == PREFIX + _id_2018_19(g, m) for (g, m), c in zip(_pair(cover_21), ids_21)),
             'a 2021-22 id is not the prefixed 2018-19 spelling of its key')

    flag = cover_21['PanelHH'].astype('string').str.strip()
    _require(set(flag.dropna()) == {'Panel', 'Non panel'} and bool(flag.notna().all()),
             f'PanelHH vocabulary changed: {sorted(flag.dropna().unique())}; review the rule')
    is_panel = (flag == 'Panel').to_numpy()
    gone = all_preloaded_left(roster_21)

    links: dict[str, str] = {}
    counts = {'panel_no_target': 0, 'recurring_non_panel': 0, 'all_preloaded_left': 0}
    for (g, m), cur, panel in zip(_pair(cover_21), ids_21, is_panel):
        prev = _id_2018_19(g, m)
        if panel and prev in baseline and (g, m) in gone:
            counts['all_preloaded_left'] += 1          # the registered exception
        elif panel and prev in baseline:
            links[cur] = prev
        elif panel:
            counts['panel_no_target'] += 1
        elif prev in baseline:
            counts['recurring_non_panel'] += 1
    counts['linked'] = len(links)
    counts['own_id'] = len(current) - len(links)
    return dict(sorted(links.items())), baseline, current, counts


def check_single_pass(links, baseline, current):
    """Raise AssertionError unless a single-pass ``id_walk`` over ``links`` is safe.

    The single-pass hazards (CLAUDE.md "Panel ID Transitive Chains", Burkina Faso's
    pattern, GH #1245): a CHAIN is a key that is also a target; a RENAME-ONTO-OCCUPIED
    is a target that a 2021-22 household holds as its own id -- ``current`` is the FULL
    2021-22 cover, not just the linked households.  Also refused: two households on one
    target, a target that is no 2018-19 household, and a link that is not the same key.
    """
    targets = list(links.values())
    _require(len(set(targets)) == len(targets), 'two 2021-22 households claim one 2018-19 household')
    _require(set(targets) <= set(baseline), 'a target is not a 2018-19 household')
    _require(all(cur == PREFIX + prev for cur, prev in links.items()),
             'a link is not the same (grappe, menage)')
    chained = set(links) & set(targets)
    occupied = set(targets) & set(current)
    _require(not chained, f'{len(chained)} chained entries: {sorted(chained)[:5]}')
    _require(not occupied, f'{len(occupied)} targets held by a 2021-22 household: {sorted(occupied)[:5]}')


def _read(wave, name, **kw):
    return get_dataframe(_HERE.parent / wave / 'Data' / name, **kw)


def _sources():
    return (_read('2018-19', 's00_me_ben2018.dta'),
            _read('2021-22', 's00_me_ben2021.dta', convert_categoricals=True),
            _read('2021-22', 's01_me_ben2021.dta', convert_categoricals=False))


def inputs_all_preloaded_left():
    """Registered inputs of ``Benin::panel_ids::all-preloaded-left-not-linked``: every
    ``s01`` row of the households the exception leaves unlinked, at ``(t, i)`` with
    their own (``E2_``) id, beside the cover's ``PanelHH`` / ``s00q07d`` / ``s00q07e``.
    Original variable names, codes as the file holds them."""
    _, cover_21, roster_21 = _sources()
    gone = all_preloaded_left(roster_21)
    keys = [(int(g), int(m)) for g, m in zip(roster_21['grappe'], roster_21['menage'])]
    rows = roster_21[[k in gone for k in keys]].copy()
    cover = cover_21.assign(_k=[(int(g), int(m)) for g, m in zip(cover_21['grappe'], cover_21['menage'])])
    cover = cover.set_index('_k')[['PanelHH', 's00q07d', 's00q07e']]
    rk = [(int(g), int(m)) for g, m in zip(rows['grappe'], rows['menage'])]
    for col in cover.columns:
        rows[col] = cover[col].reindex(rk).to_numpy()
    rows['t'] = '2021-22'
    rows['i'] = [_id_2021_22(g, m) for g, m in rk]
    return rows.set_index(['t', 'i'])


def write(links):
    """Write the two committed sources beside this script (GH #914)."""
    updated_ids = {'2018-19': {}, '2021-22': dict(sorted(links.items()))}
    recursive_D = RecursiveDict()
    for cur, prev in updated_ids['2021-22'].items():
        recursive_D[('2021-22', cur)] = ('2018-19', prev)
    with open(_HERE / 'panel_ids.json', 'w') as f:
        json.dump({','.join(k): ','.join(v) for k, v in recursive_D.data.items()}, f)
    with open(_HERE / 'updated_ids.json', 'w') as f:
        json.dump(updated_ids, f)


if __name__ == '__main__':
    links, baseline, current, counts = link(*_sources())
    check_single_pass(links, baseline, current)
    _require(counts == EXPECTED,
             f'the release no longer gives the counts the crosswalk was ruled on: {counts} '
             f'against {EXPECTED}; review the rule (_/CONTENTS.org) before updating EXPECTED')
    write(links)
    print(f"Benin 2021-22 -> 2018-19: {counts['linked']} linked; whole pre-loaded roster gone, "
          f"not linked: {counts['all_preloaded_left']}; PanelHH = Panel without a 2018-19 "
          f"household: {counts['panel_no_target']}; recurring keys flagged Non panel, not "
          f"linked: {counts['recurring_non_panel']}; 2021-22 households keeping their own id: "
          f"{counts['own_id']}")
