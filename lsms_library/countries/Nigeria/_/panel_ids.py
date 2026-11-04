#!/usr/bin/env python3
"""Build Nigeria ``panel_ids.json`` + ``updated_ids.json`` (GH #1264, design A).

The crosswalk is a COMMITTED SOURCE (GH #914, #894).  Regenerate it with
``make panel-ids`` from this directory and review the diff of the two JSON
files; nothing on the build path runs this script.

What a link asserts (ruling of 2026-11-03, GH #1264)
----------------------------------------------------
``(t, i) -> (t0, i)`` says: household ``i`` was INTERVIEWED in round ``t`` and
in round ``t0``, the round before it.

* (a) All ten rounds are nodes.  ``old_new`` (W4, W5), never a bare ``hhid``
  match, decides which sample a household belongs to: the W4 refresh draw's
  hhid range overlaps the original sample's, though its ids do not.
* (b) Interviewed, as each round records it (below).  Both ends must qualify.
* (c) Each round's links come from that round's own cover.

This changes the crosswalk only.  Who is served in any table is GH #1444's
question: a household with a roster and no recorded interview (320201, 30082
in 2016Q1) stays served and is simply not linked.

The chain
---------
``2010Q3 <- 2011Q1 <- 2012Q3 <- 2013Q1 <- 2015Q3 <- 2016Q1 <- 2018Q3 <- 2019Q1
<- 2023Q3 <- 2024Q1``: a post-harvest round to the post-planting round of the
same visit, a post-planting round to the post-harvest round of the wave before.
Every round is its own node because every round is its own ``t``
(``nigeria.wave_folder_map``).  It is ONE chain because that is what
``local_tools.panel_ids`` builds (each wave to the wave sorted before it), what
``diagnostics._check_panel_attrition_monotonic`` walks (one predecessor per
wave) and what ``sampling.yml`` declares its boundaries over.  So a household
that skips a round has no link across the gap; its ``i`` is the same on both
sides, so it is still followed by ``i``.

``hhid`` is stable across waves, so every link is an identity: every
``updated_ids`` map is the identity, ``id_walk`` renames nothing, and neither
served ``i`` nor any household's ``v`` depends on this file.  The first round
has nothing to point at, so ``updated_ids['2010Q3']`` is empty, the convention
for a baseline.

"Interviewed", per file
-----------------------
========  ==================================  =================================
rounds    source                              interviewed
========  ==================================  =================================
W1        own cover, ``saq22a`` / ``sq1``     completed, partially completed
W2, W3    ``HHTrack``, ``hhstatus_w{2,3}v*``  1 interviewed, 5 tracked (and
                                              interviewed)
W4, W5    own cover, ``interview_result``     1 complete, 2 and 3 partially
                                              complete
========  ==================================  =================================

* ``HHTrack`` code 91, unlabelled in W2's CSV, is labelled by W3's ``.dta``:
  "91. Not interviewed for unknown reason".  The two files agree on every W1
  and W2 status cell.
* A partial interview is an interview.  The W5 interviewer manual (Section R)
  defines both partial codes as "started an interview", and on the W1--W3
  covers every "partially completed" household is ``HHTrack`` 1 or 5.
* W1 is not in a W1 ``HHTrack`` (there is none), but W2's and W3's files do
  carry ``hhstatus_w1v*``, and they name exactly the same households as the
  own-cover reading used here.
* A status value outside these tables is refused rather than guessed at.

Why this is not YAML
--------------------
Until GH #1264 this script read ``panel_ids:`` blocks in ``data_info.yml``.  A
block reads one file and the library stamps it with every round of the
directory, so it can express neither (c) nor a gate on a second file (b).  The
blocks are gone rather than left to disagree with the JSON:
``LSMS_BUILD_BACKEND=make`` used to derive the old dicts from them.
"""
import json
import re
import sys
from pathlib import Path as _Path

import pandas as pd

from lsms_library.local_tools import format_id, get_dataframe, panel_ids

_HERE = _Path(__file__).resolve().parent
_COUNTRY_DIR = _HERE.parent

# ``nigeria.py`` is beside this script; import the round map and the covers the
# library uses rather than restating them.
sys.path.insert(0, str(_HERE))
from nigeria import PP_QUARTER, ROUND_COVERS, wave_folder_map  # noqa: E402

ROUNDS = list(wave_folder_map)

_HHTRACK = {'2012-13': 'HHTrack.csv', '2015-16': 'HHTrack.dta'}

_W1_RESULT = {'completed': True, 'partially completed': True,
              'refused': False, 'not at home': False, 'moved away': False,
              'other (specify)': False}
_HHTRACK_STATUS = {1: True, 5: True,
                   2: False, 3: False, 4: False, 6: False, 7: False, 91: False}
_INTERVIEW_RESULT = {1: True, 2: True, 3: True,
                     4: False, 5: False, 6: False, 7: False, 8: False}

_OLD_NEW = {1: 'old', 2: 'new'}

# The sample a round may introduce.  Only the baseline and the W4 refresh draw
# add households; every other round revisits ones already seen.
_DRAWS = {'2010Q3': 'old', '2018Q3': 'new'}


def _code(value):
    """The numeric code of a status cell: ``1``, ``1.0`` or ``'1. COMPLETE'``."""
    match = re.match(r'\s*(\d+)', str(value))
    if match is None:
        raise ValueError(f"status value {value!r} carries no numeric code")
    return int(match.group(1))


def _label(value):
    return str(value).strip().lower()


def _classify(values, table, key, where):
    """``table[key(value)]`` for each value; an unlisted value is refused."""
    keys = values.astype(object).map(key)
    unknown = set(keys) - set(table)
    if unknown:
        raise ValueError(f"{where}: unclassified status {sorted(map(str, unknown))}")
    return keys.map(table).astype(bool)


def _by_hhid(src):
    out = src.assign(i=src['hhid'].apply(format_id)).set_index('i')
    if out.index.duplicated().any():
        raise ValueError(f"{int(out.index.duplicated().sum())} repeated hhid")
    return out


def round_table(rnd):
    """One row per household on ``rnd``'s own cover (index ``i``): whether it
    was ``interviewed`` and which ``sample`` it belongs to."""
    folder = wave_folder_map[rnd]
    visit = 'pp' if rnd == PP_QUARTER[folder] else 'ph'
    cover = _by_hhid(get_dataframe(_COUNTRY_DIR / folder / 'Data' / ROUND_COVERS[folder][visit]))

    if folder == '2010-11':
        column = 'saq22a' if visit == 'pp' else 'sq1'
        interviewed = _classify(cover[column], _W1_RESULT, _label, f'{rnd} {column}')
    elif folder in _HHTRACK:
        track = _by_hhid(get_dataframe(_COUNTRY_DIR / folder / 'Data' / _HHTRACK[folder]))
        column = f"hhstatus_w{'2' if folder == '2012-13' else '3'}v{'1' if visit == 'pp' else '2'}"
        interviewed = _classify(track[column].reindex(cover.index), _HHTRACK_STATUS,
                                _code, f'{rnd} {column}')
    else:
        interviewed = _classify(cover['interview_result'], _INTERVIEW_RESULT, _code,
                                f'{rnd} interview_result')

    if 'old_new' in cover:
        sample = cover['old_new'].astype(object).map(_code).map(_OLD_NEW)
        if sample.isna().any():
            raise ValueError(f"{rnd}: old_new outside {_OLD_NEW}")
    else:
        sample = pd.Series('old', index=cover.index)  # W1--W3 precede the refresh
    return pd.DataFrame({'interviewed': interviewed, 'sample': sample})


def _refuse_bare_hhid_matches(tables):
    """An hhid names one household only if its sample never changes and it first
    appears in a round that draws that sample.  A violation would make an
    identity link (and served ``i``) join two different households."""
    seen = pd.Series(dtype=object)
    for rnd in ROUNDS:
        sample = tables[rnd]['sample']
        known = sample.index.intersection(seen.index)
        changed = known[sample.loc[known] != seen.loc[known]]
        if len(changed):
            raise ValueError(f"{rnd}: {len(changed)} hhid(s) change sample, e.g. "
                             f"{sorted(changed)[:3]}")
        fresh = sample.loc[sample.index.difference(seen.index)]
        if len(fresh) and (fresh != _DRAWS.get(rnd)).any():
            raise ValueError(f"{rnd}: {int((fresh != _DRAWS.get(rnd)).sum())} hhid(s) "
                             f"appear in a round that does not draw their sample, "
                             f"e.g. {sorted(fresh[fresh != _DRAWS.get(rnd)].index)[:3]}")
        seen = pd.concat([seen, fresh])


def build_frame(tables=None):
    """The ``(t, i) -> previous_i`` frame that ``local_tools.panel_ids`` takes.

    One row per household on each round's cover.  ``previous_i`` is ``i`` where
    the link qualifies and missing otherwise; ``panel_ids`` drops a missing one
    from both dicts, and keeps the round's place in the chain.
    """
    if ROUNDS != sorted(ROUNDS):
        # local_tools.panel_ids chains by SORTED label.
        raise ValueError(f"round labels do not sort chronologically: {ROUNDS}")
    if tables is None:
        tables = {rnd: round_table(rnd) for rnd in ROUNDS}
    _refuse_bare_hhid_matches(tables)

    frames = []
    previous = None
    for rnd in ROUNDS:
        table = tables[rnd]
        if previous is None:
            link = pd.Series(False, index=table.index)
        else:
            before = tables[previous].reindex(table.index)
            link = (table['interviewed']
                    & before['interviewed'].eq(True)
                    & before['sample'].eq(table['sample']))
        frames.append(pd.DataFrame({
            't': rnd,
            'i': table.index,
            'previous_i': pd.Series(table.index, index=table.index, dtype=object).where(link),
        }).set_index(['t', 'i']))
        previous = rnd
    return pd.concat(frames)


def main():
    frame = build_frame()
    D, updated_ids = panel_ids(frame)

    # GH #914: write beside THIS script, never into the process's cwd.
    with open(_HERE / 'panel_ids.json', 'w') as f:
        json.dump({','.join(k): ','.join(v) for k, v in D.data.items()}, f)
    with open(_HERE / 'updated_ids.json', 'w') as f:
        json.dump(updated_ids, f)

    print(f"Nigeria panel_ids: {len(D.data)} entries; updated_ids: "
          + ', '.join(f"{w} {len(m)}" for w, m in updated_ids.items()))


if __name__ == '__main__':
    main()
