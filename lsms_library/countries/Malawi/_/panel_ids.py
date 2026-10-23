#!/usr/bin/env python3
"""Build Malawi ``panel_ids.json`` + ``updated_ids.json`` (GH #894, design A).

The crosswalk is a COMMITTED SOURCE (GH #914, #894).  Regenerate it with
``make panel-ids`` from this directory and review the diff of the two JSON
files; nothing on the build path runs this script.

Where the wiring lives
----------------------
Each panel wave's ``data_info.yml`` carries a ``panel_ids:`` block naming the
cover file, the current-wave id (``idxvars: i``) and the previous-wave id
(``myvars: previous_i``).  Until GH #894 the library read those blocks at run
time ("design B") and cached the result as a parquet only.  This script reads
the SAME blocks -- it never restates a file or a column name -- so the YAML
stays the one place the wiring is written, and the committed JSON is the
reviewable result of it.  ``LSMS_BUILD_BACKEND=make`` still reads the blocks
directly, and gets the same dicts.

The frame handed to ``local_tools.panel_ids`` is built exactly as the YAML
path built it: the current id through ``format_id`` (what ``df_data_grabber``
applies to every ``idxvars`` entry), the previous id as a string, one ``t``
per wave, rows in source-file order.  Row order is not cosmetic:
``local_tools.update_id`` gives the bare id to the FIRST household that claims
a previous household and ``_1``, ``_2``, ... to the rest, so reordering rows
would move suffixes between real households.

Malawi's idiosyncrasies
-----------------------
* Two programs, five waves.  IHS2 (2004-05) is not a panel baseline: "The
  IHPS sample does NOT have any links to the IHS2 sample. The IHS3 serves as
  a baseline ONLY for the panel subsample." (``2016-17/Documentation/
  ihps_2016_bid_updated_final.pdf`` p. 4, fn. 1, quoted in CONTENTS.org).
  Its ``panel_ids:`` block is commented out, and so is 2010-11's: IHS3's
  ``case_id`` is the canonical id that every chain ends on.
* 2013-14 (IHPS, catalog 2248) is panel-only: ``y2_hhid -> case_id``.
* 2016-17 and 2019-20 each hold TWO catalog entries in one wave directory --
  the IHS4/IHS5 cross-section (``Data/Cross_Sectional/``, 2936 / 3818) and the
  IHPS 2016/2019 panel (``Data/Panel/``, 2939 / 3819).  Only the panel half
  links back (``y3_hhid -> y2_hhid``, ``y4_hhid -> y3_hhid``); cross-sectional
  households (``cs-17-``-prefixed in 2016-17 tables) are never in the
  crosswalk and keep their own ids.
* IHPS tracks split-off households, so several current households can claim
  one previous household.  ``update_id`` keeps them distinct with ``_N``
  suffixes (GH #504 guard against minting a live id).  Measured on the
  development build this replaces (9a0ed5f3c): 896 / 996 / 1,726 suffixed ids
  in the 2013-14 / 2016-17 / 2019-20 maps.
* Only the waves present in the frame appear in the output.  2013-14 is the
  first of them, so its link to 2010-11 lives in ``updated_ids['2013-14']``
  (4,000 entries) and in no ``panel_ids`` key; ``panel_ids`` holds the 2,508
  2016-17 and 3,178 2019-20 entries (5,686).  2004-05 and 2010-11 have no
  ``updated_ids`` entry at all, which ``id_walk`` treats as "leave alone".
"""
import json
from pathlib import Path as _Path

import pandas as pd
import yaml

from lsms_library.local_tools import format_id, get_dataframe, panel_ids

_HERE = _Path(__file__).resolve().parent
_COUNTRY_DIR = _HERE.parent


def _wiring(wave):
    """The wave's ``panel_ids:`` block as ``(file, i_var, previous_i_var)``,
    or ``None`` when the wave declares none.  Refuses a block shape this
    script does not reproduce (a list of files, a formatting function, a
    ``dfs:`` merge) rather than guessing at it."""
    info = _COUNTRY_DIR / wave / '_' / 'data_info.yml'
    if not info.is_file():
        return None
    with open(info, encoding='utf-8') as f:
        block = (yaml.safe_load(f) or {}).get('panel_ids')
    if block is None:
        return None
    file = block.get('file')
    i_var = (block.get('idxvars') or {}).get('i')
    prev_var = (block.get('myvars') or {}).get('previous_i')
    if (set(block) != {'file', 'idxvars', 'myvars'}
            or set(block['idxvars']) != {'i'}
            or set(block['myvars']) != {'previous_i'}
            or not all(isinstance(x, str) for x in (file, i_var, prev_var))):
        raise ValueError(
            f"{info}: panel_ids block {block!r} is not the simple "
            f"file/i/previous_i form this script reproduces; extend the script")
    return file, i_var, prev_var


def panel_waves():
    """Wave directories whose data_info.yml declares ``panel_ids``, in order."""
    waves = sorted(d.name for d in _COUNTRY_DIR.iterdir()
                   if d.is_dir() and (d / '_' / 'data_info.yml').is_file())
    return [w for w in waves if _wiring(w) is not None]


def build_frame():
    """The ``(t, i) -> previous_i`` frame that ``local_tools.panel_ids`` takes."""
    frames = []
    for wave in panel_waves():
        file, i_var, prev_var = _wiring(wave)
        src = get_dataframe(_COUNTRY_DIR / wave / 'Data' / file)
        df = pd.DataFrame({
            't': wave,
            'i': src[i_var].apply(format_id),
            'previous_i': src[prev_var].apply(format_id),
        })
        frames.append(df.set_index(['t', 'i']))
    frame = pd.concat(frames)
    # A repeated (t, i) would be collapsed by the library's index
    # normalisation but silently last-wins inside panel_ids(); refuse it.
    dup = frame.index.duplicated()
    if dup.any():
        raise ValueError(f"{int(dup.sum())} repeated (t, i) in the panel frame")
    return frame


def main():
    frame = build_frame()
    D, updated_ids = panel_ids(frame)

    # GH #914: write beside THIS script, never into the process's cwd.
    with open(_HERE / 'panel_ids.json', 'w') as f:
        json.dump({','.join(k): ','.join(v) for k, v in D.data.items()}, f)
    with open(_HERE / 'updated_ids.json', 'w') as f:
        json.dump(updated_ids, f)

    print(f"Malawi panel_ids: {len(D.data)} entries; updated_ids: "
          + ', '.join(f"{w} {len(m)}" for w, m in updated_ids.items()))


if __name__ == '__main__':
    main()
