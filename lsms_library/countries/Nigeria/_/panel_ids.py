#!/usr/bin/env python3
"""Build Nigeria ``panel_ids.json`` + ``updated_ids.json`` (GH #894, design A).

The crosswalk is a COMMITTED SOURCE (GH #914, #894).  Regenerate it with
``make panel-ids`` from this directory and review the diff of the two JSON
files; nothing on the build path runs this script.

Where the wiring lives
----------------------
Each panel wave directory's ``data_info.yml`` carries a ``panel_ids:`` block
naming the cover file, the current-wave id (``idxvars: i``) and the
previous-wave id (``myvars: previous_i``).  Until GH #894 the library read
those blocks at run time ("design B") and cached the result as a parquet
only.  This script reads the SAME blocks -- it never restates a file or a
column name -- so the YAML stays the one place the wiring is written, and the
committed JSON is the reviewable result of it.  ``LSMS_BUILD_BACKEND=make``
still reads the blocks directly, and gets the same dicts.

The frame handed to ``local_tools.panel_ids`` is built exactly as the YAML
path built it: both ids through ``format_id`` (what ``df_data_grabber``
applies to every ``idxvars`` entry; for the previous id it gives the same
string as the declared ``previous_i: str`` cast the library applied, ``hhid``
being numeric in the source), rows in source-file order.

Nigeria's idiosyncrasies
------------------------
* POST-PLANTING / POST-HARVEST.  A wave DIRECTORY (``2012-13``) is two waves
  (``2012Q3`` post-planting, ``2013Q1`` post-harvest), mapped by
  ``nigeria.wave_folder_map`` -- the same object ``Country`` reads.  The
  library ran the directory's single ``panel_ids:`` block once per round and
  stamped each copy with that round's ``t``, so the frame holds the
  post-planting cover page (``secta_plantingw2`` / ``secta_plantingw3``) twice
  per directory, once as the PP round and once as the PH round.  This script
  does the same: one read per directory, one frame per round.
* GHS-Panel ``hhid`` is stable across waves, so the wiring is
  ``previous_i: hhid`` -- an identity.  Every ``updated_ids`` map is the
  identity (4,859 / 4,859 / 4,696 / 4,696 entries) and ``id_walk`` renames
  nothing.  ``panel_ids`` (14,251 entries) chains ``2013Q1 -> 2012Q3`` (the
  PH round to the PP round of the SAME visit), ``2015Q3 -> 2013Q1`` and
  ``2016Q1 -> 2015Q3``.
* Only ``2012-13`` and ``2015-16`` declare the block.  W1 (``2010Q3`` /
  ``2011Q1``), W4 (``2018Q3`` / ``2019Q1``, which adds a refreshment sample)
  and W5 (``2023Q3`` / ``2024Q1``) are absent from both dicts, and ``2012Q3``
  is the first wave in the frame.  Reproduced as found, not extended here.
* Both rounds' ids come from the PP cover page, which lists households that
  have no ``household_roster`` rows (113 / 128 / 85 / 115 of the
  2012Q3 / 2013Q1 / 2015Q3 / 2016Q1 ids; every one IS in ``sample()``).  So
  ``check_panel_consistency`` fails ``panel_ids_targets_exist`` with 328 of
  14,251 current-wave and 328 previous-wave ids absent from the roster.  And
  because the link is an identity, 99 of the 4,696 ``2015Q3 -> 2013Q1``
  entries point at an ``hhid`` that is not on the W2 cover at all.  Both
  measured on the development build this replaces (9a0ed5f3c); reproduced
  as found, not fixed here.
* The ``moved-{hhid}`` sentinel is a CLUSTER (``v``) convention for tracked
  households whose ``ea`` is 0; it never enters ``i``, and this crosswalk
  does not touch ``v``.
"""
import json
import sys
from pathlib import Path as _Path

import pandas as pd
import yaml

from lsms_library.local_tools import format_id, get_dataframe, panel_ids

_HERE = _Path(__file__).resolve().parent
_COUNTRY_DIR = _HERE.parent

# ``nigeria.py`` is beside this script; import the round map the library uses
# rather than restating it.
sys.path.insert(0, str(_HERE))
from nigeria import wave_folder_map  # noqa: E402


def _wiring(folder):
    """The directory's ``panel_ids:`` block as ``(file, i_var,
    previous_i_var)``, or ``None`` when it declares none.  Refuses a block
    shape this script does not reproduce (a list of files, a formatting
    function, a ``dfs:`` merge) rather than guessing at it."""
    info = _COUNTRY_DIR / folder / '_' / 'data_info.yml'
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


def panel_rounds():
    """``(round, folder)`` for every round whose folder declares
    ``panel_ids``, in ``wave_folder_map`` order (chronological)."""
    return [(rnd, folder) for rnd, folder in wave_folder_map.items()
            if _wiring(folder) is not None]


def build_frame():
    """The ``(t, i) -> previous_i`` frame that ``local_tools.panel_ids`` takes."""
    sources = {}
    frames = []
    for rnd, folder in panel_rounds():
        file, i_var, prev_var = _wiring(folder)
        if folder not in sources:
            sources[folder] = get_dataframe(_COUNTRY_DIR / folder / 'Data' / file)
        src = sources[folder]
        df = pd.DataFrame({
            't': rnd,
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

    print(f"Nigeria panel_ids: {len(D.data)} entries; updated_ids: "
          + ', '.join(f"{w} {len(m)}" for w, m in updated_ids.items()))


if __name__ == '__main__':
    main()
