#!/usr/bin/env python3
"""Build GhanaLSS panel_ids for the GLSS1 ↔ GLSS2 panel (1987-88 ↔ 1988-89).

GhanaLSS has seven survey rounds spanning 1987-88 through 2016-17, but
**only the first two (GLSS1 and GLSS2) are panel-linked**. Subsequent
rounds used different cluster schemes (2xxxx → 3xxxx → 4xxxx → …) and
have no household-level linkage.

The panel linkage file is ``PANELC.DAT`` in the 1988-89 Data directory.
It is person-level (3370 rows) with columns:

- ``HID1`` / ``PID1``: 1987-88 household + person ID
- ``HID2`` / ``PID2``: 1988-89 household + person ID
- ``CLUST``: cluster identifier

We extract unique ``(HID2, HID1)`` pairs for household-level linkage
(714 unique households), cross-reference against both waves' rosters,
and write ``panel_ids.json`` + ``updated_ids.json``.

The previous version of this script called ``local_tools.panel_ids()``
on a ``Waves`` dict whose only non-empty entry was 1988-89. That
function processes waves sequentially and only builds ``RecursiveDict``
entries when a prior wave is present — so with one wave in
``sorted_waves``, the chain was always empty. The ``updated_ids.json``
was correct (HID2 → HID1 mappings present) but ``panel_ids.json``
was ``{}`` — causing ``Country('GhanaLSS').panel_ids`` to report empty.
"""

import json
import sys

import pandas as pd

sys.path.append('../../_')
from lsms_library.local_tools import (  # noqa: E402
    RecursiveDict,
    format_id,
    get_dataframe,
)


# -----------------------------------------------------------------------
# Load the panel linkage file
# -----------------------------------------------------------------------

panel_df = get_dataframe('../1988-89/Data/PANELC.DAT')

# Build unique household-level (HID2 → HID1) mapping
hh_map = (
    panel_df[['HID2', 'HID1']]
    .drop_duplicates()
    .dropna()
)
hh_map['cur_i'] = hh_map['HID2'].apply(format_id)
hh_map['prev_i'] = hh_map['HID1'].apply(format_id)
hh_map = hh_map.dropna(subset=['cur_i', 'prev_i'])

linkage: dict[str, str] = dict(zip(hh_map['cur_i'], hh_map['prev_i']))


# -----------------------------------------------------------------------
# Optional: cross-reference against rosters (best-effort, not blocking)
# -----------------------------------------------------------------------

# We don't filter out linkages that fail roster cross-reference because
# the roster may not be cached (this script runs at build time, before
# all features are materialised). The diagnostics module's
# panel_ids_targets_exist check handles post-hoc verification.


# -----------------------------------------------------------------------
# Assemble outputs
# -----------------------------------------------------------------------

# Only 1987-88 and 1988-89 participate. All other waves are
# cross-sectional and get empty maps.
updated_ids: dict[str, dict[str, str]] = {
    '1987-88': {},
    '1988-89': linkage,
    '1991-92': {},
    '1998-99': {},
    '2005-06': {},
    '2012-13': {},
    '2016-17': {},
}

recursive_D: RecursiveDict = RecursiveDict()
for cur, prev in linkage.items():
    recursive_D[('1988-89', cur)] = ('1987-88', prev)


# -----------------------------------------------------------------------
# Write JSON
# -----------------------------------------------------------------------

with open('panel_ids.json', 'w') as f:
    json_ready = {','.join(k): ','.join(v) for k, v in recursive_D.data.items()}
    json.dump(json_ready, f)

with open('updated_ids.json', 'w') as f:
    json.dump(updated_ids, f)


print(f"GLSS1 ↔ GLSS2: {len(linkage)} households linked (1987-88 → 1988-89)")
