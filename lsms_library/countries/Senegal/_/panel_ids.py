#!/usr/bin/env python3
"""Build Senegal panel_ids + updated_ids for the EHCVM panel.

Senegal has two EHCVM waves: 2018-19 (baseline) and 2021-22. The
2021-22 cover sheet (``s00_me_sen2021.dta``) carries a ``PanelHH``
flag with two values — ``"Menage Panel"`` (6127 rows) and
``"Nouveau Ménage"`` (993 rows). Unlike Niger, Senegal's cover
sheet does NOT carry explicit ``previous_grappe`` / ``previous_menage``
columns (no ``s00q07f1`` / ``s00q07f2``). Empirically, every panel
household's ``(grappe, menage)`` in 2021-22 matches a ``(grappe,
menage)`` in 2018-19 exactly — i.e. the panel linkage is pure
identity at the ``(grappe, menage)`` level.

This script:

1. Loads both wave cover sheets.
2. Filters the 2021-22 cover to ``PanelHH == 'Menage Panel'``.
3. For each such household, constructs the canonical roster ID
   using the same composite-format convention as the rest of the
   Senegal library: ``f"{int(grappe)}{int(menage):03d}"``. E.g.
   ``(grappe=74, menage=12)`` → ``'74012'``.
4. Cross-references against the 2018-19 baseline to retain only
   linkages where the previous-wave ID actually exists in the
   baseline cover.
5. Writes ``panel_ids.json`` and ``updated_ids.json``.

Output conventions match Niger's bespoke script
(``lsms_library/countries/Niger/_/panel_ids.py``):

- ``updated_ids`` has an empty dict for the baseline wave and a
  ``{cur_i: prev_i}`` dict for the follow-up wave (identity in
  Senegal's case because the ID construction is stable).
- ``panel_ids.json`` encodes the RecursiveDict as
  ``"wave,id" -> "prev_wave,prev_id"`` strings.
"""

import json
import sys

import pandas as pd

sys.path.append('../../_')
from lsms_library.local_tools import RecursiveDict, get_dataframe  # noqa: E402


def _senegal_i(grappe, menage) -> str | None:
    """Return the canonical Senegal household ID for a ``(grappe, menage)``.

    Uses the same composite format that ``df_data_grabber`` produces
    via the ``idxvars: i: [grappe, menage]`` declaration in the
    wave-level ``data_info.yml`` files, verified empirically against
    ``Country('Senegal').household_roster()`` output.
    """
    if pd.isna(grappe) or pd.isna(menage):
        return None
    return f"{int(grappe)}{int(menage):03d}"


# -----------------------------------------------------------------------
# EHCVM panel: 2018-19 ↔ 2021-22
# -----------------------------------------------------------------------

# 2018-19 baseline cover — defines which IDs exist in the baseline roster.
cover_18 = get_dataframe('../2018-19/Data/s00_me_sen2018.dta')
baseline_ids: set[str] = set()
for _, row in cover_18[['grappe', 'menage']].dropna().drop_duplicates().iterrows():
    i_value = _senegal_i(row['grappe'], row['menage'])
    if i_value is not None:
        baseline_ids.add(i_value)

# 2021-22 cover with the PanelHH flag.
cover_21 = get_dataframe('../2021-22/Data/s00_me_sen2021.dta')
panel_mask = cover_21['PanelHH'].astype(str).str.strip() == 'Menage Panel'
panel_21 = cover_21[panel_mask]

ehcvm_21_to_18: dict[str, str] = {}
for _, row in panel_21.iterrows():
    cur_i = _senegal_i(row['grappe'], row['menage'])
    if cur_i is None:
        continue
    # Senegal's panel is identity at (grappe, menage) — cur_i == prev_i
    # as long as the same combo exists in the 2018-19 baseline.
    if cur_i in baseline_ids:
        ehcvm_21_to_18[cur_i] = cur_i


# -----------------------------------------------------------------------
# Assemble outputs
# -----------------------------------------------------------------------

updated_ids = {
    '2018-19': {},                 # baseline — no rewrites
    '2021-22': ehcvm_21_to_18,     # identity for panel HHs
}

recursive_D: RecursiveDict = RecursiveDict()
for cur, prev in ehcvm_21_to_18.items():
    recursive_D[('2021-22', cur)] = ('2018-19', prev)


# -----------------------------------------------------------------------
# Write JSON
# -----------------------------------------------------------------------

with open('panel_ids.json', 'w') as f:
    json_ready = {','.join(k): ','.join(v) for k, v in recursive_D.data.items()}
    json.dump(json_ready, f)

with open('updated_ids.json', 'w') as f:
    json.dump(updated_ids, f)


print(f"EHCVM 2021-22 → 2018-19: {len(ehcvm_21_to_18)} households linked")
