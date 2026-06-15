#!/usr/bin/env python
"""Build item-level livestock for Nigeria GHS-Panel (GAP 4).

Natural grain (t, i, animal): one row per species/herd a household reports
owning, from the GHS-Panel livestock roster sect11i_planting{wN}.dta.  This
is the PRE-collapse roster the WB code reads then throws away down to a
single household engaged-in-livestock y/n binary (NGA_GHS1.do:992-998
recodes s11iq1 then `collapse (max) livestock, by(hhid)`).  We keep the
roster richer: per-animal head counts, acquisitions, sales, and the
reported per-head reservation value.

Stores REPORTED item-level fields ONLY -- HeadCount (owned/kept now),
HeadAcquired (bought to raise), HeadSold (sold alive), Value (reported
reservation value of ONE head).  NO TLU, NO herd-value total, NO
engaged-in-livestock binary (those are transformations over these rows;
their binary = groupby.any()).

`animal` (index) is the harmonize_species Preferred Label, resolved in
nigeria.livestock_for_wave from the wave's native animal code (101-123,
one stable scheme across all five waves).  No `v` level: livestock is in
the framework `_no_v_join` set, so the grain is exactly (t, i, animal).

Per-wave source structure (livestock roster lives in the post-planting
round; each wave maps to a single t = PP_QUARTER[wave]):

  W1 2010-11  sect11i_plantingw1   item_cd  ; file already filtered to
              (Post Planting Wave 1/Agriculture)  owned rows (own=1)
  W2 2012-13  sect11i_plantingw2   animal_cd; full roster grid, s11iq1 own
  W3 2015-16  sect11i_plantingw3   animal_cd; full roster grid, s11iq1 own
  W4 2018-19  sect11i_plantingw4   animal_cd; full grid; kept/owned split:
              HeadCount=s11iq2a (kept), acquire/sold/value unchanged
  W5 2023-24  sect11i_plantingw5   animal_cd; already owned rows; renumbered
              questions (HeadCount s11iq2, acquire s11iq17, sold s11iq23,
              value s11iq7)

Column-name map (HeadCount | HeadAcquired | HeadSold | Value):
  W1  s11iq2  | s11iq10 | s11iq16 | s11iq3
  W2  s11iq2  | s11iq10 | s11iq16 | s11iq3
  W3  s11iq2  | s11iq10 | s11iq16 | s11iq3
  W4  s11iq2a | s11iq10 | s11iq16 | s11iq3
  W5  s11iq2  | s11iq17 | s11iq23 | s11iq7
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import PP_QUARTER, _species_labels, livestock_for_wave

species_labels = _species_labels()

# (wave, file, animal_code, owned, HeadCount, HeadAcquired, HeadSold, Value)
SPECS = [
    ('2010-11',
     '../2010-11/Data/Post Planting Wave 1/Agriculture/sect11i_plantingw1.dta',
     'item_cd', 's11iq1', 's11iq2', 's11iq10', 's11iq16', 's11iq3'),
    ('2012-13',
     '../2012-13/Data/Post Planting Wave 2/Agriculture/sect11i_plantingw2.dta',
     'animal_cd', 's11iq1', 's11iq2', 's11iq10', 's11iq16', 's11iq3'),
    ('2015-16',
     '../2015-16/Data/sect11i_plantingw3.dta',
     'animal_cd', 's11iq1', 's11iq2', 's11iq10', 's11iq16', 's11iq3'),
    ('2018-19',
     '../2018-19/Data/sect11i_plantingw4.dta',
     'animal_cd', 's11iq1', 's11iq2a', 's11iq10', 's11iq16', 's11iq3'),
    ('2023-24',
     '../2023-24/Data/Post Planting Wave 5/Agriculture/sect11i_plantingw5.dta',
     'animal_cd', 's11iq1', 's11iq2', 's11iq17', 's11iq23', 's11iq7'),
]

pieces = []
for (wave, f, code, owned, head_now, head_acq, head_sold, value) in SPECS:
    t = PP_QUARTER[wave]
    raw = get_dataframe(f, convert_categoricals=False)
    pieces.append(livestock_for_wave(
        t, raw, animal_code=code, owned=owned, head_now=head_now,
        head_acquired=head_acq, head_sold=head_sold, value=value,
        species_labels=species_labels))

df = pd.concat(pieces, axis=0)
df = df.sort_index()

to_parquet(df, '../var/livestock.parquet')
