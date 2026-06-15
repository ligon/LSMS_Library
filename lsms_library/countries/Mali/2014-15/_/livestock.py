"""Build livestock (item-level animal roster) for Mali EACI 2014-15.

GAP 4 (parity loop).  One row per (t, i, animal) — the pre-collapse roster
the WB MLI_EACI1.do reads, recodes to a single engaged-in-livestock binary
(``s4aq03``, collapse-max per hhid), then discards.

Source: EACIS4A_p2.dta (passage 2 / post-harvest livestock module s4a).  It
is a FIXED roster: every household gets one row per possible species
(s4aq02 / s4aq01), with s4aq03 == 'Oui' for species it keeps.  We keep the
rows the household actually owns, carrying the reported item-level counts.

Variable map traced from MLI_EACI1.do + the s4a questionnaire labels:
  species code   = s4aq02   (110..910; harmonize_species Code)
  owns y/n       = s4aq03   ('Oui' / 'Non')   ["Le menage a eleve ou possede"]
  HeadCount      = s4aq04   "Nombre actuellement dans le troupeau"
  HeadAcquired   = s4aq15   "Nombre achete au cours des 12 derniers mois"
  HeadSold       = s4aq22   "Nombre vendu appartenant au menage"
  Value (sales)  = s4aq24   "Valeur brute des ventes" (FCFA)

The EACI roster carries NO current herd-value question, so Value is the
gross SALES value where reported (else NaN), matching the GAP-4 brief
("value where the source reports it; else NaN").  NO TLU, NO herd-value
total, NO engaged-in-livestock binary — those are transformations over
these rows.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from mali import i as mali_i, livestock_finalize

WAVE = '2014-15'


def _hhid(df):
    return df.apply(lambda r: mali_i(pd.Series([r['grappe'], r['menage']])), axis=1)


# Load with convert_categoricals=False so the species code arrives as the
# integer s4aq02 (the harmonize_species join key) and the owns flag as 1/2.
s4 = get_dataframe('../Data/EACIS4A_p2.dta', convert_categoricals=False).copy()
s4['i'] = _hhid(s4)

# Keep only rows the household actually keeps this species (s4aq03 == 1
# "Oui").  This is exactly the WB engaged-in-livestock signal, applied at
# the species grain instead of collapsed to a household binary.
owned = s4[s4['s4aq03'] == 1].copy()

df = pd.DataFrame({
    't': WAVE,
    'i': owned['i'],
    'animal': owned['s4aq02'],          # numeric species code -> Preferred Label
    'HeadCount': owned['s4aq04'],
    'HeadAcquired': owned['s4aq15'],
    'HeadSold': owned['s4aq22'],
    'Value': owned['s4aq24'],
})

df = livestock_finalize(df)

assert len(df) > 0, "livestock 2014-15 produced no rows"
assert df.index.is_unique, "Non-unique (t, i, animal) in livestock 2014-15"

to_parquet(df, 'livestock.parquet')
