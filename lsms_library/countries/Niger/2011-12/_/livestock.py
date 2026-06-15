"""Build livestock for Niger ECVMA 2011-12 (GAP 4, item-level).

Single source file: ecvmaas4a_p2.dta — the livestock roster, one row per
(household, species) the household was ASKED about.  The WB .do code
(NER_ECVMA1.do:1103-1108) reads this same file, recodes as4aq05 to a y/n
binary and collapses it to one row per HH; we keep the pre-collapse rows.

Columns:
  as4aq04  species code (3-digit; harmonize_species_ecvma -> animal)
  as4aq05  owned/raised this species? (1=Oui / 2=Non) — the ownership gate
  as4aq11  number belonging to the household (HeadCount owned now)
  as4aq43  number bought in the last 12 months (HeadAcquired)
  as4aq51  number sold on the hoof in the last 12 months (HeadSold)

Only rows the household actually owns are reported owned-animal records, so
we keep as4aq05==1 (the WB binary is exactly max(as4aq05==1) over these).
``hid`` already equals grappe*100+menage (the canonical 2011-12 household
id), so i() just str()s it — matching crop_production / sample.  Grain
(t, i, animal); no v level (livestock is in the framework _no_v_join set).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from niger import i as niger_i, _species_maps, _map_codes, _finish_livestock


srcn = get_dataframe(
    '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas4a_p2.dta',
    convert_categoricals=False)

ecvma_map, _ = _species_maps()

# Keep only species the household actually owns (as4aq05 == 1 = Oui).  This is
# the WB ownership gate; the engaged-in-livestock binary = any such row per HH.
owned = srcn['as4aq05'] == 1
srcn = srcn[owned.values]

hh = srcn['hid'].apply(lambda x: niger_i(x) if pd.notna(x) else pd.NA)

df = pd.DataFrame({
    'i':            hh.values,
    'animal':       _map_codes(srcn['as4aq04'], ecvma_map).values,
    'HeadCount':    pd.to_numeric(srcn['as4aq11'], errors='coerce').values,
    'HeadAcquired': pd.to_numeric(srcn['as4aq43'], errors='coerce').values,
    'HeadSold':     pd.to_numeric(srcn['as4aq51'], errors='coerce').values,
})

df = _finish_livestock(df, '2011-12')

assert len(df) > 0, 'livestock 2011-12 produced no rows'
to_parquet(df, 'livestock.parquet')
