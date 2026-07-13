"""Build livestock for Niger ECVMA 2014-15 (GAP 4, item-level).

Single source file: ECVMA2_AS4AP2.dta — the livestock roster, same layout
as 2011-12 with UPPERCASE columns.  The WB .do code (NER_ECVMA2.do:1170-
1177) reads this same file, recodes AS4AQ05 to a y/n binary and collapses
it to one row per HH; we keep the pre-collapse rows.

Columns:
  AS4AQ04  species code (3-digit; harmonize_species_ecvma -> animal)
  AS4AQ05  owned/raised this species? (1=Oui / 2=Non) — the ownership gate
  AS4AQ11  number belonging to the household (HeadCount owned now)
  AS4AQ43  number bought in the last 12 months (HeadAcquired)
  AS4AQ51  number sold on the hoof in the last 12 months (HeadSold)

i is built from (GRAPPE, MENAGE) via niger.i — matching this wave's
sample / household_roster idxvars, which omit EXTENSION (the roster file
HAS an EXTENSION column but the canonical 2014-15 id is grappe0menage).
Only owned rows (AS4AQ05==1) are kept; the WB binary = any such row per HH.
Grain (t, i, animal); no v level (livestock is in the framework
_no_v_join set).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from niger import i as niger_i, _species_maps, _map_codes, _finish_livestock


base = '../Data/NER_2014_ECVMA-II_v02_M_STATA8/'
srcn = get_dataframe(base + 'ECVMA2_AS4AP2.dta', convert_categoricals=False)

ecvma_map, _ = _species_maps()

# Keep only species the household actually owns (AS4AQ05 == 1 = Oui).
owned = srcn['AS4AQ05'] == 1
srcn = srcn[owned.values]

# i from the FULL ECVMA-II household key (GRAPPE, MENAGE, EXTENSION), matching
# sample/roster (GH #323).  EXTENSION is part of the key: 59 (GRAPPE, MENAGE)
# pairs host two distinct households.
hh = srcn.apply(lambda r: niger_i(pd.Series([r['GRAPPE'], r['MENAGE'], r['EXTENSION']],
                                            index=['GRAPPE', 'MENAGE', 'EXTENSION'])), axis=1)

df = pd.DataFrame({
    'i':            hh.values,
    'animal':       _map_codes(srcn['AS4AQ04'], ecvma_map).values,
    'HeadCount':    pd.to_numeric(srcn['AS4AQ11'], errors='coerce').values,
    'HeadAcquired': pd.to_numeric(srcn['AS4AQ43'], errors='coerce').values,
    'HeadSold':     pd.to_numeric(srcn['AS4AQ51'], errors='coerce').values,
})

df = _finish_livestock(df, '2014-15')

assert len(df) > 0, 'livestock 2014-15 produced no rows'
to_parquet(df, 'livestock.parquet')
