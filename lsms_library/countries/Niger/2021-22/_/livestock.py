"""Build livestock for Niger EHCVM 2021-22 (GAP 4, item-level).

Single source file: s17_me_ner2021.dta — the EHCVM section-17 livestock
('Élevage') roster, one row per (household, species).  Same instrument as
2018-19 EXCEPT the species code lives in s17q01 (s17q02 is absent in
2021-22); the 1-11 value scheme is identical (elevage__id).  The roster is
already restricted to owned species (s17q03 == 1 for every row).

Columns:
  s17q01  species code (1-11; harmonize_species_ehcvm -> animal)
  s17q03  owned/raised this species? (1=Oui / 2=Non) — gate (all ==1 here)
  s17q06  number belonging to the household (HeadCount owned now)
  s17q08  number bought in the last 12 months (HeadAcquired)
  s17q10  number sold on the hoof in the last 12 months (HeadSold)

No current herd-value question (see 2018-19), so Value is not emitted.  i
is the EHCVM composite id ('E_' + grappe + '0' + zero-padded menage) via
niger.i, matching sample().  Grain (t, i, animal); no v level (livestock
is in the framework _no_v_join set).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from niger import i as niger_i, _species_maps, _map_codes, _finish_livestock


srcn = get_dataframe('../Data/s17_me_ner2021.dta', convert_categoricals=False)

_, ehcvm_map = _species_maps()

# The roster only carries owned species, but keep the gate for parity / safety.
owned = srcn['s17q03'] == 1
srcn = srcn[owned.values]

hh = srcn.apply(lambda r: niger_i(pd.Series([r['grappe'], r['menage']],
                                            index=['grappe', 'menage'])), axis=1)

df = pd.DataFrame({
    'i':            hh.values,
    'animal':       _map_codes(srcn['s17q01'], ehcvm_map).values,
    'HeadCount':    pd.to_numeric(srcn['s17q06'], errors='coerce').values,
    'HeadAcquired': pd.to_numeric(srcn['s17q08'], errors='coerce').values,
    'HeadSold':     pd.to_numeric(srcn['s17q10'], errors='coerce').values,
})

df = _finish_livestock(df, '2021-22')

assert len(df) > 0, 'livestock 2021-22 produced no rows'
to_parquet(df, 'livestock.parquet')
