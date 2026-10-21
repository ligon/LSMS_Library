"""Build community_prices (item-level cluster food prices) for Mali EACI 2014-15.

GAP C (parity loop).  One row per (t, v, j, u, variety, visit): the REPORTED
price the EACI community price questionnaire records for one sale-form variety
of a food item in a cluster (grappe), in one field passage.

Sources (community questionnaire, section 04 "Prix au marché", rec_type 8):
  - EACIS04_p1.dta   passage 1 (post-planting) community prices -> visit 1
  - EACIS04_p2.dta   passage 2 (post-harvest)  community prices -> visit 2

Both files are grappe-level (no menage) — the community instrument, not a
household one.  Variable map (identical across both passages):
  item    = s04q01  (broad food label: Riz, Maïs, Oignon frais, ...)
  variety = s04q02  (sale-form variety: "Riz importé vendu en sac", ...); the
                    CODE is the index level, the label is Description
  unit    = s04q09  (relevé-3 unit label: Kilogramme, Sac moyen (50 kg), ...)
  qty     = s04q10  (the relevé-3 lot weight in kg; 999 = loose/NA)
  price   = s04q11  (the relevé-3 surveyed price, FCFA, for that lot)

Grain: (t, v, j, u, variety, visit).  v = grappe (sample().v keyspace, so the
price joins households); j = harmonize_food Preferred Label (REUSES the
consumed-food label so community_prices.j joins food_acquired.j /
crop_production.j); u = u-table Preferred Label.  CLUSTER-level: there is NO
household i, so v is native and the framework's _join_v_from_sample does not
fire.

Reported columns only: Price, Quantity, Description.  No median / mean /
index — those are transformations.py rollups over these rows — and, since
GH #840, no SELECTION either: every priced (cluster, variety, passage) row is
served.  The relevé 1 / relevé 2 readings (s04q03-s04q08) are still not read;
see CONTENTS.org.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from mali import community_prices_finalize

WAVE = '2014-15'

# Both passages share the same column layout; s04q09/q10/q11 is the
# relevé-3 unit / weight / price reading.
_SRC = {1: '../Data/EACIS04_p1.dta', 2: '../Data/EACIS04_p2.dta'}

pieces = []
for passage, fn in _SRC.items():
    src = get_dataframe(fn).copy()
    # The variety CODE (int16 in the .dta) -- stable across the two passages,
    # whose value labels differ in their quote characters.
    codes = get_dataframe(fn, convert_categoricals=False)
    pieces.append(pd.DataFrame({
        't': WAVE,
        'v': src['grappe'].astype('Int64').astype('string'),
        'j': src['s04q01'].astype('string').str.strip(),
        'u': src['s04q09'].astype('string').str.strip(),
        'variety': pd.to_numeric(codes['s04q02']).astype('Int64').astype('string'),
        'visit': passage,
        'Description': src['s04q02'].astype('string').str.strip(),
        'Price': pd.to_numeric(src['s04q11'], errors='coerce'),
        'Quantity': pd.to_numeric(src['s04q10'], errors='coerce'),
    }))

raw = pd.concat(pieces, ignore_index=True)
df = community_prices_finalize(raw)

assert len(df) > 0, "community_prices 2014-15 produced no rows"
assert df.index.is_unique, \
    "Non-unique (t, v, j, u, variety, visit) in community_prices 2014-15"

to_parquet(df, 'community_prices.parquet')
