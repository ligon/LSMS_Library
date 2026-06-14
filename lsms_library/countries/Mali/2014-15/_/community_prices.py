"""Build community_prices (item-level cluster food prices) for Mali EACI 2014-15.

GAP C (parity loop).  One row per (t, v, j, u): the REPORTED price the EACI
community price questionnaire records for a food item in a cluster (grappe).

Sources (community questionnaire, section 04 "Prix au marché", rec_type 8):
  - EACIS04_p1.dta   passage 1 (post-planting) community prices
  - EACIS04_p2.dta   passage 2 (post-harvest)  community prices

Both files are grappe-level (no menage) — the community instrument, not a
household one.  Variable map (identical across both passages):
  item  = s04q01  (broad food label: Riz, Maïs, Oignon frais, ...)
  form  = s04q02  (sale-form variety; not emitted — used only by the
                   finalize selection rule when varieties share (j, u))
  unit  = s04q09  (consolidated unit label: Kilogramme, Sac moyen (50 kg), ...)
  qty   = s04q10  (the native quantity the price refers to; 999 = loose/NA)
  price = s04q11  (the surveyed price, FCFA, for that quantity/unit lot)

Grain: (t, v, j, u).  v = grappe (sample().v keyspace, so the price joins
households); j = harmonize_food Preferred Label (REUSES the consumed-food
label so community_prices.j joins food_acquired.j / crop_production.j);
u = u-table Preferred Label.  CLUSTER-level: there is NO household i, so v is
native and the framework's _join_v_from_sample does not fire.

Reported columns only: Price, Quantity.  No median / mean / index — those are
transformations.py rollups over these rows.  community_prices_finalize maps
the labels, coerces the 999 sentinel, and selects one reported price per
(t, v, j, u) (post-harvest passage first, then questionnaire order).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from mali import community_prices_finalize

WAVE = '2014-15'

# Both passages share the same column layout; s04q09/q10/q11 is the
# consolidated unit / quantity / price reading.
_SRC = {1: '../Data/EACIS04_p1.dta', 2: '../Data/EACIS04_p2.dta'}

pieces = []
for passage, fn in _SRC.items():
    src = get_dataframe(fn).copy()
    pieces.append(pd.DataFrame({
        't': WAVE,
        'v': src['grappe'].astype('Int64').astype('string'),
        'j': src['s04q01'].astype('string').str.strip(),
        'u': src['s04q09'].astype('string').str.strip(),
        'Price': pd.to_numeric(src['s04q11'], errors='coerce'),
        'Quantity': pd.to_numeric(src['s04q10'], errors='coerce'),
        'passage': passage,
    }))

raw = pd.concat(pieces, ignore_index=True)
df = community_prices_finalize(raw)

assert len(df) > 0, "community_prices 2014-15 produced no rows"
assert df.index.is_unique, "Non-unique (t, v, j, u) in community_prices 2014-15"

to_parquet(df, 'community_prices.parquet')
