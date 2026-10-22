"""Build community_prices (item-level cluster food prices) for Mali EACI 2014-15.

GAP C (parity loop).  One row per (t, v, j, u, variety, visit, obs): the REPORTED
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
  obs     = reading 1 / 2 / 3 on the same community price form
  unit / weight / price = s04q03/04/05, s04q06/07/08, s04q09/10/11
  weight  = lot weight in kg; Price is FCFA for that lot (CONTENTS.org)

Grain: (t, v, j, u, variety, visit, obs).  v = grappe (sample().v keyspace, so the
price joins households); j = harmonize_food Preferred Label (REUSES the
consumed-food label so community_prices.j joins food_acquired.j /
crop_production.j); u = u-table Preferred Label.  CLUSTER-level: there is NO
household i, so v is native and the framework's _join_v_from_sample does not
fire.

Reported columns only: Price, Quantity, Description.  No median / mean /
index — those are transformations.py rollups over these rows — and, since
GH #840, no SELECTION either: every priced (cluster, variety, passage) row is
served.  GH #822 restores all three readings without changing eligibility.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from mali import community_prices_finalize

WAVE = '2014-15'

# Reuses the reported triples and native labels (ledger #822, sections 2-4).
_SRC = {1: '../Data/EACIS04_p1.dta', 2: '../Data/EACIS04_p2.dta'}
_READINGS = [('s04q03', 's04q04', 's04q05'),
             ('s04q06', 's04q07', 's04q08'),
             ('s04q09', 's04q10', 's04q11')]

# Passage 2 omits value labels on q03/q06. This COMMUNITY dictionary is
# identical on all three passage-1 unit fields and passage-2 q09. Restore
# only those missing labels; unlisted/fractional codes stay unresolved.
_COMMUNITY_UNIT_LABELS = {
    1: 'Kilogramme', 2: 'Gramme', 3: 'Litre', 4: 'Centilitre',
    5: 'Unit\u00e9', 6: 'Sachet', 7: 'Paquet', 8: 'Bo\u00eete',
    9: 'Sac large (100 kg)', 10: 'Sac moyen (50 kg)',
    11: 'Sac petit (25 kg)', 12: 'Verre', 13: 'Bidon', 14: 'Tas',
    99: 'Manquant',
}

pieces = []
for passage, fn in _SRC.items():
    src = get_dataframe(fn).copy()
    # The variety CODE (int16 in the .dta) -- stable across the two passages,
    # whose value labels differ in their quote characters.
    codes = get_dataframe(fn, convert_categoricals=False)
    for obs, (unit, weight, price) in enumerate(_READINGS, 1):
        units = src[unit].astype('string').str.strip()
        if passage == 2 and obs in (1, 2):
            decoded = pd.to_numeric(codes[unit], errors='coerce').map(
                _COMMUNITY_UNIT_LABELS).astype('string')
            units = decoded.where(decoded.notna(), units)
        pieces.append(pd.DataFrame({
            't': WAVE,
            'v': src['grappe'].astype('Int64').astype('string'),
            'j': src['s04q01'].astype('string').str.strip(),
            'u': units,
            'variety': pd.to_numeric(codes['s04q02']).astype('Int64').astype('string'),
            'visit': passage,
            'obs': obs,
            'Description': src['s04q02'].astype('string').str.strip(),
            'Price': pd.to_numeric(src[price], errors='coerce'),
            'Quantity': pd.to_numeric(src[weight], errors='coerce'),
        }))

raw = pd.concat(pieces, ignore_index=True)
df = community_prices_finalize(raw)

assert len(df) > 0, "community_prices 2014-15 produced no rows"
assert df.index.is_unique, \
    "Non-unique (t, v, j, u, variety, visit, obs) in community_prices 2014-15"

to_parquet(df, 'community_prices.parquet')
