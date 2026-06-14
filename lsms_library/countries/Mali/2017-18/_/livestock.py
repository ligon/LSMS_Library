"""Build livestock (item-level animal roster) for Mali EACI 2017-18.

GAP 4 (parity loop).  One row per (t, i, animal) — the pre-collapse roster
the WB MLI_EACI2.do reads, recodes to a single engaged-in-livestock binary
(``s8aq04``, collapse-max per hhid), then discards.

Sources (passage 2 / post-harvest livestock module s8):
  - eaci17_s8ap2.dta   stock roster: one row per possible species per HH,
                       with s8aq04 == 'Oui' for species it keeps and
                       s8aq06 = number currently in the herd.  This file
                       carries the HEAD COUNT but no transactions.
  - eaci17_s8b1p2.dta  12-month flow roster (acquisitions / sales), keyed
                       on the SAME (grappe, exploitation, species) — the
                       acquired / sold / sale-value the WB code never reads.

i = (grappe, exploitation) — the 2017-18 household key (cf. crop_production
/ plot_features).  HeadCount from s8a; HeadAcquired / HeadSold / Value
joined from s8b1 on (grappe, exploitation, species_code).

Variable map traced from the s8a / s8b1 questionnaire labels:
  species code   = s8aq02 / s8b1q02  (110..910; harmonize_species Code)
  owns y/n       = s8aq04   ('Oui'/'Non') ["...a-t-il eleve espece... 12 mois?"]
  HeadCount      = s8aq06   "Nombre d'animaux ... actuellement dans le troupeau"
  HeadAcquired   = s8b1q10  "Nbre d'animaux ... achetes au cours des 12 ... mois"
  HeadSold       = s8b1q13  "Nbre ... appartenant au menage vendus ... 12 ... mois"
  Value (sales)  = s8b1q14  "Valeur brute de la vente de ces animaux ... fcfa"

The EACI roster carries NO current herd-value question, so Value is the
gross SALES value where reported (else NaN).  NO TLU, NO herd-value total,
NO engaged-in-livestock binary — those are transformations over these rows.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from mali import i as mali_i, livestock_finalize

WAVE = '2017-18'


def _hhid(df):
    return df.apply(lambda r: mali_i(pd.Series([r['grappe'], r['exploitation']])),
                    axis=1)


# --- stock roster (s8a): head count per (grappe, exploitation, species) ---
# convert_categoricals=False so the species code arrives as the integer
# s8aq02 (the harmonize_species join key) and the owns flag as 1/2.
s8a = get_dataframe('../Data/eaci17_s8ap2.dta', convert_categoricals=False).copy()
# Keep only species the household actually keeps (s8aq04 == 1 "Oui") — the
# WB engaged-in-livestock signal, at the species grain.
s8a = s8a[s8a['s8aq04'] == 1].copy()
s8a['animal_code'] = pd.to_numeric(s8a['s8aq02'], errors='coerce').astype('Int64')

# --- flow roster (s8b1): 12-month acquisitions / sales / sale value ---
s8b1 = get_dataframe('../Data/eaci17_s8b1p2.dta', convert_categoricals=False).copy()
s8b1['animal_code'] = pd.to_numeric(s8b1['s8b1q02'], errors='coerce').astype('Int64')
flow = s8b1[['grappe', 'exploitation', 'animal_code',
             's8b1q10', 's8b1q13', 's8b1q14']].rename(columns={
    's8b1q10': 'HeadAcquired',
    's8b1q13': 'HeadSold',
    's8b1q14': 'Value',
})
# Collapse the flow file to one row per (grappe, exploitation, species) in
# case a species appears more than once; sum the reported flows.
flow = flow.groupby(['grappe', 'exploitation', 'animal_code'],
                    as_index=False).agg(
    HeadAcquired=('HeadAcquired', lambda s: pd.to_numeric(s, errors='coerce').sum(min_count=1)),
    HeadSold=('HeadSold', lambda s: pd.to_numeric(s, errors='coerce').sum(min_count=1)),
    Value=('Value', lambda s: pd.to_numeric(s, errors='coerce').sum(min_count=1)),
)

merged = s8a.merge(flow, on=['grappe', 'exploitation', 'animal_code'], how='left')
merged['i'] = _hhid(merged)

df = pd.DataFrame({
    't': WAVE,
    'i': merged['i'],
    'animal': merged['animal_code'],     # numeric species code -> Preferred Label
    'HeadCount': merged['s8aq06'],
    'HeadAcquired': merged['HeadAcquired'],
    'HeadSold': merged['HeadSold'],
    'Value': merged['Value'],
})

df = livestock_finalize(df)

assert len(df) > 0, "livestock 2017-18 produced no rows"
assert df.index.is_unique, "Non-unique (t, i, animal) in livestock 2017-18"

to_parquet(df, 'livestock.parquet')
