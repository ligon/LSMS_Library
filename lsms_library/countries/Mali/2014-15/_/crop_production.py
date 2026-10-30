"""Build crop_production (item-level harvest) for Mali EACI 2014-15.

Sources (post-harvest, passage 2 / cultivation, passage 1):
  - EACIS3A_p2.dta   seasonal-crop harvest roster (one row per plot-crop)
  - EACIS3B_p2.dta   perennial-tree roster (one row per HH-tree; no field grid)
  - EACICULTURE_p1.dta (s1c) cultivation roster -> planting month + intercrop

Grain: (t, i, plot, crop).  plot = "{field}_{parcel}" (s3aq01_s3aq02) for
seasonal crops; <NA> for perennial trees.  Reported, item-level columns
only — Quantity / u / Quantity_sold / Unit_sold / Value_sold /
months_harvested / planting_month / harvest_month / intercropped /
perennial, plus Derivation on the tree rows.  No harvest_kg / yield / share
/ main_crop (those are transformations over these rows).

Variable map (Stata labels and the 2014-15 PH questionnaire, sections 3A/3B):
  field=s3aq01 parcel=s3aq02 crop=s3aq03b harvest_qty=s3aq08a unit=s3aq08b
  sold_y/n=s3aq22 sold_qty=s3aq23a SOLD UNIT=s3aq23b sold_value=s3aq24
  harvest_month=s3aq07b
  perennial: crop=s3bq01 still_prod=s3bq03 months_harvested=s3bq09
             harvest_qty PER HARVEST MONTH=s3bq10a unit=s3bq10b
             sold_qty PER HARVEST MONTH=s3bq13a SOLD UNIT=s3bq13b
             sold_value=s3bq14 (printed and in the manual: "montant total")
             (served: Quantity = s3bq10a x s3bq09, the 12-month harvest;
             Quantity_sold = s3bq13a and Value_sold = s3bq14 AS RECORDED)
  cultivation: field=s1cq01 parcel=s1cq02 crop=s1cq03 plant_month=s1cq11b
             intercrop=s1cq05 (Pure / Association de cultures)

The sold unit (GH #824).  3A.23 asks the quantity sold "en UML de la colonne
suivante" -- its own unit, s3aq23b, on the same 11-label set as the harvest
unit s3aq08b -- and 3B.13 likewise (s3bq13b).  Both are carried as
``Unit_sold`` (canonicalised through the u table in crop_production_finalize),
NA wherever s3aq22 == 'Non', where the sale is filled with 0.

The tree rows (GH #1267).  Until 2026-10-23 they served Quantity = s3bq09,
the NUMBER OF HARVEST MONTHS.  That was copied from the WB MLI_EACI1.do
("qty=s3bq09 unit=s3bq10b"), which itself computes
``harvest_kg_per = s3bq09 * s3bq10b`` -- months times the unit CODE.  3B DOES
record the harvest: s3bq10a, "Durant ces mois, quelle quantite a ete recoltee
en moyenne chaque mois?", and the sale s3bq13a likewise per month, while the
3B.14 value s3bq14 is printed "montant total de cette vente", and the
enumerator manual (WB catalog 2583, PDF p. 68) asks the enumerator to
"determiner le montant total de la vente"; the price levels put it on the
same footing as the per-month quantity.  Served (rulings @ligon 2026-10-23
on PR #1272, the sale revised after the document search, following the
Ethiopia #1257 precedent): Quantity = s3bq10a x s3bq09, the 12-month harvest
(mali.DERIVATION_TREES, every tree row); Quantity_sold = s3bq13a and
Value_sold = s3bq14 AS RECORDED, not multiplied by the month count -- the key
mali.DERIVATION_TREE_SALES, on tree rows with a positive sale, names the
unsettled window.  The raw answers come back from
Country('Mali').derivation_inputs(key).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from mali import (i as mali_i, crop_production_finalize, producing_trees,
                  tree_crop_rows, stamp_tree_derivations)

WAVE = '2014-15'


def _hhid(df):
    return df.apply(lambda r: mali_i(pd.Series([r['grappe'], r['menage']])), axis=1)


def _plot(df, fcol, pcol):
    f = df[fcol].astype('Int64').astype('string')
    p = df[pcol].astype('Int64').astype('string')
    return (f + '_' + p).where(f.notna() & p.notna(), pd.NA)


# --- seasonal harvest (s3a) ---
s3a = get_dataframe('../Data/EACIS3A_p2.dta').copy()
s3a['i'] = _hhid(s3a)
seasonal = pd.DataFrame({
    't': WAVE,
    'i': s3a['i'],
    'plot_id': _plot(s3a, 's3aq01', 's3aq02'),
    'crop': s3a['s3aq03b'],
    'u': s3a['s3aq08b'],
    'Quantity': s3a['s3aq08a'],
    'Quantity_sold': s3a['s3aq23a'],
    'Unit_sold': s3a['s3aq23b'],
    # Preserve the baseline recorded field value. Additional value masks
    # are a separate decision, outside this window correction (task ledger).
    'Value_sold': s3a['s3aq24'],
    'harvest_month': s3a['s3aq07b'],
    'planting_month': pd.NA,
    'intercropped': pd.NA,
    'perennial': False,
})
# s3aq22 == 'Non' -> household reports not having sold this crop: 0 sold.
not_sold = s3a['s3aq22'].astype('string').str.strip().eq('Non')
seasonal.loc[not_sold, ['Quantity_sold', 'Value_sold']] = 0
# ...and a 0 filled in has no unit: Unit_sold stays NA there (the raw s3aq23b
# is already NA on every one of those rows; stated, not assumed).
seasonal.loc[not_sold, 'Unit_sold'] = pd.NA

# --- planting month + intercrop from cultivation roster (s1c) ---
s1c = get_dataframe('../Data/EACICULTURE_p1.dta').copy()
s1c['i'] = _hhid(s1c)
cult = pd.DataFrame({
    'i': s1c['i'],
    'plot_id': _plot(s1c, 's1cq01', 's1cq02'),
    'crop': s1c['s1cq03'],
    'planting_month': s1c['s1cq11b'],
    'intercropped': s1c['s1cq05'].astype('string').str.strip()
                       .map({'Pure': False, 'Association de cultures': True}),
})
cult = cult.dropna(subset=['plot_id', 'crop'])
cult = cult.drop_duplicates(subset=['i', 'plot_id', 'crop'], keep='first')

seasonal = seasonal.drop(columns=['planting_month', 'intercropped']).merge(
    cult, on=['i', 'plot_id', 'crop'], how='left')

# --- perennial trees (s3b): no field grid -> plot = <NA>, perennial=True ---
# s3b is a FIXED tree roster: every HH gets a row per possible species, with
# s3bq03 == 'Non' for trees it does not have/produce (3416 placeholder rows
# with no harvest).  Keep only s3bq03 == 'Oui' (currently producing) — the
# 358 rows that carry an actual harvest, matching WB `drop if s3bq03==2`.
# The measured columns come from mali.tree_crop_rows, which calls the
# registered derivations mali.derive_tree_twelve_month_total and
# mali.derive_tree_sale_window -- so the registry and the build cannot
# drift: Quantity = s3bq10a x s3bq09 (the per-month harvest times months
# harvested; missing codes masked before the product), Quantity_sold =
# s3bq13a and Value_sold = s3bq14 as recorded (missing codes masked),
# months_harvested = s3bq09.
s3b = producing_trees(get_dataframe('../Data/EACIS3B_p2.dta').copy(), WAVE)
s3b['i'] = _hhid(s3b)
perennial = pd.DataFrame({
    't': WAVE,
    'i': s3b['i'],
    'plot_id': pd.NA,
    'crop': s3b['s3bq01'],
    'harvest_month': pd.NA,
    'planting_month': pd.NA,
    'intercropped': pd.NA,
    'perennial': True,
}).join(tree_crop_rows(s3b, WAVE))

df = pd.concat([seasonal, perennial], ignore_index=True)
df = crop_production_finalize(df)
# Stamp AFTER the collapse (CLAUDE.md "Derived Values", registering step 3):
# the harvest key on every tree row, the sale-window key beside it on a tree
# row with a positive sale.
df['Derivation'] = stamp_tree_derivations(df)

assert len(df) > 0, "crop_production 2014-15 produced no rows"
assert df.index.is_unique, "Non-unique (t, i, plot, crop) in crop_production 2014-15"

to_parquet(df, 'crop_production.parquet')
