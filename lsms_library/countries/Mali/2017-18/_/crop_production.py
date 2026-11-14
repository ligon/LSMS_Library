"""Build crop_production (item-level harvest) for Mali EACI 2017-18.

Sources (post-harvest passage 2; cultivation passage 1):
  - eaci17_s7fp2.dta   seasonal-crop harvest roster (one row per plot-crop)
  - eaci17_s7gp2.dta   crop-sales roster, keyed (grappe, exploitation, crop)
                       — HOUSEHOLD-crop grain, NOT plot-crop
  - eaci17_s11fp1.dta  perennial-tree roster (one row per HH-tree; no field grid)
  - eaci17_s11cp1.dta  cultivation roster -> planting month

Grain: (t, i, plot_id, crop), with `condition` the sentinel on every row (s7fq13b
is not read; GH #1436).  plot_id = "{field}_{parcel}" (s7fq01_s7fq02) for
seasonal crops; 'Farm-level' for perennial trees (GH #1476).  i = (grappe,
exploitation) —
the 2017-18 household key (cf. sample/cluster_features data_info.yml).

Reported, item-level columns only.  IMPORTANT: the 2017-18 sales module
(s7g) records sold quantity / value at the (household, crop) grain, NOT
plot-crop.  A SALE (a positive s7gq21a or s7gq22; 661 of 16,054 records) goes
on the household's single harvest row for the crop (461: the row carries the
key Mali::crop_production::hh-crop-sale-single-row) and otherwise, on a crop
spanning two or more plots (200), on ONE row with plot_id = 'Farm-level' and no
harvest Quantity (GH #1457, ruling on GH #843 option (d), the 2021-22 rule).  No
sale is split, summed or dropped.  A record that is not a sale keeps the single-
plot rule below: a filled-in 0 on a crop's one plot, and nothing elsewhere.

Variable map (Stata labels; WB MLI_EACI2.do for the field/parcel keys):
  field=s7fq01 parcel=s7fq02 crop=s7fq03 harvest_qty=s7fq13a unit=s7fq13c
  kg per ONE harvest unit=s7fq13d (served as KgFactor, below)
  fully_harvested=s7fq06 harvest_month=s7fq12b
  sold: crop=s7gq01 sold_y/n=s7gq20 sold_qty=s7gq21a SOLD UNIT=s7gq21b
        sold_value=s7gq22
  perennial: crop=s11fq01 still_prod=s11fq03 months_harvested=s11fq10
             harvest_qty PER HARVEST MONTH=s11fq11a unit=s11fq11b
             sold_qty PER HARVEST MONTH=s11fq14a SOLD UNIT=s11fq14b
             sold_value=s11fq15 (printed "montant total de cette vente";
                                    Stata label "mensuelle moyenne")
             (served: Quantity = s11fq11a x s11fq10, the 12-month harvest;
             Quantity_sold = s11fq14a and Value_sold = s11fq15 AS RECORDED)
  cultivation: field=s11cq01 parcel=s11cq02 crop=s11cq03 plant_month=s11cq14b

The sold unit (GH #824).  s7gq21b / s11fq14b, decoded from THEIR OWN value
labels and canonicalised through the u table in crop_production_finalize,
carried as ``Unit_sold`` (NA where s7gq20 == 'Non').  The harvest and sold
label sets are NOT code-compatible: code 10 is 'Autre' on s7fq13c and
'Tonne' on s7gq21b (code 11 'Autre'), so units are compared by LABEL.  The
s7g sale is keyed (i, crop, Unit_sold); s7g holds one row per (household,
crop), so the unit key is inert today and names the grain.

The tree rows (GH #1267 check).  11F asks the harvest and the quantity sold
as an average per harvest month (11F.11 / 11F.14), beside the month count
11F.10, and then 11F.15, printed -- word for word 3B.14 -- "Quel a ete le
montant total de cette vente ?" (French p1 pp. 44-45; English pp. 42-43,
"What is the total amount of this sale?"), and the enumerator manual
(WB catalog 3409, PDF p. 76) says "demander a l'enquete la valeur en F CFA
de la quantite totale vendue", although the Stata label of s11fq15 reads
"Montant de la vente mensuelle moyenne" and the price levels put the value
on the per-month quantity's footing.  Served (rulings @ligon 2026-10-23 on
PR #1272, the sale revised after the document search, following the
Ethiopia #1257 precedent) -- the 2014-15 rule: Quantity = s11fq11a x
s11fq10, the 12-month harvest (mali.DERIVATION_TREES, every tree row);
Quantity_sold = s11fq14a and Value_sold = s11fq15 AS RECORDED, keyed
mali.DERIVATION_TREE_SALES on tree rows with a positive sale.  The WB
MLI_EACI2.do:242/313/353 multiplies the harvest and sold kg equivalents
(s11fq11c, s11fq14c) and the value by s11fq10; the evidence is in
Mali/_/CONTENTS.org.

The harvest kg factor (GH #1749, ruling @ligon 2026-11-13, "Okay, let's
wire it").  Q7F13's fourth sub-column, "Equivalent en Kg" (s7fq13d, DDI
V876), is the instrument's kilograms per ONE unit of s7fq13c.  The P2
enumerator manual says so (eaci17_manuel_p2_french.pdf, PDF p. 30, 7F.13:
"on écrit le poids en Kg d’une unité de l’UML reportée", with a worked
example of 97 sheaves at 2.5 kg each), it is 1 on every Kilogramme row, and
in no unit with 100 or more rows does it rise with the quantity as a total
would.  It is served as recorded in the canonical
KgFactor column, the `reported` rung of transformations.harvest_kg_factors,
as Uganda serves UNPS q6d; a 0 is no answer and reads NA, the Uganda rule.
REPORTED, so it carries no Derivation key.  The ladder is unchanged: its
reported-factor screen refuses a factor above KG_FACTOR_MAX (250 kg), and
such a row falls to its unit's survey_median.  A few rows record a factor
equal to the quantity and are served as recorded too.  Tree rows (11F) and
Farm-level sale rows carry no factor.  Counts, by unit, and the rows that
look like totals: Mali/_/CONTENTS.org (2026-11-13, GH #1749).

Unanswered sales (ruling @ligon 2026-10-23, PR #1272).  The s7g sale is
summed with sum(min_count=1), so a sale whose quantity or value was not
recorded stays NA; 0 is filled ONLY where s7gq20 == 'Non'.  The rows it
affects are mostly Leger-questionnaire households: Leger's s7g asks 7G.20
(sold?) but not 7G.21 / 7G.22 (how much, for how much) -- every one of the
1,607 'Oui' rows without a quantity is Leger (Mali/_/CONTENTS.org).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from lsms_library.build_transforms import reduce_to_agreed
from lsms_library.country import grain_reports
from mali import (eaci17_i as mali_i, crop_production_finalize, producing_trees,
                  tree_crop_rows, stamp_tree_derivations, FARM_LEVEL,
                  derive_hh_crop_sale_attachment, ehcvm_is_sale,
                  stamp_hh_crop_sale, _is_quantity_sentinel)

WAVE = '2017-18'


def _hhid(df):
    return df.apply(lambda r: mali_i(pd.Series([r['grappe'], r['exploitation']])),
                    axis=1)


def _plot(df, fcol, pcol):
    f = df[fcol].astype('Int64').astype('string')
    p = df[pcol].astype('Int64').astype('string')
    return (f + '_' + p).where(f.notna() & p.notna(), pd.NA)


# --- seasonal harvest (s7f) ---
s7f = get_dataframe('../Data/eaci17_s7fp2.dta').copy()
s7f['i'] = _hhid(s7f)
seasonal = pd.DataFrame({
    't': WAVE,
    'i': s7f['i'],
    'plot_id': _plot(s7f, 's7fq01', 's7fq02'),
    'crop': s7f['s7fq03'],
    'u': s7f['s7fq13c'],
    'Quantity': s7f['s7fq13a'],
    # The instrument's kg per ONE unit of s7fq13c, as recorded (GH #1749); a 0 is
    # no answer and reads NA (Uganda q6d).  No sentinel mask: none occurs.
    'KgFactor': (lambda kg: kg.where(kg > 0))(pd.to_numeric(s7f['s7fq13d'], errors='coerce')),
    'harvest_month': s7f['s7fq12b'],
    'planting_month': pd.NA,
    'intercropped': pd.NA,   # 2017-18 s11cq05 has no documented monoculture/
                             # intercrop binary; left NaN (honest missing).
    'perennial': False,
})

# --- sold (s7g): household-crop grain ---
s7g = get_dataframe('../Data/eaci17_s7gp2.dta').copy()
s7g['i'] = _hhid(s7g)
sold = pd.DataFrame({
    'i': s7g['i'],
    'crop': s7g['s7gq01'],
    'Quantity_sold': pd.to_numeric(s7g['s7gq21a'], errors='coerce'),
    # The SOLD unit, decoded from s7gq21b's own labels (GH #824).
    'Unit_sold': s7g['s7gq21b'].astype('string'),
    # FIELD kg per sold unit; do not map the tree total-equivalent field.
    'KgFactor_sold': pd.to_numeric(s7g['s7gq21d'], errors='coerce'),
    'Value_sold': pd.to_numeric(s7g['s7gq22'], errors='coerce'),
})
not_sold = s7g['s7gq20'].astype('string').str.strip().eq('Non')
sold.loc[not_sold, ['Quantity_sold', 'Value_sold']] = 0
sold.loc[not_sold, 'Unit_sold'] = pd.NA   # a filled-in 0 has no unit
sold.loc[not_sold, 'KgFactor_sold'] = pd.NA
# One sale record per (household, crop, SOLD UNIT) -- never a sum across
# units (Niger 2021-22).  dropna=False keeps an unrecorded unit as its own
# key.  sum(min_count=1), vectorised (no per-group callable: pandas
# deep-copies attrs once per group, CLAUDE.md GH #871): an unrecorded sale
# stays NA rather than becoming 0 -- the 0 above is filled only on an
# explicit s7gq20 == 'Non' (ruling 2026-10-23; #1250 / #1253 / #1263).
# s7g has one row per (household, crop), so each group is one row.
# Reuse the explicit factor-only agreement policy (ledger #824 §2/4).
# A missing report conflicts with a populated one; keep existing amounts.
agreed_factor = reduce_to_agreed(
    sold.set_index(['i', 'crop', 'Unit_sold'])[['KgFactor_sold']],
    on_conflict='na', na_is_conflict=True,
    country='Mali', table='crop_production', wave=WAVE)
sold = (sold.groupby(['i', 'crop', 'Unit_sold'], dropna=False)
        [['Quantity_sold', 'Value_sold']].sum(min_count=1))
sold['KgFactor_sold'] = agreed_factor['KgFactor_sold'].reindex(sold.index)
sold = sold.reset_index()
# ...and attributed only to a (household, crop) sold in ONE unit (a second
# unit would duplicate the plot-crop row).  Measured: none sells in two.
units_per = sold.groupby(['i', 'crop']).size()
one_unit = units_per[units_per == 1].index
sold = sold[sold.set_index(['i', 'crop']).index.isin(one_unit)]

# s7f has one row per (household, plot, crop), so one row is one plot.
assert not seasonal.duplicated(['i', 'plot_id', 'crop']).any(), 's7f repeats a plot-crop'
for frame in (seasonal, sold):
    frame['crop'] = frame['crop'].astype('string')

# A sale: on the crop's single harvest row, else on ONE Farm-level row (GH #1457).
SOLD_COLUMNS = ['Quantity_sold', 'Unit_sold', 'KgFactor_sold', 'Value_sold']
# 9999 / 99 mask below (crop_production_finalize): such a quantity is not a sale.
is_sale = ehcvm_is_sale(sold['Quantity_sold'].mask(_is_quantity_sentinel(sold['Quantity_sold'])),
                        sold['Value_sold'])
sales = sold[is_sale]
seasonal = derive_hh_crop_sale_attachment(seasonal, sales, columns=SOLD_COLUMNS)
seasonal['t'] = WAVE

# Conservation: every sale record is on exactly one row, unchanged.
on_a_row = seasonal[seasonal['Quantity_sold'].notna() | seasonal['Value_sold'].notna()]
assert len(on_a_row) == len(sales), 'crop_production 2017-18: a sale moved or doubled'
assert on_a_row['Value_sold'].sum() == sales['Value_sold'].sum()
assert on_a_row['Quantity_sold'].sum() == sales['Quantity_sold'].sum()
# crop_production_finalize SUMS a repeated key, so a sale's Farm-level row must not
# meet a tree row, which sits on the same plot_id.  Asserted below, once both exist.

# A record that is not a sale serves only a crop on ONE plot: the filled-in 0 of a
# 'Non' or an unrecorded answer's NA, as before (ruling 2026-10-23).
unsold = sold[~is_sale]
one_plot = (seasonal[seasonal['plot_id'].ne(FARM_LEVEL).fillna(True)]
            .groupby(['i', 'crop'])['plot_id'].nunique())
one_plot = one_plot[one_plot == 1].index
unsold = unsold[unsold.set_index(['i', 'crop']).index.isin(one_plot)]
seasonal = seasonal.merge(unsold, on=['i', 'crop'], how='left', suffixes=('', '_unsold'))
for col in SOLD_COLUMNS:
    seasonal[col] = seasonal[col].astype('object').where(
        seasonal[col].notna(), seasonal.pop(f'{col}_unsold').astype('object'))

# --- planting month from cultivation roster (s11c) ---
s11c = get_dataframe('../Data/eaci17_s11cp1.dta').copy()
s11c['i'] = _hhid(s11c)
cult = pd.DataFrame({
    'i': s11c['i'],
    'plot_id': _plot(s11c, 's11cq01', 's11cq02'),
    'crop': s11c['s11cq03'],
    'planting_month': s11c['s11cq14b'],
}).dropna(subset=['plot_id', 'crop']).drop_duplicates(
    subset=['i', 'plot_id', 'crop'], keep='first')
seasonal = seasonal.drop(columns=['planting_month']).merge(
    cult, on=['i', 'plot_id', 'crop'], how='left')

# --- perennial trees (s11f): no field grid -> plot = 'Farm-level', perennial=True
# The canonical structural sentinel (GH #1476): a NULL here is a deferred silent
# deletion, because `groupby` drops NaN keys.  135 of the 270 rows also record
# where the trees stand (s11fq05c block, s11fq05d parcel); that is unwired. ---
# Like 2014-15, s11f is a FIXED tree roster (every HH gets a row per species,
# s11fq03 == 'Non' for trees it does not have).  Keep only s11fq03 == 'Oui'
# (currently producing) — the ~270 rows with an actual harvest; matching WB
# `drop if s11fq03==2`.
s11f = producing_trees(get_dataframe('../Data/eaci17_s11fp1.dta').copy(), WAVE)
# perennial roster uses 'ménage' as the within-cluster household key
key2 = 'exploitation' if 'exploitation' in s11f.columns else 'ménage'
s11f['exploitation'] = s11f[key2]
s11f['i'] = _hhid(s11f)
# The measured columns come from mali.tree_crop_rows, which calls the
# registered derivations mali.derive_tree_twelve_month_total and
# mali.derive_tree_sale_window: Quantity = s11fq11a x s11fq10 (the
# per-month harvest times months harvested), Quantity_sold = s11fq14a and
# Value_sold = s11fq15 as recorded; months_harvested = s11fq10 (labelled
# '1 mois' .. '12 mois', 0 = 'Aucune récolte').
perennial = pd.DataFrame({
    't': WAVE,
    'i': s11f['i'],
    'plot_id': FARM_LEVEL,
    'crop': s11f['s11fq01'],
    'harvest_month': pd.NA,
    'planting_month': pd.NA,
    'intercropped': pd.NA,
    'perennial': True,
}).join(tree_crop_rows(s11f, WAVE))

df = pd.concat([seasonal.drop(columns='Derivation'), perennial], ignore_index=True)
# crop_production_finalize would add a sale's Farm-level row into a tree row's harvest.
assert not df.duplicated(['i', 'plot_id', 'crop']).any(), 'a sale met a tree row on Farm-level'
df = crop_production_finalize(df)
# Stamp AFTER the collapse (CLAUDE.md "Derived Values", registering step 3):
# the harvest key on every tree row, the sale-window key beside it on a tree
# row with a positive sale, the attachment key on a harvest row holding an s7g sale.
df['Derivation'] = stamp_hh_crop_sale(stamp_tree_derivations(df), df)

assert len(df) > 0, "crop_production 2017-18 produced no rows"
assert df.index.is_unique, "Non-unique (t, i, plot, crop, condition) in crop_production 2017-18"

to_parquet(df, 'crop_production.parquet',
           grain_audit=grain_reports('Mali', 'crop_production'))
