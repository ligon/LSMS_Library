"""Build crop_production for Niger ECVMA 2011-12 (GAP 1, item-level).

Two source files, one per season:
  - ecvmaas2e_p2.dta : the rainy-season harvest module (section 2E).
  - ecvmaas3e_p1.dta : the dry-season harvest module (section 3E, GH #1462), by
    crop and "all fields combined", so its rows carry plot_id = 'Farm-level'.
    Niger has no `season` level: these are the 'Farm-level' rows of this wave, and
    each carries a `Derivation` key.  See the block below the rainy frame.

The rainy module: one
row per reported (field, parcel, crop) harvest line; harvest qty + unit
(as02eq07a / as02eq07b), the survey's own kilograms for the line (as02eq07c -> the
``Quantity_kg`` column, GH #1436), sold qty + unit (as02eq12a / as02eq12b -> the
``Unit_sold`` column, GH #824), the sold kilograms (as02eq12c -> the
``Quantity_sold_kg`` column, a total, GH #1560) and sale value (as02eq13) are all
at this plot-crop grain; the module's unlabelled
"don't know" codes (999999 on the quantities, 9999999 on the value) are
mapped to NA, exact value only (GH #865).  ``hid`` already equals
grappe*100+menage (the canonical 2011-12 household id), so i() just
str()s it.  No intercrop / perennial / date variables in this module
(left NaN).

plot = "{as02eq01}_{as02eq03}".  The pre-EHCVM waves have no plot_features
recipe yet, so this plot id is internal to crop_production (it still
aligns by construction with any future ECVMA plot_features build).
Index = (t, i, plot_id, crop, u, condition, production_obs).  ``production_obs``
is ``reported:<as02eq0>``, the module's own harvest-line number ("numero
d'ordre"): one (plot, crop, unit) can carry several reported lines (27 key
groups, 57 rows), and the line number is what tells them apart (GH #1360).
The module asks no product-state question (2E.7a/7b/7c are quantity, unit,
kilograms), so ``condition`` is the ``unknown_condition`` sentinel (GH #1436).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from niger import (i as niger_i, _crop_maps, _crop_labels, _unit_labels,
                   _finish_crop_production, _na_exact,
                   _crop_reported_observations, FARM_LEVEL,
                   derive_dry_season_cycle_total, derive_dry_season_sale_window,
                   stamp_dry_season_derivations)

# "Don't know" codes of this module, unlabelled in the .dta (GH #865).  Exact
# values only.  Raw counts, ecvmaas2e_p2.dta (12,091 rows): as02eq07a ==
# 999999 on 11 rows (all with unit code 99 'manquant', four crops: mil 3,
# sorgho 3, niebe 3, riz paddy 2), as02eq12a == 999999 on 3, as02eq13 ==
# 9999999 on 2 (both hid 20607, which carries all three codes).  as02eq07c ==
# 999999 on the same 11 rows as as02eq07a (GH #1436).
QTY_SENTINELS = (999999,)
VALUE_SENTINELS = (9999999,)
# Section 3E's quantity code, a different width (GH #1462).  ecvmaas3e_p1.dta (576
# rows): as03eq09a == 99999 on 1, and its sibling kg columns as03eq09c / as03eq11c
# on 42 / 39.  Its value code is the rainy one: as03eq12 == 9999999 on 1.
DRY_QTY_SENTINELS = (99999,)
# The rainy SOLD kilograms' code (GH #1560, ruling 2026-11-12: sold kg are served as
# totals).  as02eq12c == 9999 on 10 of its 1,296 answers, NARROWER than the module's
# 999999 and unlabelled, and TREATED AS a code on the file's own evidence: 10 rows sit
# on exactly 9999 against 13 anywhere in 5,000-9,998 and 1 above (max 11,000); 2 of the
# 10 are all-code lines (as02eq12a 999999, as02eq13 9999999); the other 7 with a sold
# quantity would be 500-5,000 kg per Tiya / Botte; 7 of the 8 beside a harvest kg exceed
# the SAME line's as02eq07c (24 of 1,286 other sales do); and the one sold in Kg would
# break kg == quantity, which holds on 151 of 151 other Kg sales.  The dry module's code
# for its sold kg (as03eq11c) is DRY_QTY_SENTINELS, 39 rows.  See CONTENTS.org.
SOLD_KG_SENTINELS = (9999,)


src = get_dataframe(
    '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas2e_p2.dta',
    convert_categoricals=True)
srcn = get_dataframe(
    '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas2e_p2.dta',
    convert_categoricals=False)

crop_map, unit_map = _crop_maps()

hh = srcn['hid'].apply(lambda x: niger_i(x) if pd.notna(x) else pd.NA)
field = srcn['as02eq01'].apply(format_id)
parcel = srcn['as02eq03'].apply(format_id)
plot = field.astype(str) + '_' + parcel.astype(str)

df = pd.DataFrame({
    'i':             hh.values,
    'plot_id':          plot.values,
    # The recorded harvest-line number, unique within hid (GH #1360).
    'production_obs': _crop_reported_observations(hh, srcn['as02eq0']).values,
    'crop':          _crop_labels(srcn['as02eq06'], src['as02eq06'], crop_map).values,
    'u':             _unit_labels(src['as02eq07b'], unit_map).values,
    'Quantity':      _na_exact(srcn['as02eq07a'], QTY_SENTINELS).values,
    # A total for the line, not a factor per unit (GH #1436).
    'Quantity_kg':   _na_exact(srcn['as02eq07c'], QTY_SENTINELS).values,
    'Quantity_sold': _na_exact(srcn['as02eq12a'], QTY_SENTINELS).values,
    # The sale in kilograms, a TOTAL for the line's sale (GH #1560): 2E12 asks quantity,
    # unit and kg side by side, and kg == quantity on all 151 Kg sales.
    'Quantity_sold_kg': _na_exact(srcn['as02eq12c'], SOLD_KG_SENTINELS).values,
    'Value_sold':    _na_exact(srcn['as02eq13'], VALUE_SENTINELS).values,
    # The SOLD unit (as02eq12b), a second unit on the same line (GH #824);
    # same 12-code label set as the harvest unit, same `u` table.
    'Unit_sold':     _unit_labels(src['as02eq12b'], unit_map).values,
})

# --- the dry season (section 3E, GH #1462; registered in _/derivations.yml) -----
# The 2010/11 contre-saison, asked in passage 1.  A crop line (as03eq01) with no field
# or parcel.  as03eq09a and as03eq11a are per cycle (the English instrument and DDI:
# "the first cycle"; the Stata label "en moyenne pour chaque cycle" is a template, the
# rainy as02eq07a has it too) beside the count as03eq06.  as03eq09c, "Quantity in Kgs",
# is a line total and not per-unit (kg equals the quantity on 5 of 7 Kg lines, a median
# 86.7 per 100 kg sack), and per cycle like as03eq09a, so Quantity_kg takes the same
# product under the same key.  Its 99999 (42 rows) is masked first, and the tail masks
# its 0 beside a positive quantity (74 rows), so 116 of 549 harvests here have no
# kilogram (GH #1436).  as03eq11c, the sold kilograms, is Quantity_sold_kg (GH #1560): a
# total in the window of as03eq11a, the first cycle, so NOT multiplied by as03eq06; its
# 99999 (39 rows, 33 of them beside as03eq09c's own 99999) is masked, and the tail masks
# its 0 beside a positive sold quantity (36 rows).
# One row of 576 is byte-identical to another, with one (hid, line): the same
# record, dropped as the s16d repeats are, or the line token would repeat.
dry = get_dataframe(
    '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas3e_p1.dta',
    convert_categoricals=True)
dryn = get_dataframe(
    '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas3e_p1.dta',
    convert_categoricals=False)
_dup = dryn.duplicated(keep='first').values
dry, dryn = dry[~_dup], dryn[~_dup]

dry_hh = dryn['hid'].apply(lambda x: niger_i(x) if pd.notna(x) else pd.NA)
dry_sale = derive_dry_season_sale_window(_na_exact(dryn['as03eq11a'], DRY_QTY_SENTINELS),
                                         _na_exact(dryn['as03eq12'], VALUE_SENTINELS),
                                         _na_exact(dryn['as03eq11c'], DRY_QTY_SENTINELS))
dry_df = pd.DataFrame({
    'i':             dry_hh.values,
    'plot_id':       FARM_LEVEL,
    'production_obs': _crop_reported_observations(dry_hh, dryn['as03eq01'], module='3E').values,
    'crop':          _crop_labels(dryn['as03eq03'], dry['as03eq03'], crop_map).values,
    'u':             _unit_labels(dry['as03eq09b'], unit_map).values,
    'Quantity':      derive_dry_season_cycle_total(_na_exact(dryn['as03eq09a'], DRY_QTY_SENTINELS),
                                                   dryn['as03eq06']).values,
    'Quantity_kg':   derive_dry_season_cycle_total(_na_exact(dryn['as03eq09c'], DRY_QTY_SENTINELS),
                                                   dryn['as03eq06']).values,
    'Quantity_sold': dry_sale['Quantity_sold'].values,
    'Quantity_sold_kg': dry_sale['Quantity_sold_kg'].values,
    'Value_sold':    dry_sale['Value_sold'].values,
    'Unit_sold':     _unit_labels(dry['as03eq11b'], unit_map).values,
})
df = pd.concat([df, dry_df], ignore_index=True)

df = _finish_crop_production(df, '2011-12')
# After the tail, so the stamp rides on served rows (CLAUDE.md "Derived Values").
df['Derivation'] = stamp_dry_season_derivations(df)

assert df.index.is_unique, 'crop_production 2011-12 repeats a native harvest line'
assert len(df) > 0, 'crop_production 2011-12 produced no rows'
to_parquet(df, 'crop_production.parquet')
