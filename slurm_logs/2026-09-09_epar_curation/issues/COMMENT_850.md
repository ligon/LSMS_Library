The gap is present at every step of `conversion_to_kgs`, not only at the baseline the issue names. Reading the current body (`lsms_library/transformations.py:782-878`):

1. `pkg = v[price].divide(v['Kgs'], axis=0)` then `.groupby(index).median().median(axis=1)` (`:870-871`) -- groupby `['t','m','i']`. No `j`.
2. `po = v_infer[price].groupby(index + ['u']).median().median(axis=1)` (`:873`) -- groupby `['t','m','i','u']`. No `j`.
3. `kgper = (po / pkg).dropna()` then `.groupby('u').median()` (`:874-875`) -- final key is `u` alone, returned as `dict[str, float]`.

`_get_kg_factors` never widens it: `group_levels = [n for n in ['t','m','i'] if n in idx_names]` (`:1048`), `inferred = conversion_to_kgs(df, index=group_levels)` (`:1051`). So there is no item axis at any stage, and the module comment at `:710` already states the consequence in a different context ("`conversion_to_kgs` returns ONE factor per unit label"). The crop-side ladder carries `item_keys=('j',)` (`:2863`), so the two sides of the library disagree. (The reported-factor route for crops is being wired separately in #852 and #859; this inference is the fallback where no reported factor exists, which is where the missing `j` bites.)

Distinct from the `j` widening, and unmeasured: step 2 takes the median of `Expenditure` itself (`:873`), not of `Expenditure/Quantity`, while the docstring describes the result as a per-unit price (`:791-792`). `kgper` is therefore kilograms per transaction ROW of unit `u`, equal to kilograms per unit only where `Quantity == 1`. Nobody has pulled the per-row `Quantity` distribution, so the size of that effect is unknown.

A `u`-only factor table is already live: `Uganda/_/conversion_to_kgs.json` is joined on `u` alone inside `uganda.food_acquired` (`uganda.py:404-410`), which all eight Uganda wave `food_acquired.py` scripts call. `Uganda/_/kg_per_other_units.py:11-18` is the standalone ancestor of `conversion_to_kgs` and runs the same three-step chain against the same `['t','m','i'] -> +['u'] -> 'u'` keys.

Ethiopia has a same-shaped file built by a different route -- `df.groupby('unit_cd')['mean_cf_nat'].median()` over `Food_CF_Wave1..4.dta`, per the tangled block at `Ethiopia/_/CONTENTS.org:145-172` -- but nothing reads it: the only reference in the tree is a comment at `ethiopia.py:282`. `CONTENTS.org:279-283` already records that it omits `food_cf_wave5.dta`.

Source: slurm_logs/2026-09-09_epar_curation/LEARNINGS.org
