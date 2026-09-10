# Prior-Art Ledger — 854-malawi-shipped-factors

**Search tier used:** ripgrep + git floor. The `gitnexus` MCP server did not
connect this session (CONNECTION_CLOSED) and this mirror carries no
`.gitnexus/` index, so per `CLAUDE.md` §"Code intelligence: GitNexus is
OPTIONAL" the substitutes were declared, not skipped: `rg` sweeps of
`_harvest_block` / `assemble_crop_production` / `condition` /
`SHIPPED_FACTOR_JOIN_LEVELS` / `harmonize_crop_unit` across `*.py`, `*.yml`,
`*.org`, `*.md` and `tests/`. Blast radius found and acted on:
`_harvest_block` has exactly four call sites (the four wave
`crop_production.py`); `assemble_crop_production` the same four;
`SHIPPED_FACTOR_JOIN_LEVELS` is read in `transformations.py` (2 sites) and
asserted once in `tests/test_shipped_factors.py:295` (membership, not
equality — so adding a member is safe, and it was checked before editing).

## §1 Task, restated

Wire the two crop-side conversion tables Malawi's IHS5 (2019-20) v04 release
ships — `ihs_seasonalcropconversion_factor_2020.dta` and
`ihs_treeconversion_factor_2020.dta`, both DVC-tracked under
`Malawi/2019-20/Data/Cross_Sectional/` and listed in `CONTENTS.org:659-662`
as "held, unwired" — as a **shipped-factor table** for
`transformations.harvest_kg(shipped_factors=...)`, the mechanism merged at
`675fc34a`. Decide, in the same change, what `_harvest_block`'s dead
`condition=` parameter is for: all four wave scripts have passed
`condition='ag_g13c'` since GAP 1 and the function accepted and discarded it.

The decision is EXPLICIT loaders, no registry (`harvest-kg-shipped-factors.md`
§6 option 1). `harvest_kg` is an analyst-callable transform, not a registered
or derived table, so the loader is a plain function on the country module and
nothing auto-discovers it.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_shipped_factor_lookup` | `transformations.py:2000` | joins an external factor table onto `crop_production`; refuses ambiguity; warns on a dropped key and a zero match | yes (`test_shipped_factors.py`) | **reuse verbatim** |
| `SHIPPED_FACTOR_JOIN_LEVELS` | `transformations.py:1971` | the join vocabulary | membership asserted | **extend** — `'crop'` added (§5) |
| `harvest_kg` / `harvest_kg_factors` | `transformations.py:2477` / `:2179` | the layered factor stack and its tallies | yes | **reuse** (docstrings updated) |
| `_screen_reported_factors` | `transformations.py:1739` | `KG_FACTOR_MAX = 250`, kg-must-weigh-1, year-band; counts, never clips | yes | **reuse** — see §6 Q1 |
| `_malawi_code_map` | `Malawi/_/malawi.py:605` | `{int Code: Preferred Label}` from a `categorical_mapping.org` table | indirectly | **reuse** for crop, unit AND the new condition table |
| `_crop_codes` | `Malawi/_/malawi.py:903` | raw crop code → `harmonize_crop` label, `+1000` for perennials | indirectly | **reuse** (the loader shares its code→label table, so the two sides cannot drift) |
| `_harvest_block` / `assemble_crop_production` | `Malawi/_/malawi.py:915` / `:1033` | the shared harvest reshape + assembly for the four waves | via wave builds | **extend** (`condition` wired; grain widened) |
| `uganda._harvest_condition_map` / `harvest_conditions` | `Uganda/_/uganda.py:1146`, `Uganda/_/categorical_mapping.org:856` | the only prior `condition` level in the corpus | yes (`test_uganda_crop_condition.py`) | **precedent only** — Uganda's vocabulary is a dryness×form cross and Malawi asks only the form (§3) |
| a loader for a shipped crop CF table | — | **none existed.** The only external-table reader on the food side is `Malawi/2019-20/_/food_acquired.py:17` | — | so `crop_conversion_factors` is not reinvention |
| a `region`-onto-`crop_production` resolver | — | none; `transformations` deliberately never re-enters `Country` | — | `with_region` is new, and lives on the country module for that reason |

## §3 Definitions & conventions in force

- **`condition` vocabulary** — `lsms_library/data_info.yml`,
  `Columns.crop_production.condition`. It crosses a DRYNESS family with a
  physical FORM, and says outright: "a country asking something Uganda does
  not should extend this list rather than force-fit a foreign category".
  Malawi extends the FORM axis (§5).
- **`unknown_condition` is never NA** — same note: "it is a real value, never
  NA, because a null index key is silently dropped by the duplicate collapse
  in a country's build script."
- **`KgFactor` is REPORTED, never constructed, and a shipped table is NOT
  written to it** — `data_info.yml` (tightened in the #852/#854 prerequisite
  PR): a shipped table is applied at read time through
  `harvest_kg(shipped_factors=...)`, which is what keeps `reported_vs_shipped`
  auditable.
- **A duplicate join key is REFUSED, never reduced** —
  `_shipped_factor_lookup` docstring; `CLAUDE.md` §"Grain Collapse"
  ("Duplicates on a declared index mean the IDENTIFIER IS BROKEN … fix the
  index; do not declare a reducer"). De-duplication is the loader's
  deliberate act, and `_collapse_varieties` is that act.
- **Count, never clip** — `_screen_reported_factors` docstring; #854's own
  "What it must NOT do": "Never average or silently resolve a table lookup
  miss; report it."
- **`region` is lower case and matched exactly** —
  `SHIPPED_FACTOR_JOIN_LEVELS` note; `cluster_features` ships `Region`.
- Per `STANDING.md` §3/§4 for IO (`get_dataframe` only), pandas 3.0 targets,
  and `attrs` survival.

## §4 Invariants & assumptions

- **`crop_production.crop` is COARSER than the shipped files' crop key.** The
  files write a VARIETY label — `'MAIZE LOCAL'`, `'RICE FAYA'`,
  `'GROUNDNUT CG7'` — which are the Module-G value labels for codes 1-4 /
  17-26 / 11-16, all of which `harmonize_crop` maps to one Preferred Label.
  Collapsing them is a reduction and must be refused where they disagree.
- **The shipped unit namespace is WIDER than the harvest module's.** The
  harvest asks codes 1-13; the files add `'14'` (PAIL MEDIUM), `'31A/B/C'`
  (BUNDLE sizes), `'98'` (HEAP) and `'8A/B/C'` (BUNCH sizes). No harvest row
  can carry those. Folding `8A/8B/8C` onto code 8 `'Bunch'` would be the same
  disagreeing collapse in a different costume.
- **`region` cannot be collapsed.** 259 of 315 seasonal (crop, unit,
  condition) triples and 53 of 56 tree (crop, unit) pairs carry more than one
  distinct regional factor.
- **The file spells the south `'South'`; `cluster_features.Region` spells it
  `'Southern'`** (the `region` table in `categorical_mapping.org`). Untranslated
  it matches nothing, silently, for a third of the country.
- **The tree table's `condition` must be the sentinel STRING**, not NA: NA
  normalises to the join's private `_KEY_NA` and matches only a NULL
  condition, which no `crop_production` row has by construction.
- **`_harvest_block`'s output index is positional.** `condition` is built
  inside the same `pd.DataFrame({...})` as the other columns rather than
  joined afterwards, because the block filters rows after construction.
- **`ag_g13c` carries NON-INTEGRAL junk** (a `4.5` in the 2016-17 panel).
  `.astype('Int64')`, which every other Malawi code map uses, raises
  `TypeError: cannot safely cast non-equivalent float64 to int64` on it under
  pandas 3.0 — measured, and the reason `_crop_conditions` maps element-wise.
- **`attrs` do not survive a disagreeing merge** — `with_region` merges a
  frame that has a population record against one that has none, so it copies
  `attrs` explicitly (`CLAUDE.md` §"Panel ID Transitive Chains").
- Malawi's own idiosyncrasies cross-referenced from `_/CONTENTS.org`, as the
  dispatcher required: the three `groupby().first()` sites (#637, all key-sound
  and no-ops); the 2026-09-08 sale-unit fix (#824 — "the SALE has its OWN
  unit", merge on `(i, _crop_code, u)`); the `:201-207` note that `ag_g13c` is
  "accepted and never used … a schema change and out of scope"; the
  region-specific food unit factors; and the IHS cross-section vs IHPS panel
  split, which is why every wave assembles two halves with different hhid
  columns and, in 2016-17, a `cs-17-` prefix.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| the shipped join / screen / tallies | **reuse** | `675fc34a` built exactly this; nothing here is re-implemented |
| `condition` on Malawi `crop_production` | **extend the schema** | §6 D1 |
| the `condition` vocabulary | **extend `data_info.yml`** with `shelled` / `unshelled` / `shell_not_applicable` | the file's own rule; Uganda's tokens would assert a dryness Malawi never asked |
| crop code → label | **reuse `harmonize_crop`** via `_malawi_code_map` | the loader and `_crop_codes` share one table, so a drift is impossible; only the file's variety-STRING → code step is new, and it is a literal map read off the wave's own Stata value labels |
| unit code → label | **reuse `harmonize_crop_unit`** | same |
| variety de-duplication | **new, `_collapse_varieties`** | nothing in the library collapses an external table onto a coarser label; the rule (agreement-or-drop) is the loader's deliberate act the transform demands |
| `region` onto `crop_production` | **new, `with_region`** on the country module | `transformations` never re-enters `Country`; `cluster_features` is a `Country` call |
| `'crop'` in `SHIPPED_FACTOR_JOIN_LEVELS` | **extend** | 10 countries declare `crop` in `crop_production`'s index and 3 declare `j` (measured). Not the `Region`/`region` laxity the note refuses: that is two spellings of ONE level on one frame; this is two level names on DIFFERENT frames. Without it, #854's own named call shape raises |
| EPAR's interpolation ladder | **NOT ported** | #854 "What it must NOT do"; imputed values wearing a `source` tag. Only the two raw tables are `reported` |
| EPAR's shelled/unshelled crop allowlist | **NOT ported** | same |
| `ag_i02c` (the SALE's own S/U) | **out of scope** | #854 scopes it out; instead the sale gate is tightened so a two-condition plot-crop attaches no sale at all (§6 M4) |

## §6 Decisions and their measurements

### D1 — `condition` becomes an index level

The rule applied: **a level is added when the instrument asks it and a
downstream consumer needs to key on it — not only when it splits a key.**

*Splitting*, measured cold over all Module-G rows in all four waves (112,691
rows): `condition` splits a `(t, i, plot, crop, u)` key on **7** of them, all
in 2010-11. On that number alone the delete branch would have won.

*Keying*, measured on the built table: it is decisive.

| shipped table joined onto | `shipped_matched` | `shipped` served |
|---|---|---|
| `crop_production` WITH `condition` | **86,657** | 82,018 |
| the same frame with `condition` stripped | **refused** — `ValueError: shipped_factors is ambiguous on the join keys ['crop','u','region']: 55 duplicated key(s)` | — |
| a condition-BLIND table (only the 261 of 316 `(crop,u,region)` keys whose conditions agree) | 36,915 | 36,712 |

So the honest "without condition" answer is *no join at all* until a loader
throws the shelled/unshelled distinction away, and the best a loader that did
so could manage is **43% of the matches**. `condition` more than doubles the
shipped layer's reach (2.35x).

The third reason is the one `data_info.yml` states: 240 kg of shelled and
100 kg of unshelled groundnut off one plot are two facts, not one 340 kg
fact. 68,147 rows are `shelled` and 17,900 `unshelled` corpus-wide.

### D2 — vocabulary: extend the FORM axis, do not borrow a dryness

`ag_g13c`'s value labels are identical in every wave that asks it:
`1 'S: SHELLED'`, `2 'U: UNSHELLED'`, `3 'NOT APPLICABLE'` (2013-14 drops the
`S:`/`U:` prefixes). There is no dryness question anywhere in Module G. So
the Preferred Labels are `shelled` / `unshelled` / `shell_not_applicable`,
added to `data_info.yml` beside Uganda's cross. `dried_grain` /
`dried_with_shell` were rejected: they would claim a dryness the instrument
never recorded, and the level exists precisely to say what was measured.

`shell_not_applicable` is kept distinct from `unknown_condition` because they
are different facts: the first is the survey's own answer, the second is
malawi.py's sentinel for a NULL or off-scheme code and for every perennial
Module-P row (which asks no condition question). Distribution on the built
table: `shelled` 68,147 / `unknown_condition` 26,333 / `shell_not_applicable`
19,168 / `unshelled` 17,900.

### D3 — no `t` level; the 2019-20 vintage applies to every wave

Stated in the loader docstring rather than hidden. Supporting: the units are
physical containers, not instruments; the file's own `collectionround` says
each factor was collected in 2016, 2019 or both, so it already spans two
rounds; EPAR does the same and says why. Qualifying: nothing measured here
shows a 2019 pail weighed what a 2010 pail weighed. `waves=['2019-20']` adds
a `t` level and fences the join for a caller who wants it fenced.

### D4 — what the build change moved (before `a73437fd` → after)

| wave | rows before | after | Quantity before | after | `Value_sold` non-null before | after |
|---|---|---|---|---|---|---|
| 2010-11 | 33,349 | 33,420 | 970,155.8 | 970,155.8 | 6,104 | 6,104 |
| 2013-14 | 12,602 | 12,616 | 257,839.0 | 257,839.0 | 1,999 | 1,999 |
| 2016-17 | 36,003 | 36,048 | 842,684.4 | 842,774.4 | 6,807 | 6,807 |
| 2019-20 | 49,425 | 49,464 | 1,206,369.9 | 1,206,369.9 | 8,911 | 8,914 |

+169 rows, decomposed exactly: **7** are the condition splits (2010-11), and
**162** are rows the OLD defensive collapse had been destroying — it keyed on
`(t, i, plot, crop)` with `u` a column, so a plot-crop harvested in two units
was collapsed with `u='first'`. That is the residual `CONTENTS.org` recorded
on 2026-09-08 as "nearly a no-op, but NOT provably one"; it is now closed,
because the collapse keys on the full declared grain. 40 of the 162 are
2016-17 perennial rows with a NULL `plot` (homestead trees with no numbered
plot) that the old `groupby(level=...)` was deleting on a NaN key — which is
also the whole of the +90.0 Quantity move.

### M4 — the sale gate had to be tightened, and it cost one sale

With `condition` on the harvest key, a single plot can carry a shelled AND an
unshelled row in the same `u`; the household-crop sale merge on
`(i, _crop_code, u)` would stamp the same `Quantity_sold` / `Value_sold` on
both. The gate now also requires exactly one harvest row per
`(i, _crop_code, u)` — restoring the invariant the single-plot gate used to
imply. Measured: it suppresses exactly **one** attachment (2010-11
`303041430013`, Maize, 50 kg Bag: `Quantity_sold` 1.5 / `Value_sold` 2,500
MWK, now NA on both rows, because the household-crop total cannot be assigned
to the shelled row or the unshelled row and `ag_i02c` is unwired). 2019-20
gains 3 attachments from the un-collapse. Net `Value_sold`: 2010-11
131,736,555 → 131,735,855; 2019-20 521,586,176 → 522,938,276; the other two
unchanged.

### M5 — the loader's raw grain and match rates

| | rows | crop matched | unit matched | condition matched | region matched | keys kept | keys refused |
|---|---|---|---|---|---|---|---|
| seasonal | 857 | 857/857 | 703/857 | 857/857 | 857/857 | 291 | 23 |
| tree | 153 | 153/153 | 110/153 | n/a (no column) | 153/153 | 84 | 8 |

Zero duplicate rows in either file on its own key. The unit misses are the
codes the harvest module never asks (`14`, `31A/B/C`, `98`; `14`, `8A/B/C`).
The 23 + 8 refused keys are where the collapsed varieties disagree — worst
spread Rice / 50 kg Bag / unshelled / Central, 29.04 vs 49.00 (1.69x); Citrus
/ 50 kg Bag / Southern, 38.0 vs 55.0 (1.45x).

### M6 — `harvest_kg` before / after

`harvest_kg(cp)` vs `harvest_kg(malawi.with_region(cp), shipped_factors=...)`:

| | reported | shipped | survey_median | inferred | none | shipped_implausible | shipped_matched |
|---|---|---|---|---|---|---|---|
| before | 0 | 0 | 0 | 90,075 | 41,473 | 0 | 0 |
| after | 0 | 82,018 | 0 | 24,250 | 25,280 | 4,639 | 86,657 |

| wave | Harvest_kg before | after | change |
|---|---|---|---|
| 2010-11 | 8,509,405.2 | 8,689,757.2 | +2.1% |
| 2013-14 | 2,336,456.0 | 2,554,344.0 | +9.3% |
| 2016-17 | 6,501,016.7 | 6,880,919.6 | +5.8% |
| 2019-20 | 18,529,157.8 | 20,053,977.8 | +8.2% |

Output rows 60,057 → 67,054: the shipped layer serves 16,193 rows the
inferred machinery could not (pails, ox-carts, baskets, bunches, pieces,
bales — the inferred layer only parses `Kilogramme` / `50 kg Bag` /
`90 kg Bag` here).

Agreement where both layers have a factor (65,825 rows): **median
shipped/inferred ratio 1.0200**, share outside [0.5, 2] **7.28%**. By unit:
`Kilogramme` 1.0000 exactly on all 9,475 rows (the sanity check that the join
is not mis-keyed); `50 kg Bag` median 1.0200, range 0.26-1.32; `90 kg Bag`
median 1.0658, range 0.24-1.25. So the library's "a 50 kg bag holds 50 kg"
under-states the survey's own measurement by ~2% on average and by up to 32%,
and over-states it by up to 4x on the light tail.

## §6b Red-team round (2026-09-10) — what moved

`slurm_logs/2026-09-09_epar_curation/REDTEAM_P2_854_malawi.org` graded the
numbers PASS with one FAIL (Feature exclusion, not this branch's to fix) and
four CONCERNs. Taken here:

### The join now keys on the VARIETY, not the collapsed crop (item 2)

The refusal rule was correct *given the coarse key*, and the red-team measured
what the coarse key costs: **6,890 served rows**, all on Groundnut, Rice and
Citrus, with the sole dissenter usually the `OTHER … (SPECIFY)` catch-all —
and, in four cells, a real split (unshelled paddy in a 50 kg bag: `RICE LOCAL`
29.04 vs eight improved varieties at 49.00, 1.69×). EPAR keys the same merge
on `crop_code_long` and says so in capitals.

`crop_production` now carries `crop_variety` (a **column**, never a level — it
is a function of the crop code and splits no key), decoded through a new
`harmonize_crop_variety` table, and `crop_conversion_factors(by_variety=True)`
is the default.

| join key | `shipped_matched` | `shipped` | refused keys |
|---|---|---|---|
| `crop` (`by_variety=False`) | 86,657 | 82,018 | 31 |
| `crop_variety` (default) | **95,906** | **91,267** | **0** |

- +9,249 matched rows; every one of the 31 refused keys resolves; the table
  needs **no de-duplication at all** (813 rows, zero duplicate keys).
- **Identical on all 81,886 rows both keys can serve** — so it moves no number
  the collapsed key already produced.
- It **withdraws 132 rows**, and that is a correction: agreement-or-drop
  cannot distinguish "every variety agrees" from "only one variety is in the
  file", so the collapsed key was serving Tobacco *Burley's* factor to
  flue-cured / NNDF / SDF / oriental tobacco (118 rows) and one Citrus figure
  to all three citrus species (5). The file has no factor for those.
- **The label is not a usable key; the code is.** 12 of 48 seasonal codes are
  spelled differently across waves (2016-17's colons, 2010-11's `RISE LOCAL`
  typo and three truncations); a label join loses 13,769 rows to that drift.
  All 12 are spelling, none is a change of meaning — checked code by code.
  Perennial labels are stable (0 of 23).
- Revised `Harvest_kg` vs the no-shipped baseline: 2010-11 **−0.14%**,
  2013-14 +6.03%, 2016-17 +3.24%, 2019-20 +2.78%; rows out 60,057 → 68,376.
  2010-11 goes negative because of the tobacco withdrawal — those rows now get
  nothing rather than a borrowed factor, and the inferred parser cannot read
  "Bale".

`SHIPPED_FACTOR_JOIN_LEVELS` gains `'crop_variety'`. Second extension to that
tuple in this PR, and the same justification: a shipped table keyed finer than
the served label is the normal case, not Malawi's quirk (Ethiopia's
`Crop_CF_Wave*` will meet it too).

### The sale suppression is counted in code (item 6)

`assemble_crop_production` emits a `SaleAttachmentWarning` naming the sales,
the candidate rows and the MWK, and stashes the same tally on
`df.attrs['sale_suppressed']`. Fires exactly once on the current build:
2010-11, 1 sale, 2,500 MWK, 2 candidate rows. The red-team's point stands and
is now met — the old number lived only in prose and would not have moved when
the data did.

### Items 4 and 5

The per-unit cap proposal (`ox-cart: 800` + a floor) is in §7 with the rows it
moves; the cassava drop is filed as **GH #869** and cited in `CONTENTS.org`
with the full +2,990 / +385 / +206 decomposition.

### The `Feature('crop_production')` exclusion (item 1b) — recorded, not fixed

Adding `condition` moves Malawi out of the 8-country modal index shape, so
`Feature.__call__`'s modal-shape filter drops it: **131,548 rows, 81% of the
assembly**, with a named `UserWarning`. Re-measured on this branch —
`Feature('crop_production')(['Malawi','Togo','Benin'])` returns 20,619 rows
and keeps only Benin and Togo. **The level is kept on purpose**: without it
the shipped table is refused outright, and Uganda already carries `condition`
and is already excluded, so the rule needs fixing for Uganda regardless. The
fix is `index_info` + `fabricate_missing_levels` (the `interview_date` /
`visit` mechanism, GH #506) on branch
`fix/feature-canonical-index-crop-production`; this branch's merge is held
until it lands. GH #775 / #569.

### Latent nit taken

`_collapse_varieties` now `dropna(subset=['KgFactor'])` before the groupby:
`nunique()` skips NaN, so a key holding one NaN and one real factor would have
read `nunique == 1` and `drop_duplicates` could have kept the NaN row. Both
shipped files have zero NaN in `conversion`, so it could not fire today.

## §7 Open questions for the human

1. **A PER-UNIT cap, plus a floor — the concrete proposal, NOT implemented
   here.** `KG_FACTOR_MAX = 250` rejects 12 shipped factors that are probably
   right, and they cost **4,639 rows** that land in `none`, *not* `inferred`
   (the label parser cannot read "Ox-Cart" either, so a rejected shipped
   ox-cart factor leaves the row with no weight at all).
   - **Keep 250 as the default; add `_KG_FACTOR_MAX_BY_UNIT` for
     bulk-transport containers** — `ox-cart: 800`, `cart: 800`,
     `wheelbarrow: 200` — matched on the same stripped-lower-cased unit text
     `_KG_UNIT_KEYS` already uses. One lookup inside
     `_screen_reported_factors`, which already reads the row's unit from `df`.
     Moves 4,639 rows (Maize 4,452, Sweet Potato 132, Sugar Cane 45, Irish
     Potato 10) from `none` to `shipped`. Do **not** raise the global cap: it
     exists to catch a quantity or a calendar year keyed into the factor
     field, and those live on kg / bag / tin units.
   - **Add a FLOOR.** The screen has a ceiling and none, and the live instance
     is in this table: Citrus / Ox-Cart = **18.9 kg**. A 19 kg cartload of
     oranges is as wrong as 682 is high and passes unremarked. (The variety
     key withdraws that particular row, but the missing floor is general.)
   - **The one factor not to defend**: Maize / unshelled / Central = 682.00,
     15.4 bag-equivalents against its siblings' 8.4–8.8. `ox-cart: 800` admits
     it; flagging it in `CONTENTS.org` beats picking a cap to exclude one row.
   - **Gate**: whoever changes the cap must first measure **Uganda's** reported
     (250, 1000] band — Uganda is the country the cap was tuned on and the
     red-team pass could not build it. Nigeria was measured: 0 of 19,678
     non-null reported `KgFactor` rows exceed 250.
   - The check that these are measurements needs no outside number: each
     ox-cart factor over the SAME crop/condition/region's 50 kg Bag factor is
     8.4–9.4 bags for maize and 6.5 for the potatoes. Only 682.00 falls out.
2. **Cassava is missing from 2016-17 and 2019-20 `crop_production` entirely
   — a pre-existing defect this task found and did NOT fix; now GH #869.** Those waves'
   perennial module codes Cassava as `100` (not `1`), so `_crop_codes`'
   `+1000` yields `1100`, which `harmonize_crop` does not carry, and the rows
   are dropped as "no crop identity". Measured: **2,990 rows** (982 + 90 in
   2016-17, 1,557 + 361 in 2019-20). The same mechanism drops 385 rows coded
   `1021` ("OTHER (SPECIFY)", which those waves number 21 rather than 18) and
   206 coded `2800`/`2900`/`3000` (fodder / fertiliser / fuel-wood trees).
   Fixing it is five lines in `harmonize_crop` but ADDS ~3,200 rows to a
   served table and needs its own before/after; it also caps what the tree
   conversion table can reach, since Cassava is its largest crop. Filed here
   and in `CONTENTS.org` rather than fixed silently.
3. **`shipped_factors='auto'`** remains `harvest-kg-shipped-factors.md` §6
   Q1's open question. This is now the second explicit loader (Ethiopia #852
   is the other). Their call shapes do rhyme — `crop_conversion_factors()` on
   the country module, plus a country-side `region` resolver — which is the
   evidence that question was waiting for.
4. **`yield_kg(..., shipped_factors=)` passthrough** is still not added
   (deferred from the prerequisite PR). An analyst asking for yields still
   gets no shipped layer.

---
### Phase 3 — verification (measured)

- `malawi.crop_conversion_factors` — **OK (anchored on §2, §4, §5)**: no
  existing loader to duplicate (§2, row 9); shares `harmonize_crop` /
  `harmonize_crop_unit` with `_crop_codes` rather than re-listing labels;
  refuses its own duplicates before the transform can (§4); does not port
  EPAR's ladder (§5).
- `malawi._collapse_varieties` — **OK (anchored on §3, §4)**: agreement-or-
  drop, never an average, a median or a `.first()`; the dropped keys are
  returned, not swallowed. Pinned by a hand-built conflict in
  `TestVarietyCollapse::test_nothing_is_averaged`.
- `malawi.with_region` — **OK (anchored on §3, §4)**: lives on the country
  module because `transformations` never re-enters `Country`; renames
  `Region` → `region` because the join matches exactly; copies `attrs` across
  a disagreeing merge.
- `_harvest_block(condition=)` — **OK (anchored on §6 D1/D2)**: the parameter
  the four wave scripts already fed is now read; the vocabulary extends the
  form axis rather than borrowing Uganda's dryness.
- `assemble_crop_production` grain — **OK (anchored on §6 D4)**: the widened
  collapse key closes the 2026-09-08 residual rather than adding a reducer;
  no reducer was declared and `aggregation:` was not touched.
- `SHIPPED_FACTOR_JOIN_LEVELS` + `'crop'` — **OK (anchored on §5)**:
  membership-only assertion in `test_shipped_factors.py:295` checked before
  editing; 72 existing shipped/kg-factor tests still pass.
- Tests — `tests/test_malawi_shipped_factors.py` 30 new, all passing.
- Cache — see the commit message and `CONTENTS.org`; `malawi.py`,
  `data_scheme.yml` and `categorical_mapping.org` are ALL cache inputs, so
  every Malawi table's hash moves. Measured, not assumed.
