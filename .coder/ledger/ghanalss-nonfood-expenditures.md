# Prior-Art Ledger — GhanaLSS `nonfood_expenditures`

**Search tier used:** ripgrep + git floor. The `gitnexus` MCP server failed to
connect this session (`CONNECTION_CLOSED`) and this mirror has no `.gitnexus/`
index, so the substitutions named in `CLAUDE.md` §"Code intelligence" were used
and are declared here rather than skipped silently.

## §1 Task, restated

Add `nonfood_expenditures` to GhanaLSS — a table the country has never
declared (`.coder/coverage/latest.csv`: `undeclared / absent / not declared by
this country for any wave`). The canonical shape is registered in
`lsms_library/data_info.yml` as `(t, v, i, j)` with `Expenditure` (required,
monetary) and `RecallWindow` (optional); `v` is joined from `sample()` at API
time, so a country declares `(t, i, j)`. Source is GLSS Section 9 Part A,
"NON-FOOD EXPENSES (LESS FREQUENTLY PURCHASED ITEMS)".

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| canonical schema | `lsms_library/data_info.yml`, `Columns: nonfood_expenditures` | `Expenditure` + `RecallWindow`; sparsity is the point of the shape | `tests/test_schema_consistency.py` | **reuse** |
| `index_info` entry | `lsms_library/data_info.yml`, `Index Info` | `(t, v, i, j)` — `v` present, so `_join_v_from_sample` fires and no `skip_extra` entry is needed | yes | **reuse** |
| Togo instance (canonical) | `Togo/2018/_/nonfood_expenditures.py` | gate → decode → long melt → uniqueness assert → `to_parquet` | PR #773 | **reuse the shape verbatim** |
| `uganda.nonfood_expenditures` | `Uganda/_/uganda.py:533` | long melt; `Expenditure` = **sum of purchased + away + produced + given**, `sum(min_count=1)`, strictly-positive rows only | `tests/test_nonfood_expenditures_schema.py` | **reuse the composition rule** |
| `_harmonized_codes` | `Togo/2018/_/nonfood_expenditures.py:103` | `{int code -> Preferred Label}` from `categorical_mapping.org`, `---`/blank → NA | — | **reuse (inline)** |
| food decode + asserts | `GhanaLSS/2016-17/_/food_acquired.py:168-240` | `get_categorical_mapping` on a per-module `Code_*` column, `_assert_decoded`, D6 drop of unharmonised codes, `na > 1e99` sentinel guard | yes | **extend** (same idioms, new table) |
| `harmonize_food` layout | `GhanaLSS/2016-17/_/categorical_mapping.org` | `Preferred Label \| Aggregate Label \| Code_9b \| Label_9b \| Code_8h \| Label_8h` — per-MODULE code columns at wave level | yes | **extend** → `nonfood_items` |
| `code_label_map` | `local_tools.py:1478` | the safe decode entry point | yes | **reuse** |

Prior ledgers inherited rather than re-derived: `817-nonfood-long.md` (why the
shape is long and sparse), `togo-nonfood-expenditures.md` (the `#+name:` trap,
the silent-`{}` trap), `783-food-vocabulary-consolidation.md` (one vocabulary
per country, injectivity of `j`).

## §3 Definitions & conventions in force

Cited, not paraphrased:

- `Expenditure`: "Amount spent on item j by household i in wave t, over that
  item's recall window, in nominal local currency. Reported, not annualised.
  Rows exist only where the household reported the item." —
  `lsms_library/data_info.yml`, `Columns: nonfood_expenditures`.
- `RecallWindow`: "a country whose modules mix windows and does NOT carry this
  column is pooling incommensurable numbers." — same block. **GhanaLSS mixes
  windows (see §4), so this column is mandatory here, not optional.**
- Sparsity: "A household that did not report an item has NO ROW — never a
  fabricated 0." — same block.
- Core never aggregates: `CLAUDE.md` §"Grain Collapse"; `SkunkWorks/grain_aggregation_policy.org` §3a.
- Section numbering: "Household expenditure | GLSS1/GLSS2 Sections 11 + 12 |
  GLSS3–GLSS7 Section 9" — `GhanaLSS/_/CONTENTS.org:3519`.
- GLSS7 Section 9 module scope: `GhanaLSS/_/CONTENTS.org`, "Section 9B is the
  whole frequently-purchased basket" (added 2026-09-12) — 484 codes, 320 food,
  164 non-food.

## §4 Invariants & assumptions

Repo-wide ones are `STANDING.md §4`. Task-specific, each measured this session:

- **The GLSS5 → GLSS6 item codes are OFF BY ONE from code 4 onward.** GLSS6
  inserts `imported (china)` at code 4 and shifts the rest: GLSS5 `5 = kente
  men` is GLSS6 `6`; GLSS5 `100 = shovels,rakes,wheelbarrows` is GLSS6 `101`.
  Measured: of 250 shared codes only **4** carry identical labels. *A single
  shared `Code` column across waves would mislabel almost an entire wave in
  silence.* Every wave gets its own code column.
- **`lfreqcd` carries Stata value labels in GLSS5 (250) and GLSS6 (251) and
  NONE in GLSS7**; GLSS7 instead ships `s9aname`, populated on 275,282 of
  275,293 reported rows (100.0%) and unambiguous for 507 of its 508 codes.
- **GLSS3 (1991-92) and GLSS4 (1998-99) have no label layer at all** on
  `nfdex1cd` / `nfdex2cd` (0 value labels), and `CONTENTS.org` records that the
  `.DCT` label layer does not exist for GLSS1/GLSS2 either.
- **The missing-value sentinel is a repdigit FAMILY and is per-wave**
  (corrected 2026-09-12; this entry first named only `99999999`). GLSS7
  `s9aq2`/`s9aq4` carry 999999, 9999996, 9999999 and 99999999, identified by
  their spread across many distinct items; 2005-06 and 2012-13 carry none at
  any width. `food_acquired.py`'s `na > 1e99` guard reaches none of them
  (1e8 < 1e99), so this needs its own rule — but **a threshold is not that
  rule**: see the Phase 3 entry for what `>= 99999999` cost in both
  directions. The set is measured per wave and tripwired.
- **The source is a DENSE grid.** `g7sec9a.dta` is 7,077,839 rows = 13,940
  households x ~508 items; only **275,293** carry `s9aq1 == 1`. The gate is what
  produces the canonical sparse shape.
- GLSS3/GLSS4 ship at `.dta` format 105 and their missing doubles are 2^333 —
  `GhanaLSS/_/CONTENTS.org`, "GLSS3 and GLSS4 ship at =.dta= format 105".
- `#+name:` in an org table needs exactly one space after the colon
  (`local_tools.py:1636`); `get_categorical_mapping` with no value kwarg returns
  `{}` silently (`togo-nonfood-expenditures.md` §4).
- `data_scheme.yml` is hashed by name into `Country._table_cache_hash`, so
  editing it cold-rebuilds every GhanaLSS table. Expected, not a defect.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| index shape `(t, i, j)` | **reuse** Togo | canonical; `v` joins at API time, the script must not emit it |
| `i` | **reuse** `hh_id(clust, nh)` from `GhanaLSS/2016-17/_/food_acquired.py:76` | the tested composite, matches `sample()` and `household_roster` |
| `Expenditure` composition | **reuse Uganda's rule**: `s9aq2` (spent, 12 mo) **+** `s9aq4` (value used out of own output or received as gift, 12 mo), `sum(min_count=1)` | `uganda.nonfood_expenditures` sums purchased+away+produced+given, and Nigeria follows it. Togo is not evidence against — EHCVM s09 records only *montant dépensé*, so it has nothing to sum. Serving purchased-only here would make GhanaLSS silently incommensurable with the two countries `Feature()` pools it with |
| `RecallWindow` | **reuse** the column, **mandatory** here | GLSS3/GLSS4 9A(1) carry `s9a1q3` (12-month) *and* `s9a1q4` (3-month) on one row — two windows, two rows |
| item labels `j` | **new**: `nonfood_items` in `GhanaLSS/_/categorical_mapping.org`, one `Code_<wave>` column per wave | the off-by-one in §4 forbids a shared code column; no existing GhanaLSS table decodes 9A codes |
| sentinel handling | **new rule** (`>= 99999999 -> NA`) | the inherited `na > 1e99` guard does not reach 1e8; measured 15x error |
| **waves built** | **GLSS5, GLSS6, GLSS7** (2005-06, 2012-13, 2016-17) | the three whose item labels are machine-available |
| GLSS3 / GLSS4 | **extend-later**, documented | codes present, label layer absent — needs the questionnaire. An `absent_verdicts` `todo` row (data is there, config is not), not `unsure` |
| GLSS1 / GLSS2 | **extend-later**, documented | Section 11 is four heterogeneous `.DAT` files (11A/B amounts, 11C durable acquisitions, 11D transfers-to-persons) with no `.DCT` label layer. An acquisition-and-decode project, not a wiring task |
| frequently-purchased non-food | **excluded**, documented | @ligon's decision this session. It lives inside 9B at *visit* grain (6 visits GLSS7, 10 GLSS3) and canonical `nonfood_expenditures` has no `visit` level, so including it forces a separate grain decision. Measured cost of the omission, GLSS7 on the 30-day exposure: 1.931e6 against 9A's 2.457e6 = **44.0%** (2012-13: 38.7%). **Corrected 2026-09-12 from "~7%"**, which divided by a sentinel-inflated denominator. The scope decision was **re-put to @ligon at the corrected figure, with both alternatives costed (9B summed over visits under `RecallWindow: 30 days`, or a `(t, visit, i, j)` grain needing a canonical `index_info` change), and 9A-only was re-affirmed** — so the omission is a standing decision, not an artefact of the bad number. Vocabulary now recorded as `nonfood_items_9b`. |

## §6 Open questions for the human

- The undercount above is real and is stated in `data_scheme.yml` and
  `CONTENTS.org`. The follow-up — a `visit`-grain frequent-non-food table, or a
  window-carrying sum — is named but not designed.
- GLSS7 `s9aname` is ambiguous for exactly 1 of 508 codes; resolved by taking
  the modal name and asserting the count, not by silent `first()`.

---
### Phase 3 — verification (fill at task end)

Anchored on this ledger, not a general review.

- `GhanaLSS/{2005-06,2012-13,2016-17}/_/nonfood_expenditures.py` — **OK (§5)**.
  Shape, gate-then-decode-then-melt order, injectivity assert, unknown-code
  assert, `(t,i,j)` uniqueness assert and `to_parquet` are Togo's verbatim
  (§2); the three deviations are each recorded in §4/§5 and carry the
  measurement in the docstring.
- `Expenditure = s9aq2 + s9aq4` — **OK (§5)**, not REINVENTION: it is
  `uganda.nonfood_expenditures`'s composition reused, not a new rule. Checked
  by formula, not by name — Uganda sums purchased+away+produced+given with
  `sum(min_count=1)` and keeps strictly-positive rows; this sums the two
  columns GLSS ships with `min_count=1` and keeps `> 0`.
- **The gate is on the VALUE, not on `s9aq1`** — **OK (§4)**, and it is where
  copying Togo verbatim would have been a CONTRADICTION of §3's sparsity rule.
  Togo gates `q02 == 1` because EHCVM ships only bought rows; GLSS Q1's `No`
  branch skips to Q3, so own-production survives it. Measured: a `q1` gate
  drops 14,703 rows / 2.274e6 cedi on 2016-17 (7.6%; the "1.163e8 / 28%" first recorded here shared the sentinel-inflated denominator).
- ~~`SENTINEL = 99999999`~~ — **WRONG, CORRECTED 2026-09-12 before landing.**
  This was the one item Phase 3 passed and should not have. It was calibrated
  on 2016-17 and applied to all three waves, and it failed in both directions:
  - *Too loose on its own wave.* The sentinel is a repdigit FAMILY — 999999,
    9999996, 9999999, 99999999 — and `>= 99999999` catches only the widest
    tier. 40 rows of 9999999 carried 4.0e8 of a served 4.509e8: **93% of the
    delivered 2016-17 total was sentinel.** True total **2.99e7**.
  - *Destructive on a sibling wave.* 2005-06 is in OLD cedi, where 1e8 is an
    ordinary large purchase. The threshold **deleted 28 genuine rows carrying
    4.17e10 — 43% of that wave** — serving 5.43e10 for a true 9.60e10.

  Replaced by a **per-wave measured set** written literally in each script
  (2016-17 four values; 2005-06 and 2012-13 `frozenset()`, neither containing
  any repdigit at any width), plus a **tripwire**: `_amount()` asserts on any
  repdigit-family value not in its declared set, so a new field width fails the
  build instead of being served as a purchase.

  **The generalisable lesson, and why Phase 3 missed it:** every check I ran was
  *internal* — assertions, `is_this_feature_sane`, schema tests, row counts.
  None of them can see a number that is 15x too large, because a sentinel is
  correctly typed, non-null, uniquely indexed and passes every structural test.
  It is exactly the class `quantity_audit.py` (Site Q) was built for: *present,
  non-null and impossible*. The fix was an **external answer key**, which the
  survey ships and I had not opened.

- **Validated against GSS's own aggregate** (added 2026-09-12). 2016-17
  `Data/15_GHA_2017_E_final.dta` carries `TOTNFD`, "Total annual expenditure on
  non-food items", on the same `hid`. Over 13,764 matched households the fixed
  table is **39.7%** of `TOTNFD` (median 1,011 vs 3,437, household total 7,863)
  — the right share for a less-frequently-purchased module, which is one
  component of GSS non-food beside housing, utilities, education, health and
  transport. The pre-fix 4.51e8 was **six times GSS's entire non-food
  aggregate**. *Any wave-level expenditure table built here should be checked
  against these files; GLSS ships them for every round from GLSS5 on.*

- **The residual tail is served, not clipped** — **OK (§3)**, the
  `quantity_audit` rule. 2005-06's ten largest rows still carry 38.9% of the
  wave (top: 8,000,008,000 old cedi for one item); 2016-17 exceeds GSS's whole
  non-food total for 645 households (4.7%). Counted and named in
  `CONTENTS.org`, not dropped. Extending `_SCREENED_COLUMNS` to
  `nonfood_expenditures.Expenditure` is the named follow-up.
- `nonfood_items` org table, one `Code_<wave>` column per wave — **OK (§4/§5)**.
  A shared code column would have been the CONTRADICTION: GLSS6 shifts GLSS5's
  codes from 4 on and only 4 of 250 shared codes agree.
- Rank disambiguation of within-wave duplicate labels — **OK (§4)**, preserves
  the `j` injectivity that §3 (GH #323/#783) requires; asserted in the script.
- `currency` — **OK, no new code**: `Expenditure` is `monetary: true` in the
  canonical `Columns` block, and GhanaLSS's GHC→GHS 2007 override already
  exists in `data_info.yml`. Verified served: 2005-06 `GHC` (median 60,000),
  2012-13 / 2016-17 `GHS` (23 / 35). Writing a deflator here would have been
  REINVENTION of an existing layer.
- No `country.py` / `feature.py` / canonical-schema edit was needed — the
  registration in §2 already covered this table, so `v` joins at API time with
  no `skip_extra` entry (confirmed: the served index is `(i, t, v, j)`).

**Verification run.** `is_this_feature_sane(country='GhanaLSS',
feature='nonfood_expenditures')`: **17 checks, 15 pass, 2 warn, 0 fail,
`report.ok = True`**, on 977,213 rows over 39,034 households. Both warnings are
expected and named here so a later reader does not re-investigate them:
`index_levels_match_scheme` reports an extra `v` (that is `_join_v_from_sample`
working as designed, §2), and `no_constant_columns` reports `RecallWindow`
constant at `'12 months'` (correct for a single-window module; the column is
declared anyway because GLSS3/GLSS4 carry a second window — §5).
`Feature('nonfood_expenditures')` now lists **GhanaLSS, Nigeria, Togo,
Uganda** and assembles GhanaLSS+Togo to 1,085,658 rows on
`(country, t, v, i, j, currency)`.
