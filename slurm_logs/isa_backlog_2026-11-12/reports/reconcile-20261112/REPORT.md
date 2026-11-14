# Open-issue reconciliation, 2026-11-12

**Reviewer.** Claude, model Opus 5.5 (`claude-opus-5-5`), working read-only for Sue (Slurm job 41201840, n0035.savio4). It was assisted by 13 forked workers of the same model, each holding one batch.

**Base.** `development` = `e5fdd2fa831c4d03a247d264be7f339da459e6b0`.

**Inventory.** Refreshed 2026-11-12 06:49 PST (14:49Z): 137 open issues. #1457 closed after the 11-10 snapshot, at 14:37Z on 2026-11-12. Open PR #1604 is also listed. Re-checked at 15:14Z: no issue opened, closed or updated since the fetch, no issue created since 2026-11-11, and the `development` head is still `e5fdd2fa8`.

**Merged since the 2026-11-10 reconciliation base `16c3f4d9c`.** Only #1690 (`1e549e96a`), #1694 (`7f1a9c954`), #1695 (`d5a2b1f9e`) and #1693 (`e5fdd2fa8`). Issues were matched to these by touched path and country, not only by number.

**Write policy.** No GitHub write and no repository edit was made. Every draft below is for Sue to apply, and each is signed "-- Sue".

## 1. Coverage and method

**Covered: all 137 open issues.**
- 135 were reviewed in depth.
- #587 and #1275 are recorded as `excluded-in-flight`. PR #1604 is also listed as excluded.
- #1457, now closed, has an informational row.
- The row set in `TRIAGE.tsv` was asserted equal to the open-issue set, with no duplicates and 10 fields per row.

**Per issue, the reviewer:**
- read the full body and all comments, pre-fetched to `issues/<N>.md`;
- read the REST timeline (cross-referencing PRs and issues, renames);
- matched merged PRs by number, keyword and touched path;
- read the country's `CONTENTS.org`, each row naming the idiosyncrasy found;
- checked the cited code on `e5fdd2fa8`;
- read the maintainer rulings in `isa-decisions.org`.

**Mechanical checks on `TRIAGE.tsv`:**
- **SHAs.** 195 distinct SHAs are cited. All are ancestors of `e5fdd2fa8` except 9. Each of the 9 is explicitly marked in its row as not on development: archived work (#1442 `d81358091`, #1556 ×3, the 851 branch `cf117d101` in #1148) or investigation commits quoted from issue bodies (#1509, #1519 ×2, #1562).
- **path:line references.** 163 resolve to existing files with enough lines. One cites an archived-ref path, which is legitimate (#1566). About 50 use country-relative paths, such as a bare `CONTENTS.org` beside a named country.
- **Corrections.** One worker claim was found wrong and fixed. #1556 said the four 2021-22 EHCVM-2 waves have no `data_info.yml`; they have two-line stubs.

**Not done.**
- No data builds, library imports or `dvc`. Every data claim that needs a build is marked `unverified-data` in its row, with the command that would verify it.
- GitNexus was not connected. Call-site sweeps used `rg`.

## 2. Summary

| status | n | issues |
|---|---|---|
| resolved | 1 | 778 (and the already-closed 1457) |
| partial | 25 | 140 449 552 562 791 798 824 837 845 1044 1045 1118 1119 1136 1226 1247 1281 1288 1436 1437 1441 1454 1460 1483 1529 |
| open | 50 | 226 458 468 567 698 699 701 702 740 830 905 920 926 939 956 975 977 1009 1048 1061 1072 1082 1083 1084 1100 1101 1103 1107 1122 1131 1134 1135 1138 1139 1141 1142 1145 1235 1287 1438 1442 1451 1482 1518 1533 1539 1556 1566 1570 1598 |
| needs-decision | 44 | 279 438 439 450 737 787 792 794 828 833 862 913 1064 1068 1069 1073 1079 1080 1085 1102 1108 1116 1121 1129 1130 1148 1228 1230 1485 1497 1502 1509 1519 1531 1536 1537 1551 1557 1559 1560 1562 1565 1571 1577 |
| needs-acquisition | 15 | 560 647 706 804 816 821 976 1000 1179 1276 1525 1530 1552 1555 1574 |
| excluded-in-flight | 2 (+PR) | 587 1275 (PR #1604) |

| action | n | issues |
|---|---|---|
| close | 3 | **778** (resolved by #1030 `dbab68d8c` and #1162 `5519de059`); **798** (code fixed in #1025 `f8e14a7c2`; the remaining regrade *is* #1438, so it folds into #1438); **913** (duplicate of #1130, which takes over options A/B/C; the warning half is #828) |
| split | 5 | **552** (new Tier B: wire the four held EHCVM-2 2021-22 waves); **562** (new Tier C: phase 3b quantity-from-price imputation; parent closes); **845** (new Tier B: Panama 2008 `Price` is a mean beside summed Q/E); **1460** (new Tier B/C: the documented Nigeria `yield_kg(..., area_t=pp_ph_area_t(c))` call now raises after #1690, see `transformations.py:5637` and `tests/test_gh1460_yield_area_t.py:140`); **1483** (new Tier C: parcel-level s16a and DAP nutrient basis) |
| narrow | 45 | 140 279 438 449 468 560 567 647 791 816 821 824 833 837 905 939 975 1000 1044 1045 1103 1107 1118 1119 1130 1136 1148 1179 1226 1235 1247 1276 1281 1287 1288 1436 1437 1438 1441 1442 1497 1529 1531 1556 1565 |
| ask (C item to put to the maintainer) | 22 | 737 792 926 1064 1073 1085 1102 1108 1142 1228 1502 1509 1519 1536 1537 1551 1557 1559 1560 1562 1571 1577 |
| none | 60 | the rest; each has a one-line reason, and many `needs-decision` rows carry a ready C-item text |

Tier counts over all rows: A 7, B 55, C 57, D 20.

**New issues drafted by the coordinator (§5, 4a):**
- Malawi 2010-11 `Value_sold` 9,999,999 (Tier B, S).
- `crop_diversity` docstring counts (Tier B, S).
- Optional split from #1571: a condition-keyed sale-price pool (Tier C).

## 3. Cross-batch findings worth reading first

1. **Togo, Benin and Côte d'Ivoire 2021-22 wiring exists only on archived refs.** The refs are `refs/archive/2026-11-11/isa/wire-{togo,benin,civ}-2021-20261104`, 13, 26 and 22 commits ahead. Development has two-line stubs only.
   - Blocked on these: #1598, #1566, #1557, #1556, the #552 split, and #791's residual.
   - Restoring them, after fresh review, is the cheapest route to four waves of data.
2. **Implementation of the ruled #1442 is archived only, behind #1509.** Its commit `d81358091` is on `refs/archive/2026-11-11/isa/burkina-bundle-20261104`, and #1509's fix `cc962ee15` is also archived only, on `refs/archive/2026-11-11/isa/framework-1509-purchases-20261105`. Ruling #1509 unblocks two issues.
3. **#1460 split overlaps #587, which another agent is working on.** #1690 added PH-round `plot_features` for Nigeria W1-W3, so the documented call `yield_kg(cp, pf, area_t=pp_ph_area_t(c))` (`transformations.py:5637`) now raises, as the updated test pins. Route the docstring fix through the #587 owner.
4. **Paired questions that should be ruled together:**
   - #1068 with #1129 (`unitvalue` for `U_UNKNOWN`/`Value` rows; CLAUDE.md's "gives 1" sentence must change with it).
   - #1502 with #1518 (Nigeria W2/W3 shipped conversion tables, food side and harvest side).
   - #1107 with #1483's Guinea-Bissau item, and #1119-GNB with #1539 (one Guinea-Bissau re-warm).
   - #1061 with #1044 (EthiopiaRHS `harmonize_unit`).
   - #1100 and #1101 with #698 (the umbrella cannot close until these are fixed).
   - #737 with #468 (the EthiopiaRHS "WRONG WAVE" finding).
   - #1441 with #449 (one NBS/LSMS bridge request).
5. **Stale statements found in tracked files** (none needs a code change; listed for a docs sweep):
   - `CLAUDE.md:161` / `AGENTS.md:161`: "Still open, a DIFFERENT route … Own issue". This appears superseded by #1599 (`c964c6c4f`), at `country.py:6150-6166`. See 4c.
   - `GhanaLSS/_/CONTENTS.org` gives the 2007 redenomination as 100:1 at `:2888` and as 10,000:1 at `:3122` (raised under #1533).
   - `GhanaLSS/_/CONTENTS.org:3405-3410` predates the affinity-only ruling (#1100).
   - `Uganda/_/CONTENTS.org:3097` still says "no verdict" (#647, ruled 2026-11-04).
   - `Senegal/_/CONTENTS.org:857` says Site Q does not screen `food_prices`. It does, since #1544.
   - Guinea-Bissau `data_scheme.yml:155` and `crop_production.py:18,120` say there is no `harmonize_food`; there is one.
   - `Togo/_/CONTENTS.org:466` contradicts #1557 on whether codes 659/660/662 are labelled anywhere.
   - `docs/guide/label-selection.md` gives Malawi 42,206 rows at `:100` and 42,240 at `:122` (#787).
   - `GhanaLSS/1991-92/_/data_info.yml` declares `sample:` twice. The served (second) block carries the stale comment (#1142).
   - `CotedIvoire/_/CONTENTS.org:1113` (#1562) is accurate, but the #1562 investigation commit is not on development.
6. **Archived ref safe to drop:** `refs/archive/2026-11-11/docs/1436-mali-p2-manual-20261110`. Its content was carried by #1687 (`Mali/_/CONTENTS.org:3870`).

## 4. How to apply

- **`REPORT.md` is authoritative.** `parts/REPORT-*.md` are the raw per-batch drafts, kept for provenance. The coordinator's corrections were applied only here and in `TRIAGE.tsv`: #1118 gains Guinea-Bissau; #1556 states that the stubs exist; #1148 is narrowed; the #1571 split drops Tanzania and Ethiopia; and the 851 count and #778 call are noted.

- `TRIAGE.tsv` (tab-separated; header row) has one row per open issue plus #1457 and PR #1604. Columns: number, title, countries, tier, status, evidence, action, effort, depends_on, notes.
- Every `close` / `split` / `narrow` / `ask` row has a ready-to-paste draft in §8 under `### #N`. `split` drafts include the residual issue (title, body, tier) and the parent comment; file the residual first, then substitute its number for `#NEW`.
- The coordinator re-read every close and split. Close #778: its evidence was re-checked on development (`country.py:5222-5228`; the four private-root tests exist, and `tests/test_ghanalss_visit_derivations.py:201` is also private, which the close comment can mention). **#778 is the coordinator's call:** its carried policy note, that `assume_cache_fresh=True` serves an unstamped country-script parquet, loses its home if #778 closes, because #1071 is closed (2026-11-11). Either close #778 as drafted, treating the note as the documented contract (CLAUDE.md limitation (c)), or split the note into a Tier C residual ('should assume_cache_fresh refuse a hashless country parquet?') and then close. Close #798 and close #913 are consolidations into #1438 and #1130 respectively; apply the #1438 / #1130 narrows in the same sitting so nothing is lost. #913 is a DECISION-titled issue: closing it as a duplicate is a reconciliation call, not a decision.

## 5. Specific items from the brief

## 5.1 (brief 4a) Side findings of the #1457 sale work

Source: `isa-sm-20261103/reports/multi-plot-sales/REPORT_rulings.md` Residuals §3 and `REPORT_final.md` §9. #1457 was closed 2026-11-12 14:37Z (Sue); its closing comment says these are "being reconciled separately". Each one was checked against existing issues and against development `e5fdd2fa8` (source reading only).

| # | Finding | Already filed? | Holds on development? | Outcome |
|---|---|---|---|---|
| 1 | Site Q does not screen sold columns | **Yes**: #1569, closed 2026-11-09 by PR #1650 (`42a25fabc9`) | **No.** `lsms_library/quantity_audit.py:457` screens `crop_production` `Quantity`, `Quantity_kg`, `Quantity_sold` and `Sale_price` (`Value_sold / Quantity_sold` in the sold unit, lines 341-354, 775-850) | **Drop** (resolved). The residual "`Value_sold` alone is never screened" is folded into item 4. |
| 2 | Niger's `KgFactor_sold` not wired | **Yes**: #1571 (coverage/eligibility), with #1560 as the field-meaning root | Yes (Niger serves no `KgFactor_sold`; #1571 holds the field-meaning question) | **Drop** (already filed; B03 row for #1571). |
| 3 | A condition-keyed price pool cannot see `Farm-level` rows | **Partly**: one sentence in #1571's body ("Separately, a price pool keyed by `condition` cannot see `Farm-level` rows"), filed as a Niger issue | Yes, and not only in Niger. Malawi `Farm-level` rows carry `condition='unknown_condition'` with the sale's state in `Condition_sold` (`Malawi/_/malawi.py:2396-2400`); Mali and Niger use the same `CONDITION_NA = 'unknown_condition'` (`mali.py:927`, `niger.py:611,704-706`). `transformations.median_price_valuation` stratifies on whatever `item_keys` it is given (`transformations.py:8492`, `:8585-8590`), so `('j','condition')` pools every Farm-level sale price in the `unknown_condition` cell | **Split** out of #1571 as a cross-country Tier C item (draft below). Alternative: keep it in #1571 and retitle #1571 to say the pool point is cross-country. |
| 4 | Malawi 2010-11 `Value_sold` of 9,999,999 | **No** (searches for `9999999` / `9,999,999` find only #1457, #1569) | Source: `Malawi/_/malawi.py:1564-1566` reads `value_sold` with `pd.to_numeric(..., errors='coerce')` and no sentinel mask; Site Q sees it only through `Sale_price`, which needs a `Quantity_sold` and a sold unit. Whether the row still serves: `unverified-data` | **File** (draft below). |
| 5 | Ethiopia W1 §9: short file, value-only rows, unread grams and Birr decimals | **Yes**: #1570 (body line 39 lists all four, plus the W1 `crop_diversity` sold-only count) | Yes (restated by #1690's Ethiopia note, `Ethiopia/_/CONTENTS.org:4613-4615`) | **Drop** (already filed; B03 row for #1570). |
| 6 | Stale `crop_diversity` docstring numbers | **No** | Yes. `transformations.py:8228-8242` names only Mali 2021-22 (70 rows) and Nigeria W3-W5 (2,303) as sources of Farm-level sale rows. Since #1567 (`83e96b39c`), #1568 (`6073f09ad`) and `b90ec6e40` (via #1690), Malawi, Mali 2017-18, Niger, Tanzania and Ethiopia serve them too (round-2 report: Malawi 6,282, Ethiopia W1 2,869), and #1644 (`11714b185f`) records that a Farm-level crop row can carry a harvest. The Nigeria numbers there are already labelled historical (`f18c8e5e8`) | **File** (draft below), Tier B documentation, effort S. |

### Draft issue (item 4)
**Title:** Malawi 2010-11 crop_production: a `Value_sold` of 9,999,999 is served as a sale value
**Tier:** B. **Effort:** S.

> From the #1457 round-2 work (multi-plot-sales `REPORT_final.md` §9 item 4). One Malawi 2010-11 `crop_production` harvest row carries `Value_sold = 9,999,999` MWK. That is the shape of a missing-value code, the sibling of Niger's 999999 in #865, rather than a sale.
>
> On development (`e5fdd2fa8`), `Malawi/_/malawi.py:1564-1566` (`_sale_block`) reads the value column (2010-11: Module I `ag_i03`, Module Q `ag_q03`; `Malawi/2010-11/_/crop_production.py:59-64`) with `pd.to_numeric` and no sentinel mask. Site Q screens `Quantity_sold` and `Sale_price = Value_sold / Quantity_sold` (#1650, `quantity_audit.py:457`), so a value with no `Quantity_sold` is never screened.
>
> To do:
> 1. Confirm the row and its raw source (`unverified-data`): `Country('Malawi').crop_production(waves=['2010-11'])` filtered to `Value_sold >= 9_999_999`, then the same household and crop in Module I/Q.
> 2. Read the 2010-11 agriculture questionnaire or codebook to see whether 9,999,999 is a documented don't-know or refused code.
> 3. If it is, mask exactly that value, as #1594 did for Mali's 99,999,999. Leave everything else as recorded.
>
> Cost: `malawi.py` is the country module, so every Malawi table must be re-warmed. Bundle this with the next Malawi rebuild; #1604 is about to force one. -- Sue

### Draft issue (item 6)
**Title:** crop_diversity docstring: Farm-level counts predate the #1457 rows in five countries
**Tier:** B (documentation of a served transform). **Effort:** S.

> The `crop_diversity` docstring at `transformations.py:8228-8242` ("A `Farm-level` row is not a further occurrence…") names Mali 2021-22 (70 rows) and Nigeria W3-W5 (2,303) as the sources of these rows. It gives Mali's household effect as 50 of 2,857.
>
> Since #1567 (`83e96b39c`), #1568 (`6073f09ad`) and `b90ec6e40` (#1690), Malawi, Mali 2017-18, Niger, Tanzania and Ethiopia also serve Farm-level sale rows. #1457's round-2 report counts Malawi at 6,282 and Ethiopia W1 at 2,869. Separately, #1644 (`11714b185f`) established that a Farm-level row can carry a harvest.
>
> To do:
> 1. Either re-measure `crop_diversity(weight='count')` per country, or label the existing numbers historical, as `f18c8e5e8` did for Nigeria.
> 2. Name every country that now produces these rows.
> 3. Point at #1570 for Ethiopia W1's sold-only count.
>
> Cost: `transformations.py` reaches no `build_transforms_fingerprint`, but it does reach GhanaLSS `nutrition` through `framework_imports_fingerprint` (CLAUDE.md), so even a docstring-only edit moves GhanaLSS. -- Sue

### Draft split from #1571 (item 3)
**Title:** median_price_valuation keyed on `condition` files every Farm-level sale price under `unknown_condition`
**Tier:** C (which state keys an observed sale price). **Effort:** M.

> From #1457 / #1571.
>
> **Background.** In Malawi, Mali and Niger, a sale that no harvest row holds is served on a `plot_id='Farm-level'` row. That row's harvest `condition` is `unknown_condition`; the sale's own state is in `Condition_sold` (`Malawi/_/malawi.py:2396-2400`, `mali.py:927`, `niger.py:611,704-706`). So `median_price_valuation(item_keys=('j','condition'))` pools every Farm-level sale price in the `unknown_condition` cell, while a plotted row's sale price sits under its harvest state. Tanzania and Ethiopia also serve Farm-level sale rows, but their `crop_production` has no `condition` level and no `Condition_sold`, so the point does not arise there. On a plotted row, `Unit_sold` is its `u` by construction and `Condition_sold` is its own.
>
> **Measured on Malawi** (round-2 report): `median_price_valuation` rose 1.6%. Groundnut accounted for +52.9M of the +77.9M, because 34% of Farm-level sales were shelled against 12% of plotted rows. Keying on `('crop','condition')` moved the total far less.
>
> **Question.** Should an observed sale price be keyed on the sale's state (`Condition_sold`) and the valued row on its harvest state (`condition`)? That would mirror #824's ruling for units: `Unit_sold` for the price, harvest `u` for the valued quantity.
>
> **Options.**
> - (a) A separate `price_state_col` that defaults to `Condition_sold` where present.
> - (b) Document only.
> - (c) Leave it to callers.
>
> **Evidence.** #1571's body; `REPORT_final.md` §"Consumers".
>
> **Recommendation.** (a). It mirrors the approved #824 unit rule and moves no build fingerprint: `transformations.py` is read-path, apart from GhanaLSS `nutrition`. -- Sue

Parent comment for #1571, if split: "The cross-country price-pool point (state of an observed sale price) is now #NEW. This issue keeps Niger's sold-kilogram field meaning, coverage and eligibility. -- Sue"

## 5.2 (brief 4b) CONTENTS.org passages that still call #1457 R2b open

I ran `rg -n 'R2b|lone row|states no harvest' lsms_library/countries/*/_/CONTENTS.org` (5 hits), then widened it to "#1457 … held/pending/open". The ruling is recorded in `isa-decisions.org` § "2026-11-12: Single-plot zero/missing-harvest sales, GH1457 R2b -- ANSWERED": the user selected "Keep on plot row (Recommended)". #1457's 2026-11-12 closing comment quotes @ligon: "Keep on plot row".

| path:line | current text | status | proposed replacement |
|---|---|---|---|
| `lsms_library/countries/Malawi/_/CONTENTS.org:5681-5682` | "R2b's singleplot zero/NA choice remains HELD." | **stale (calls it held)** | "R2b was answered 2026-11-12 (GH #1457, "Keep on plot row"): a sale on a household-crop's sole plot row stays on that row when its harvest Quantity is zero or missing, so no Malawi row moves; the r2b_* patches are not applied." |
| `lsms_library/countries/Malawi/_/CONTENTS.org:5660-5661` | "No land/plot allocation, R1/R2a/R2b, price-pool, quantity, unit, ID, weight or scientific-default choice is made; those decisions remain held." (R3 section dated 2026-11-10) | **stale ("remain held")** | "No land/plot allocation, price-pool, quantity, unit, ID, weight or scientific-default choice is made here; R1 and R2a were answered 2026-11-10 and R2b 2026-11-12 (GH #1457)." |
| `lsms_library/countries/Ethiopia/_/CONTENTS.org:4613-4614` | "…Section 12 Q3 harvest, its window, partial grams/Birr arithmetic, and the other #1457/#1570 decisions remain open." | **stale for #1457**: all four #1457 items are settled | "…and the other #1570 decisions remain open (#1457's R1/R2a/R2b were settled 2026-11-10/12; R2b keeps a sale on its sole plot row, which the 2026-10-23 ruling already did here)." |
| `Malawi/_/CONTENTS.org:5695` | "R2a and R2b status, all amounts… stay unchanged." | accurate (says the R1 clarification changed nothing) | none |
| `Nigeria/_/CONTENTS.org:2556`, `:2650` | Nigeria ruling A moves sales "off a lone row that states none"; crop_diversity counts | accurate (the ruling leaves Nigeria's handling as is) | none |
| `Ethiopia/_/CONTENTS.org:4576` ("212 + 182 sales on a lone line stating no harvest keep it, unlike Nigeria"); `Niger/_/CONTENTS.org:3074` ("A lone line stating no harvest keeps its sale … which differs from Nigeria") | describe current placement | accurate | none |

The repo convention is dated sections, with earlier entries keeping their dates (e.g. `Ethiopia/_/CONTENTS.org:4616`). A less intrusive alternative to rewriting the three stale lines is to append one dated entry to Malawi and one to Ethiopia: "2026-11-12 -- R2b answered (GH #1457): keep on plot row", superseding them. CONTENTS.org is excluded from every cache hash, so either edit is free.

## 5.3 (brief 4c) Archived branches with unpublished commits

### `refs/archive/2026-11-11/fix/851-recall-period`
Tip `cf117d101`, merge-base `1485a1484d`; `gh pr list --head` finds no PR.

**Content check.** `git cherry -v e5fdd2fa8 <ref>` marks 20 commits `+`. That does not prove the content is absent: #1671 (`1c1ef12da4`, its commit `00536f900` "feat(851): recover declared-period conversion on current dispatch") re-landed the work under different patch ids. I therefore compared added lines file by file. **Count mismatch with the brief:** the brief says +7. Here 24 commits are unreachable from #1671 and 20 are `+` by patch id. The 7 at the tip, since the merge `4caff6215` (6 non-merge commits: r2 `b2eb966d7`, the WIP salvage `ef66263af`, option A `8539cf965` / `90e4517c9` / `cf117d101`, and one base merge `eae609f94`), are presumably the brief's 7. For that range specifically, the code is fully on development: `recall.py` 93/93 added lines, `country.py` 19/19, `feature.py` 29/29, `data-methods.md` 10/10. Absent: 35 lines of `docs/guide/recall.md`, about 188 of 250 test lines, and the one-line `recall.yml` / `food_recall.csv` notes. The recommendation below holds for both counts.
- **Code that landed:**
  - `recall.py`: 413 of 419 added lines present
  - `country.py`: 67/70
  - `errors.py`: 37/40
  - `feature.py`: 114/142
  - `recall_table.py`: 9/9
  - `docs/guide/data-methods.md`: 25/25
- **Data choices that landed:**
  - Burkina 2014 `n_asks_by_column: {Quantity: 1}`
  - Mali 2014-15 `n_asks: 2`
- **Not ported:**
  - **The `Mali/2014-15/_/mapping.py` docstring (0/7 lines).** Development's `mapping.py:89-91` still says "`visit` (passage) is dropped: as with the EHCVM `vague` it is a sample split, not a repeated measure". That contradicts development's own `Mali/_/recall.yml` (`n_asks: 2`, both passages summed). It is the only factual error left in source.
  - CONTENTS prose: Mali 0/43, Burkina 0/8, GhanaLSS 0/12.
  - The recall evidence/notes strings: Mali `ask_count_basis` is `declared` on development, `measured` on the branch.
  - About 14 branch test functions absent by name from development's `tests/test_recall_period.py` (e.g. `test_mali_2014_15_week_is_half_the_served_two_passage_total`, `test_burkina_2014_quantity_is_the_passage_1_week`). Whether their assertions survive under other names is unverified.

**Bearing on open issues.** #1148's historical section calls this branch "an implementation/integration dependency". #1671 has since satisfied that, so the sentence is stale. The branch does not bear on #1084 (B10a) or #1083 (B09a).

**Recommendation: drop, after a harvest.** Do not restore it as a PR: its base is a 2026-10-20 tree and #1671 supersedes its code.
- **Harvest into one small PR:**
  - the Mali 2014-15 docstring correction
  - the CONTENTS recall notes
  - the `food_recall.csv` evidence strings, regenerated via `scripts/promote_recall_records.py`
  - optionally, the real-data pins as cache-gated tests
- **Cost:** `mapping.py` is in Mali's `binp:` hash, so the docstring alone forces a Mali whole-country re-warm; bundle it with the next Mali rebuild. The CONTENTS/CSV/YAML parts are free.
- Keep the archive ref until that harvest lands.

### `refs/archive/2026-11-11/fix/866-stale-country-parquet-not-laundered`
Tip `cb6ec1ceb`, 2 commits after PR #924 merged.

**Content.** Both unique commits (`31849fdb9`, `cb6ec1ceb`) edit only `.sucoder/handoff.org`: agent session notes, not product code.

**The handoff's three "next actions" are all done:**
- PR #924 merged 2026-09-16.
- PR #923 merged 2026-09-16.
- Deferred issue 1 (`LSMS_BUILD_BACKEND=make` reaching `run_make_target`) became #1039, closed 2026-10-21; the rest is tracked in #1288.
- Deferred issue 2 (`run_make_target(..., wave=w)` laundering the country parquet one wave at a time) was never filed under its own number. It is addressed by GH #1590 / PR #1599 (`c964c6c4f`, commit `3b6a064fa`): `lsms_library/country.py:6150-6166` now certifies a wave fallback's country output only if the producer changed it.

It does not matter to any open issue. **Recommendation: drop.**

**Side finding.** `CLAUDE.md:161` and `AGENTS.md:161` still say "**Still open, a DIFFERENT route (not fixed here).** `run_make_target(method_name, wave=w)` appends the country target … Own issue." #1599 looks to have superseded that. Proposed text: "Closed by GH #1590 (PR #1599, `c964c6c4f`): a wave fallback that reaches the shared country parquet is certified only when the producer changed it (`country.py:6150-6166`)." Before editing, confirm the `use_legacy` branch honours the certification.

### Other archived refs the workers noticed (not in the brief)
- **`refs/archive/2026-11-11/isa/wire-{togo,benin,civ}-2021-20261104`** (13 / 26 / 22 commits ahead, per B01).
  - These hold the only Togo/Benin/Côte d'Ivoire 2021-22 table wiring; development's 2021-22 `data_info.yml` files are stubs.
  - #1598, #1566 and #1557 are blocked on them, as is the wiring issue split from #552 (B10a).
  - **Recommend: restore** as the starting point for that split issue, after a fresh review.
- **`refs/archive/2026-11-11/docs/1436-mali-p2-manual-20261110`** (B03). Its note was already carried by #1687 (`Mali/_/CONTENTS.org:3870`). **Drop.**

## 6. The fix queue: Tier A/B items tractable now, highest leverage first

This queue is ordered by dependency, then leverage: root causes that close several issues come first, and so does silent loss. Items that share a country are bundled so the re-warm is paid once. Fingerprint costs follow CLAUDE.md:
- A country module, a wave `mapping.py`/`data_info.yml`, a country `_/` `.org` or `data_scheme.yml` is a **whole-country re-warm**.
- Fingerprinted framework callables are **corpus-wide**.
- Read-path code, sibling `.yml`, CONTENTS.org and tests are **none**.
- `transformations.py` reaches only GhanaLSS, through `nutrition.py`.

No queue item below is corpus-wide. Every per-item verification should run in a private, empty `LSMS_DATA_DIR` (base vs candidate), with `OPENBLAS_NUM_THREADS=1` and the Makefile's `--dist=loadfile`.

**Q1. Guinea-Bissau label bundle.**
- **Issues:** #1107 (close), #1539 (close), #1119 (GNB half), #1226 (GNB product-94 half), and the GNB item of #1483 (point it to #1107).
- **Countries:** Guinea-Bissau.
- **Fix:**
  - Add `harmonize_input`, mapping the four Portuguese inorganic labels to Urea/Phosphate/NPK, with DAP handled per the Togo precedent or as Other. Add kg routes for `Quilograma`/`Grama`.
  - Add `harmonize_seed_crop` from the country's own `s16bq01` labels. Seed rows with a recorded crop are currently served `(not crop-specific)`, which violates the ruled contract (`2018-19/_/plot_inputs.py:45,125`).
  - Map `Djagatu` to `Jaxatu` and `Beringela` to `Aubergine`, and drop the trailing comma.
  - Bridge community-price product 94 to 95 by code.
- **Files:** `Guinea-Bissau/_/categorical_mapping.org`, `2018-19/_/plot_inputs.py`, `2018-19/_/mapping.py`, the stale comments in `_/data_scheme.yml:155` and `2018-19/_/crop_production.py:18,120`.
- **Fingerprint:** one country (GNB).
- **Verify:**
  - `nitrogen_kg(GNB)` has more than 0 matched rows.
  - `plot_inputs` is frame-equal apart from the label columns.
  - `seed_kg` is conserved.
  - `Feature('plot_inputs')` has a unique index.
  - `community_prices` gains the product-94 rows and nothing else moves.

**Q2. Silent declared-wave skips in country concatenators (#1287).**
- **Countries:** GhanaLSS, Senegal.
- **Fix:** In `GhanaLSS/_/community_prices.py:30` and `Senegal/_/plot_features.py:28`, replace `except Exception: continue` with a declared-wave refusal (the #1241 pattern).
- **Fingerprint:** one country each. Bundle with Q4 (GhanaLSS) and Q3 (Senegal).
- **Verify:** a private-root broken-wave repro per country (it must now raise, naming the wave), and base frame equality on a healthy build. `unverified-data`: whether the default route reaches the skip today.

**Q3. EHCVM minted `Manquant` unit (#1118).**
- **Countries:** Benin, Togo, Burkina Faso, Senegal, Côte d'Ivoire, Guinea-Bissau.
- **Fix:** Replace `fillna('Manquant')` with the missing-unit sentinel `'Unknown'` in the six 2018 `plot_inputs.py` and `Burkina_Faso/_/s16b_2021.py:133`. None of these countries maps `Manquant` to `Unknown`. The coordinator found the sixth site, `Guinea-Bissau/2018-19/_/plot_inputs.py:126`; do it inside Q1's re-warm.
- **Fingerprint:** six countries, one re-warm each.
  - Bundle Senegal with #1119-SEN (align maize/cowpea `harmonize_seed_crop` to `crop_production`, the #1203 pattern) and with Q2-SEN.
  - Bundle Burkina with Q6/Q7.
- **Verify:**
  - The per-wave count of `u=='Manquant'` goes to 0.
  - Country output equals Feature output.
  - `nitrogen_kg`/`seed_kg` are frame-identical except the relabelled rows.

**Q4. GhanaLSS bundle, one re-warm (plus GhanaSPS for #1100).**

*Do now:*
- **#1142:** recover the 29 `S0A` households into 1991-92 `sample` with `v=clust`, and delete the duplicate first `sample:` block, moving its correct comment to the served block. Their 142 roster rows are served with NaN `v` today. Ask the `Rural`/`strata` sub-question first; serving them NA meanwhile is acceptable.
- **#1072:** pass `no_kg=` in `GhanaLSS/_/nutrition.py:180`, and add a gated `no_kg_factor.yml` hash part (option A, measured to touch 17 of 17 GhanaLSS tables only).
- **#1100 / #1101:** give the conflating parent-or-parent-in-law code `[1, 0, null]` and code 6 `[null, null, null]` distinct labels, and correct `CONTENTS.org:3405-3410`. `kinship.yml` is read-path.
- **#1082:** rename the duplicate `FOODCD` tables after a questionnaire check. The proposed within-file duplicate `#+name:` guard in `local_tools` is **unmeasured**; stash-probe it, because it may sit in a fingerprinted closure.
- **#926:** drop `encoding='ISO-8859-1'` on the three UTF-8 org reads in `ghanalss.py`. The output is byte-identical.
- **#1122:** the three stale registration comments that were deferred to "the next country rebuild".
- Q2's GhanaLSS half.
- The 4a `crop_diversity` docstring, since `transformations.py` moves GhanaLSS anyway.

*Diagnose first, then fix in the same re-warm if ready:*
- **#1145 + #1141:** measure blank-screener rows that carry answers. Gate on `!= 'no'`, and apply the same gate in `inputs_produced_farmgate`.
- **#1138:** cluster 2198, HID 112516.
- **#1139:** the 114518/114519 cover-page comparison.

- **Fingerprint:** one country (GhanaLSS); GhanaSPS too for #1100.
- **Verify:** per item, a before/after on a private cold build:
  - `sample` 1991-92 gains 29 households.
  - The 2,962 #1100 rows move to an NA `Affinity`, and nothing else moves.
  - Egg kg in `nutrition` falls by the measured amounts.
  - `derivation_inputs('8h-farmgate')` joins 1:1 to the stamped rows.

**Q5. Wire EHCVM-2 2021-22 for Benin, Côte d'Ivoire, Guinea-Bissau and Togo (the #552 split).**
- **Fix:** Start from the archived `wire-{togo,benin,civ}-2021-20261104` refs, rebased and freshly reviewed. Guinea-Bissau has not been started. Order: `sample`, `household_roster`, `cluster_features`, then food (with #1598's own-country NSU weights), shocks (#1556's ruling) and assets (#1566's s12q09 check).
- **Files:** `{C}/2021-22/_/data_info.yml`, `{C}/2021-22/_/mapping.py`, `{C}/_/data_scheme.yml`, CONTENTS.org.
- **Fingerprint:** one country each.
- **Verify:** a cold private-root build per new table, `scan.py` restricted to the four countries, and the CONTENTS idiosyncrasies (Togo's Lomé domain discrepancy, the 2021 same-cluster revisit).
- **Unblocks:** #1598, #1566, #1557, #1556, and #791's residual.

**Q6. Burkina Faso 2021-22 `crop_production` driver.**
- **Issues:** the #1436 second residual, the Burkina half of #1437, and #824.
- **Fix:** Add a driver from panel-component s16c plus s16d sales on Mali 2021-22's #1466 pattern: a single harvest row where one exists, else Farm-level, per the #843 option-d ruling. Keep harvest-from-dispositions as a separate C question.
- **Files:** `Burkina_Faso/2021-22/_/crop_production.py` (new), `_/data_scheme.yml`, `burkina_faso.py`, CONTENTS.org.
- **Fingerprint:** one country (Burkina); bundle with Q3-BFA and Q7.
- **Verify:** a cold build. `Value_sold` must equal the raw s16d sum, and `harvest_kg`/`yield_kg` row counts must be reported.

**Q7. Burkina date spec (#1045, Burkina 2021-22 first).**
- **Fix:** Move `interview_date` to a declarative `datetime:` spec, as Ethiopia W4/W5 did in #1413.
- **Fingerprint:** Burkina only; bundle with Q6.
- **Verify:** a test beside `tests/test_gh1045_parse_datetime.py`, and `datetime_reports(country='Burkina_Faso')` showing no new `unparsed`.
- **Later:** #1442 follows once #1509 is ruled. Rebase the archived `d81358091` (C05/C06 as Expenditure) into this same Burkina re-warm.

**Q8. Kosovo 2000 kinship code list never fires (#1103).**
- **Countries:** Kosovo.
- **Problem:** `Kosovo/2000/_/data_info.yml:31-46` holds the code list, but the labels arrive truncated (`Other re`, `Niece/Ne`). In-law codes 8, 9 and 11 are apparently served as blood labels with Affinity `consanguineal`. `unverified-data`: run a cross-tab of `s1a_q03` codes against the served `Relationship`.
- **Fix:** make the mapping reach the code (string keys or a code read).
- **Fingerprint:** one country.
- **Verify:** the cross-tab, and before/after Affinity counts.

**Q9. EthiopiaRHS units (#1061 with #1044).**
- **Fix:**
  - Recode 1994a units through `Codeunit1unit1234.SPS` (round-1 scheme) before decoding.
  - Finish the 1989 `food89` unit decode (#1044's residual). The float-code half landed in #1067 (`277226e56`).
- **Fingerprint:** one country (`harmonize_unit` / `ethiopiarhs.py`).
- **Verify:** a per-code row count before/after, and `kg_factor_sources` for 1989 and 1994a.

**Q10. Panama (#845 split, then #1009).**
- **#845 split:** serve 2008 `Price` as a ratio of sums, or register the mean as a derivation, in `Panama/2008/_/food_acquired.py:185` (8 purchased rows expected to move).
- **#1009 (Tier A):** `_market_lookup` raises before the keep-unlabelled-`m` path (`country.py:3352-3356`), so `Feature(..., market='Region')` drops Panama. It needs a source-backed planning Region per wave. Choosing the source is a small documented choice, not a framework change.
- **Fingerprint:** one Panama re-warm for both.
- **Verify:** a private-root rebuild, and Country plus Feature with `market='Region'`.

**Q11. Nigeria `yield_kg` docstring (the #1460 split).**
- **Fix:** Correct the `pp_ph_area_t` example at `transformations.py:5637` to subset the PP rounds, as the updated test does.
- **Owner:** route through the #587 owner (in flight).
- **Fingerprint:** probably none (outside GhanaLSS `nutrition`'s import closure); confirm with a stash-probe.
- **Verify:** `tests/test_gh1460_yield_area_t.py`.

**Q12. Items that move no fingerprint (cheap; can go any time):**
- **#740:** remove the Pakistan `xfail` (`tests/test_pakistan_cluster_features.py:82-93`). Assert there is no `GrainCollapseWarning` and that there are 300 `(t, v)` rows on a private cold build.
- **#1136 item 5:** make `bench/feature_audit/scan.py` drain `sampling_reports()`.
- **#1247:** add `panel:` boundary records for Iraq, Timor-Leste and Albania in `sampling.yml`. The gloss half landed in #1677 (`67b9406c1`).
- **4b:** the three CONTENTS.org R2b sentences.
- **4c:** the `CLAUDE.md:161`/`AGENTS.md:161` sentence.
- **#702:** the Afghanistan CONTENTS note.

**Q13. Measurement and verification only (no code expected):**
- **#1483:** public Country/Feature cold+warm validation of the #1687 Burkina/Mali household-input routes.
- **#1281:** a footer census of `var/` manifests written before `c964c6c4f`.
- **#1288:** literal source declarations, one country per PR. A wave script's text is in that country's hash.
- **#1482:** a Tonne-row table against the questionnaire unit card.
- **#1235:** a diagnosis of the CIV `350` quantity against the held CAPI questionnaire.
- **#1441:** why EthiopiaRHS 1989, Tajikistan 2007 and Pakistan 1991 clusters lack `cluster_features` rows.

**Q14. Bundle with a rebuild that is already coming:**
- **Malawi 2010-11 `Value_sold` 9,999,999 (4a draft):** bundle with #1604's Malawi re-warm. The #1604 ID guard moves every fingerprint anyway.
- **Mali 2014-15 `mapping.py:89-91` docstring (4c harvest of the 851 branch):** bundle with the next Mali re-warm.

**Q15. Lowest priority:**
- **#905:** the vectorised `min_count` sweep (10 sites, 8 one-country re-warms; performance, not data).
- Non-ISA wiring: #458, #567, #701, #977 and #1287's non-ISA notes. Each is one country.

## 7. For the human: C items (decisions) and D items (acquisition)

Each C item has an ~8-line Question / Options / Evidence / Recommendation draft under its issue in §8 (or, for coordinator items, in §5). None was decided here.

### C items (57 rows tiered C, plus 1228 which is B with a decision): action `ask` = put to the maintainer now; `narrow`/`none` = the decision is already stated in the (narrowed) body

| # | action | countries | title |
|---|---|---|---|
| 226 | none | cross-country (Malawi/Uganda baseline; GhanaL | feat: principled auto-aggregation script for harmonize_food's Aggregate Label column |
| 279 | narrow | cross-country (framework; worked example Ethi | Feature: bespoke label schemes without modifying the package tree |
| 438 | narrow | Burkina Faso; Benin; CotedIvoire; Guinea-Biss | Non-ISA item stack: finish source-backed label foundations and module-specific extensions |
| 439 | none | Cambodia; Ethiopia; Malawi; Tanzania; Uganda | Exploit LSMS+ individual/intra-household (male vs female) disaggregation — new feature direction |
| 449 | narrow | Tanzania | Pull WB geovariables (weather/distance/soil) for older Tanzania NPS waves (W1–W3) |
| 450 | none | Ethiopia; Malawi; Mali; Niger; Nigeria; Tanza | Expose the LSMS-ISA harmonised dataset (geovariables/weather) as a feature via the household-id bridge |
| 562 | split | GhanaLSS | GhanaLSS Phase 3: community_prices feature + inverse price->quantity imputation (split from #109) |
| 737 | ask | EthiopiaRHS | DECISION: EthiopiaRHS TLU -- relabel the *lvs5 files to t='1989' and apply the library's tlu() (A), or serve I |
| 787 | none | cross-country (Niger, Burkina Faso, Senegal,  | labels=: decide ambiguous targets, curate variants and finish numeric-target examples |
| 792 | ask | GhanaSPS; Tajikistan (+ unspecified strata/Ru | Document the remaining sample-design gaps after closing the handle leak |
| 794 | none | Benin; Burkina Faso; Senegal; Togo | Format118 mojibake: design paired decode/crosswalk migration for strategy-B countries |
| 828 | none | cross-country (Guatemala-type and every EHCVM | food_expenditures(basis='total'): choose and implement missing nonpurchase-value disclosure |
| 830 | none | cross-country (GhanaLSS, GhanaSPS records) | Extend monetary scale diagnostics beyond isolated neighbor jumps |
| 833 | narrow | Uganda; Ethiopia (Niger, Tanzania ruled/closa | Food/crop identity: finish source-backed processing-state decisions and Niger code166 |
| 837 | narrow | Ethiopia; Tanzania | Resolve source-grain cluster disagreements without bypassing the signed audit |
| 845 | split | Panama; GhanaLSS; Guatemala; Serbia; Uganda ( | Price provenance: is Price elicited or derived for Panama/GhanaLSS/Guatemala/Serbia; explain Uganda's ~5% gap; |
| 862 | none | Uganda; Tanzania; Ethiopia (cross-country sch | Schema proposal: plot_cultivation, a per-season plot decision-maker table (#167's reserved name, #439's conven |
| 913 | close | GhanaLSS | DECISION: GhanaLSS 1991-92 own production -- value the 28% of rows carrying both factors (A), define u='All' a |
| 920 | none | GhanaLSS (framework read path) | Profile remaining per-call read-path cost after cache-rebuild diagnostics landed |
| 926 | ask | Burkina Faso (format118 key); GhanaLSS/GhanaS | Curated encodings: finish live format118 keys and the paired transcoding policy |
| 939 | narrow | GhanaLSS (Uganda reference) | Check whether partition ranking is stable in min_goods before relying on the aggregation selection |
| 1064 | ask | framework (data access) | DECISION: data_access still accepts the in-tree countries/.dvc/s3_creds as a credential fallback (reader rung  |
| 1068 | none | cross-country (EthiopiaRHS, Niger, Nigeria, P | DECISION: native-unit modes treat U_UNKNOWN as a pooled pseudo-unit (food_prices(units='unitvalue'), food_quan |
| 1069 | none | framework (all) | Cache mechanism still inside the build fingerprint after #908/#1040 (eviction, replay, pre-warm, footer reader |
| 1073 | ask | GhanaLSS | DECISION: GhanaLSS egg units priced like one egg (All, Nut, Finger, Stick, Barrel, Bunch) get ~1 kg each -- ex |
| 1079 | none | cross-country (framework) | DECISION: a declared NON-empty categorical table lacking the requested value column still falls through to a l |
| 1080 | none | cross-country (framework) | DECISION: the read-time categorical merge treats an empty declared table as a silent override (and can lose gl |
| 1084 | none | Cambodia; EthiopiaRHS; GhanaSPS | Recall research: finish remaining non-ISA questionnaire evidence |
| 1085 | ask | framework (catalog/Country) | DECISION: a country entry that is a symlink escaping countries_root() is listed by countries() but refused by  |
| 1102 | ask | Timor-Leste; Cambodia; Guyana; Azerbaijan; Ko | DECISION: kinship conflations of the #698 shape outside its wording (Timor-Leste Adopted/stepchild, Cambodia A |
| 1108 | ask | GhanaLSS | GhanaLSS `Fertilizer` (1,091 rows with a quantity) has no product identity, so it gets no N share |
| 1116 | none | Nigeria 2018-19; Niger 2011-12; Senegal 2021- | #600 residual: override records take their note and validation from _OVERRIDES, over a hand edit to the file |
| 1121 | none | GhanaSPS; GhanaLSS | plot_labor.source: decide GhanaSPS/GhanaLSS taxonomy and containment |
| 1129 | none | GhanaLSS (Panama to measure) | food_prices(units='unitvalue') serves exactly 1.0 for every u='Value' row: 1,107,464 GhanaLSS rows across all  |
| 1130 | narrow | GhanaLSS | GhanaLSS 1991-92 own-produce valuation: decide the `All` unit, then extend `8h-farmgate` to GLSS3 |
| 1134 | none | cross-country (sampling.yml) | Sampling design tags: add structural tags with their first evidenced records (#793 residual) |
| 1135 | none | Nigeria; Tanzania; Uganda; Burkina_Faso; Ethi | Cluster identity: verify complete-mover and physical-cluster measurements |
| 1136 | narrow | cross-country (sampling.yml / strata) | Sampling: implement approved design-vocabulary expansion, curate records and drain reports |
| 1148 | none | Burkina Faso; GhanaLSS | Household recall exposure: adjudicate passage-status evidence before normalization (Burkina 2014; GhanaLSS fol |
| 1228 | ask | Malawi | Malawi crop_production: seasonal crop "Other" is mostly cowpea, which harmonize_crop has no label for |
| 1230 | none | cross-country (Uganda, Niger declared; Peru,  | Org-key parsing: decide lexical storage and numeric-key compatibility |
| 1485 | none | Malawi; Ethiopia (cross-country transform) | Anonymous fertilizer-rate routing: decide automatic recovery policy; omission diagnostics implemented |
| 1497 | narrow | Tanzania | Tanzania 2019-20: updated_ids links 13 split-offs rather than the frame household on the same y4_hhid |
| 1502 | ask | Nigeria | Nigeria W2 and W3 ship food conversion tables the build does not read; W3's inferred factors are 0.556x of the |
| 1509 | ask | Burkina Faso; Cambodia; Serbia; Uganda; Ghana | food_kg_factors pools self-valued non-purchased rows into the kg inference |
| 1519 | ask | Senegal | Senegal: decide treatment of near-zero quantities already present in released data |
| 1529 | narrow | Niger | Niger 2011-12: resolve section 3E crop-cycle area semantics and yield attachment |
| 1531 | narrow | Togo; Benin; Guinea-Bissau | EHCVM GPS plot area: resolve units and export fields in Togo, Benin and Guinea-Bissau |
| 1536 | ask | Uganda | Uganda ea in 2009-10: fill the 569 NA rows from the household's label in another wave? |
| 1537 | ask | Nigeria | Nigeria: generic Pepper food-price label lacks an Aggregate mapping |
| 1551 | ask | Nigeria | Nigeria #843: attach W3-W5 sales by the sale's own crop code? |
| 1557 | ask | Togo | Togo: codes 659, 660, 662 are bare in 2018 and labelled in 2021-22 |
| 1559 | ask | Niger; Mali | Site Q cannot see a survey kilogram wrong against its own quantity |
| 1560 | ask | Niger; Mali | Sold kilograms are recorded and unread in Niger and Mali |
| 1562 | ask | Niger; Mali; Senegal; Burkina Faso; Benin; Co | EHCVM 2018-19 coping order: hooks list strategies by code |
| 1565 | narrow | Niger; Burkina Faso; Benin; Senegal; Guinea-B | EHCVM leavers: all-NA roster rows in Niger and Burkina 2021-22, and reasons for leaving served nowhere |
| 1571 | ask | Niger | Niger sold kilograms: resolve field meaning and state/eligibility policy |
| 1577 | ask | cross-country (Mali, Burkina Faso, Niger, Sen | No machine-readable record of what strata means per country and wave |
| new | file | cross-country | (4a split from #1571) median_price_valuation keyed on `condition` files every Farm-level sale price under `unknown_condition` |

**Ask first (highest leverage):** #1509 (food_kg_factors eligibility; unblocks #1442 and the archived `cc962ee15`); #1068 + #1129 together (`unitvalue` on `U_UNKNOWN`/`Value` rows); #1502 + #1518 together (Nigeria shipped conversion tables); #1559/#1560/#1571 (sold kilograms, Niger/Mali; one root); #1577 (machine-readable strata meaning; #792 and #1574 feed it); #1142 sub-question (Rural/strata for 29 recovered GhanaLSS households, unblocks Q4).

### D items (acquisition / external data)

| # | status | countries | title |
|---|---|---|---|
| 552 | partial | Benin; CotedIvoire; Guinea-Bissau; Togo; Jama | WB microdata coverage gaps (June 2026 audit): EHCVM-2 ×4 + classic-LSMS, tiered by obtainability — post-0.8.0 |
| 560 | needs-acquisition | Cambodia | Cambodia nutrition: obtain + wire the Food Conversion Table (SMILING/ASEANFOODS) — split from #108 |
| 647 | needs-acquisition | Uganda | question: Uganda 2019-20 crop_production silently drops the PART-harvest columns (s5aq06*_11/_21) |
| 706 | needs-acquisition | CotedIvoire | CotedIvoire 1985-89: four unwired community-questionnaire files (COMM85-88.DAT), rural-only |
| 791 | partial | Benin; CotedIvoire; Guinea-Bissau; Togo; (8 E | Add UEMOA PHMECV as a source: 4 missing EHCVM 2021-22 waves + 8 ERI-ESI studies (access is human-reviewed) |
| 804 | needs-acquisition | Tajikistan | Tajikistan 1999 household_roster: code 14 served as 'lodger' but the questionnaire prints 14 = Other (13 = Lod |
| 816 | needs-acquisition | Burkina_Faso | Burkina Faso coverage starts at 2014: the Enquête Prioritaire series (1994, 1998) exists and is a different in |
| 821 | needs-acquisition | Mali | Mali 2021-22 community prices: verify availability of a cluster-grain source |
| 975 | open | Kyrgyz Republic | Kyrgyz Republic: four waves of data (1993, 1996, 1997, 1998) and no _/ config at all |
| 976 | needs-acquisition | Afghanistan; Bosnia-Herzegovina; Brazil; Bulg | Sentinel sweep: 2,180 of 6,803 .dta blobs were unobtainable (absent from L1, unfetchable from S3); 20 waves en |
| 1000 | needs-acquisition | Uganda | Reconcile Uganda FCT 4003's generic zero note with its specific yellow-variety carotenoid derivation |
| 1179 | needs-acquisition | Mali | Mali EAC-I community prices: diagnose positive-priced entries with unresolved unit codes |
| 1276 | needs-acquisition | Burkina_Faso | Burkina 2021-22: acquire and wire both disjoint EHCVM components with separate provenance |
| 1438 | open | cross-country (Tanzania, Mali, Burkina_Faso e | Coverage matrix: latest.csv (2026-09-18) is stale; regrade |
| 1525 | needs-acquisition | Uganda | Uganda 2019-20: Cassava codes 1081 and 1082 do not behave as their labels say |
| 1530 | needs-acquisition | Niger | Niger: Oignon frais falls to 4 rows in 2021-22 |
| 1552 | needs-acquisition | Nigeria | Nigeria W1/W2: resolve native cross-plot sale repeats with source evidence |
| 1555 | needs-acquisition | Niger | Niger 2018-19: crop kilograms need release and calibration authority |
| 1574 | needs-acquisition | Mali | Mali 2017-18 strata serves EAC-I design codes |

Notes: #975 (Kyrgyz Republic) and #567 (India) look like acquisition but the documents are already held (B10b); #1084 is reading work, all nine cells have a questionnaire held (B10a); #1438 is an operational regrade (`make matrix` as a burst job), not acquisition.

## 8. Per-issue drafts

Order: LSMS-ISA / EHCVM batches first, then framework, then GhanaLSS, then other countries. Each worker read the issue body and all comments, the timeline, merged PRs by number/keyword/path, CONTENTS.org (naming the idiosyncrasy), and the cited code on `e5fdd2fa8`.

## 8.0 Coordinator supplements

A completeness check ran over every `TRIAGE.tsv` row against its §8 section. It looked for these marks:
- **narrow:** a title, a "Resolved so far" header and "-- Sue".
- **split:** a residual and a parent comment.
- **ask / needs-decision:** Question, Options, Evidence and Recommendation.

It flagged 17 sections. One was a false positive: #1288's signature sits inside a code block. The other 16 are completed here.

**C items whose worker draft lacked one or more of the four parts:**

- **#1497 (Tanzania 2019-20 split-offs).**
  - *Question:* the `_map_with_head_tracking` fallback when a former head is absent or no longer head.
  - *Options:* (a) keep the first-cover-row fallback and document it; (b) give the canonical id to the original (`hh_a10`) frame household; (c) leave those families unlinked.
  - *Evidence:* PR #1615 (`20afc61575`) gives 20 fallback pair occurrences across 17 families, 17 of them absent-head. In the original observation, 218 of 238 single-frame split-offs link the frame household only.
  - *Recommendation:* (b). It matches the dominant observed pattern. Ship it as a reviewed `updated_ids.json` diff. Note that it is a panel-ID change, which the 2026-11-12 standing hold reserves for explicit approval.
- **#833 (food/crop identity).**
  - *Question:* the Uganda fold for each of the five families (Beans 140/141, Peas 145/162, Rice 110/1101-1103, Ginger 1352/1353, Soybean 1761/1762).
  - *Options:* per family, keep today's single Aggregate (status quo) or split by state.
  - *Evidence:* `Uganda/_/PROCESSING_STATE_REVIEW.md`. The six-family rule is not authority to extend it.
  - *Recommendation:* rule family by family from the memo's cells. Default to status quo wherever the codes do not name a state (Peas 145; Rice's flour entry). Treat Millet 115 as an evidence task (the questionnaire code list), not a decision.
- **#1565 (EHCVM leavers).**
  - *Recommendation:* (1) keep departed persons out of `household_roster`. That is today's served Burkina result, and it is consistent with `roster_to_characteristics` excluding departed members. (2) If reasons are wanted, add a separate departures table for Burkina only, from WB 6224's coded `s01q00b`. Niger's release has no coded reason.
- **#926 (curated encodings).**
  - *Question:* fold #926 into #794 and close it?
  - *Options:* (a) fold and close now; (b) keep #926 open for the three GhanaLSS ISO-8859 reads.
  - *Evidence:* worker inventory. `ghanalss.py:584,1137,1225` read UTF-8 org files as ISO-8859-1, and the tables are pure ASCII today.
  - *Recommendation:* (b) until the reads are fixed inside the GhanaLSS bundle (queue Q4), then close into #794.
- **#1142 (GhanaLSS 1991-92 sample), evidence for the sub-question.**
  - `GhanaLSS/1991-92/_/data_info.yml` declares `sample:` twice (lines 13 and 71, verified). `POV_GH.DTA` holds 4,523 households against 4,552 in `S0A`/`S1`, and GLSS3 is self-weighting.
  - *Recommendation:* (C) now: recover `v` and leave `Rural`/`strata` NA, which Site B reports. Then (B) if `S0A` carries a locality code, which is `unverified-data`.
- **#1148 (Burkina 2014 recall exposure). Body correction plus C item.**
  - *Narrow (small).* Prepend: "**Resolved so far:** PR #1671 (`1c1ef12da4`, 2026-11-09) integrated the approved #851 option-A conversion (`recall.to_period`, `period=`, the per-household ask-shortfall disclosure on `attrs['period']`). Item 1 below is done, and the archived `fix/851-recall-period` (`cf117d101`) is no longer a dependency. -- Sue". Then strike item 1.
  - *Question:* how should `period=` treat a Burkina 2014 household whose passage status is unknown or conflicting? The counts by passage are 9, 188, 575 and 85.
  - *Options:* (a) refuse the household (NA); (b) keep wave-level Contract B and disclose the unknown/conflicting counts on `attrs['period']`; (c) treat an accepted status as a completed ask.
  - *Evidence:* the body's 2026-10-21 diagnosis. The official accepted totals disagree with the retained-status union, and accepted statuses occur without food rows.
  - *Recommendation:* (b), with no household mask until a cover dataset is held. Never infer interviews from food rows.
- **#450 and #862 (harmonised geovariables; `plot_cultivation`).** Both are already ruled in scope (2026-10-23), "after the defect queue drains, design note first". There is no open decision now. The C item is only *when* to schedule each design note. *Recommendation:* leave both until the Tier A/B queue in §6 drains.

**Corrective narrows.** #816, #939, #1107 and #1235 correct a stale body but have no merged fix, so a "Resolved so far" header is optional. For uniformity, open each with "Resolved so far: no fix merged; body corrected 2026-11-12 (Sue)."

**Other flags, resolved:**
- **#1436:** keeps its existing "Resolved so far" header verbatim.
- **#1531:** a header prepend; its title is unchanged.
- **#913:** a close-as-duplicate whose C item now lives under #1130.
- **#1454:** `partial`/`none` is deliberate. The body was re-narrowed on 2026-11-09 and still describes the remaining work exactly. B01 drafted an optional split of the Senegal `Très petit`/`Très petite` synonym ruling as a C issue.

## 8.B01-ehcvm-food-sample

# Batch B01 — EHCVM food / sample (worker fork, Claude Opus 5.5)

### #1577 — No machine-readable record of what strata means per country and wave
Verdict: needs-decision/ask, tier C. Nothing in the issue's ask (a per-country-wave record that code can query) has landed. Since filing, #1693 (`e5fdd2fa8`) registered `<C>::sample::region-milieu-stratum` for Benin, Togo, Guinea-Bissau and CotedIvoire 2018 (e.g. `lsms_library/countries/Benin/_/derivations.yml:144`). That joins the earlier Mali, Niger, Senegal and Burkina Faso entries, so constructed strata are now queryable via `ll.derivations()` and the `Derivation` column. Native codes (Mali 2017-18 `strate`), survey labels and "nothing" have no record. #1690 (`1e549e96a`) added a Mali four-wave source trace (`Mali/_/CONTENTS.org:3899-3946`): 2014-15 extracts no strata, 2017-18 serves the native EAC-I code, and the EHCVM waves serve the `s00q01`+`s00q04` label construction. The trace recommends a per-wave `_/sampling.yml` account outside `panel`. No hash-cost measurement has been run.
C item:
- Question: where should "what `strata` holds" live, so code can ask it per (country, wave)?
- Options: (a) a prose note under `sample` in the canonical `data_info.yml` (cheap, not per-wave, cannot express Mali's four meanings); (b) a per-wave field in `_/sampling.yml` (needs a #793 vocabulary extension via #1134, plus a census of ~35 countries); (c) lean on the derivations registry for constructed strata, plus a minimal `sampling.yml` tag for native-code, native-label and absent cases.
- Evidence: 8 EHCVM countries already carry `::sample::region-milieu-stratum`; Mali trace `CONTENTS.org:3899-3946`; `docs/guide/feature.md:103` ("strata Is Not One Vocabulary"); `sampling.py:76-83` (read-time placement; a `.yml` moves no hash per CLAUDE.md).
- Recommendation: (b) or (c), decided together with #1134, since both touch #793's frozen vocabulary. First measure the hash cost of a `sampling.yml` addition; CLAUDE.md says `.yml` siblings move none, but that should be confirmed.
- Optional status comment: "Progress since filing: #1693 (e5fdd2fa8) registered region-milieu strata derivations for Benin/Togo/Guinea-Bissau/CotedIvoire 2018; #1690 (1e549e96a) recorded the Mali four-wave trace (CONTENTS.org:3899). The placement/vocabulary decision is still open. -- Sue"

### #1574 — Mali 2017-18 strata serves EAC-I design codes
Verdict: needs-acquisition/none, tier D. Narrowed 2026-11-09; no merged work since changes it. #1690's Mali note (`CONTENTS.org:3916`) explicitly preserves the 51-code gap. Idiosyncrasy: `EACI17_ECHANTILLON.strate` carries the literal label `codestrate` and no value-label set, and `2017-18/_/mapping.py:22` formats it with `format_id`. Remaining work is acquiring an authoritative 51-code list bound to `strate`.
No write needed (narrowed 2026-11-09, current).

### #1598 — Wire own-country NSU food weights for Togo, Benin and CotedIvoire 2021-22
Verdict: open/none, tier B, blocked. No merged work. On `e5fdd2fa8`, `Togo/2021-22/_/data_info.yml`, `Benin/2021-22/_/data_info.yml` and `CotedIvoire/2021-22/_/data_info.yml` are two-line stubs (Country/Wave), so there is no 2021-22 `food_acquired` to attach NSU weights to. `nsu-national-unit-weight` is registered only for Burkina Faso, Mali, Niger and Senegal (#1597, `0c4004872`). The wiring work sits in archived, unmerged refs `refs/archive/2026-11-11/isa/wire-{togo,benin,civ}-2021-20261104` (+13, +26 and +22 commits ahead of development). Restoring or publishing them is the real prerequisite (cross-batch: #552/#791).
No write needed; the body is accurate. Optionally add to the dependency line: "Depends on the archived 2021-22 wiring branches refs/archive/2026-11-11/isa/wire-{togo,benin,civ}-2021-20261104 being restored and merged. -- Sue"

### #1566 — Togo 2021-22: s12q09 values are not multiples of 25 on some items
Verdict: open/none, tier B. No merged work. Togo 2021-22 is unwired on development; `s12q09` is wired only in the archived branch (`.../wire-togo-2021-20261104:lsms_library/countries/Togo/2021-22/_/data_info.yml:150`). Investigation waits on that wiring.
No write needed.

### #1565 — EHCVM leavers: all-NA roster rows in Niger and Burkina 2021-22, and reasons for leaving served nowhere
Verdict: needs-decision/narrow, tier C. #1687 (`16c3f4d9c`) landed documentary evidence only:
- `b00e91d32`, `Niger/_/CONTENTS.org:3570-3611`: own 2021 forms ask 1.00a "still lives here" then reason 1.00b and timing 1.00c, but the released file has `s01q00aa` (still a MEMBER) and no coded `s01q00b`.
- `d32592828`, `Burkina_Faso/_/CONTENTS.org:3420-3445`: WB 6224's dictionary has `s01q00b` with 4,161 answers. `_finalize_result` drops all-null payload rows, so the warm-parquet count and the served count describe different stages, and adding a reason column changes which rows survive.
Nothing is decided; the body's counts need correcting.
New title: EHCVM 2021-22 leavers: decide roster membership for departed persons and where departure reasons are served
New body:
> **Resolved so far (evidence only, no behaviour change):** PR #1687 (`16c3f4d9c`): Niger own-form authority (`b00e91d32`, `Niger/_/CONTENTS.org:3570`) and Burkina catalog/finalizer evidence (`d32592828`, `Burkina_Faso/_/CONTENTS.org:3420`).
>
> Some 2021-22 `s01` files list people who have left the household, with sex, relationship, age and `s01q12` blank. Reported counts: Benin 6,138, Senegal 15,202, Niger 5,327, Burkina 4,161, Guinea-Bissau 341. Benin, Senegal and Guinea-Bissau 2021-22 rosters are not on development.
>
> What is now known:
> - **Burkina:** `_finalize_result` drops a row whose non-index columns are all null, so the all-NA rows seen in the warm parquet are not served. Serving a departure reason would make those rows survive, so the reason column cannot be added without first deciding membership.
> - **Niger:** the own form asks 1.00a (still lives here?) and then reason 1.00b with 13 printed reasons (COVID and other deaths separate) and timing 1.00c (month/year). The released export has `s01q00aa` ("still a MEMBER") but no coded `s01q00b`; `s01q00b_autre` is a string. Reasons cannot be served for Niger from the held release.
> - **Burkina:** WB 6224's dictionary lists a coded `s01q00b` (4,161 answers, 14 categories). Row-level equivalence with the retained rows is not established.
>
> **Decide:**
> 1. Do departed persons belong in `household_roster`? Options: drop them (today's de facto result in Burkina), serve them with a departure flag, or serve them in a separate table.
> 2. Where should departure reasons live? Options: a roster column, a new `departures` table, or nowhere.
>
> Then measure per country and wire. Tier C. -- Sue

### #1557 — Togo: codes 659, 660, 662 are bare in 2018 and labelled in 2021-22
Verdict: needs-decision/ask, tier C. No merged work. **Contradiction with CONTENTS.org:** `Togo/_/CONTENTS.org:466-473` says residual codes 114/659/660/662 "have no label in *any* EHCVM source .dta" and accepts them as undecodable. The issue, found on the held, unmerged 2021-22 branch, says 2021-22 labels them `Main`, `Quartier` and `Tete`. If that holds, the note is stale.
C item:
- Question: decode Togo 2018's bare 659/660/662 with the 2021-22 value labels?
- Options: (a) apply the 2021-22 labels to 2018, which changes 2018's served `u` and is a Togo re-warm; (b) keep 2018 bare and record the 2021-22 labels as a different vintage.
- Evidence: CONTENTS.org:466 (no label then known; the neighbours 663 Agoua and 664 Kpogban are local names); the 2021-22 labels exist only on the archived branch.
- Recommendation: first restore the 2021-22 wiring and confirm that the two waves share the codebook (same item set per code). Only then choose (a), and correct CONTENTS.org:466 either way.

### #1539 — Guinea-Bissau: harmonize_food folds Djagatu and Beringela onto one label
Verdict: open/none, tier B, tractable. Unchanged on `e5fdd2fa8`: `Guinea-Bissau/_/categorical_mapping.org:307` maps `Djagatu` to `Aubergine,` and `:397` maps `Beringela` to `Aubergine,`. Both are consumed by `2018-19/_/crop_production.py:120` and by food via `2018-19/_/data_info.yml:121`.
Stale comments: `data_scheme.yml:155-158` and `crop_production.py:18,120` still say GNB has no `harmonize_food` table, but one exists (`categorical_mapping.org:257`).
Mali keeps `Jaxatu` distinct (`Mali/_/categorical_mapping.org:456`).
Side note: Togo `:390` and CotedIvoire `:275` also serve `Aubergine,` with a trailing comma (cosmetic; no fold).
No write needed.
QUEUE: #1539 | Guinea-Bissau | map `Djagatu` to `Jaxatu` (Mali's spelling) and `Beringela` to `Aubergine`, drop the trailing comma, and fix the two stale comments | `lsms_library/countries/Guinea-Bissau/_/categorical_mapping.org`, `_/data_scheme.yml`, `2018-19/_/crop_production.py` (comments) | one country: a country `_/` `.org` is a build input of every GNB table, so this is a GNB re-warm | cold GNB `crop_production()` and `food_acquired()` under `LSMS_GRAIN_STRICT=1`: the 14 collapsing groups disappear, and food rows for the two items separate (expenditure total unchanged)

### #1530 — Niger: Oignon frais falls to 4 rows in 2021-22
Verdict: needs-acquisition/none, tier B. Narrowed 2026-11-09 after #1675 (`7098b0316`) and #1648 (`9514be02e`). No post-base merge touches it: #1690's Niger CONTENTS additions do not mention onion or the gate.
Idiosyncrasy: Niger 2021-22's released agriculture file is positive-only on the 16A rainy-campaign gate `s16aq00`. All 5,729 rows answer Oui, and 3,154 cover-only households are absent rather than answering No.
What remains needs a fielded questionnaire/export spec or a full gate roster (document acquisition), plus the cross-wave explanation.
No write needed (narrowed 2026-11-09, current).

### #1519 — Senegal: decide treatment of near-zero quantities already present in released data
Verdict: needs-decision/ask, tier C. Narrowed 2026-11-09; current.
C item:
- Question: what should happen to the three 2018-19 product-132 rows whose released `s07bq03a`/`s07bq07a` are ~1e-19/1e-20, giving `food_prices` near 1e22?
- Options: (a) retain as recorded, with the report-only `audit_near_zero` / Site Q `food_prices` screen from #1544; (b) a scoped source-field treatment for exactly these values, e.g. NA on the Senegal 2018-19 product-132 quantity and a registered derivation.
- Evidence: `Senegal/_/CONTENTS.org:836-858`. In all three rows `s07bq03a` is exactly 40× `s07bq07a`. No other positive value in the file is below 1e-3, and none of the 11 other EHCVM country-waves has such values. The cause is unknown, and #1519's investigation rules out the library's subtraction.
- Recommendation: (a), unless the maintainer accepts a documented exact-value NA rule (the Mali 99,999,999 sentinel ruling, GH1594, is the precedent for an exact-value rule).
Stale doc to fix whenever this is touched: `Senegal/_/CONTENTS.org:857-858` says "Site Q ... does not screen food_prices". Since #1544 (#1493), `food_prices.Price` is screened (CLAUDE.md, Site Q table).

### #1562 — EHCVM 2018-19 coping order: hooks list strategies by code
Verdict: needs-decision/ask, tier C. Narrowed 2026-11-09 and current. No post-base merge touches coping, and `CotedIvoire/_/CONTENTS.org:1113` still lists #1562 as unresolved.
C item:
- Question: how should `HowCoped0..` be ordered, given that CIV 2018's released `s14q05__k` holds only 0/1/3?
- Options: (a) keep code order and document that the order is not importance; (b) treat 1 as first and 3 as "also chosen", which needs an export-contract source; (c) stop serving an order and serve an unordered set.
- Evidence: two CIV CAPI renderings mark `s14q05` as an ordered multi-select (PDF p.170/175, workbook `S14_Chocs!K5`). The released values are 0/1/3: 11,581 rows hold a single 1, and 2,197 rows hold one 1 and one 3.
- Recommendation: (a) now, with explicit documentation, plus a request for the EHCVM export/cleaning bridge. Do not sort by magnitude without it.
- Kept separate: whether to serve livestock impact `s14q04d`, and the CIV recall conflict (12 months vs 3 years).

### #1454 — EHCVM sized-unit small follow-ups from #1224
Verdict: partial/none, tier B (small). #1647 (`6d9563f7aa`) fixed the two repaired sub-items (mapper fingerprint, stale prose), and the body was re-narrowed on 2026-11-09 to the four residuals. No post-base merge touches them. The only size synonym is still `unite de taille unique` → `taille unique` (`lsms_library/ehcvm.py:295`).
The residuals are of mixed kind:
- Senegal `Très petit`/`Très petite` needs a synonym ruling (a small C item).
- Togo code 98 needs a Togo questionnaire (D).
- The bare-to-sized `u_kg` fallback and the `'100. Kg'` parse edge have no live case.
If the coordinator wants each residual to carry one kind of work, split the Senegal ruling into its own C issue. Otherwise no write is needed.
Optional split draft — Title: "Senegal: fold size code 7 'Très petit' (2018-19) and 'Très petite' (2021-22) into one served size?" Body: "From #1454. Senegal serves size code 7 as the instrument wrote it in each wave: `Très petit` (2018-19, 4,145 rows) and `Très petite` (2021-22, 149 rows), so a cross-wave `u` panel splits on one code. The only synonym ruled so far is `Unite de taille unique` → `Taille unique` (`lsms_library/ehcvm.py:295`). Decide whether code 7 folds the same way; folding moves Senegal's served `u` and is a Senegal re-warm. Tier C. -- Sue" Parent comment: "Split the Senegal size-7 spelling ruling to #NEW; #1454 keeps Togo code 98 and the two latent parser items. -- Sue"

### #1451 — Serve EHCVM metric units with their recorded size (185 rows stay bare after #1224)
Verdict: open/none, tier B (framework). Unchanged on `e5fdd2fa8`: `lsms_library/ehcvm.py:386` (`_EXACT_METRIC_LABELS`) and `:533` (`carry_unit_size` keeps them bare). Since filing, #1505 (`a81cd29d7`) recorded that a Benin `Litre (Quart)` is not a litre (same-market median 0.20, n=4). #1597 (`0c4004872`) landed own NSU weight tables for Burkina Faso and Senegal, which bear on `Litre`/`Grand`. Benin's tables wait on #1598.
No write needed; optionally add a comment: "New evidence since filing: #1505 (a81cd29d7) shows Benin's `Litre (Quart)` prices at ~0.20 of a litre; #1597 (0c4004872) holds own NSU weights for Burkina/Senegal that can answer `Litre`/`Grand`. -- Sue"
QUEUE: #1451 | Burkina Faso, Benin, Senegal | teach `_seeded_kg_factors` to read the metric base under a trailing size, drop `_EXACT_METRIC_LABELS`, and take the size factor from the NSU tables where held, else NA | `lsms_library/ehcvm.py`, `lsms_library/transformations.py`, `tests/test_ehcvm_unit_size.py` | EHCVM countries: `carry_unit_size` is fingerprinted through the EHCVM build delegation (GH #1454), so all eight EHCVM countries re-warm; the `transformations.py` part moves none except GhanaLSS `nutrition` | cold EHCVM `food_acquired` `u` diff (185 rows become sized, nothing else moves), plus `food_quantities(units='kgs')` on the 185 rows. Needs a factor decision first (Tier C gate)


## 8.B02-ehcvm-misc

# Batch B02-ehcvm-misc

### #1442 — Burkina 2014 food: decide whether to serve the C05/C06 value fields (autocons, cadeau)
Verdict: open/narrow, tier B (was C). The question the body asks was answered on 2026-11-04 (serve `autocons`/`cadeau` as `Expenditure` on produced/inkind rows, direct answers, no derivation; Uganda `value_own`/`value_inkind` precedent), and the implementation exists as `d81358091`, but it is NOT on development: it sits only on `refs/archive/2026-11-11/isa/burkina-bundle-20261104`. It is held because it moves 2014 kilograms (+8.8%, worse against the survey's unit weights) through `food_kg_factors`, which is #1509 (whose own fix `cc962ee15` is also archived, not merged, and whose eligibility definition awaits the maintainer). Development still says the values "stay unread" (`Burkina_Faso/2014/_/mapping.py:265-270`; `CONTENTS.org:3013` "held and not on development"). Idiosyncrasy: EMC 2014 passages 2–4 record money only; only passage 1 fields the quantity grid; 1,090 + 353 `(i, j, visit)` carry the same money in two blocks.

New title: Burkina 2014 food: land the ruled C05/C06 values (`autocons`, `cadeau`) as Expenditure once #1509 settles the kg pool

New body:
> **Resolved so far:** the 2026-11-04 ruling answered the question this issue asked: `autocons` and `cadeau` ("Montant en fcfa C05 / C06") are served as `Expenditure` on `produced` and `inkind` rows, as direct survey answers with no derivation (the Uganda `value_own` / `value_inkind` precedent). A money-only block is a row with `Quantity` NaN and `u = 'Unknown'`; `mtt_dep` (C02) stays unread. PR #1431 (`f6ed07ae9`) earlier settled the passage-1 quantities (#864).
>
> **Remaining:** land commit `d81358091` (archived at `refs/archive/2026-11-11/isa/burkina-bundle-20261104`; it is not on development). It is held for one reason: `food_kg_factors` pools every row with `Expenditure` and `Quantity`, whatever its `s`, so the 12,842 passage-1 produced/inkind blocks carrying both enter the price-ratio inference. Measured cold (base `115b6ab71`): 2014 kg 334,742 -> 364,230 (+8.8%), and against the survey's unit weights 2014 within-2x falls 58.6% -> 43.3%. Blanking non-purchased `Expenditure` for the estimate reproduces base on all 767,330 rows. So this waits on #1509 (restrict the estimator's evidence; its eligibility definition is with the maintainer).
>
> Measured effects of `d81358091` (cold): `food_acquired` 2014 559,180 -> 672,718 rows (113,538 money-only); default `food_expenditures()` unchanged on every value; `basis='total'` 2014 +160,943,445 FCFA (+47.0%); 1,090 + 353 `(i, j, visit)` carry equal money in two blocks and `basis='total'` counts them twice (the money side of the #864 "serve as recorded" ruling).
>
> **When #1509 lands:** rebase `d81358091` (expect a conflict only in `CONTENTS.org`), re-measure kg against the survey unit weights, update `CONTENTS.org` (the "held and not on development" passage near line 3013) and the `mapping.py` comment at lines 265-270. Cost: one Burkina re-warm (all Burkina country hashes and the 2014 wave hashes; no `build_transforms_fingerprint`).
>
> Tier B, depends on #1509. -- Sue

QUEUE: #1442 | Burkina_Faso | after #1509, rebase archived d81358091 (C05/C06 as Expenditure) and re-measure kg | Burkina_Faso/2014/_/mapping.py, Burkina_Faso/_/CONTENTS.org, tests/test_gh1442_burkina_section_c_values.py, tests/test_burkina_faso_2014_passages.py | one country (Burkina re-warm) | cold Burkina base/candidate food_acquired + derived food tables; kg vs ehcvm unit weights

### #1276 — Burkina 2021-22: acquire and wire both disjoint EHCVM components with separate provenance
Verdict: needs-acquisition/narrow, tier D (then B wiring, C population representation). Landed: PR #1322 (`a19390fcc`: `sample`, `household_roster`), PR #1418 (`4f3488041`: `cluster_features`, `interview_date`), and the 2026-11-04 measurement notes (`d87d153dc`, `acfc53e2e`; `Burkina_Faso/_/CONTENTS.org:2751`, `:2787`). Nothing acquired since. On development `Data/` still holds 53 loose 6224 sidecars plus 3 in `Data/Cross_Section/`, and `SOURCE.org` still says `#+CATALOG_IDNO: BFA_2021_EHCVM-P_v04_M`. The body (narrowed 2026-11-03) does not carry the 2026-11-05 comment's blockers, so a reader of the body alone misses them. Idiosyncrasy: one fieldwork round published as two disjoint WB entries (6224 longitudinal, clusters 2–585; 6277 new frame, clusters 586–1185), each weighted to the national total on its own.

New title: Burkina 2021-22: relocate 6224 into Data/Panel, acquire 6277 into Data/Cross_Section, and wire the remaining cross-section tables

New body:
> **Resolved so far:** PR #1322 (`a19390fcc`: `sample`, `household_roster` from the three held cross-section files, `CS_` identity, mixed-frame population text) and PR #1418 (`4f3488041`: `cluster_features`, `interview_date` read the cross-section cover). The 7,176 cross-section households reach `market=` frames and interview dates. PR #1293 (`eea68586e`) supplies `add_wave(subdir=)`, `#+CATALOG_SUBDIR` routing and per-entry HELD/LATEST lines. Measurements of what both entries now ship are in `CONTENTS.org` (2026-11-04 entries).
>
> **Rulings in force (@ligon, 2026-10-23):** serve both components Malawi-style (`Data/Panel/` for 6224, `Data/Cross_Section/` for 6277); each component's own cross-sectional weight in `weight`, `panel_weight` only for the panel; document that a pooled mean blends two frames.
>
> **Blockers found 2026-11-05 (not acquired):**
> 1. `add_wave(subdir='Cross_Section')` is refused before download because 53 of 6224's sidecars sit loose in `Data/`; past that it would be refresh-only, since the subdirectory already holds three sidecars (byte-identical to 6277's members; 52 of 6277's 55 files are not held). So the 6224 relocation into `Data/Panel/` comes first, through a reviewed source-preserving plan.
> 2. The held 6224 files are **v01 by content** (the held DDI PDF reads `BFA_2021_EHCVM-P_v01_M`; the differing files are exactly those the version notes name), while `#+CATALOG_IDNO:` says v04 because it was stamped from the catalog. The correction is the maintainer's (a held release moves only on reviewed evidence).
> 3. The current 6224 release (v05) adds household GPS (3,227 of 3,227) and `hhweight`, and lacks the parcel coordinates (`s16aq46__*`, `s16a01a`, `s16aq03b`), which by the version notes left at v03 with no stated reason. `plot_features` serves those positions today. Whether to acquire v05, and what to do with the parcel GPS, needs a ruling.
>
> **Remaining work:**
> - Relocate 6224 into `Data/Panel/`; acquire 6277 into `Data/Cross_Section/` with acquisition and push receipts and per-entry provenance. Never point one entry's acquisition at the other's same-named files.
> - Wire the remaining 2021-22 tables to both components: food (s07b), housing (s11), assets (s12), shocks (s14b), s08a, s20a, individu and agriculture (s16*) are panel-only until then. Keep explicit household and cluster namespaces, panel-only linkage and separate weight columns. Re-measure the 9 cross-section ids that equal a 2018-19 id and the 2 crosswalk-target collisions under the merged single-pass `id_walk` (#1245).
> - Population representation (Tier C): `population.yml` is per wave, but the components represent different frames. Do not silently assign one universe to both.
> - Verify 6224 conservation, full 6277 row, household and EA coverage (10,403 households expected; establish it on the final source), no cross-component or unintended cross-wave id merges, and honest Country/Feature disclosure.
>
> Tier D, then B and C. -- Sue

### #1226 — EHCVM community prices: Guinea-Bissau 94, one variety convention, helper move (residual of #1226)
Verdict: partial/narrow (small), tier B. Landed: #1374 (`0b1effb22`, Burkina 2018-19) and #1505 (`a81cd29d7`, Benin, Guinea-Bissau, Senegal 2018-19; Burkina's sized `u`). All three items ruled on 2026-11-04 are still open on development:
- GNB product 94 is not bridged: `Guinea-Bissau/2018-19/_/mapping.py:448` ("Product 94 … takes its own"), `CONTENTS.org:688`.
- Variety conventions still differ: Mali 2018-19 always uses the code (`Mali/2018-19/_/mapping.py:380`); Benin, GNB and Senegal use it only when lumped. The s05 helpers are still per wave (`_S05_INDEX` in each `2018-19/_/mapping.py`); only `community_price_slots` (`lsms_library/ehcvm.py:204`) is shared.
- Burkina still folds the tomato tin onto `Boîte` (`Burkina_Faso/_/categorical_mapping.org:349,396`).

The tin item was made conditional on #1452 (comment 7), and #1452 closed 2026-11-07, so it can be re-measured now. Idiosyncrasy: the GNB price form carries `Feijão seco` (94), which has no `s07bq01` counterpart; the nearest is 95, `Feijão-frade, feijão seco`. The title's "(residual of #1226)" refers to itself; drop it.

New title: EHCVM community prices: bridge Guinea-Bissau 94, one variety convention with the s05 helper move, and re-measure Burkina's tomato tin

New body:
> **Resolved so far:** PR #1374 (`0b1effb22`, Burkina Faso 2018-19) and PR #1505 (`a81cd29d7`): Benin, Guinea-Bissau and Senegal 2018-19 serve `community_prices`, and Burkina's labels take the household's sized form. Community prices pair with household unit values at a median ratio of 1.00 in all four.
>
> **Remaining (ruled by the maintainer 2026-11-04):**
> - [ ] Bridge Guinea-Bissau product 94 (`Feijão seco`, 213 rows) to the household's 95 (`Feijão-frade, feijão seco`). Today it is served under its own label and meets no household row (`Guinea-Bissau/2018-19/_/mapping.py`, `_s05_base`).
> - [ ] One `variety` convention, Benin's (code only where `harmonize_food` lumps two priced codes into one `j`, else `unknown_variety`), as `data_info.yml` prescribes. Mali 2018-19 currently serves the code on every row. Do this when the s05 helpers move from the four `2018-19/_/mapping.py` modules (and `niger.py`) to `lsms_library/ehcvm.py`, beside the already shared `community_price_slots`.
> - [ ] Stop folding Burkina's tomato tin (`Boîte de tomate`, 2014 `grosse boite de tomate`) onto `Boîte`, measuring kg factors first against the line fixed before looking. The 2026-11-04 measurement crossed that line, and the re-measure was deferred until #1452 landed the EHCVM unit weights. #1452 closed 2026-11-07, so re-measure now.
>
> Cost: each country's wave `mapping.py` edit is a whole-country re-warm. The helper move to `ehcvm.py` reaches every EHCVM country that imports it. -- Sue

QUEUE: #1226 | Guinea-Bissau (+Benin/Senegal/Mali/Niger) | bridge GNB s05 product 94->95 by code; move s05 helpers to ehcvm.py with Benin's variety rule | Guinea-Bissau/2018-19/_/mapping.py, {Benin,Senegal,Mali}/2018-19/_/mapping.py, Niger/_/niger.py, lsms_library/ehcvm.py | one country per mapping.py edit; helper move = each importing EHCVM country re-warms | cold community_prices per country base/candidate; household pairing ratio; Feature(community_prices) index unique

### #1179 — Mali EAC-I community prices: diagnose positive-priced entries with unresolved unit codes
Verdict: needs-acquisition/narrow, tier D. The diagnosis the body first asked for is done. Both 2014 community questionnaires were authenticated and read (2026-10-31 checkpoint), and both enumerator manuals are held and read (#1506, `df8faa75f`). None of them gives a recovery rule (`Mali/_/CONTENTS.org:3105`). The 129 entries were re-measured at `3a33f2869` (`CONTENTS.org:2313-2335`). `Poids` is now converted by rule on gram and centilitre rows (#1515, `ad44c8d46`), but that does not touch the 129. The only remaining route is producer instructions or a released correction, and no channel to either is named. Idiosyncrasy: the 58 "other integer" codes take only round values (100, 40, 50, …), are not a weight- or price-into-unit swap, and sit on 84 source rows in 34 clusters. Code 99 = Manquant is in the Stata dictionary but not in the printed 1–14 list.

New title: Mali EAC-I 2014-15 community prices: 129 positive-priced entries with unresolved unit codes need producer evidence

New body:
> **Resolved so far:** PR #1180 restored the passage-2 reading-1/2 dictionaries (#822) with the old keep/drop sets intact. Both 2014 community questionnaires (`EACI2014_COM_P1_17062014.pdf`, `EACI2014_COM_P2_11082014.pdf`) are held and authenticated, and both enumerator manuals are held (PR #1506, `df8faa75f`). All four were read. They print unit codes 1–14, never 99, 0 or a fractional code, and give no recovery rule beyond "Si une variété est absente, il faut laisser la ligne à blanc". `Poids` is converted to kg by an explicit rule on gram and centilitre rows (PR #1515, `ad44c8d46`), which does not touch these entries.
>
> **What stands (re-measured at `3a33f2869`, `Mali/_/CONTENTS.org`):** 129 positive-price entries with a resolvable item and an unresolved unit: 49 code 99 (Manquant in the Stata dictionary), 19 code 0 (passage 1 only), 58 other integers taking only round values (100, 40, 50, 30, …), and 3 fractional. They sit on 84 source rows in 34 clusters. Adjacent readings carry no recovery rule (only 4 of 43 match on price and weight), and using them would be unit inference.
>
> **Remaining:** obtain producer instructions or a released correction that says what these codes mean, with the exact passage, reading and field scope. Until then, keep the existing exclusions. No rounding, no weight or price inference, no borrowing of household-food labels, and no eligibility change.
>
> **Question for the maintainer:** if no channel to the producer exists, close this as "excluded; unrecoverable from held evidence", with the `CONTENTS.org` entry as the record?
>
> Tier D. -- Sue

### #821 — Mali 2021-22 community prices: verify availability of a cluster-grain source
Verdict: needs-acquisition/narrow, tier D. The 2018-19 wiring is done (#1356, `a06b7a62f`). For 2021-22, the wave's own BID was read on 2026-11-04 (`Mali/_/CONTENTS.org:3110-3127`). It describes the price component as a monthly market sample with at least two localities per region, which fits `ehcvm_prix_mli2021`'s region × milieu grain. All eight EHCVM-2 releases stop at `s04_co`. Two pieces of evidence still cut the other way: the DDI line saying the community questionnaire solicits "local retail price information", and Burkina's printed community questionnaire, which has "SECTION 5. RELEVE DES PRIX". Mali's own community questionnaire is not on WB 6275 or IHSN 12257. UEMOA's `Fichiers_Communautaire.rar` is licence-gated. The coverage verdict stays `unsure` (`absent_verdicts.csv:38`). The body predates the BID finding.

New title: Mali 2021-22 community prices: obtain the community questionnaire to settle whether a cluster-grain price module was fielded

New body:
> **Resolved so far:** PR #1356 (`a06b7a62f`) wires 2018-19 `community_prices` from `s05_co_mli2018.dta`: two quotes per cluster, product and unit size, `Quantity` missing (the file has no weight or quantity field), all 549 price clusters in the 551-cluster sample.
>
> **2021-22, what is established:**
> - The held `ehcvm_prix_mli2021.dta` is at region × milieu × time grain and has no `grappe` field. It cannot be attached to clusters without an invented crosswalk.
> - WB 6275's published `s00_co`–`s04_co` dictionaries carry no food product-by-unit quote block (`s03` land rent and wages are different concepts). All eight EHCVM-2 releases stop at `s04_co`.
> - The wave's BID (`mli_ehcvm2_information_de_base_eng.pdf`, PDF pp. 6, 8, 9; recorded in `Mali/_/CONTENTS.org` 2026-11-04) names four questionnaires and describes the price component as a monthly market sample with at least two localities (one urban, one rural) per region. That matches the regional file.
>
> **Still against a "not asked" verdict:** the DDI says the community questionnaire solicits "local retail price information", and Burkina's printed EHCVM community questionnaire still has "SECTION 5. RELEVE DES PRIX A LA CONSOMMATION".
>
> **Remaining (C4):** Mali's own 2021 community questionnaire. It is on neither WB 6275 nor IHSN 12257. UEMOA `MLI-EHCVM-V2.0` lists `Fichiers_Communautaire.rar`, which is licence-gated. Alternatively, a provider statement on whether a cluster-price module was fielded and released. Until then the coverage verdict stays `unsure` (`C1-partial;C4-notrun`), with no crosswalk and no speculative loader.
>
> Tier D. -- Sue

### #816 — Burkina Faso coverage starts at 2014: the Enquête Prioritaire series (1994, 1998) exists and is a different instrument
Verdict: needs-acquisition/narrow, tier D (with one Tier C design point). The maintainer ruled on 2026-10-23 to acquire 1994 and 1998 as new Burkina waves (comment 1). Nothing has happened since: no `Burkina_Faso/1994` or `/1998` directory on development, and no commit cites #816. The EP files are not on the WB NADA (INSD catalog/40, INED demostaf 87/88), so `discover_waves` and `add_wave` cannot fetch them. They need the `external` provenance path. The body's work items predate the ruling. Idiosyncrasy: Burkina has never had an LSMS-ISA panel; it reaches the library through EMC 2014 and the WAEMU EHCVM block, and EP is the lighter SDA priority-survey instrument.

New title: Burkina Faso: acquire the Enquête Prioritaire 1994 and 1998 as new waves (ruled), with a "different instrument" coverage marker

New body:
> **Ruling (@ligon, 2026-10-23):** acquire the 1994 and 1998 Enquête Prioritaire as new Burkina Faso waves.
>
> **What exists (from the original report, unchanged):** EP-I 1994 ([INED demostaf catalog/88](https://demostaf.web.ined.fr/index.php/catalog/88/datafile/F13)) and EP-II 1998 ([INSD NADA catalog/40](https://nada.insd.bf/index.php/catalog/40), [demostaf catalog/87](https://demostaf.web.ined.fr/index.php/catalog/87/datafile/F4/V38)). EP-1998 is national, urban/rural, 10 regions, with census ZDs at stage one and 20 households per ZD at stage two. EP-III/EBCVM 2003 and EICVM 2009-10 are **unverified** leads and are not part of the ruling.
>
> **Remaining:**
> - [ ] Access: EP-1998 requires prior INSD authorization. Request it and record the terms per wave (the #791 pattern: the archive decides entitlement). Re-test `nada.insd.bf` reachability from a second host (it did not resolve on 2026-09-08).
> - [ ] Acquisition: these are not World Bank catalog entries, so record `#+PROVENANCE_SOURCE: external` in each wave's `Documentation/SOURCE.org`, and push through `push_to_cache_batch()`, not `add_wave`.
> - [ ] Before declaring any feature, assess instrument comparability against the canonical columns in `data_info.yml`. Do not map whatever the files contain.
> - [ ] Design (Tier C, for the maintainer): how coverage and `population.yml` express "held, different instrument", so the timeline does not read as one continuous series with the EMC/EHCVM waves.
> - [ ] Correct `Burkina_Faso/_/CONTENTS.org`: its "Gaps and what was searched" section never asked about earlier waves, so its silence is not a negative finding.
>
> Tier D. -- Sue

### #791 — Add UEMOA PHMECV as a source: 4 missing EHCVM 2021-22 waves + 8 ERI-ESI studies (access is human-reviewed)
Verdict: partial/narrow, tier D. The issue's first gap is closed, by a different route. PR #1506 (`df8faa75f`, on development) holds Benin 6272, Côte d'Ivoire 6273, Guinea-Bissau 6274 and Togo 6279 2021-22 from the **World Bank** catalog through `add_wave`; their `SOURCE.org` files read `*_2021_EHCVM-2_v01_M`. They are held but not wired (#552 / #1598 territory). What remains is UEMOA-only. ERI-ESI-V1.0 2018 is held nowhere. `Fichiers_Communautaire` (16 of 24 studies) is the only place the un-displaced-GPS claim can be tested, and also the likely source of #821's Mali 2021 community questionnaire. There is no UEMOA support on development: no `phmecv`/`.rar` handling in `data_access.py`, and the `wb-gps-unjittered` access class is not built. Idiosyncrasy: since these EHCVM-2 waves are now WB-sourced, a UEMOA copy would be a second catalog entry (the #1276/#1283 `subdir=` pattern), not a first acquisition.

New title: UEMOA PHMECV as a source: ERI-ESI 2018 studies and the community files (licensed, human-reviewed access)

New body:
> **Resolved so far:** PR #1506 (`df8faa75f`) closed the EHCVM-2 2021-22 gap from the **World Bank** catalog instead: Benin (6272), Côte d'Ivoire (6273), Guinea-Bissau (6274) and Togo (6279) are held through `add_wave` (BIDs, household questionnaires, `LICENSE.org`). They are not yet wired; that is #552 / #1598, not this issue.
>
> **Remaining (UEMOA-only):**
> - **ERI-ESI-V1.0 (2018):** employment and informal-sector studies for all eight EHCVM countries, held nowhere.
> - **`Fichiers_Communautaire`** (16 of 24 studies): the community files. They are the cheap test of the unconfirmed claim that UEMOA files carry **un-jittered** GPS (the offset against our WB `cluster_features` coordinates is the displacement; WB/DHS offsets are up to 5 km rural, 2 km urban). They may also hold Mali's 2021 community questionnaire (#821).
> - **Access:** every study is `licensed`, with human review. The Benin `EHCVM-V2.0` request has been pending since 2026-09-06. When it lands, run the discriminating test in `slurm_logs/uemoa/README.org` (Benin requested versus Senegal control). The licensed-request workflow has no `api/` surface (comment 1), so the realistic shape is: a human downloads in a browser, and the library ingests from a local path (the Togo/Novissi and KenyaLPS precedent).
> - **Machinery:** `_find_stata_zip` cannot see UEMOA's 24 `.rar` archives and 39 bare `.dta` resources. Give the source an `ACCESS_CLASS` per `SkunkWorks/data_access_restrictions.org`. If un-jittered GPS is confirmed, build `wb-gps-unjittered` before acquiring.
> - **Already decided:** model credentials, not entitlements. The server decides whether this user may have this file.
>
> Inventory: `slurm_logs/uemoa/` (`ef719ed7`, `8190e071`). Tier D. -- Sue

### #1556 — Shocks: apply purchases/stock ruling in unfinished wave integrations
Verdict: open/narrow (small), tier B, blocked on wave wiring. The ruling (AffectedConsumption = food purchases, AffectedFoodStock = food stocks) is implemented on every existing wave (#1596, `8558fefe0`, after #1564 `e3f9d5728` and #1579). The four EHCVM-2 2021-22 waves (Togo, Benin, Côte d'Ivoire, Guinea-Bissau) are still unwired: each has only a two-line `2021-22/_/data_info.yml` stub (Country/Wave) on development [coordinator correction]. Their held branches now live only at `refs/archive/2026-11-11/isa/wire-{togo,benin,civ}-2021-20261104`. Open PR #1604 (Malawi 2024-25) carries no shocks work. #1561, named in the body as a separate live caveat, is CLOSED (via #1661). Idiosyncrasy: 2018-19 `s14q04d` is the food stock only in vague 1 of Mali, Senegal, Burkina, Benin and Togo; elsewhere the `.dta`/DDI label is wrong and it is livestock. In 2021-22 `s14bq04d` is livestock and `s14bq04e` is the food stock.

New title: (unchanged) Shocks: apply purchases/stock ruling in unfinished wave integrations

New body:
> **Resolved so far:** #1564 (`e3f9d5728`) corrected the 2021-22 core-country livestock and coping wiring; #1579 corrected the core 2018-19 layout selection; #1596 (merge `8558fefe0`) implements the 2026-11-06 ruling across every existing affected wave: **AffectedConsumption = food purchases; AffectedFoodStock = food stocks**. #1561 (Côte d'Ivoire 2018-19 Stata `.d` not-concerned answers) is closed via #1661. No live livestock-selector defect remains in development's waves.
>
> **Remaining:** apply and verify the ruling when the unfinished wave integrations land.
> - Togo, Benin and Côte d'Ivoire 2021-22 (held work archived at `refs/archive/2026-11-11/isa/wire-{togo,benin,civ}-2021-20261104`) and Guinea-Bissau 2021-22 (not started) must adopt the separate purchase and stock fields when they are wired under #552 / #1598. Verify each wave's own source layout and its source-specific boolean maps. In 2021-22, `s14bq04d` is the livestock consequence and `s14bq04e` the food stock (labels read in Togo and Niger). Preserve coping answers, missingness and source rows.
> - Malawi 2024-25 must adopt the same ruling as part of its wave work (#1275). Open PR #1604 is food and sample only and does not carry shocks.
>
> Tier B, blocked on those wirings. -- Sue

### #1531 — EHCVM GPS plot area: resolve units and export fields in Togo, Benin and Guinea-Bissau
Verdict: needs-decision/narrow (small header edit), tier C (the underlying symptom is Tier B silent loss). Documentation only has landed: #1649 (`fb04f101d`, Guinea-Bissau note) and, since the 2026-11-10 reconciliation base, #1693 (`e5fdd2fa8`). #1693 merged the Benin and Togo own-instrument and released-header notes (`Benin/_/CONTENTS.org`, `Togo/_/CONTENTS.org`, 2026-11-10 entries; `.coder/ledger/1531-benin-togo-own-instruments-20261110.md`). The body already describes those findings but does not say they are merged. Runtime is unchanged: the GH #327 1,000 ha clamp still applies. Idiosyncrasy: all three countries print GPS Q47 in hectares, but the released `s16aq47` label states no unit. Only Guinea-Bissau ships a separate `s16aq47a` labelled m² (1,191 rows, all in vague 2). Togo's covers print 2017/2018 while its BID says 2018/19.

Narrow: prepend this header to the existing body (rest unchanged):
> **Resolved so far (documentation only; no runtime change):** PR #1649 (`fb04f101d`): Guinea-Bissau 2018-19 instrument and header evidence in `Guinea-Bissau/_/CONTENTS.org`. PR #1693 (`e5fdd2fa8`): the Benin and Togo own-instrument and released-header findings described below, in `Benin/_/CONTENTS.org` and `Togo/_/CONTENTS.org` (2026-11-10 entries). **Remaining:** a source policy for `s16aq47` (decision below), then a verified area correction and a re-measure of the #1460 yield land route. -- Sue

C item (for the maintainer):
- **Question:** In Togo, Benin and Guinea-Bissau 2018-19, how should `plot_features.Area` treat `s16aq47`? The printed question asks hectares, but the released field carries no unit and its magnitudes behave like m² on many rows.
- **Options:** (a) a row-wise classifier against the farmer's own estimate, registered as a derivation per country; (b) Guinea-Bissau only: use the explicit m² `s16aq47a` where present (exact conversion) and leave `s16aq47` as is elsewhere; (c) treat GPS area above a stated bound as missing and fall back to the farmer estimate (a stated tolerance, disclosed); (d) serve as recorded and rely on disclosure (Site Q cannot see it, because the p90 is itself contaminated); (e) first seek the producer's export/cleaning definition (Tier D).
- **Evidence:** original measurements, not re-measured. GPS plots at 50–1,000 "ha" run a median of 7,778× the farmer estimate in Togo (280 plots), 438× in Benin (773) and 10,000× in Guinea-Bissau (2,592). The clamp blanks 1,528, 1,256 and 1,302 plots. Yields on plots over 50 "ha" are 0.38–2.43 kg/ha, against 646–1,188 on the rest.
- **Recommendation:** (e) with a short time box, then (a) as a registered derivation (the farmer estimate is the only same-row evidence), with (b) for Guinea-Bissau's 1,191 `s16aq47a` rows as the exact part. Re-measure before choosing; the counts above are stale.

### #1107 — Guinea-Bissau `nitrogen_kg` returns 0 rows — its four Portuguese inorganic labels are keyed to nothing
Verdict: open/narrow, tier B, effort S. Unfixed on development. `Guinea-Bissau/_/categorical_mapping.org` has no `harmonize_input`, and `data_scheme.yml:190-193` says the Portuguese labels "pass through unchanged". `CONTENTS.org:632-640` (2026-11-04, under #1445) re-measured it: `nitrogen_kg` still matches 0 rows. That entry adds a gap this body lacks: `Quilograma` and `Grama` have no kg factor, so 38 + 2 NPK rows have no mass even once labelled. A template exists (`Togo/_/categorical_mapping.org:137`; `_NITROGEN_CONTENT` at `transformations.py:6169`). Overlap: #1483's checklist item "Resolve Guinea-Bissau's missing input-label harmonization and kg-unit routes" is this issue. Keep #1107 as the concrete one and have #1483 link here. Idiosyncrasy: Guinea-Bissau's labels are Portuguese. `s16a` asks fertilizer per parcel (`s16aq29c1`, 60 positive), which contradicts the `data_scheme.yml` grain note (that part belongs to #1483's parcel item).

New title: Guinea-Bissau `nitrogen_kg` returns 0 rows: add `harmonize_input` for the Portuguese inorganic labels and kg routes for `Quilograma`/`Grama`

New body:
> **Status (re-measured 2026-11-04, `Guinea-Bissau/_/CONTENTS.org`, GH #1445 entry):** still 0 matched rows. There is no `harmonize_input`, so every `s16bq01` label passes through raw and none keys `_NITROGEN_CONTENT`. Separately, the units `Quilograma` and `Grama` have no kg factor, so 38 + 2 applied NPK rows have no mass even once labelled.
>
> **Fix (country config):** add `harmonize_input` to `Guinea-Bissau/_/categorical_mapping.org` on Togo's pattern (`Togo/_/categorical_mapping.org`, `#+NAME: harmonize_input`):
> - `Adubos inorgânicos - ureia` (190) -> `Urea`
> - `Adubos inorgânicos - fosfatos` (57) -> `Phosphate`
> - `Adubos inorgânicos - NPK/Fórmula única` (46) -> `NPK`. The instrument prints no grade. Decide whether the generic `npk` (0.17 N) applies, or only a stated grade: #1445 moved Niger's "NPK 15/15" to 0.15.
> - `DAP e outros adubos inorgânicos` (41): a mixed bucket. Togo maps its twin ("DAP et autres engrais inorganiques") to `DAP`. Either follow that, or send it to `Other Fertilizer`/unmatched, and write the decision down.
> - Organic labels (`estrume animal` 365, `lixo doméstico` 150) stay refused and named, per the #867 ruling.
>
> Also route `Quilograma` -> 1 kg and `Grama` -> 0.001 kg in the plot-input unit table.
>
> **Related:** #1483 (EHCVM fertilizer) lists this as its Guinea-Bissau item and should point here. The parcel-level `s16aq29c1` question belongs to #1483's parcel-representation item, not here.
>
> **Cost:** a country `categorical_mapping.org` edit rebuilds Guinea-Bissau's hashes once. Read `Guinea-Bissau/_/CONTENTS.org` first. Source of the original census: `slurm_logs/gh867_nitrogen/census_after.json`. Tier B. -- Sue

QUEUE: #1107 | Guinea-Bissau | add harmonize_input (Portuguese -> Urea/Phosphate/NPK, DAP bucket per Togo precedent or Other) and kg routes for Quilograma/Grama | Guinea-Bissau/_/categorical_mapping.org (+ a GNB nitrogen test) | one country (GNB re-warm) | nitrogen_kg(Guinea-Bissau) > 0 matched rows; plot_inputs frame-equal except input labels; Feature('plot_inputs') index unique

### #1235 — CotedIvoire 2018-19 plot_inputs: quantity is exactly 350 on 39.5% of applied lines across all input types
Verdict: open/narrow, tier B (a Tier C sentinel ruling follows if the diagnosis supports one). No diagnosis has landed. `CotedIvoire/_/CONTENTS.org:930-940` still reads "Measured, NOT adjudicated … Served as recorded". One sentence in the body is now stale. #1618 (`728992c76`) added `plot_inputs.Quantity` to Site Q (`quantity_audit.py:450`), but a value that is 39.5% of its cell sets its own p90, so Site Q structurally cannot flag it (the "repeated sentinel" limit in CLAUDE.md). The diagnosis is tractable now: the household CAPI and Excel questionnaires for both vagues are held (`CotedIvoire/2018-19/Documentation/CIV-EHCVM1-QNR-MEN-CAPI-VAGUE{1,2}.pdf`, `…-MEN-EXCEL-…xlsx`). A CAPI default or validation limit is the most likely origin of a 350. Idiosyncrasy: the declared missing codes are 2147483622 (Non réponse) and 2147483635 (Ne sait pas), on no applied line. 350 appears in both vagues and on purchased and unpurchased lines alike.

New title: CotedIvoire 2018-19 plot_inputs: diagnose the modal quantity 350 (39.5% of applied lines) from the held CAPI questionnaires

New body:
> **Finding (2026-10-22, `CotedIvoire/_/CONTENTS.org`, unchanged):** `s16bq03a` (`Data/Menage/s16b_me_CIV2018.dta`) is exactly 350 on 4,444 of 11,259 applied lines (39.5%), 4,442 of them in `Kilogramme`, across every input code (urea 730 of 1,072, maize seed 536 of 568, rice seed 538 of 570). It occurs in both vagues and on purchased and unpurchased lines. It has no value label. The declared missing codes (2147483622 Non réponse, 2147483635 Ne sait pas) occur on no applied line. Senegal 2018-19 has 0.5%, Niger 2018-19 0.01%. Served as recorded, it drives every Côte d'Ivoire `seed_kg` and `nitrogen_kg` total.
>
> **Site Q does not see it.** Since PR #1618 (`728992c76`), Site Q screens `plot_inputs.Quantity`, but a value this frequent is its own cell's p90, so the screen cannot flag it. (The 125 `Tonne` NPK rows, 38.2% of the NPK slot's kilograms, are a separate matter: #1482.)
>
> **Diagnose before any change:**
> - [ ] The held instruments: `CotedIvoire/2018-19/Documentation/CIV-EHCVM1-QNR-MEN-CAPI-VAGUE1.pdf` / `-VAGUE2.pdf` and the `-MEN-EXCEL-` workbooks. Look at the s16b quantity item for a printed code, a CAPI default, or a validation bound of 350. Compare the sibling EHCVM CAPI forms.
> - [ ] Neighbouring columns: does 350 co-occur with a missing or zero unit, purchase or gift value on the same line?
> - [ ] Distribution by input type, region and vague, and by enumerator if available.
> - [ ] **Then a ruling.** If 350 is a sentinel, serve it as missing and count it (the `plot_inputs_finalize` Mali precedent and the 999/9999 handling). Never drop or rescale it.
>
> Related: #1123, #1482, #857. Tier B. -- Sue

QUEUE: #1235 | CotedIvoire | read-only diagnosis: inspect held CAPI/Excel s16b quantity item for a 350 default/bound; co-occurrence census; then bring sentinel ruling | (diagnosis: none; fix would touch CotedIvoire/2018-19/_/plot_inputs.py + CONTENTS.org) | fix = one country (CIV re-warm) | instrument page citation; cold CIV plot_inputs/seed_kg/nitrogen_kg base vs candidate if a sentinel rule lands


## 8.B03-isa-crop-sales

## Batch B03 — ISA crop sales (worker fork, Claude Opus 5.5)

### #1571 — Niger sold kilograms: resolve field meaning and state/eligibility policy
Verdict: needs-decision/ask, tier C. Only documentation landed: PR #1617 (`1b0e4e079073`) appended the own-form conflict to `Niger/_/CONTENTS.org:3201-3270`. The investigation's `DECISION-PACKET.md` (`burst/isa-sm-20261103/reports/1560-sold-kilograms-investigation-20261108/`) lists six questions marked UNSENT. Nothing after the 2026-11-10 narrowing touches it: #1690's Niger addition is the #1529 3E review. Idiosyncrasy: both own 2021 vagues say "poids en kg d'UNE UML", while the released `s16dq05c` label says TOTAL. 3E asks about the FIRST cycle. `CONTENTS.org:3267`: "KgFactor_sold remains kg per ONE unit: a total must not be inserted there."
This issue already covers two #1457 side findings: Niger's unwired `KgFactor_sold`, and the condition-keyed price pool that cannot see `Farm-level` rows (body, second sentence).
C item:
- Question: may Niger's reported sold kilograms (`as02eq12c`, `as03eq11c`, `AS02EQ12C`, `s16dq05c`) price `Farm-level` sales? If so, through which carrier, and which state (`Condition_sold` donor or `condition` recipient) keys the price pool?
- Options: (a) a literal carrier `Quantity_sold_kg` with no automatic consumer; (b) (a) plus explicit opt-in `median_price_valuation(kg_qty=...)`; (c) separate per-cycle and cycle-derived carriers for 3E; (d) source/provenance exposure only while the 2021 meaning is held; (e) progress 2011/2014 only, holding 2021.
- Evidence: usable-price support is 251/1,051 (23.9%) today and 1,041/1,051 with literal raw kg. Harvest-state pools price 13,430 native rows; sold-state pools price 11,894.
- Recommendation for the human: rule #1560's field meaning first (it gates every option). Then (a)+(e) is the smallest step. Keep the state question separate, and fold #1571 into #1560 once ruled.
- Nothing is decided here. -- Sue

### #1560 — Sold kilograms are recorded and unread in Niger and Mali
Verdict: needs-decision/ask, tier C. Documentation only: #1617 (`1b0e4e079073`), Niger `CONTENTS.org:3201`, Mali `CONTENTS.org:3501-3546`. No carrier, mask or default has been selected. Idiosyncrasy: the released labels say total, but the own 2021 forms (Niger and Mali, both vagues) instruct weight per ONE UML. Stored kg-unit controls look like totals (Niger 45/48, Mali 495/495 equal the sold quantity). The issue's original claim "All are totals" is already qualified in its own later paragraph, so no re-narrowing is needed.
C item:
- Question: for each own 2021 release, is `s16dq05c` the weight of one UML or a total after CAPI/export? What do the older waves' 9999/99999 and zero sold-kg codes mean?
- Options: see #1571 (a)-(e). Alternatively, seek producer/export metadata first (questions 1-3 of the decision packet are UNSENT).
- Evidence: Niger sold-kg rows 1,296 / 403 / 1,062 / 1,818; Mali 1,136 rows, 4.87M kg, max 1,150,000. Seven dry-season kg-only reports total 8,740 kg, and their own sold flag says Non on 6 of them.
- Recommendation for the human: decide whether to send the packet's questions to the producers. Until then only (d) is safe.
- Nothing is decided here. -- Sue

### #1570 — Ethiopia crop sales: W1 section 9 is short, W1 trees have a sale and no harvest
Verdict: open/none, tier B. Narrowed 2026-11-10 after #1687 (`16c3f4d9c`). The only merged work since is #1690 (`1e549e96a`), which adds `Ethiopia/_/CONTENTS.org` "2026-11-10 -- Sold-source correspondence (GH #824)". That section restates W1/W2 `ph_s11q03_b` grams as unread and W1 `ph_s11q04_b` as BIRR Decimal, and it changes no producer. Idiosyncrasy: section 9 and section 4 share 6,983 one-to-one keys once float/int storage is ignored. The W1 manual (pp79–80) excludes tree/root crops from section 9.
This issue already IS the #1457 side finding "Ethiopia W1 §9: short file, value-only rows, unread grams and Birr decimals", and the finding still holds on development. Its body also carries the W1 `crop_diversity` sold-only count (1,910→2,416 households, mean 0.774→0.947) that the coordinator tracks as the stale-docstring finding.
No write needed: narrowed 2026-11-10, and #1690 only restates it.

### #1559 — Site Q cannot see a survey kilogram wrong against its own quantity
Verdict: needs-decision/ask, tier C (labelled B). #1582 (`324fa56b84c0`) landed `kg_survey_off_label` (`transformations.py:4711-4721`, `:4763`). The narrowed remainder is, in the issue's own words, "a new choice". `git log -S kg_survey` since 2026-11-05 shows no later work. This is NOT the #1457 "Site Q sold columns" side finding, which #1650 (`42a25fabc9`) resolved: `quantity_audit.py:457` now screens `Quantity_sold` and `Sale_price` for `crop_production`.
C item:
- Question: should units with no stated mass (Niger `Botte`, `Tiya`, `Panier`: 67.7% of Niger survey kg; Mali 16.5%) get a `kg_survey` screen against the `(t, u)` median?
- Options: (a) none (status quo); (b) a report-only 0.1x–10x band around the `(t, u)` median, cells ≥ 30 rows, on the `harvest_kg_factors` record; (c) (b) wired into Site Q channels (`quantity_reports`, `LSMS_QUANTITY_STRICT`, `scan.py`).
- Evidence: (b) names 218 of 24,033 Niger rows (128,734 kg). The existing label band names 107 of 5,642 sized rows.
- Recommendation for the human: (b), as a flag only, consistent with the 2026-11-05 "report, never refuse" ruling. (c) would be a new ratchet.
- Nothing is decided here. -- Sue

### #1555 — Niger 2018-19: crop kilograms need release and calibration authority
Verdict: needs-acquisition/none, tier D. Narrowed 2026-11-09 with #1657 (`86077a65f2`; `Niger/_/CONTENTS.org:3422-3457`), with no merged work since. Idiosyncrasy: the held XLS `S16c_Agri!M11` lists 7 = Autre, while the release/DDI has 7 Botte, 8 Tia, 9 Autre. State 5 is Entier on the form and Non concerné in the release. 2018-19 `s16c` asks no kilograms (`CONTENTS.org:2725`), so `harvest_kg` stays at 36,695 kg. What remains is external evidence: the fielded-CAPI/export bridge and an official kg-per-harvest-unit calibration.
No write needed: the body already says exactly this.

### #1552 — Nigeria W1/W2: resolve native cross-plot sale repeats with source evidence
Verdict: needs-acquisition/none, tier D. Narrowed 2026-11-09 with #1657 (`86077a65f2`; `Nigeria/_/CONTENTS.org:4080-4114`), with no merged work since. Idiosyncrasy: the own forms organize plot-crop roster lines with main-buyer sales since harvest. They carry no transaction ids, so equal payloads are not transaction identity (72/67 excess occurrences). Remaining: fielded A3/export-custodian evidence; contacting the custodian is not authorized.
No write needed.

### #1551 — Nigeria #843: attach W3-W5 sales by the sale's own crop code?
Verdict: needs-decision/ask, tier C. No ruling is recorded. The 2026-11-10 R2a ruling ("Keep ambiguous sales on Farm-level rows") covers "exactly one of several harvest rows has a positive Quantity". It does not cover this case, where two lines of ONE plot both state a harvest and the sale record carries its own crop code. The landed state is #1550 (`07174098d`) under the #843 rulings A–D (`Nigeria/_/CONTENTS.org:2449-2470`, `:2755-2775`). Idiosyncrasy: from W3 the sale sits in A3.ii (`secta3ii`) per household and crop, with no plot. The 123 cases are 0.83% of W3–W5 sale value.
C item:
- Question: should a W3–W5 sale attach by its own crop code to one line of the single known plot, instead of `Farm-level`, when two lines of that plot state a harvest?
- Options: (a) keep today's `Farm-level` (rule A as ruled; consistent in spirit with R2a); (b) attach each sale record by its own crop code (202/204 land on one line, 283 attach in all, 81 in pairs spanning plots), which dissolves most of rule B's summed rows; (c) keep the plot with a sentinel line. In simulation (c) equals today on `crop_diversity`, `gross_crop_revenue` and `harvest_kg`.
- Evidence: `CONTENTS.org:2765-2771`. 12 of the 42 single-record cases used to sit on a line of a different code.
- Recommendation for the human: (a) or (c). (b) changes the ruled grain and partly reverses rule B.
- Nothing is decided here. -- Sue

### #1529 — Niger 2011-12: resolve section 3E crop-cycle area semantics and yield attachment
Verdict: partial/narrow, tier C (transport was B, now done). New since the 2026-11-09 narrowing: #1690 (`1e549e96a`) added `Niger/_/CONTENTS.org` "2026-11-10 -- Independent French 3E review", and the two French PDFs are now DVC-managed with verified readbacks. The 3E residual is unchanged in substance, and the six-native-column companion proposal is UNIMPLEMENTED. Idiosyncrasy: 3E is household-crop across parcels, with several harvests allowed within one cycle. Niger has no `season` level.
New title: Niger 2011-12: resolve section 3E crop-cycle area semantics and yield attachment (unchanged)
New body:
> **Resolved so far:** PR #1632 (`5a06d85cd0`, 3C applied inputs); PR #1658 (`87df6b6bf0`, 3B `cultivation_records`, 3D `farm_cost_operands`); PR #1690 (`1e549e96a`): the own French questionnaire (WB2050 resource 31148) and enumerator manual (resource 31157) are DVC-managed and read back, with q3E.8 quoted in `Niger/_/CONTENTS.org` ("2026-11-10 -- Independent French 3E review").
>
> **Remaining: section 3E crop-cycle area (`as03eq08`).**
> - First versus average: the questionnaire asks "la superficie consacrée au premier cycle" in m²; the manual heading reads "Superficie moyenne en mètre carré consacrée au premier cycle"; the released Stata label says average per cycle. Neither witness is presumed stale. A version-bound fielded/export correspondence would settle it; failing that, a ruling on which reading to serve.
> - Code 999: no rule in either document.
> - Grain/attachment: 3E is household-crop across parcels, with several harvests within a cycle. Establish a source-supported rule before using `as03eq08` as parcel area or a yield denominator. No physical-parcel allocation or land-union denominator follows from the documents.
> - The six-native-column companion proposal (ledger `1529-niger-primary-peer-archive-77b4ce99-20261110.md`) is unimplemented and its six controls are unrun. Serving the raw 3E columns as-recorded, with no interpretation, is the bounded next step if approved.
>
> No area, season, yield or derived-cost default is selected. The #1560/#1571 unit, window and state qualifications remain separate; `plot_cultivation` stays reserved for #862. -- Sue
C item (embedded):
- Question: serve `as03eq08` as-recorded only (six-native-column companion), or also rule first-cycle versus average and 999?
- Options: (a) as-recorded companion, no yield use; (b) adopt the documentary first-cycle reading for yield; (c) hold until export evidence is found.
- Recommendation for the human: (a) now, (c) for any yield use. -- Sue

### #1437 — Mali/Burkina 2021-22 EHCVM: harvest quantity s16cq16a is filled only for unfinished harvests
Verdict: partial/narrow, tier B (Burkina part) + D (CAPI authority). Mali's half was done by #1515 (`ad44c8d461`): `Mali/_/derivations.yml` `Mali::crop_production::harvest-from-dispositions` (function `mali:derive_hh_crop_harvest_total`) plus `tests/test_gh1437_mali_harvest_total.py`. The body was never narrowed after the 2026-11-05 comment. Burkina 2021-22 still has no `crop_production` builder (`Burkina_Faso/_/data_scheme.yml:192-200`, "2018-19 only"; `CONTENTS.org:2747`). Idiosyncrasy: the held 2021 forms route a finished harvest to Q16C.16, yet released `s16cq16a` is blank on 0/7,663 finished Burkina rows and 22/5,237 Mali rows, while Niger fills all 11,910.
New title: Burkina 2021-22 EHCVM: harvest quantity s16cq16a is blank for finished harvests (Mali done by #1515)
New body:
> **Resolved so far:** PR #1515 (`ad44c8d461`) for Mali 2021-22, per the 2026-11-04 ruling. 3,360 harvest totals come from the `s16d` dispositions under the registered key `Mali::crop_production::harvest-from-dispositions` (multi-unit NA), plus the window key; `tests/test_gh1437_mali_harvest_total.py`. Documentation: #1496 (`cf5a4bbf68`) and #1668 (`a9e15b3abf`). The held Mali and Burkina 2021 forms and Burkina's manual print the finished-harvest route to Q16C.16.
>
> **Remaining:**
> 1. Burkina Faso 2021-22: once its `crop_production` builder exists (#1436, second residual; agriculture is panel-component only, #1276), decide whether to apply Mali's `harvest-from-dispositions` derivation. `s16cq16a` is filled on 179 of 813 unfinished and 0 of 7,663 finished harvests. That needs its own ruling and registered key.
> 2. Both countries: the deployed CAPI program / export-field binding that explains the released blanks (tier D). The printed route is not proof of the fielded program, and Mali passage 2's metadata title conflicts.
>
> -- Sue

### #1436 — Crop production: Mali 2017 condition authority and remaining 2021–22 integrations
Verdict: partial/narrow (one-sentence edit), tier B/C/D mixed. Narrowed 2026-11-10 10:14Z, before #1687 merged at 17:51Z. Its sentence "Metadata candidate `63bf28cb` is reviewed and queued for a later carry" is now stale: #1687 (`16c3f4d9c`) carried it (`Mali/_/CONTENTS.org:3870`, "own2017 P2 enumerator manual held; state bridge remains open"). `git cherry` marks `63bf28cbe` "+" only because of the omnibus merge. The commit also sits on `refs/archive/2026-11-11/docs/1436-mali-p2-manual-20261110`, which can be dropped. No other merged work since. Idiosyncrasy: EAC-I Q7F13 prints a three-option state card, but the export has a fourth native label, `Egrener`.
New title: (unchanged)
New body: keep the current body verbatim, except replace the first paragraph's last two sentences with:
> The P2 manual note and its holding facts were carried to development by PR #1687 (`16c3f4d9c`; `Mali/_/CONTENTS.org`, "2026-11-10 -- own2017 P2 enumerator manual held; state bridge remains open"). Neither note changes quantity, units, condition maps or weights. The remaining Mali requirement is the native code-4 / printed-card / fielded-version bridge, not access to this manual. -- Sue
Note for the coordinator: the second residual (the Burkina 2021-22 driver) is the same work that #1437's Burkina half and #824's Burkina 2021-22 scope wait on. Let #1436 own it and link the other two.

### #824 — Sold-unit valuation: remaining tree-factor questions and wave source audits
Verdict: partial/narrow, tier B. Since the 2026-11-04 body, these are done:
- Mali tree equivalents: stay unwired by ruling 2026-11-03 (comment 9).
- Nigeria W1/W2 lost positive-sale rows: #1550 (`07174098d`, closing #1433), all 822 discarded lines served under `production_obs`.
- Ethiopia source audit: done as documentation by #1690 (`1e549e96a`; `Ethiopia/_/CONTENTS.org` "Sold-source correspondence after #1257/#1274 (GH #824)"; `.coder/ledger/824-ethiopia-sold-source-audit.md`: "No concrete executable wrong-column/role repair is established").
Still open:
- Nigeria W1/W2 other-buyer block `sa3q16a/b` and `sa3q17` is unread (`Nigeria/_/CONTENTS.org:2312-2316`; `Nigeria/_/crop_production.py:160` reads only `sa3q11a/b`, `sa3q12`).
- Burkina 2014 and 2021-22 wiring proposals.
- Non-ISA lanes.
Idiosyncrasy (`Burkina_Faso/_/CONTENTS.org:1843-1857`): the 2014 EMC is a different instrument, with sale block `Y22A/Y22B/Y22C/Y23` and a reported kg-per-sold-unit factor. 2021-22 moves the sale to crop-level `s16d`. Wiring either "is a decision".
New title: Sold-unit valuation: Burkina 2014/2021-22 sale wiring, Nigeria W1/W2 other-buyer sales, Ethiopia export cards
New body:
> **Resolved so far:** PR #993 (`f9d3cbbc2f`), #1270 (`f19864464b`), #1272 (`e6ed24ee52`), #1326 (`813a73aedd`), #1355 (`dfb7628ae4`, sold-unit consumer), #1392 (`b06abb5d5d`, Mali EAC-I FIELD factors), #1466 (`659b2479a`, Mali EHCVM `crop_production` 2018-19/2021-22), #1550 (`07174098d`, Nigeria W1/W2 discarded positive-sale lines served under `production_obs`; #1433 closed), #1690 (`1e549e96a`, Ethiopia sold-source correspondence audit; no executable wrong-column repair found). Ruled 2026-11-03: Mali EAC-I tree "Equivalent en Kg" (`s3bq13c`, `s11fq14c`) stay unwired.
>
> **Remaining:**
> 1. **Burkina Faso 2014 and 2021-22 harvest/sale wiring proposals.** Neither wave has a `crop_production` builder. 2014 (EMC) has its own sale block (`Y22A/Y22B/Y22C/Y23`) with a reported kg-per-sold-unit factor. 2021-22 records the sale at crop level in `s16d`, with agriculture panel-component only (#1276) and finished-harvest quantities blank (#1437). The 2021-22 driver is owned by #1436. Each proposal needs a sparse-coverage explanation; this is not a copy of the 2018-19 audit.
> 2. **Nigeria W1/W2 other-buyer sales.** A3 also carries `sa3q16a/b` (quantity and unit sold to OTHER buyers) and `sa3q17` (their value). The build reads only the main buyer (`sa3q11a/b`, `sa3q12`), so a served farmgate price is a main-buyer price. Wire or document as out of scope. Keep it distinct from #1552 (cross-plot equal-payload repeats).
> 3. **Ethiopia:** the audit's residual export cards (W2 `sect12_ph_w2`, W3 `sect11/12_ph_w3`, W4 `sect11_ph_w4`, W5) and the held W1/W2 grams / W1 BIRR Decimal questions. The latter are tracked in #1570.
> 4. Non-ISA sold-unit audits remain with their owning lanes.
>
> Preserve Tanzania's source-confirmed Kg sales, the EAC-I Lourd/Leger distinction and every recorded sale window and amount. Do not repeat the merged consumer, window or Unit_sold wiring. -- Sue
Optional split (if the coordinator prefers one issue per kind of work): residual 2 as its own issue.
> Title: Nigeria W1/W2: the other-buyer sale block (sa3q16a/b, sa3q17) is unread
> Body: Split from #824. The W1/W2 A3 files record a main-buyer sale (`sa3q11a/b`, `sa3q12`, served as `Quantity_sold`/`Unit_sold`/`Value_sold`) and a separate sale to OTHER buyers (`sa3q16a/b` quantity and unit, `sa3q17` value). `Nigeria/_/crop_production.py:160,175` reads only the main buyer, so W1/W2 sales and any farmgate price built from them are main-buyer only (`Nigeria/_/CONTENTS.org:2312-2316`). Decide whether to serve the other-buyer block (new columns, or a sale-observation level), then wire it, measuring W1/W2 `Value_sold` before and after. W3-W5 sales (A3.ii) have no such block. Tier B. -- Sue
> Parent comment: Split the Nigeria W1/W2 other-buyer block to #NEW; this issue keeps the Burkina wiring proposals and the Ethiopia export cards. -- Sue
QUEUE: #824 (residual 2) | Nigeria | read sa3q16a/b + sa3q17 in the W1/W2 crop_production config, as separate other-buyer columns (needs a column-shape choice first) | Nigeria/_/crop_production.py, Nigeria/_/data_scheme.yml, lsms_library/data_info.yml (optional columns), Nigeria/_/CONTENTS.org | one country (Nigeria: data_scheme.yml/nigeria.py are in every Nigeria table's hash) | cold Nigeria crop_production build; Value_sold conservation vs raw A3; gross_crop_revenue before/after
QUEUE: #1436 (residual 2) / #1437 / #824 | Burkina Faso 2021-22 | add a crop_production driver from panel-component s16c plus s16d sales on Mali 2021-22's #1466 pattern (option d: single harvest row, else Farm-level); then decide harvest-from-dispositions | Burkina_Faso/2021-22/_/crop_production.py (new), Burkina_Faso/_/data_scheme.yml, burkina_faso.py, CONTENTS.org | one country (Burkina) | cold Burkina crop_production; sale conservation vs raw s16d; Feature(crop_production) assembly; mixed-component disclosure per #1276. Confirm first that the 2026-11-03 option-d ruling extends to Burkina.


## 8.B04-isa-plot-inputs

# Batch B04 — ISA plot inputs / yield / crop labels

### #1483 — EHCVM fertilizer: finish household-input wiring, parcel representation and nutrient-basis gaps
Verdict: partial/split, tier B (parent) + C (new). Landed: Burkina 2021-22 and Mali 2018-19/2021-22 s16b household adapters (PR #1687, `16c3f4d9c`; `Burkina_Faso/_/data_scheme.yml:253`), Senegal 2021-22 s16b (PR #1695, `d5a2b1f9e`; `Senegal/_/data_scheme.yml:219-227`, `Senegal/_/s16b_2021.py`). Body was updated 2026-11-11 after #1695; nothing merged since. Two defects in the current text: the DAP catch-all item is parked "under #1445", but #1445 closed 2026-11-05 (#1527, `997f33c8a`) having handled only NPK 15/15; and the Guinea-Bissau item duplicates #1107. The remaining items are of two kinds (B wiring/validation, C rulings), so split the two rulings out.
Idiosyncrasy (CONTENTS.org): EHCVM s16b is household × input grain with no parcel column, while s16a separately asks parcel fertilizer (`s16aq29c1/c2`); older "no parcel-level input question" notes were too broad (Mali `CONTENTS.org:3746`, Burkina `CONTENTS.org:3387`, GNB `CONTENTS.org:633`).

**Parent — new title:** EHCVM household inputs: validate the Burkina/Mali routes publicly and reconcile Niger/Senegal documentation authority

**Parent — new body:**
> **Resolved so far:** PR #1687 (`16c3f4d9c`): Burkina Faso 2021-22 and Mali 2018-19/2021-22 own-wave s16b household-input adapters (explicit `q02=1` membership, separate used/gift/purchased quantities and units, native identity, full DAP catch-all label). PR #1695 (`d5a2b1f9e`): Senegal 2021-22 s16b with its three distinct unit cards (code 1 = `Kg` for use, `Gramme` for gift/purchase); exact-head full-data run 42953118393 passed.
>
> **Moved elsewhere:** Guinea-Bissau input labels and kg-unit routes are #1107 (same work: no `harmonize_input`, `Quilograma`/`Grama` have no kg factor). The parcel-level `s16a` representation and the `DAP et autres engrais inorganiques` nutrient basis are maintainer decisions, now #NEW (the basis item used to point at #1445, which closed on 2026-11-05 having handled NPK 15/15 only).
>
> **Remaining (Tier B):**
> - [ ] Public serving validation of the three #1687 routes: `Country('Burkina_Faso').plot_inputs(waves=['2021-22'])`, `Country('Mali').plot_inputs(waves=['2018-19'])` and `(waves=['2021-22'])`, plus `Feature('plot_inputs')` for those cells, cold and warm, in private roots. The ledger (`.coder/ledger/1483-bfa-mali-integration.md` §4) records adapter checks only: BFA 70,994 source rows -> 12,223 applications; Mali 12,507 -> 12,507 and 62,854 -> 12,416.
> - [ ] Reconcile the Niger and Senegal 2021-22 documentation holdings with the design claims that still need authority. Both folders hold French/English BIDs and both household questionnaires; the old "SOURCE.org only" premise is stale and is not an acquisition task.
>
> Tier B. -- Sue

**New issue — title:** EHCVM fertilizer: decide parcel-level s16a representation and the DAP catch-all nutrient basis
**Tier:** C
**Body:**
> Split from #1483. Two maintainer rulings, neither inferable from the landed household adapters (#1687, #1695).
>
> 1. **Parcel-level `s16a`.** EHCVM `s16b` is household × input with no parcel column, but `s16a` separately asks parcel fertilizer (`s16aq29c1/c2`; Guinea-Bissau 60 positive, GNB `CONTENTS.org:633`). Options: (a) leave `s16a` unserved; (b) serve `s16a` parcel quantities as their own observations beside the household `Farm-level` rows, with no reconciliation; (c) a separate parcel-input table. Household quantities must not be allocated to plots under any option.
> 2. **`DAP et autres engrais inorganiques` (code 6).** It maps to `DAP` in the Benin, Côte d'Ivoire, Niger, Togo and Senegal `categorical_mapping.org` `harmonize_input` tables. The new Burkina/Mali/Senegal 2021 adapters keep the full catch-all label instead. Options: (a) keep `DAP` (pure-DAP N share on a mixed bucket); (b) map it to `Other Fertilizer` and leave it unmatched for N; (c) a documented catch-all share. The 2026-10 measurements of the catch-all N share (Niger 14.9%/4.5%, Senegal 8.1%, Burkina 2.3%, CIV 2.1%, Benin 1.0%, Togo 0.2%) are dated and were not remeasured.
>
> Evidence: #1483 body; #1445 closing comment (only NPK 15/15 was handled); `lsms_library/countries/*/_/categorical_mapping.org` rows quoted above. -- Sue

**Parent comment:**
> Narrowed to the remaining B work. Guinea-Bissau's labels and kg units are #1107. The parcel-representation and DAP-basis rulings are now #NEW; the basis item used to point at #1445, which closed without touching it. -- Sue

QUEUE: #1483 | Burkina/Mali | public Country/Feature cold+warm validation of the #1687 household-input routes (no code expected) | none, unless a defect turns up (`*/_/s16b_2021*.py`, `Mali/_/s16b_header_adapter.py`) | none (verification only) | private-root cold/warm `plot_inputs` for BFA 2021-22, Mali 2018-19/2021-22, plus `Feature('plot_inputs')`

### #1485 — Anonymous fertilizer-rate routing: decide automatic recovery policy; omission diagnostics implemented
Verdict: needs-decision/none, tier C. Narrowed 2026-11-08 after #1614/#1618/#1620/#1621 (all ancestors of `e5fdd2fa8`). The only later `transformations.py` change is #1694's `nitrogen_kg` role filter (`transformations.py:6502`), which does not touch `fertilizer_rate` routing. No write needed; the body already states only the residual.
C item:
- **Question:** should `fertilizer_rate`'s default anonymous parcel route try to recover rates when the full plot-key vocabulary is unknown, or stay as is (warn, don't recover)?
- **Options:** (a) keep the current behaviour: the warning from #1621 discloses whole-cell omissions, and callers choose `on='plot'` explicitly; (b) recover only when both inputs carry agreeing country-provenance attrs (the documented Ethiopia/Malawi literal route), and refuse otherwise; (c) make `on=` required, with no default.
- **Evidence:** the historical Malawi result `on='parcel'` gave 14,909 rows against 37,010, losing 2016-17 and 2019-20 (from #1435, not remeasured). The issue body rules out inferring keys from underscores, one-sided attrs or match counts.
- **Recommendation for the human:** (b) or (c). Both are explicit, and neither guesses keys. Each needs its own design and verification. Re-measure the Malawi loss on `e5fdd2fa8` before choosing (unverified-data).

### #1482 — Tonne-dominated plot-input quantities: adjudicate quantity basis; Site Q screening is available
Verdict: open/none, tier B (diagnosis), then C. Narrowed 2026-11-08 after #1618 (`728992c76`; `quantity_audit.py:450` puts `plot_inputs.Quantity` in the screen). No later merged work touches Tonne input rows. The body already states only the residual.
Idiosyncrasy: EHCVM NPK is served at 0.17 N with no stated grade, and a few `Tonne` rows carry most of a slot's nitrogen (Côte d'Ivoire `CONTENTS.org:953`, 38.2%; Benin `CONTENTS.org:683`, 30.2%; Burkina `CONTENTS.org:2704`, 69.6%). Burkina `CONTENTS.org:3335` marks that concentration as a dated measurement. Current shares are unverified-data.
No write needed. Next step: a read-only diagnosis that reads each wave's input unit card (Tonne code) and the flagged rows from `quantity_reports(table='plot_inputs')`, then brings each cell back to the maintainer as a question.
QUEUE: #1482 | Mali 2017-18, CIV/Burkina/Benin 2018-19 | diagnosis only: per-cell table of Site Q-flagged Tonne rows against the questionnaire unit card, with no correction | none (report only) | none | private-root `Country(C).plot_inputs()` per cell plus `quantity_reports(table='plot_inputs')`; questionnaire pages cited

### #1460 — yield_kg: resolve held land-route area authority after merged routing fixes
Verdict: partial/split, tier B. The parent was narrowed 2026-11-10 18:57Z (after #1492/#1543/#1545/#1548), and its residual (Benin, Togo and Guinea-Bissau held off the land route pending #1531) is unchanged.
**New since the narrowing.** #1690 (`1e549e96a`, commit `729cf1cf0`) rewrote `tests/test_gh1460_yield_area_t.py:124-147`. It subsets Nigeria `plot_features` to PP rows before `area_t=`, and it now asserts that the documented call `yield_kg(cp, pf, area_t=pp_ph_area_t(c))` raises `ValueError` "already" (`:140`; refusal in `transformations._area_on_harvest_rounds`, `transformations.py:5682-5686`). The cause is the #587 work in the same merge, which appends PH-round `plot_features` for `2011Q1/2013Q1/2016Q1` (`Nigeria/_/CONTENTS.org:4144-4170`). So the usage example in the `pp_ph_area_t` docstring (`transformations.py:5636-5637`) no longer runs on Nigeria, and the parent's "area_t serves Nigeria 17,751 yields" predates native PH areas.
Idiosyncrasy: Nigeria is PP/PH (area recorded on post-planting quarters, harvest on post-harvest). #587 established that the PH plot omissions were real half-missing features, so W1-W3 now have area in both rounds.

**Split, new issue — title:** Nigeria yield_kg: the documented `area_t=pp_ph_area_t(c)` call raises now that W1-W3 serve PH-round plot areas
**Tier:** B (docs and helper), with one C question
**Body:**
> Residual of #1460, caused by the #587 work merged in #1690 (`1e549e96a`).
>
> `#1690` appended PH-round `plot_features` for Nigeria `2011Q1/2013Q1/2016Q1` (`lsms_library/countries/Nigeria/_/CONTENTS.org:4144-4170`). `transformations._area_on_harvest_rounds` correctly refuses to move PP area onto a PH round that already serves area (`transformations.py:5682-5686`). The documented call in `pp_ph_area_t`'s docstring (`transformations.py:5636-5637`), `yield_kg(c.crop_production(), c.plot_features(), area_t=pp_ph_area_t(c))`, therefore raises on Nigeria. `tests/test_gh1460_yield_area_t.py:130-145` now pins that: it subsets to PP rows for `area_t=` and computes PH-round yields directly.
>
> - [ ] Fix the docstring example (subset to PP rounds, as the test does), and recheck the #1460/#1548 "17,751 Nigeria yields" figure on current development.
> - [ ] **Question for the maintainer (C):** for W1-W3, should the default Nigeria yield use the PH round's own area (now served) or the PP area moved by `area_t`? W4/W5 have no fresh PH area (W4 `sa1q11` lacks a unit bridge, W5 has no field), so only the PP route serves them. Leave this unanswered until ruled. Do not sum the two areas.
>
> Verify (unverified-data here): `yield_kg(cp, pf_pp, area_t=pp_ph_area_t(c))` against `yield_kg(cp, pf_ph)` on `Country('Nigeria')`, as row counts and totals per round. -- Sue

**Parent comment:**
> Narrowed text still stands: Benin, Togo and Guinea-Bissau remain held pending #1531. A separate Nigeria residual from #1690 (the `area_t` docstring call now raises, because W1-W3 serve PH area) is now #NEW. -- Sue

Note for the coordinator: #587 is excluded-in-flight (Nigeria). Its owner may prefer to fold the new issue into #587's PH plot_features work.

QUEUE: #1460-split | Nigeria | correct the `pp_ph_area_t` docstring example to subset PP rounds; remeasure yields | `lsms_library/transformations.py` (docstring only) | probably none (a docstring in `pp_ph_area_t`, which is outside GhanaLSS nutrition's import closure, the one route by which transformations.py reaches a fingerprint; confirm with a stash-probe) | `pytest tests/test_gh1460_yield_area_t.py`; private-root Nigeria yield counts per round

### #1119 — plot_inputs: apply the ruled crop markers and finish Senegal/Guinea-Bissau crop labeling
Verdict: partial/narrow, tier B. Every LSMS-ISA marker implementation has landed: #1203 `d432c3f51`, #1231 `856d9ddbf`, #1295 `9c1619828`, #1323 `3e2f22f69`, #1350 `123fef6af`, #1365 `8578ec314`, #1366 `15590e089` and #1370 `cb2cf5545`, all ancestors of `e5fdd2fa8`. Three sites remain, and nothing touching them has merged since the 2026-10-30 update. The Guinea-Bissau site is a live contract violation, not just unfinished work: `Guinea-Bissau/2018-19/_/plot_inputs.py:24-30,45,125` serves every seed row as `crop='(not crop-specific)'`, because the country has no `harmonize_seed_crop`. Those rows carry a recorded crop in the Portuguese label (`Sementes de arroz`), and the 2026-10-23 ruling says a recorded-but-unmapped value passes through. #1695 (`d5a2b1f9e`) reuses Senegal's `harmonize_seed_crop` for 2021-22, so the spelling residual now covers that wave too.
Idiosyncrasy: GNB's `categorical_mapping.org` has neither `harmonize_input` nor `harmonize_seed_crop`, and a bare `get_categorical_mapping` on a missing table silently returns the first org table (`u`), hence the `_table_exists()` guard (`plot_inputs.py:23-32`). GhanaLSS writes `j='Farm-level'` for non-crop-specific blocks by its four-grain convention (GhanaLSS `CONTENTS.org:5376-5392`).

**New title:** plot_inputs crop axis: Guinea-Bissau seed crops, Senegal maize/cowpea spelling, GhanaLSS `j='Farm-level'` marker

**New body:**
> **Resolved so far:** the 2026-10-23 marker ruling is applied in every LSMS-ISA country. #1203 (`d432c3f51`) Burkina maize/cowpea spelling; #1231 (`856d9ddbf`) Mali; #1295 (`9c1619828`) canonical vocabulary; #1323 (`3e2f22f69`) Malawi; #1350 (`123fef6af`) Uganda; #1365 (`8578ec314`) Ethiopia; #1366 (`15590e089`) Tanzania; #1370 (`cb2cf5545`) Nigeria. Burkina, Mali and Niger are not reopened.
>
> **Contract (ruled @ligon 2026-10-23):** a structural noncrop record is `(not crop-specific)`; an unrecorded crop on a crop-specific row is `unknown_crop`; a recorded but unmapped crop passes through. All three stay distinct from the `plot_id` sentinel `Farm-level`.
>
> **Remaining (Tier B):**
> - [ ] **Guinea-Bissau 2018-19, contract violation.** `2018-19/_/plot_inputs.py` fills every seed crop with `(not crop-specific)` (`:45`, `:125`), because `_/categorical_mapping.org` has no `harmonize_seed_crop`. Seed rows record their crop in the Portuguese label (`Sementes de arroz`, …). Add a GNB `harmonize_seed_crop` from the wave's own `s16bq01` labels, and pass any unmapped seed label through. Bundle it with #1107's `harmonize_input` in the same file, so GNB pays one re-warm.
> - [ ] **Senegal 2018-19 and 2021-22.** `harmonize_seed_crop` maps `Semences de maïs` -> `Maïs en grain` and `Semences de haricots/niébé` -> `Niébé/Haricots secs` (`categorical_mapping.org:767,772`). Align them with the labels Senegal's `crop_production` serves, as #1203 did for Burkina. Measure the join gain first. #1695 reuses this map for 2021-22.
> - [ ] **GhanaLSS (non-ISA).** The `*/_/plot_inputs.py` non-crop-specific blocks still write `j='Farm-level'` (GhanaLSS `CONTENTS.org:5381-5385`). Migrate them to `(not crop-specific)`, leaving `plot_id='Farm-level'` unchanged.
>
> Acceptance per country: before/after accounting shows that only the crop axis changes, with source rows, other keys, values and attrs conserved. Record each country's hash cost (every `categorical_mapping.org` edit is a one-country re-warm). Code21 authority and acquisition-channel semantics remain separate issues. -- Sue

QUEUE: #1119 | Guinea-Bissau | add `harmonize_seed_crop` (and #1107's `harmonize_input`) from own `s16bq01` labels; unmapped seed labels pass through | `Guinea-Bissau/_/categorical_mapping.org`, `Guinea-Bissau/2018-19/_/plot_inputs.py` | one country (GNB re-warm) | before/after: only crop/input labels change; `nitrogen_kg(GNB)` rows > 0; `seed_kg(GNB)`
QUEUE: #1119 | Senegal | align maize/cowpea `harmonize_seed_crop` Preferred Labels with `crop_production` (the #1203 pattern) | `Senegal/_/categorical_mapping.org` | one country (Senegal re-warm, all 22 tables) | household/crop join counts before/after; seed_kg conserved

### #1118 — plot_inputs: finish non-ISA missing-unit normalization
Verdict: partial/narrow, tier B. Landed: #1222 `6ffeb3caf` (Mali), #1223 `9a0ed5f3c`, #1251 `675e544d5` (generic string/NA case) and #1341 `4c8da351f` (Nigeria). Malawi needed no patch. The title is misleading, because the live sites are EHCVM. The issue names Benin (1 row) and Togo (9), yet the same minted `fillna('Manquant')` appears in five 2018 wave scripts and in Burkina's new 2021-22 adapter, `Burkina_Faso/_/s16b_2021.py:133` (`1a029791e`, merged through #1687 on 2026-11-10, after this issue's last update). None of those five countries maps `Manquant` -> `Unknown` in `categorical_mapping.org`; Niger (`:203`) and Mali (`:758`) do. The Mali and Senegal 2021 adapters already use `Unknown` (`Mali/_/s16b_header_adapter.py:96`). The transforms read `Manquant` as no unit (`transformations.py:2638`), so the defect is a non-canonical `u` key on the served index, not lost kilograms. Row counts are unverified-data.

**New title:** plot_inputs: replace the minted `Manquant` unit with `Unknown` in six EHCVM countries, and settle GhanaLSS cost-only rows

**New body:**
> **Resolved so far:** #1222 (`6ffeb3caf`) Mali 42,089 rows -> `Unknown`; #1223 (`9a0ed5f3c`) Mali plot-level use answers served explicitly; #1251 (`675e544d5`) the generic string/`pd.NA` missing-unit fill and the cold-path alignment; #1341 (`4c8da351f`) Nigeria after channel reduction. Malawi needed no patch (179,917 rows, 240 `Unknown`). Ethiopia (#1181) and Tanzania (#1185) are landed.
>
> **Contract (#1041):** a missing or blank unit code is `U_UNKNOWN` = `Unknown` (`transformations.py:4148`). It is never a minted label, a silent drop or NA on a canonical key.
>
> **Remaining (Tier B):**
> - [ ] These wave scripts fill a missing input unit with the minted `Manquant`, and the country has no `Manquant -> Unknown` row in `categorical_mapping.org`:
>   - `Benin/2018-19/_/plot_inputs.py:93`
>   - `Togo/2018/_/plot_inputs.py:47` (`UNIT_NA`)
>   - `Burkina_Faso/2018-19/_/plot_inputs.py:92`
>   - `Burkina_Faso/_/s16b_2021.py:133` (the 2021-22 route added by #1687; the Mali and Senegal 2021 adapters already write `Unknown`)
>   - `Senegal/2018-19/_/plot_inputs.py:104`
>   - `CotedIvoire/2018-19/_/plot_inputs.py:85`
>   - `Guinea-Bissau/2018-19/_/plot_inputs.py:126` (coordinator addition; Guinea-Bissau has no `Manquant` row either)
>
>   Switch each to `Unknown`. Where `Manquant` is also a raw source label, keep the raw answer in its own column, as Niger does. Count the affected rows per wave first: the historical figures (Benin 1, Togo 9) predate the Burkina 2021-22 route.
> - [ ] GhanaLSS: cost-only records stay real records even without quantities. Settle their `u` without dropping them.
>
> Acceptance: only `u` changes on the affected rows; rows, quantities, recorded units and attrs are conserved; Country and Feature agree; record each country's hash cost (each wave script is a one-country re-warm). -- Sue

QUEUE: #1118 | Benin, Togo, Burkina Faso, Senegal, Côte d'Ivoire, Guinea-Bissau (coordinator addition) | replace the minted `fillna('Manquant')` with `'Unknown'` in 2018 `plot_inputs.py` and `Burkina_Faso/_/s16b_2021.py` | the six files listed | five countries, one re-warm each (wave scripts and a country `_/` module) | per-wave count of `u=='Manquant'` before/after; Country==Feature; nitrogen_kg/seed_kg frame-identical

### #1228 — Malawi crop_production: seasonal crop "Other" is mostly cowpea, which harmonize_crop has no label for
Verdict: needs-decision/ask, tier B (blocked on C questions). Narrowed 2026-11-04 after #1523 (`14c0f5790`, commit `752f2a148`): 2010-11's own-text G rows serve `Cowpea (Khobwe)` (Malawi `CONTENTS.org:821-836`). The Malawi `categorical_mapping.org`/`malawi.py` edits merged since (`6e46615dc`, sugar cane; `85eab021c`, Farm-level sale label; both #1457) do not touch cowpea. No body rewrite is needed; the narrowed header is accurate.
Idiosyncrasy: after 2013-14, Module G has no crop-name specify field. The later waves' name lives in plot-level Module D (`ag_d20_oth`/`ag_d20b_oth`), joined on (hh, `gardenid`, `plotid`). 27 plots in the 2019-20 cross-section carry two "Other Crop" texts. 2016-17 `Cross_Sectional/ag_mod_d.dta` reads only through the restricted-reader precedent (latin-1 bad byte), and 2013-14 holds no text (`CONTENTS.org:886-927`, TODO).
C item:
- **Question:** may the later waves (2016-17 CS 238, Panel 64; 2019-20 CS 584, Panel 113) be relabelled `Cowpea (Khobwe)` from Module D text?
- **Sub-questions:** (1) accept D's text-slot correspondence and reference period as G's identity where exactly one "Other Crop" text exists; (2) move Module I code-48 sales with the harvest, or leave them; (3) local names NKHOBWE / NSEULA / MSEWULA; (4) whether any synthetic code is needed (the 2010-11 step reserved none).
- **Options:** (a) the D-join recovery, restricted to single-text plots, with I sales moved by the same rule; (b) the D-join for harvest only, with a counted sale-detach report; (c) stop at 2010-11.
- **Evidence:** the 2026-10-30 diagnostic counts in the body (1,156 literal G rows; ambiguous/missing D identity 2/1/27/3).
- **Recommendation for the human:** (a), but only after a read-only check that D and G share the season reference period. Leave the local names unmapped until a citable source exists.

### #1525 — Uganda 2019-20: Cassava codes 1081 and 1082 do not behave as their labels say
Verdict: needs-acquisition/none, tier D. Narrowed 2026-11-09 after #1672 (`ba03bf85a`; Uganda `CONTENTS.org:6034-6071`). None of the four post-base merges touches `countries/Uganda`. No write needed: the body already says that the only remaining step is obtaining an attributable questionnaire version, export specification or producer erratum for `GSEC15B` CEB01 1081/1082.
Idiosyncrasy: both forms print `108` Dry and `108_1` Flour, yet 2019-20's 1081 has 2018-19 1082's unit mix and 2019-20's 1082 is mostly heaps, so this is not a clean swap. It is served as labelled (`CONTENTS.org:1534`). (A food_acquired item, outside this batch's plot-input theme.)

### #1518 — Nigeria W1-W3 harvest/yield coverage: integrate verified W3 helper and resolve W1/W2
Verdict: open/none, tier B (it contains C sub-questions). Narrowed 2026-11-08 after #1622 (`ff281acb8`; `lsms_library/nigeria_harvest.py:272`, `native_w3_harvest_kg`, opt-in, not wired as the default). The Nigeria commits merged since (`ed1df1ea2` PH plot_features under #587; `eb682a6da`/`c37a4d806` nonfood declarations, all through #1690) do not touch harvest conversion. No write needed. Coordinator note: #1690 now serves W1-W3 PH-round plot areas, which changes the "yield/land integration" item, since W3 helper yields can join PH area directly. See the #1460 split, and #587's owner.
Idiosyncrasy: W3 ships no reported harvest factor (W4/W5 do) and has no route for Sack/Bag/Basket, so 2016Q1's 360 yields are the kg-reported plots, a selected sample. The fluid/bundle basis and the national vs own-zone factor choice are C questions inside this B issue. Their wording in the body ("no guessed size, density, cross-zone fill … approved") already states the boundary.

### #647 — question: Uganda 2019-20 crop_production silently drops the PART-harvest columns (s5aq06*_11/_21)
Verdict: needs-acquisition/narrow, tier D. The maintainer ruled on 2026-11-04 (comment 2): status quo, keeping the ordinary-only default. The exclusion is now written at the site (`Uganda/_/uganda.py:2380-2386`) and in Uganda `CONTENTS.org:4896-4950`, landed through #1684 (`cbde38fef`). The title and body still read as an open "is the exclusion correct?" question. One stale line remains: `CONTENTS.org:3097-3098` says "(#647, no verdict)".
Idiosyncrasy: "full" against "PART" is not a population split. All 97 PART-bearing rows also carry ordinary quantities, and among 138 initially partial rows the follow-up answers are 92 Yes, 5 No and 38 missing.

**New title:** Uganda 2019-20 PART-harvest slots (5A1): ruled status quo; ask UBOS/World Bank whether 5A1 Q6 is incremental or cumulative

**New body:**
> **Resolved so far:** the maintainer ruled 2026-11-04 to keep the ordinary-only default. `CROP_COLMAPS` reads neither the `_11`/`_21` PART harvest slots nor their sale slots (`s5aq07{a,b,c}_{11,21}`), so the two sides stay consistent. The reason is written at the site (`lsms_library/countries/Uganda/_/uganda.py:2380-2386`) and in `Uganda/_/CONTENTS.org` §"2026-10-30 -- PART harvest is separate from condition…" (#1684, `cbde38fef`). Size: 97 rows, 477 native units, against about 601k full-harvest units.
>
> **Remaining (Tier D, needs a person):**
> - [ ] Ask UBOS / the World Bank: for an initially partial plot-crop, is 5A1 Q6 additional harvest since ordinary 5A, or a revised cumulative total that includes it? Does the same relationship hold for Q7 (quantity sold) and Q8 (sale value)? Which CAPI routing or version rule covers the 5 No and 38 missing follow-ups? Record the answer in `Uganda/_/CONTENTS.org`.
> - [ ] Correct the stale line at `Uganda/_/CONTENTS.org:3097-3098` ("#647, no verdict") to cite the 2026-11-04 status-quo ruling.
>
> Until an answer arrives, nothing changes: no append, replacement, flag exclusion or default aggregation. -- Sue

### #862 — Schema proposal: plot_cultivation, a per-season plot decision-maker table (#167's reserved name, #439's convention)
Verdict: needs-decision/none, tier C (enhancement). Ruled in scope on 2026-10-23, to be designed and built after the defect queue drains, with a design note first. Nothing is implemented: on development `plot_cultivation` appears only in reserved-name comments (`data_info.yml:586,623,666`). #1658 (`87df6b6bf`, Niger `cultivation_records`) says explicitly that it keeps the name reserved for #862, so it is not this table. No write needed now. When the design note is posted, refresh the body's line citations (`data_info.yml:228-232` and `:577-578` have moved).
Idiosyncrasy: the grain must keep `season`. Uganda and Tanzania each have two seasons, and only Uganda and GhanaSPS declare `season` on `crop_production`. Ethiopia's `plot_id` already embeds `holder_id`.


## 8.B05-isa-other

### #1537 — Nigeria: generic Pepper food-price label lacks an Aggregate mapping
Verdict: needs-decision/ask, tier C. PR #1631 (`609b7720f8`, ancestor of development) landed the Groundnuts and Maize (shelled) aliases; the body was narrowed on 2026-11-08 to Pepper only, and nothing has touched Nigeria's `harmonize_food` since. `Nigeria/_/CONTENTS.org:4059-4071` records that Pepper stays unqualified. Idiosyncrasy: in Nigeria's `categorical_mapping.org`, the quarter cells label code 76 (`:109`) and code 2140 (`:213`) as plain `Pepper`, and both have the Preferred label `Fresh Pepper`. Code 77 (`Dry Pepper`) and code 144 (`Ground Pepper`) are separate rows.
C item:
- Question: which Aggregate bucket should the served generic `Pepper` j get?
- Options: (a) map it to the existing Fresh Pepper Aggregate. The evidence: its quarter-cell sources (codes 76 and 2140) are Preferred `Fresh Pepper`, though that is inferred from the code, not stated by the label. (b) Map it to `Condiments`. (c) Create a new general `Pepper` bucket. (d) Leave the pass-through as it is.
- Evidence: categorical_mapping.org rows 109, 110, 151, 213; CONTENTS.org:4059-4071; PR #1631.
- Recommendation: before choosing, have a source check confirm which item codes the served `Pepper` rows actually come from. If every one is code 76 or 2140, (a) has source support. Otherwise (d).
No GitHub write needed beyond putting the question to the maintainer; the body already states the residual exactly.

### #1536 — Uganda ea in 2009-10: fill the 569 NA rows from the household's label in another wave?
Verdict: needs-decision/ask, tier C. No merged work. `Uganda/_/CONTENTS.org:256-264` repeats the issue's numbers: 569 NA rows hold 29.32% of the weight, 514 of those households carry a label in another wave, and the fill is drafted as a follow-up. The #1459 `comm`→`ea` link landed in `a2cf04761`. Idiosyncrasy: Uganda's `ea` is the frame EA, carried with movers the way `comm` is. The 2009-10 NAs are mostly movers and split-offs whose `comm` is blank.
C item:
- Question: should 2009-10's NA `ea` be filled from the same household's label in another wave?
- Options: (a) fill it, as a registered derivation (an `ea-carried-from-other-wave` key in Uganda `derivations.yml`). (b) Leave it NA as now. The #1459 ruling did not ask for a fill.
- Evidence: CONTENTS.org:256-264. After a fill, 3.53% of the weight would stay NA, against 0.12% in 2010-11.
- Cost: one Uganda re-warm.
- Recommendation: (a). It is consistent with the documented "carried when a household moves" semantics, and the 29% weight hole is material for any `groupby('ea')`. Register the construction rather than serving it silently.
No body change needed.

### #1509 — food_kg_factors pools self-valued non-purchased rows into the kg inference
Verdict: needs-decision/ask, tier C (framework; B once ruled). No merged fix. `transformations.py:2427` `food_kg_factors` was not touched by the post-base merges: #1694 edited only `nitrogen_kg`, in `95aef402d`. The 2026-11-07 body already states the pending question. The cited ledger commit `ff955905c6` is not an ancestor of development; the evidence lives only in `isa-sm-20261103/reports/framework-1509-comparison-20261107/`. #1442 is held on this issue.
C item:
- Question: for the kg-factor estimator, does "eligible evidence" mean (i) independently observed purchase prices only, or (ii) every source-labelled purchase, including quantities allocated from a total? Cambodia's purchased quantity is `total_quantity * spent / (spent + obtained)`, which makes the price endogenous to the obtained value.
- Options: (i) restrict to purchases whose quantity was observed directly, which needs a per-country provenance flag for allocated quantities. (ii) Restrict to every purchase row: a literal one-line filter. Measured: Cambodia −1.08% kg, Serbia −2.12% kg, no factor loss. (iii) Keep the current pooling.
- Evidence: the comparison report's EVIDENCE-MEMO.md and REPORT.md. Burkina 2014 under #1442 would gain +8.8% kg, and its fit against the survey's unit weights would worsen from 0.618 to 0.693.
- Recommendation: (ii) now, with an explicit `allocated-quantity` caveat for Cambodia. (i) can follow as a separate refinement. Either way, measure Uganda and GhanaLSS before landing, since GhanaLSS nutrition hashes move.
No body change needed.

### #1502 — Nigeria W2 and W3 ship food conversion tables the build does not read; W3's inferred factors are 0.556x of them
Verdict: needs-decision/ask, tier C (B once ruled). No merged work. `Nigeria/_/CONTENTS.org:518-528` says `food_conv_w2pp.csv`, `food_conv_w2ph.csv` and `2015-16/Data/food_conv_w3` are "wired to nothing". Old readers exist in `2015-16/_/units.py:61` and `2012-13/_/units.py:32-38`, but neither `Nigeria/_/Makefile` nor `data_scheme.yml` references them. Idiosyncrasy: Nigeria is post-planting/post-harvest. W2 ships separate PP and PH tables, and W4's own `cvn` matches the W3 table at a median ratio of 1.000 (`CONTENTS.org:373-376`).
C item:
- Question: should the W2 and W3 shipped food conversion tables supply a per-row `Quantity_kg` (the `survey_kg` rung), as #1452 approved for EHCVM 2021-22?
- Options: (a) wire both tables as `survey_kg` per row, PP and PH tables by round. (b) Wire W3 only, since only W3 was measured. (c) Keep inference (status quo; W3 serves 0.556x the table's kilograms).
- Evidence: the issue body (164,823 kg served against 330,793 kg at the table on 106,152 rows), and the W4 `cvn` against W3 table check in CONTENTS.org.
- Recommendation: (a), after a bounded measurement of W2's table and of the effect on `food_prices`. Coordinate with #1518, which covers the harvest-side W3 table `ag_conv_w3`.
- Cost: one Nigeria re-warm.
No body change needed.

### #1497 — Tanzania 2019-20: updated_ids links 13 split-offs rather than the frame household on the same y4_hhid
Verdict: needs-decision/narrow, tier C. PR #1615 (`20afc61575`, ancestor of development) answered the factual question: of the 13, six follow the current-head / previous-member-1 rule, one is the observed former head now a non-head (first-cover-row fallback), and six have no former head observed (fallback). All 238 pair occurrences and all 907 committed SDD links reconcile. Recorded at `Tanzania/_/CONTENTS.org:3029-3107`. `panel_ids.py` and `updated_ids.json` are unchanged since. What remains is a policy decision, and the title still poses the answered question. Idiosyncrasy: SDD `hh_b06` is the previous NPS4 person number (99 means not previously present), and 30 people come from a previous household different from their cover's `y4_hhid`. So the cover id plus person 1 is not a valid continuation key (`CONTENTS.org:3046-3053`).

New title: Tanzania 2019-20 split-offs: decide the first-cover-row fallback when the former head is absent or no longer head

New body:
> **Resolved so far:** PR #1615 (`20afc61575`) reconciled the 13 split-off continuations against the country's own NPS4/SDD cover, rosters and individual panel keys. Six satisfy the current-head / previous-member-1 rule. One contains the observed former head as a non-head and follows the inherited first-cover-row fallback. Six have no observed former head and use that fallback. All 238 frame/split pair occurrences and all 907 committed SDD links are reproduced. No incorrect observed-head continuation or different-family selection was found. Production IDs and the continuation rule are unchanged (`Tanzania/_/CONTENTS.org`, 2026-11-08 #1497 entry).
>
> **Remaining (decision):** how should `_map_with_head_tracking` treat a family whose former head is absent, or is present but no longer a head? Today it takes the first cover row. That covers 20 fallback pair occurrences across 17 families, 17 of them absent-head occurrences across 14 families. Options: keep the first-cover-row fallback and document it as the rule; give the canonical id to the ORIGINAL (`hh_a10`) frame household; or leave those families unlinked. Any change goes in `panel_ids.py` or the committed `updated_ids.json` as a reviewed diff (`tanzania.py` is pinned by the #905 digest). It affects longitudinal use of 2014-15 → 2019-20, not `v` (#1441 ruling, PR #1498).
>
> Original observation (238 split-offs with one frame household on their `y4_hhid`: 218 link the frame household only, 13 the split-off only, 7 neither, 0 both) is retained in history. -- Sue

### #1000 — Reconcile Uganda FCT 4003's generic zero note with its specific yellow-variety carotenoid derivation
Verdict: needs-acquisition/narrow, tier D. The maintainer ruled status quo on 2026-11-04 (issue comment 2), and the last body update (2026-11-10) adds only a failed locator. No merged work since. The body's acceptance criteria still read as an open adjudication; what is actually left is getting the paper. `Uganda/_/CONTENTS.org:6096-6110` records that a public reproduction shows the raw samples were peeled (pp. 401 and 403), with no authenticated PDF or table. Idiosyncrasy: row 4003 (`nutrition.org:497`) is labelled W/SKIN, while row 4008 (without skin) carries identical values and notes. Also, `nutrition.org` is a hashed `*.org` build input, so any wording fix costs one Uganda re-warm.

New title: Uganda FCT row 4003 (yellow sweet potato): acquire Kidmose et al. 2007 and the electronic FCT version (status quo ruled)

New body:
> **Resolved so far:** ruled 2026-11-04, status quo: the served 225 mcg RAE/100 g and the current source notes stay (issue comment of 2026-11-04; `Uganda/_/CONTENTS.org`, 2026-11-09 holds entry). Internal consistency is checked: 2697/12 + 10/24 = 225.17 under HarvestPlus Monograph 9's divisors. The #967 fixture approval stands.
>
> **Remaining (acquisition):** obtain the authenticated full text of Kidmose et al. (2007), *IFSET* 8(3):399–406, DOI 10.1016/j.ifset.2007.03.025. Also obtain the HarvestPlus Uganda electronic FCT release/version that compiled row 4003. Then adjudicate:
> - the three cultivar cells and their weighting and fresh-weight basis;
> - the provenance of water 67.4 g and alpha-carotene 10;
> - the with-skin assignment of row 4003 against peeled samples, and its identity with row 4008;
> - whether the generic "vitamin A and carotenoids assumed = 0" wording in `fct_source_descr` should be rewritten to match the specific override.
>
> A public reproduction (`CONTENTS.org`) confirms peeled raw samples but has scrambled table columns, so no numbers are taken from it. If a numeric or wording change follows, update `nutrition.org` and the generated `fct_uganda.csv` together (one Uganda re-warm) and report food/household/nutrient deltas. The container-mass and volume-as-mass assumptions are separate and are not validated by this issue. -- Sue

### #833 — Food/crop identity: finish source-backed processing-state decisions and Niger code166
Verdict: needs-decision/narrow, tier C. PR #1534 (`3aefbbedf7`) and PR #993 (`f9d3cbbc2f`) landed; both are ancestors. Two comments settle parts the body still lists as remaining: comment 1 rules that Niger keeps code166 and the seven split pairs, and comment 2 says Tanzania's part can close. The title and body are therefore stale. The memo `Uganda/_/PROCESSING_STATE_REVIEW.md` (2026-11-09) binds the 12 residual food rows without choosing a fold. Idiosyncrasy: food 145 is a bare `Peas` while 162 is `Peas (dry)`, and food 141 is Beans (dry) while crop 141 is Millet. Numeric codes are not join keys.

New title: Food/crop identity: Uganda's five unruled processing-state families, Millet code 115, and Ethiopia's per-item unit comparison

New body:
> **Resolved so far:**
> - PR #993 (`f9d3cbbc2f`): Niger cabbage/sugar spellings and Aubergine.
> - PR #966: Malawi sale-side shelled/unshelled wiring and the sale-basis ladder.
> - PR #1534 (`3aefbbedf7`): Uganda `Aggregate` keeps states apart for Maize, Millet, Cassava, Sweet Potatoes, Ground Nuts and Sim Sim.
> - Ruled 2026-11-03: Niger keeps `'166'` (468 rows in 2021-22, absent from both questionnaires and the value labels) and the seven unfolded split pairs. Niger's EHCVM-2 CAPI product list stays an acquisition lead.
> - 2026-11-04: Tanzania's labels already separate state, and its shared labels name the same state on both sides, so its part is closed.
>
> **Remaining:**
> 1. **Uganda, decision.** Beans (140/141), Peas (145/162), Rice (110/1101/1102/1103), Ginger (1352/1353) and Soybean (1761/1762) each hold two states under one Aggregate. Food 145 names no state, and Rice mixes variety words with a flour entry. Exact cells and options are in `Uganda/_/PROCESSING_STATE_REVIEW.md`. The six-family rule is not authority to extend it.
> 2. **Uganda, evidence.** What code 115 (`Millet`) holds is not settled: no wave answers both 115 and 1151, and 2018-19 holds 3,172 empty 115 rows (`Uganda/_/CONTENTS.org`, Millet). Cassava 2019-20 is #1525.
> 3. **Ethiopia.** The per-item physical-unit comparison needs a sold unit first (#824).
>
> Screen every proposed fold independently. Do not combine orthographic normalisation with a raw/processed-state collapse, and quantify any change before editing source keys. -- Sue

### #449 — Pull WB geovariables (weather/distance/soil) for older Tanzania NPS waves (W1–W3)
Verdict: partial/narrow, tier C then B. The acquisition the title asks for is done. PR #1514 (`1f59c978e`, ancestor) holds 8 members in `Tanzania/2008-15/Data/NPS-R{1,2,3}/`. Since the last update, PR #1690 (`1e549e96a`) added `Tanzania/_/449_OFFICIAL_BRIDGE.md`: UPD BID p.12 names `hhid`/`y2_hhid`/`y3_hhid` as the inputs condensed into `r_hhid`, and the doc gives a five-step conditional plan. `Tanzania/_/data_scheme.yml` declares no geovariables table, and `CONTENTS.org:1120` is still `TODO Wire`. Idiosyncrasy: `2008-15/` is the UPD, one folder holding four rounds, and it carries no geovariables. Native per-round keys must therefore be bridged to `(round, r_hhid)` before the committed id walk. W2 also ships duplicate variants, `HH.Geovariables_Y2` vs `TZY2.HH.Geovariables` and `Plot.Geovariables_Y2` vs `TZY2.PlotGeovaribales`.

New title: Tanzania W1–W3 geovariables: choose the W2 variant and grain, then wire a native extractor through the r_hhid bridge

New body:
> **Resolved so far:**
> - Ruled 2026-10-23: acquire.
> - PR #1514 (`1f59c978e`) holds all three rounds' geovariables zips in `Tanzania/2008-15/Data/NPS-R{1,2,3}/`, 8 files. Household files link to `r_hhid` at 2,990/2,990, 3,917/3,917 and 4,986/4,988.
> - PR #1690 (`1e549e96a`) documents the producer's key bridge in `Tanzania/_/449_OFFICIAL_BRIDGE.md`. UPD BID p.12 says R1 `hhid`, R2 `y2_hhid` and R3 `y3_hhid` are condensed into UPD `r_hhid`, distinguishable by `round`. It also records the official v2.0 cleaners' native keys and the source units (`dist01-03` km, `soil01` m, `soil02` %).
> - The module does not exist for W4, UPD, SDD 2019-20 or R5 2020-21.
>
> **Remaining:**
> 1. **Decision:** which W2 member is authoritative, `HH.Geovariables_Y2.dta` or `TZY2.HH.Geovariables.dta`, and likewise for the plot pair. Also decide whether to ask NBS/LSMS for the UPD `r_hhid` construction manifest first; the request text is in the bridge doc.
> 2. **Decision:** grain and covariates. A household table only or household plus plot, and which fields. Coordinates are excluded.
> 3. **Wiring:** a minimal native extractor that keeps the full source key and units. Then a bridge stage, original key → `(round, r_hhid)`, that keeps unmatched or ambiguous keys for review, followed by the existing committed id walk exactly once. `v` stays owned by `sample()`.
>
> Do not import #450's harmonised dataset or lineage ids into this work. It is orthogonal to #113's community-price linkage. -- Sue

### #450 — Expose the LSMS-ISA harmonised dataset (geovariables/weather) as a feature via the household-id bridge
Verdict: needs-decision/none, tier C. No merged work. The maintainer ruled it in scope on 2026-10-23, to be "designed and built after the defect queue drains, with a design note here first". No design note has been posted, and `Tanzania/_/449_OFFICIAL_BRIDGE.md:130` deliberately keeps this issue's dataset and version decision out of #449. Idiosyncrasy: the harmonised `cluster_id`/`ea` share no values with `sample().v`. Only the household-id bridge works (TZ 2019: `hh_id_original` == `sdd_hhid`, 569/569), and `lat`/`lon` are jittered and must not be ingested.
No write needed now. When the queue drains, the five open choices in the body form the design-note agenda: version (v2.0 vs pre-v2.0 snapshot vs union), the bridge key per country and wave, grain, the covariate subset, and the no-coordinates rule.

### #1441 — Region/District: remaining source gaps and attributable code-map authority
Verdict: partial/narrow (one-sentence refresh), tier B. The body was narrowed 2026-11-10 05:25Z and says "The documentation branch is pending integration". That branch has since landed. Commits `03976d43c`, `a19325d2f`, `155918d2d` and `768b9f1dd` (all ancestors) reached development through #1687 (`16c3f4d9c`, 2026-11-10 17:51Z). They hold the R1 XLS and R2 PDF dictionaries under DVC (e.g. `Tanzania/2008-15/Documentation/NPS-R1/NPS Household Qx - English (Year 1).xls.dvc`) and add `Tanzania/_/1441_PUBLIC_AUTHORITY_ROUTE.md`. Nothing else has changed, and the five checklist items stand. Idiosyncrasy: Tanzania's `2008-15/` is the UPD. Its `ha_01_1`/`ha_02_1` are not shown to keep original cover coding, so the original dictionaries cannot replace the registered round-3 construction without a preservation bridge. This is the same producer-manifest gap as #449.

New title: (unchanged) Region/District: remaining source gaps and attributable code-map authority

New body: keep the current body verbatim, except replace the sentence "The documentation branch is pending integration; the production mapping remains unchanged." with:
> The documentation landed on development through PR #1687 (`16c3f4d9c`): `Tanzania/_/1441_PUBLIC_AUTHORITY_ROUTE.md` and the DVC-held R1/R2 originals under `Tanzania/2008-15/Documentation/` (commits `03976d43c`, `a19325d2f`, `155918d2d`, `768b9f1dd`). The production mapping is unchanged. The original-to-UPD bridge request overlaps #449's producer-manifest request (`Tanzania/_/449_OFFICIAL_BRIDGE.md`), so send them as one request to NBS/LSMS.

and add to the "Resolved so far" header: "Tanzania original R1/R2 code dictionaries held: PR #1687 (`16c3f4d9c`)." -- Sue

QUEUE: #1441 | EthiopiaRHS 1989, Tajikistan 2007, Pakistan 1991 | read-only diagnosis of why those clusters/households lack a cluster_features (t,v) row (a missing source route vs a key mismatch), then a per-country wiring fix | countries/{EthiopiaRHS,Tajikistan,Pakistan}/<wave>/_/data_info.yml or mapping.py | one country each (wave mapping.py / data_info => that country's re-warm) | cold private-root build of sample()+cluster_features() for that wave, comparing NA m/Region counts against the e42fb8812 snapshot (338/54/11)

### #1148 — Household recall exposure: adjudicate passage-status evidence before normalization (Burkina 2014; GhanaLSS follow-up)
Verdict: needs-decision/none, tier C. Narrowed 2026-11-10 18:28Z, after PR #1671 (`1c1ef12da4`, ancestor) landed the #851 period/exposure API. The only post-base merge touching Burkina 2014 is #1690's strata-label decode (`Burkina_Faso/2014/_/mapping.py` `_EMC_2014_STRATA_LABELS`, #1573), which is unrelated to passage status. `recall.py` and GhanaLSS are unchanged since `16c3f4d9c`. The body is current. Idiosyncrasy: Burkina EMC 2014 has four passages. Quantity is asked in passage 1 only, while expenditure is asked in all four. Result fields live in module extracts, not a cover dataset, and p4 has no numeric code dictionary.
No write needed. The household-mask specification (unknown/conflict/out-of-panel refusal) is the decision to put to the maintainer. The collapsed historical section still cites `origin/fix/851-recall-period` `cf117d101`, which ties to archived-branch item 4c.


## 8.B06-sampling-dates

## Batch B06 — sampling, cluster identity, dates, coverage (worker fork, Opus 5.5)

### #1438 — Coverage matrix: latest.csv (2026-09-18) is stale; regrade
*(Two sections for #1438: this one applies if #798 stays open; if #798 is closed into #1438 as drafted under #798, apply "#1438 (revised)" below instead. TRIAGE records `narrow`, i.e. the revised one.)*
Verdict: open/none, tier D (operational rebuild). `.coder/coverage/latest.csv` was last committed in `12c0dec2b` (2026-09-18) and still grades Tanzania `interview_date` 2019-20/2020-21 `dropped` (latest.csv:2205-2206) and Burkina 2014 `food_acquired` at 557,692 rows (latest.csv:172). The only merged work since filing is `.coder/ledger/1438-burkina-food-coverage-continuation.md` (through #1690, `1e549e96a`): a private-root measurement of 559,180 rows for Burkina 2014, graded `sane` by the unchanged grader. That agrees with the body and commits no snapshot. Burkina's idiosyncrasy, per the ledger and CONTENTS.org: four independent 7-day passages, with quantities only in passage 1. The issue still describes exactly the remaining work: a `make matrix` regrade run as a burst job.
No write needed. The body already names 559,180, and no regrade has landed. `unverified-data`: run `make matrix` (burst), then diff `latest.csv` for Tanzania `interview_date` and Burkina `food_acquired` 2014.

### #792 — Document the remaining sample-design gaps after closing the handle leak
Verdict: needs-decision/ask, tier C. Nothing has merged against this since the 2026-10-15 narrowing. The weight facts the residual asks to "record" are already in prose:
- `GhanaSPS/_/CONTENTS.org:449-466`: weights exist for wave 1 only (`hhweight3`, the third revision, an expansion weight). Waves 2-3 have none, and that is positive evidence: all 189 `.dta` files were swept, and `key_hhinfo_panel.xlsx` rows 17-18 leave wave 2 blank. No panel weight exists in any wave.
- `Tajikistan/_/CONTENTS.org:130-141`: the 1999 BID says verbatim "there are no weighting variables".

`.coder/sampling/fingerprint.csv:42-43,88` shows `wt_n = 0` for all three cells. What is missing is a wave-grain, machine-readable record. CLAUDE.md's Site B "known granularity mismatch" says no such mechanism exists: `optional:` is country-grain. The clause "existing strata/Rural gaps … remain unadjudicated" has no list behind it, and strata meaning is now tracked by #1577 (#812 closed 2026-11-05, #1575 closed 2026-11-11).

DRAFT (C item, for the maintainer):
- **Question:** what counts as "sample documentation" for a wave with no weights by design: the CONTENTS.org prose already present, or a wave-grain machine-readable record?
- **Options:**
  - (a) CONTENTS.org suffices. Close #792 citing the two passages, and leave strata meaning to #1577.
  - (b) Add a wave-grain "weights absent by design" record (e.g. a `sampling.yml` field, which is a vocabulary widening under #1134's one-tag-at-a-time rule, or a `population.yml` note). Close #792 into that.
  - (c) Keep #792 open and enumerate the strata/Rural gaps it means.
- **Evidence:** the paths above. Site B currently warns on these cells, and nothing can mark them as judged.
- **Recommendation:** (a) for #792. The machine-readable question is the same one #1577 asks for strata, so fold it into #1577 or #1134 rather than keep an unbounded clause here.

Draft closing comment if (a) is chosen:
> Closing. The handle leak was fixed in `e6c87feba`. Both weightless cases are recorded with positive evidence: `GhanaSPS/_/CONTENTS.org` §3 ("Weights exist for wave 1 ONLY": no sampling weight in 2013-14/2017-18, 189 files swept) and `Tajikistan/_/CONTENTS.org` ("there are no weighting variables", 1999 BID). Panama and Serbia & Montenegro are #977. A machine-readable per-wave record of what `strata` and weights mean is #1577. -- Sue

### #837 — Resolve source-grain cluster disagreements without bypassing the signed audit
Verdict: partial/narrow, tier C. Since the 2026-10-15 narrowing:
- @ligon ruled on the Tanzania District part on 2026-11-03 (comment 1, option iii): "District is left as served".
- PR #1410 (`a39ad3200`) corrected `Tanzania/_/CONTENTS.org:2265-2310`. Of the 118 contested cells, only 5 are spelling; the rest turn on boundary vintage and district-versus-council grain.
- PR #1488 (`4ea95a0aa`, under #1441) named District for rounds 1-2.
- #689 (Uganda parish grain), which the body cites, closed on 2026-11-04.

Two items remain, and both are scientific choices:
- **Ethiopia:** shipped coordinate disagreements. `Ethiopia/_/CONTENTS.org:3002-3032` counts distinct GPS in 0/38/72/0/72 EAs. Ethiopia's idiosyncrasy is that the raw cover-geovariable 1:1 join shows zero Region/District/Rural conflicts in every wave, and the W1 loss is missing-versus-recorded GPS only.
- **Tanzania Rural:** 264 contested EA cells. `Tanzania/_/CONTENTS.org:2314-2317`: `urb_rur`/`sdd_rural`/`y5_rural` are recorded per household, and the modal share is 0.75-0.88.

DRAFT (narrow):
New title: `Cluster disagreements: decide Ethiopia's shipped GPS conflicts and Tanzania's household-grain Rural, without bypassing the signed audit`

New body:
> **Resolved so far:** PR #997 (`1973242b4`) rejected the recovered `c8bf36d81` early `reduce_to_agreed` hooks. They would blank 364 Ethiopia GPS cells and 119 Tanzania attribute cells, and remove household grain before the Site-2 audit. Since #871, core selects the most complete observed row and fabricates no composite. Tanzania District was ruled 2026-11-03 (@ligon, option iii): "District is left as served". The Site-2 audit already reports the 118 contested cells, and per PR #1410 (`a39ad3200`) only 5 are spelling variants; the rest turn on boundary vintage and district-versus-council grain. A vintage crosswalk is revisited only if District is needed for cross-wave analysis. PR #1488 (`4ea95a0aa`, #1441) named District for Tanzania rounds 1-2. Uganda's parish-versus-EA question was #689, closed 2026-11-04.
>
> **Remaining, each needing its own explicit scientific decision (no new completion or blanking exception without one):**
> 1. **Ethiopia, shipped coordinate disagreements.** A direct raw cover/geovariable 1:1 join finds zero Region/District/Rural conflicts in all five waves. Distinct non-missing GPS conflicts affect 0 / 38 / 72 / 0 / 72 EAs; mixed missing/recorded GPS affects 34 / 5 / 0 / 0 / 49 (`Ethiopia/_/CONTENTS.org` "2011-12 cluster conflicts are missing GPS versus recorded GPS"). Current W1 serves all 333 GPS fixes, and W5 432/435. Decide whether a source-specific treatment of the W2/W3/W5 coordinate disagreements is wanted, or whether the most-complete-row selection plus the audit report is the answer.
> 2. **Tanzania, household-grain Rural.** 264 contested cluster cells (`Tanzania/_/CONTENTS.org` "The residue, honestly"). `urb_rur`/`sdd_rural`/`y5_rural` are recorded per household, and a town-boundary EA genuinely holds both, with a modal share of 0.75-0.88. Decide whether that stays as served and reported, or is answered another way.
>
> Raw administrative agreement alone does not certify every coordinate or exclude miscoding within one administrative area. Evidence: `slurm_logs/release_landing_2026-10-14/cluster837.org` and its aggregate/raw-source JSON; `.coder/ledger/837-landing-review.md`. Tier C. -- Sue

### #1247 — Site C: finish Iraq/Albania/Timor-Leste records and the ruled redefined gloss
Verdict: partial/narrow, tier B (filed as C, but the gloss was already ruled).

Item 2 (the ruled widening of the `redefined` gloss) has landed. Commit `67b9406c1` ("sampling: port typed cluster-size records and warning disclosure", merged in PR #1677, `d6044e280`) rewrote the vocabulary comments in `lsms_library/sampling.py:296-306,316,320-321`. They now cover a same-kind unit from an independent draw whose serials overlap (Burkina 2014→2018-19, Niger 2014-15→2018-19), while Ethiopia 2015-16→2018-19 stays `unobservable` (`sampling.py:310-314`). `tests/test_sampling.py:1235-1251` pins the Niger/Burkina/Mali `redefined` and Uganda EA→parish records.

Item 1 is open:
- Iraq (2006-07, 2012) and Timor-Leste (2001, 2007-08) have no `sampling.yml`.
- Albania's file holds only the 2008 `not-stated` design record (PR #1541, `1b0a4cfcb`) and no `panel:` block.

Item 3 (the optional candidate-warning diagnostic) is untouched. Albania's idiosyncrasy: Region agreement is partly affected by label spelling, and 2008 has no design statement (the DDI p.3 reads "No content available").

DRAFT (narrow):
New title: `Site C: add evidence-backed boundary records for Iraq, Albania and Timor-Leste`

New body:
> **Resolved so far:**
> - PR #1248 (`83ee22f3a`): Niger 2014-15→2018-19 boundary record. Burkina's independent-frame boundary is in #1246.
> - Commit `67b9406c1` through PR #1677 (`d6044e280`): the ruled widened `redefined` gloss. In `lsms_library/sampling.py`, `redefined` now also covers the same kind of unit drawn independently with overlapping serials, and legitimately shared EAs such as Ethiopia 2015-16→2018-19 stay `unobservable`. `tests/test_sampling.py` pins the records.
> - PR #1541 (`1b0a4cfcb`): Albania 2008 `design_tag: not-stated` (design record only, no `panel:` block).
>
> **Remaining:**
> 1. Establish and add evidence-backed `panel:` boundary records (`household_continuity` × `cluster_id_continuity`, each with source_type/confidence/locator) for:
>    - Iraq 2006-07→2012 (`Iraq/_/CONTENTS.org` "Sampling universe, exclusions and sub-samples" has the IHSES-II sampling text);
>    - Timor-Leste 2001→2007-08;
>    - Albania's boundaries across 1996/2002/2003/2004/2005/2008/2012.
>
>    Preserve the original caveat: Region agreement in Albania/Timor-Leste is partly affected by label spelling, so do not infer an independent sampling design solely from a low label-match fraction.
> 2. Optional, separately bounded: a candidate warning for overlapping `v` with disagreeing Region/District and absent boundary metadata. It is a diagnostic candidate generator, not source authority.
>
> `sampling.yml` is not a build input; a record moves no cache hash. -- Sue

QUEUE: #1247 | Iraq/Timor-Leste/Albania | add `panel:` boundary records to new `Iraq/_/sampling.yml`, `Timor-Leste/_/sampling.yml` and Albania's existing file, from documented sampling text plus a measured `v`/Region overlap | `lsms_library/countries/{Iraq,Timor-Leste,Albania}/_/sampling.yml`, `tests/test_sampling.py` | none (sibling .yml) | source reading for each boundary; a measured shared-`v` Region/District agreement per boundary (data read, private root); `test_sampling.py`

### #1134 — Sampling design tags: add structural tags with their first evidenced records (#793 residual)
Verdict: open/none, tier C (approved curation). The issue was narrowed on 2026-11-07. Since then no merged work has touched `DESIGN_TAGS`: `lsms_library/sampling.py:146-153` still holds the two Uganda tags plus `not-stated` (PR #1541, `1b0a4cfcb`). #1690 and #1693 wired strata values (Burkina 2014, four EHCVM 2018 countries), not design tags. One small open word stays with the maintainer, per comment 2: Albania 2008 ships `confidence: low`. Per the 2026-10-19 ruling, the likely first structural tag is GhanaLSS GLSS1-3 self-weighting (#713).
No write needed. The body already describes the remaining work exactly.

### #1136 — Sampling: implement approved design-vocabulary expansion, curate records and drain reports
Verdict: partial/narrow, tier C (umbrella; per the 2026-10-23 ruling it is built after the defect queue). The 2026-11-07 body predates three merges that complete parts of its plan:
- PR #1677 (`d6044e280`): typed boundary-size records, 13 records across seven countries (#1135).
- PR #1690 (`1e549e96a`): Burkina 2014's 26 strata codes decoded to Region × milieu (#1573, closed 2026-11-11).
- PR #1693 (`e5fdd2fa8`): documented 2018 strata wired for Benin, Togo, Guinea-Bissau and Côte d'Ivoire (#1575, closed 2026-11-11).

The body also still lists Mali among the pending v-continuity records, but `Mali/_/sampling.yml:27-77` already carries 2017-18/2018-19/2021-22 `panel:` blocks (last edited `62ab1c14b`, 2026-11-05). Malawi still has no `sampling.yml`. `bench/feature_audit/scan.py` still never calls `sampling_reports()`.

DRAFT (narrow), keeping the title:
> **Post-merge reconciliation, 2026-11-12. Tier C (after the defect queue, ruling 2026-10-23).**
>
> ## Resolved so far
> - PR #1137 (`238a4c15d`): loader, `Country.sampling`, per-boundary `panel:` declarations.
> - PR #1246 (`3c41a3717`): Burkina boundary record. PR #1248 (`83ee22f3a`): Niger.
> - PR #1541 (`1b0a4cfcb`): `not-stated` design tag, first record Albania 2008 (option A of #1134).
> - PR #1677 (`d6044e280`): typed boundary-size records (13 records across seven countries) and the widened `redefined` gloss (#1135, #1247).
> - PR #1690 (`1e549e96a`): Burkina Faso 2014's 26 own stratum codes decoded to Region × milieu labels (#1573, closed).
> - PR #1693 (`e5fdd2fa8`): documented 2018 region × milieu strata wired for Benin, Togo, Guinea-Bissau and Côte d'Ivoire (#1575, closed). Togo's Lomé Commune / Grand Lomé discrepancy stays qualified.
> - Mali's `sampling.yml` already carries 2017-18, 2018-19 and 2021-22 boundary records.
>
> ## Remaining work
> 1. Structural design tags, one evidenced tag at a time: #1134 tracks this. Do not request the staged decision again.
> 2. Probe the NADA DDI sampling-procedure evidence from an authorized host, and curate per-country evidence.
> 3. Wire genuinely available strata where they are still missing: Mali 2017-18 (#1574), and the waves the old fingerprint shows with `n_strata = 0` or no strata column (Albania, GhanaLSS, Niger, Tajikistan, Senegal, Kazakhstan, Kosovo, China, Azerbaijan, Pakistan). Refresh that inventory first; the old 24/104 count is not a census. What `strata` means per wave is #1577.
> 4. v continuity: Malawi has no `sampling.yml`, and Iraq/Albania/Timor-Leste are #1247. Preserve `stable`/`derived-from-data` where movers are unobservable because household ids embed `v`.
> 5. Shared scopes:
>    - the `panel_documented` distinction;
>    - Q6's cold Ethiopia geo-crosswalk evidence;
>    - Q7 baseline-EA versus current-location semantics;
>    - make `bench/feature_audit/scan.py` drain `sampling_reports()` (it does not today);
>    - retain attrs on the roster-derived `household_characteristics` path.
> 6. Nigeria's 13 reassigned EAs and Tanzania's 82 round-3 renumberings remain separate served-`v` crosswalk proposals with country cache cost. They are not permission to stop warnings without a crosswalk.
>
> A boundary record alone does not provide a full design triple or missing strata. -- Sue

QUEUE: #1136 (item 5, harness drain) | cross-country | make `bench/feature_audit/scan.py` drain `sampling_reports()` into structured findings, as it already does for `null_read_reports` | `bench/feature_audit/scan.py`, its tests | none (bench code is outside every fingerprint) | a unit test with a synthetic `ClusterIdentityWarning` report, and a scan run on one country (for example Uganda) in a private root

### #1135 — Cluster identity: verify complete-mover and physical-cluster measurements
Verdict: open/none, tier C (measurement and authority research). The issue was narrowed on 2026-11-09 after PR #1677 (`d6044e280`) delivered the ruled engineering: an optional typed boundary-size record with provenance, first-line sizes, and "not measured" where unknown. None of the four post-base merges touches `sampling.yml` or `sampling.py`.

What remains is per-boundary evidence:
- a movement classifier tied to each instrument and its linked population;
- physical-cluster identity evidence.

Nigeria's idiosyncrasy is why this cannot be shortcut: the net-of-movers count depends on which marker is trusted (`moved-` sentinel 10, `tracked_obs` 25, `hhsmlc` 50-56 of 1,493), so by the 2026-10-23 ruling it prints "not measured".
No write needed.

### #1045 — Calendar parser: finish Phases 2–4, stamp/replay coverage and the time-of-day ruling
Verdict: partial/narrow, tier A (as filed), plus two embedded maintainer questions.

Since the 2026-10-25 narrowing:
- PR #1413 (`62f6aaa99`, #1056) moved Ethiopia W4/W5 `interview_date` onto the build-time parser. `Ethiopia/2018-19/_/data_info.yml:215-218` declares an ISO format, `calendar: gregorian`, the `'##N/A##'` sentinel and a catalog-cited window. This is Phase-2 progress the body does not record.
- Commit `36b0a646f` (#1443) updated the calendar inventory ledger.
- PR #1418 (`4f3488041`) states that Burkina 2021-22 `interview_date` (now reading both covers) still uses the old `pd.to_datetime(errors='coerce')` route (`Burkina_Faso/2021-22/_/data_info.yml:370-392`, `burkina_faso.py:311`).

Declarative datetime specs now exist in 8 waves: Ethiopia ×2, Mali ×4, Tanzania ×2. There are 80 `to_datetime` calls in 46 country `.py` files, and `datetime_parse.py:129 READ_CAST_WARNS_ON_STRINGS = False` (the read cast cannot warn yet). Nigeria's idiosyncrasy: W4 `InterviewStart` hours peak at 03:00-06:00 in both rounds, implausible as local time (`Nigeria/_/CONTENTS.org:3327` keeps the time pending the ruling).

DRAFT (narrow), keeping the title:
> **Post-merge reconciliation, 2026-11-12. Tier A.**
>
> ## Resolved so far
> - PR #1025 (`f8e14a7c2`): mixed-datetime warm-read loss and the Mali comment repair.
> - PR #1105 (`e87a551e8`): run 1 of the shared build-time parser.
> - PR #1413 (`62f6aaa99`, #1056): Ethiopia W4/W5 `interview_date` through the parser (ISO format, `calendar: gregorian`, `'##N/A##'` sentinel, catalog-cited fieldwork window; out-of-window dates counted as `outside_window`, not nulled).
> - The calendar inventory ledger was updated in `36b0a646f` (#1443).
> - Mali's and Tanzania's `interview_date` build-time migrations are complete.
>
> ## Remaining work
> Reconcile the Phase 0 inventory (`.coder/ledger/1045-calendar-inventory.org`) against current source, then migrate the remaining producers through the shared parser:
> - interview dates still on `pd.to_datetime`, including Burkina Faso 2021-22, which reads both covers since PR #1418 (`4f3488041`) and still coerces;
> - other served month/year/onset quantities;
> - birth-date/age inputs.
>
> About 80 `to_datetime` calls remain in 46 country `.py` files. Only switch `READ_CAST_WARNS_ON_STRINGS` (`datetime_parse.py`) to warn-on-string once the string producers have migrated. Durations and season labels remain out of scope. Nigeria W1-W3 migration may change blank-component handling relative to its coerce-to-NaT path, so it needs its own bounded source comparison.
>
> Preserve the decided contract:
> - table-scoped build-transform coupling;
> - a declared calendar with no Gregorian default;
> - sentinel/range/ambiguity accounting;
> - registered derivations for partial-date imputation;
> - nullable 1-12 month-of-year;
> - footer stamping and replay;
> - the separate `LSMS_DATE_STRICT` / `DatetimeParseError` / report accessor.
>
> ## Open questions for the maintainer
> 1. **Nigeria W4 time of day.** `InterviewStart` hours peak at 03:00-06:00 in both rounds, so a non-local device or server clock is likely, and about 12 rows with an hour ≥ 19 could be dated a day off. Keep time of day until a source/timezone ruling. The ruling should cover Survey Solutions exports generally.
> 2. **Partial-date derivation inputs** (comment of 2026-10-31). `datetime_parse.partial_date_inputs` returns process-level counts with no source components or row ids. Should these derivations follow the raw/joinable `derivation_inputs` contract instead? If so, a source-backed resolver scoped to country/table/wave/source is needed.
>
> Acceptance: the source inventory is mapped to parser/spec or a documented exception; served calendar types are consistent; dates and intended grain are preserved or explained; partial dates carry derivation keys; warm reports replay. State hash costs per change: a `data_info.yml` spec moves that country's hashes, and a parser edit moves only `CALENDAR_TABLES`. -- Sue

C items embedded (for the human):
- **Time of day (Nigeria W4).** Question: keep the served `InterviewStart` time, normalise to date, or shift to an assumed timezone? Options: (a) keep as served with the caveat; (b) `.dt.normalize()` to date only; (c) shift by an assumed offset. Evidence: comment 1's hour histogram, and no other W4 date field to cross-check. Recommendation: (a) until a source states the clock basis, since (c) would invent a timezone.
- **Partial-date inputs.** Question: should `partial_date_inputs` honour the raw/joinable `derivation_inputs` contract? Options: (a) yes, with a source-backed scoped resolver; (b) a documented exception (counts only). Evidence: comment 5's source anchors. Recommendation: (a), which matches every other registered derivation.

QUEUE: #1045 | Burkina Faso 2021-22 (first), then the inventory | move `interview_date` into a declarative `datetime:` spec (`format`/`calendar`/`sentinels`/`window`), as Ethiopia W4/W5 did in #1413 | `Burkina_Faso/2021-22/_/data_info.yml`, `Burkina_Faso/_/burkina_faso.py` (the `interview_date` hook), a test beside `tests/test_gh1045_parse_datetime.py` | one country (`data_info.yml` and the country module move every Burkina hash) | cold build of Burkina `interview_date`, with value equality against the base on common rows and the parse-report counts checked

### #798 — Interview-date coverage: refresh final-source raw-cache and served-API matrix
Verdict: partial/close (fold into #1438), tier A. The code defect is fixed and tested: PR #1025 (`f8e14a7c2`) parses declared datetime columns per value and pins it in `tests/test_gh798_datetime_mixed.py`; PR #1105 (`e87a551e8`) is run 1 of #1045's parser. The ONLY remaining work in the body is to refresh `.coder/coverage/latest.csv` on the final source, which is the same regrade #1438 tracks. The snapshot is still the 2026-09-18 one (`12c0dec2b`): `latest.csv:1382-1383` grades Mali `interview_date` 2018-19/2021-22 `dropped`, and `:2205-2206` does the same for Tanzania 2019-20/2020-21. Two open issues therefore describe one job. Mali's idiosyncrasy: its EHCVM waves melt per-visit timestamps onto a fabricated `visit` level (`fabricate_missing_levels: interview_date`, #506), one vague per grappe.

DRAFT closing comment:
> Closing into #1438. The code defect here is fixed on development: PR #1025 (`f8e14a7c2`) parses declared datetime columns per value, which repairs the mixed-format warm-read loss (`tests/test_gh798_datetime_mixed.py`), and PR #1105 (`e87a551e8`) landed run 1 of the build-time parser (#1045). What was left is refreshing `.coder/coverage/latest.csv`. That is the same `make matrix` regrade #1438 tracks; its snapshot still grades Mali `interview_date` 2018-19/2021-22 and Tanzania 2019-20/2020-21 `dropped`. #1438 now carries this issue's acceptance: record known failures, distinguish raw-cache from served Country/Feature coverage, and do not certify served completeness from footer metadata. Remaining parser migrations are #1045. -- Sue

### #1438 (revised) — narrow only to absorb #798
Verdict revised from none to narrow, conditional on #798 being closed into it.
DRAFT (narrow), keeping the title:
> **Resolved so far:** nothing committed to the snapshot. A private-root measurement is recorded in `.coder/ledger/1438-burkina-food-coverage-continuation.md` (through #1690, `1e549e96a`): Burkina 2014 `food_acquired` returns 559,180 rows and grades `sane`. #798's code defect is fixed (PR #1025, `f8e14a7c2`), and its coverage refresh is folded in here.
>
> The committed coverage snapshot `.coder/coverage/latest.csv` (2026-09-18, `12c0dec2b`) predates many merges and misgrades cells. Examples:
> - Tanzania `interview_date` 2019-20 and 2020-21 are graded `dropped`, but the API serves 1,184 / 4,709 rows.
> - Mali `interview_date` 2018-19 and 2021-22 are graded `dropped` (#798).
> - Tanzania `crop_production` and `plot_inputs` counts predate #637.
> - Burkina 2014 `food_acquired` still records 557,692 rows (now 559,180).
>
> **Work:** a `make matrix` regrade, run as a burst job, committed as the new snapshot. Record known failures, and distinguish raw-cache coverage from served Country/Feature coverage. Do not certify served completeness from footer metadata alone. Preserve genuine missing dates and Mali's one-vague-per-grappe EHCVM design. Related to #587. -- Sue


## 8.B07a-compilers-cache

# Batch B07a — compilers / cache (#1287, #1069, #920, #905)

Worker: fork of the reconciliation agent (Claude Opus 5.5), read-only, development `e5fdd2fa8`.

### #1287 — Country compilers: finish non-ISA sites and recipe/runtime reconciliation after the ISA inventory
Verdict: open/narrow, tier B. Narrowed 2026-11-10 18:57Z (after #1687 `16c3f4d9c`); the ISA compiler inventory (62 cells) is complete per that body. None of the four later merges touches a non-ISA country-level compiler: #1695 (`d5a2b1f9e`) adds Senegal `2021-22/_/plot_inputs.py` only (no country `_/plot_inputs.py`, no Makefile rule, so `materialize: make` builds it from waves); #1690 (`1e549e96a`) re-routes Nigeria's `plot_features` wave targets in `Nigeria/_/Makefile:157-166` (ISA, already-accepted cell; the old receipt predates this edit, which is #1288/#587 territory). The defect is that the narrowed body says "the original six non-ISA countries/sites" without naming them, so a reader cannot tell what is left. A source read of every non-ISA country `_/*.py` that reads `{wave}/_/<table>.parquet` finds exactly six with a skip/rescue branch: Benin, Guinea-Bissau, Togo, Senegal (`plot_features`), GhanaLSS (`community_prices`), Tajikistan (`interview_date`, `plot_features`, `life_satisfaction`). Of those, only TWO can still silently drop a declared wave:
- `GhanaLSS/_/community_prices.py:23-36` — six declared waves (each has a `{wave}/_/community_prices.py`), `except Exception: continue` at :30; `assert pieces` at :36 fires only when ALL are missing.
- `Senegal/_/plot_features.py:21-34` — two declared waves (both have wave scripts), `except Exception: continue` at :28.
The other four cannot lose a declared wave today: Benin/Togo/Guinea-Bissau `plot_features` list one wave each, which is the only wave with a `plot_features.py`, and `assert pieces` (:32/:33/:33) refuses an empty result; Tajikistan's three scripts run the missing wave's script with `check=True` (`Tajikistan/_/plot_features.py:28-30` etc.) and re-read, so a failure is loud. Panama/GhanaSPS/Guatemala/Serbia/Cambodia country scripts call `get_dataframe` with no `try`, so a missing wave raises. CONTENTS.org idiosyncrasy: `GhanaLSS/_/CONTENTS.org:1943-1948` records `_/household_roster.py` as a second `change_id` concatenator reachable only via `make all`, never the API (its loop has no `try`, so it is loud — not a skip site). `Senegal/_/CONTENTS.org:432-437` documents the country `plot_features.py` as a thin concatenator of both EHCVM waves. Which six the original inventory meant is not recorded anywhere I found (no `.coder/ledger/1287-*` names non-ISA countries); the six above are my source-reading match — `unverified-data` that the GhanaLSS/Senegal skip is reached on the default route (verify: private empty `LSMS_DATA_DIR`, make one declared wave's script fail, `Country('Senegal').plot_features()` and `Country('GhanaLSS').community_prices()`, compare `t` coverage).

Draft (narrow):
New title: `Country compilers: two non-ISA concatenators still skip a declared wave (GhanaLSS community_prices, Senegal plot_features)`
New body:
> **Resolved so far:** PR #1307 (`3c661d56a`), #1310 (`f0819a6b8`), #1311 (`7fc3ad83c`), #1312 (`71475585f`), #1313 (`6924541e3`), #1315 (`bff799496`), #1316 (`cfaacd44a`), #1317 (`89b477503`), #1319 (`d96a8152b`), #1409 (`3a33f2869`): the eight ISA country-compiler integrations plus Ethiopia's food compiler, and the selected 62-cell ISA receipt inventory (zero remaining). All are ancestors of development `e5fdd2fa8`.
>
> **Remaining (source reading at `e5fdd2fa8`).** Of the non-ISA country-level scripts that read `{wave}/_/<table>.parquet`, six have a skip or rescue branch; two can still drop a declared wave with no error:
> - `GhanaLSS/_/community_prices.py:23-36`: six declared waves, `except Exception: continue` (:30); `assert pieces` (:36) fires only when every wave is missing.
> - `Senegal/_/plot_features.py:21-34`: both EHCVM waves declared, `except Exception: continue` (:28).
>
> Fix pattern (#1241 / #1289): refuse when a wave that DECLARES the table (has a `{wave}/_/<table>.py` or a `data_info.yml` entry) produced no parquet; keep the legitimate skip for a wave that does not wire it. Chain any `;` recipe with `&&` only together with that refusal.
>
> **Checked and not at risk today:** Benin, Togo, Guinea-Bissau `plot_features` (one listed wave, the only one with a script; `assert pieces` refuses an empty result); Tajikistan `interview_date`/`plot_features`/`life_satisfaction` (run the missing wave's script with `check=True`, then re-read); Panama, GhanaSPS, Guatemala, Serbia, Cambodia (`get_dataframe` with no `try`, so a missing wave raises). A new wave added to any single-wave list re-opens the risk.
>
> **Per-cell limits retained:** each accepted ISA cell keeps its route, historical-source, fixed-lookup, strict-failure and scientific qualifications as recorded in the history below. Freshness and wider caller work stay in #1288.
>
> **Cost:** each fix edits a country `_/*.py`, which is in every table's input hash for that country: one GhanaLSS and one Senegal re-warm.
>
> **Verification:** private empty data root; break one declared wave's script; confirm the country call raises naming the wave, and that an unbroken build is frame-equal to base.
>
> (Keep the existing `<details>` history block unchanged below.)
>
> -- Sue
QUEUE: #1287 | GhanaLSS, Senegal | replace `except Exception: continue` with a declared-wave refusal in two country concatenators (#1241 pattern) | `GhanaLSS/_/community_prices.py`, `Senegal/_/plot_features.py`, tests | one country each (GhanaLSS, Senegal re-warm) | private-root broken-wave repro per country + base frame equality

### #1069 — Cache mechanism still inside the build fingerprint after #908/#1040 (eviction, replay, pre-warm, footer readers)
Verdict: needs-decision/none, tier C. Narrowed 2026-11-10 16:16Z at pin `9f89151dd` (an ancestor of `e5fdd2fa8`). Since that pin, five first-parent merges landed (`16c3f4d9c`, `1e549e96a`, `7f1a9c954`, `d5a2b1f9e`, `e5fdd2fa8`); none touches `lsms_library/_build_registry.py` (`git log 9f89151dd..e5fdd2fa8 -- lsms_library/_build_registry.py` is empty), and the only framework `.py` change is 6 lines in `transformations.py` (#1694). None of the six candidates (`_evict_hashless_wave_caches`, `_evict_stale_country_cache`, `_replay_grain_audit`, `_warm_dvc_cache_for_feature`, `read_parquet_cache_waves`, `_rebuild_failure_error`) or `_dvc_cache_path` appears in `_build_registry.py`; `_replay_wave_stamp` is the one excluded (merged #1045). Body is current; no write needed.
Draft: none — narrowed 2026-11-10, no merged work since touches the registry or the listed callables.
C item:
- Question: exclude (or extract) any of the six mechanism/reporting callables from the build fingerprint, and if so bundle it with which corpus re-warm?
- Options: (a) leave all six fingerprinted (status quo; any logic edit to one cold-rebuilds the corpus); (b) evaluate each individually with the #908/#1040 no-op + retained-content positive controls and exclude those proven not to shape content, one bundled corpus re-warm; (c) defer until the next unavoidable corpus re-warm (e.g. #1604's ID guard, which already moves every fingerprint per isa-decisions 2026-11-12) and ride it.
- Evidence: body's current-reconciliation section; `_replay_grain_audit` is load-bearing under `LSMS_GRAIN_STRICT` in private Make validation and #1066's replay; coverage-footer parsing gates Other-prerequisite completeness; `_dvc_cache_path` selects actual bytes and is a separate identity hold.
- Recommendation: (c) — evaluate per (b) but schedule the exclusion to land in the same window as an already-required corpus re-warm, so the cost is paid once. Keep `_dvc_cache_path` out of scope.

### #920 — Profile remaining per-call read-path cost after cache-rebuild diagnostics landed
Verdict: open/none, tier C (performance investigation, not data loss; agent-tractable with a GhanaLSS data grant, no design choice pending). Narrowed 2026-11-10 20:26Z, after #1687 (`16c3f4d9c`), whose GH920 test-oracle correction (report `isa-sm-20261103/reports/1687-gh920-source-order-correction-20261110/`) concerns the already-done part (a) only. Part (b) — a scoped profile of the per-call read-path transforms on GhanaLSS `food_acquired` (5.3M rows), cold/warm/fresh-process — has no merged evidence: `git log --since=2026-10-19 -i --grep='profil'` on development finds only an unrelated Uganda compiler commit, and none of the four post-base merges touches GhanaLSS or `bench/`. Body is current.
Draft: none — narrowed 2026-11-10, no merged work since.

### #905 — Vectorized min_count reducers: finish remaining food and nonfood sites
Verdict: open/narrow, tier A per the issue's own 2026-10-25 label (in substance a performance sweep under a <=4 ULP preservation contract, not data loss; low priority). Narrowed 2026-11-10 18:28Z after #1687 (`16c3f4d9c`, Tanzania singleton-only reductions). None of the four later merges touches any listed site (#1694's Malawi edits are outside this scope; Malawi was already removed via `af4338784`). The "Current residual" paragraph names no sites; the actual list sits only inside the collapsed history. Source read at `e5fdd2fa8` shows every historically listed site except the landed ones still uses a Python `lambda s: s.sum(min_count=1)` callback:
- food: `GhanaSPS/2009-10/_/food_acquired.py:109-110`, `GhanaSPS/2013-14/_/food_acquired.py:81-82`, `GhanaSPS/2017-18/_/food_acquired.py:80-81`, `Cambodia/2019-20/_/food_acquired.py:102-103`, `Serbia/2007/_/food_acquired.py:128-129`, `Guatemala/2000/_/food_acquired.py:152-153`, `GhanaLSS/_/ghanalss.py:929` (`reduce_duplicate_food_rows`, def at :897);
- nonfood: `GhanaSPS/_/ghanasps.py:806` (`sum_duplicate_harvest_records`, def at :796), `Iraq/2012/_/assets.py:44-46`, `Albania/2002/_/assets.py:41`;
- Tanzania multirow (held, four-ULP counterexample): `Tanzania/_/tanzania.py:1698-1721` (`plot_inputs_for_wave` `_sum1`), `:2010-2020` (`plot_labor_for_wave` `_sum1`).
Draft (narrow):
New title: `Vectorized min_count reducers: 10 remaining Python-callback sites (GhanaSPS, Cambodia, Serbia, Guatemala, GhanaLSS, Iraq, Albania) and Tanzania's held multirow branch`
New body:
> **Resolved so far:** PR #1687 (`16c3f4d9c`): Tanzania `plot_inputs`/`plot_labor` singleton-only reductions; PR #1314 (`665a58264`): Tanzania `livestock_for_wave`; `af4338784`: Malawi purchase-quantity site (`_agree_purchase_units`); earlier release increments: canonical food dedupe tail, Panama's three scripts; Mali `plot_inputs` already vectorized since `8d6aa0feb`.
>
> **Remaining sites (source at `e5fdd2fa8`; each still a Python `lambda s: s.sum(min_count=1)` callback):**
> - Food: `GhanaSPS/{2009-10,2013-14,2017-18}/_/food_acquired.py` (:109, :81, :80), `Cambodia/2019-20/_/food_acquired.py:102`, `Serbia/2007/_/food_acquired.py:128`, `Guatemala/2000/_/food_acquired.py:152`, `GhanaLSS/_/ghanalss.py:929` (`reduce_duplicate_food_rows`).
> - Nonfood: `GhanaSPS/_/ghanasps.py:806` (`sum_duplicate_harvest_records`), `Iraq/2012/_/assets.py:44`, `Albania/2002/_/assets.py:41`.
> - Held: Tanzania `plot_inputs_for_wave` / `plot_labor_for_wave` multirow scalar sums (`tanzania.py:1698`, `:2010`). Invented operands `[1e16, 1, 1, -1e16]` return 0 vs 2 under a grouped sum, beyond the four-ULP bound; finish only with a strategy that keeps the bound, never by relaxing it or clamping sources.
>
> **Contract:** isolated before/after frame comparison per site, explicit column order, missingness preserved, <=4 ULP. A grouped `.sum(min_count=1)` sums pairwise and is not byte-identical to the in-order callback (see CLAUDE.md, GH #871 note), so each site needs its own comparison.
>
> **Cost:** each edit is a country/wave script, so one re-warm per country: GhanaSPS, Cambodia, Serbia, Guatemala, GhanaLSS (whole country, via `ghanalss.py`), Iraq, Albania, Tanzania.
>
> No corpus performance claim, decoder/API audit or cache rebuild is made here. (Keep the existing `<details>` history block below.)
>
> -- Sue
QUEUE: #905 | GhanaSPS, Cambodia, Serbia, Guatemala, GhanaLSS, Iraq, Albania | replace lambda min_count callbacks with grouped `.sum(min_count=1)` under the 4-ULP before/after contract; Tanzania multirow stays held | the 10 files listed | one country each (8 re-warms) | per-site isolated before/after frame equality within 4 ULP + missingness


## 8.B07b-make-tests-access

# Batch B07b — make backend, tests, data access (reviewer: Claude Opus 5.5 fork)

### #1288 — make backend: remaining caller exposure and undeclared-input freshness
Verdict: partial/narrow, tier B. Body was narrowed 2026-11-09 through #1662 (`97463a699`). Since then #1690 (`1e549e96a`) carried three more source-declaration commits for the "undeclared/computed inputs" residual: `eb682a6da` (Nigeria 2010-11/2012-13/2015-16 nonfood declarations), `c37a4d806` (Nigeria 2018-19: `lsms_library/countries/Nigeria/2018-19/_/nonfood_expenditures.py:22-39`, `_DECLARED_NONFOOD_SOURCES` with a `RuntimeError` guard against drift from `files`), `9f89151dd` (Niger `2014-15/_/plot_inputs.py:30-31`, literal full path replacing the `base +` operand that produced a false bare-leaf reference). Tests: `tests/test_gh1288_nigeria_nonfood_sources.py`, `tests/test_gh1288_niger_plot_input_source_literal.py`. These are declaration fixes, not runtime caller cohorts; the other two residuals (caller exposure beyond eight cells; default/JSON/legacy failure paths) are unchanged. Nigeria idiosyncrasy recorded in the ledger: nonfood recall windows are POOLED with no column recording it (CONTENTS.org WAITING), and `t` is the PP/PH round.

New title: make backend: remaining caller exposure and undeclared-input freshness (unchanged)

New body:
```
> **Resolved so far:** PR #1303 (`3dcdb23b6`) strict nonzero-recipe failure propagation on the direct country-builder route; PR #1327 (`05cc6babc`) requested-wave selection after a direct country target read; PR #1645 (`82ee0bc94`) private native-input preparation and direct-builder freshness; PR #1652 (`dfaeed64b`), #1653 (`f9c01737e`), #1654 (`7f0587ba7`) Niger/Malawi/Uganda source declarations; PR #1656 (`dec2f9967`) explicit-empty coverage manifest; PR #1662 (`97463a699`) Niger/Malawi livestock post-walk index; PR #1690 (`1e549e96a`: `eb682a6da`, `c37a4d806`, `9f89151dd`) Nigeria W1–W4 nonfood source declarations and the Niger 2014-15 plot_inputs phantom bare-leaf reference.

## Current residuals — 2026-11-12

Bounded real caller cohorts have completed for all eight LSMS-ISA countries at their recorded source pins (Tanzania/Nigeria/Niger/Malawi/Ethiopia/Mali livestock, Uganda anthropometry, Burkina Faso plot_features); each accepted cohort passes six unchanged strict whole-frame/typed-attrs comparisons. These are individual country/table checks, not a fresh all-country corpus audit.

**Keep open for:**

- Dependencies computed or undeclared beyond the existing input scanner's certifiable source closure. Nigeria nonfood (W1–W4) and the Niger 2014-15 plot_inputs reference are now declared (source-declaration acceptance only; no runtime cohort was run for them).
- Real country/table caller exposure beyond the eight qualified cells.
- Remaining default/JSON/legacy failure-path qualifications outside the direct opted-in builder contract; do not silently broaden the accepted available-data-return policy.

Do not solve freshness by stamping unverified Make output with the current country hash, blindly forcing prerequisites, or clearing shared caches (#479, #866 constraints). No shared harmonized caches were cleared or re-warmed and no master release occurred; fingerprint changes describe next-access invalidation, not verified corpus builds.

<details><summary>Verified implementation history, bounded cohorts and original reports</summary>

(retain the existing 2026-11-09 details block unchanged)

</details>

-- Sue
```
QUEUE: #1288 | cross-country | continue literal source declarations for the remaining computed-path wave scripts found by `scan_script_data_refs`, one country per PR | `lsms_library/countries/<C>/<wave>/_/*.py`, `tests/test_gh1288_*` | one country per PR (wave script text is in that country's hashes) | per-country declaration test + `make -n` closure check; runtime cohort optional

### #1281 — Declared-wave KeyErrors: remaining corpus exposure and warm-cache audit
Verdict: partial/narrow, tier B. The repair is PR #1300 (`8aa4d86b6`). Separately, PR #1599 (`c964c6c4f`, GH #1590) made the coverage manifest record COMPLETED waves (`lsms_library/country.py:6638-6643`: "Coverage is producer evidence, not the surviving t values"), so the body's statement that `lsms_cache_waves` "records requested coverage" now describes pre-#1599 parquets only. Since the 2026-11-10 03:21 edit, #1690 carried `9e9c1361a`: a healthy Malawi 2010-11 `housing` cold/fresh-warm pair (12,271 rows both; `.coder/ledger/1281-malawi-housing-current-pair.md`; `Malawi/_/CONTENTS.org:5610-5642`). Measurement only; the inventory and the historical warm-cache audit remain. Malawi idiosyncrasy: the 2013-14 `cluster_features` KeyError comes from the PRIMARY `dfs:` sub-df (`CONTENTS.org:2849`), and IHS3 housing is module F, where IHS2 uses module G.

New title: Declared-wave KeyErrors: corpus exposure inventory and audit of pre-#1599 warm caches

New body:
```
> **Resolved so far:** PR #1300 (`8aa4d86b6`) — a `KeyError` escaping a declared wave raises before legacy fallback, concatenation or publication of the shortened country table. PR #1599 (`c964c6c4f`, GH #1590) — the parquet coverage manifest now records waves whose build COMPLETED, not the waves requested, so a new build can no longer list a wave that produced nothing. Bounded checks: Malawi `tests/test_gh1260_malawi_panel_geo.py::test_2013_14_config_keyerror_is_not_cached_as_a_valid_build` passed on held sources (2026-11-10, `reports/1281-malawi-held-regression-v4-20261110`); Malawi 2010-11 `housing` healthy cold/warm pair, 12,271 rows each, strict comparison passed (`9e9c1361a` via PR #1690, `.coder/ledger/1281-malawi-housing-current-pair.md`).

Remaining scope:
- Measure actual source builds reaching the old catch boundary, including primary-`dfs:` and single-file failures; distinguish unavailable sources from configuration errors.
- Audit warm caches written BEFORE #1599: their `lsms_cache_waves` manifest records the requested coverage and can list a wave that contributed nothing, so manifest membership alone cannot identify poisoned caches. Compare actual contributing build outcomes with those caches.
- Full-suite and derived/Feature fallback behaviour remain unverified.

Mechanism correction (review comment 6376116241): Malawi 2013-14 `cluster_features` has a primary `dfs:` block whose error escapes; it is not a no-`dfs:` example. The general defect also applies to single-file declarations.

No shared-cache clear, data acquisition, fixture regeneration or survey-policy change occurred.

(Original report retained verbatim below.)

-- Sue
```
QUEUE: #1281 | cross-country | read-only footer census of warm `var/*.parquet` manifests built before c964c6c4f vs. a cold per-wave build outcome, one country at a time; no code change expected | none (measurement) | none | per-country cold build with `build_wave` outcome capture in a private root

### #778 — Tests that rebuild in the SHARED root under LSMS_NO_CACHE still open the #866 delete-then-rebuild window; assume_cache_fresh has an exists()-then-read race
Verdict: resolved/close, tier A. The body (2026-10-20) already says PR #1162 implements both actionable residual items. Verified on development: PR #1162 (`5519de059`) makes `Country._read_assumed_fresh_cache` read with `open_errors=(FileNotFoundError,)` and fall through to the ordinary path (`lsms_library/country.py:5222-5228`), moves the Guyana/India/Togo forced-build controls to private L2 roots (`tests/test_guyana_index_uniqueness.py`, `tests/test_india_employment_activity_index.py`, `tests/test_gh866_stale_country_parquet_not_laundered.py`), and adds `tests/test_gh1040_wave_read_gates.py`. The GhanaLSS and gh323-site4 tests already used `tmp_path` roots (`tests/test_ghanalss_interview_date.py:69-71`, `tests/test_gh323_site4_dfs_merge.py:259-271`). Earlier items landed through PR #1030 (`dbab68d8c`). What is left, the carried note that `assume_cache_fresh` serves an unstamped country-script parquet, is the documented contract: the hash and the #675 grade are not consulted (`country.py:5176-5178`, CLAUDE.md "Cache Behavior" limitation (c)). It is not an open defect.

Closing comment:
```
Closing: both actionable residuals are on development.

- Shared-root forced rebuilds: PR #1030 (`dbab68d8c`) isolated the gh866 poison and gh809 tests; PR #1162 (`5519de059`) moved the remaining Guyana, India and Togo forced-build controls to private L2 roots with only L1 linked (`tests/test_guyana_index_uniqueness.py`, `tests/test_india_employment_activity_index.py`, `tests/test_gh866_stale_country_parquet_not_laundered.py`). The GhanaLSS and gh323 site-4 tests already build under `tmp_path`.
- `assume_cache_fresh` exists()-then-read race: since PR #1162, `Country._read_assumed_fresh_cache` reads with `open_errors=(FileNotFoundError,)` and falls through to the ordinary path when the parquet disappears (`lsms_library/country.py:5222-5228`); regression in `tests/test_gh1040_wave_read_gates.py`.

The carried note (an unstamped country-script parquet is served under `assume_cache_fresh=True`) is that mode's documented contract: neither the content hash nor the #675 degraded grade is consulted (`country.py:5176-5178`; CLAUDE.md "Cache Behavior", limitation (c)). If that contract should change, it needs its own design issue; nothing in this one remains.

-- Sue
```

### #740 — test_extraction_is_cluster_grain fails on any data-credentialed machine, and CI cannot see it
Verdict: open/none, tier A. Narrowed 2026-10-14. Nothing has merged since that touches `tests/test_pakistan_cluster_features.py`: its last commit is `5fc68217f` (2026-09-02), and the `xfail(strict=False)` is still at `:82-93`. PR #1126's full-suite log (2026-10-19) records this test as the one XPASS, which matches the narrowed body. Pakistan idiosyncrasy (`Pakistan/_/CONTENTS.org:296-361`): `cluster_features` is built from `REGIONS.DTA` by `1991/_/cluster_features.py` under `materialize: make`, with no `_/Makefile`, so the build runs through `try_script`. No write needed; the body states the remaining work exactly.
QUEUE: #740 | Pakistan | remove the xfail; assert no GrainCollapseWarning on a private-root cold build and that the wave parquet holds 300 (t, v) rows | tests/test_pakistan_cluster_features.py | none (test-only) | credentialed `pytest tests/test_pakistan_cluster_features.py` in a private LSMS_DATA_DIR

### #976 — Sentinel sweep: 2,180 of 6,803 .dta blobs were unobtainable; 20 waves entirely unscanned
Verdict: needs-acquisition/none, tier D. No merged PR cites #976. The nearest acquisitions are `ab685fda7` (Tanzania_Kegera KHDS 1991-94, WB 359, via #552) and #1514, and neither re-pushes any of the 20 listed waves. Kegera 2004-05 and 2010-11 are still in scope. Whether the blobs are present on the remote is `unverified-data`: re-running the `slurm_logs/sentinel_audit/` sweep, or a sidecar-plus-remote probe over the 20 waves, would settle it. No write needed.

### #1064 — DECISION: data_access still accepts the in-tree countries/.dvc/s3_creds as a credential fallback
Verdict: needs-decision/ask, tier C. Unchanged on development. The reader rung is at `lsms_library/data_access.py:914-918`: `_check_remote_access` falls back to `dvc_dir / credentialpath` when no user `s3_creds` exists. The writer rungs are at `:936-938` (in-tree `s3_write_creds`) and `_resolve_write_credentialpath` at `:1958`. The body's line numbers (640-643, 664-669, 1235, 806) are stale. `isa-decisions.org` has no ruling.
C item:
- **Question:** should `permissions()` keep reporting `read` from an in-tree `countries/.dvc/s3_creds` that `DVCFileSystem` never reads (`_build_dvcfs` overrides `credentialpath`)? And should the in-tree writer rungs stay?
- **Options:** (1) drop the reader rung only, so `permissions()` agrees with DVCFS, and keep the writer rungs documented as the acquisition exception; (2) drop all three in-tree rungs; (3) keep everything and document the divergence.
- **Evidence:** #1031 removed the same rung from the test gate and the reachability script for exactly this divergence. `_check_remote_access` at `data_access.py:885-940` still has it. The CLI path resolves `credentialpath = s3_creds` in-tree whenever `_reader_credentials_env()` (`:1070`) returns None.
- **Recommendation:** option (1). It removes a false `read` on old clones. Writes stay inside the sanctioned acquisition exception (`add_wave` / `populate_and_push`). Cost: no build fingerprint moves, since no `data_access` symbol reaches one (CLAUDE.md, #917 note).
- **After a ruling:** a Tier A edit plus a test that builds a tree holding only an in-tree `s3_creds` and asserts `permissions()` is not `read`.

### #1085 — DECISION: a country entry that is a symlink escaping countries_root() is listed by countries() but refused by Country() as path traversal
Verdict: needs-decision/ask, tier C. Still undecided on development, and the code says so: `lsms_library/country.py:2696-2704` carries the comment "An ENTRY symlinked out of the root is still refused (#915-a, undecided)". `catalog._is_country_entry` (`lsms_library/catalog.py:23-41`) checks only that the name is a single component and appears in `os.listdir`, with no resolve check, so `countries()` still lists such an entry. PR #1098 (#915) fixed only the symlinked or relative ROOT half. No real tree is affected: there are no top-level symlinks under `lsms_library/countries`.
C item:
- **Question:** a country entry that is a symlink to a directory outside `countries_root()`: listed and accepted, or neither?
- **Options:** (a) `_is_country_entry` refuses an entry whose resolved path escapes the root, so `countries()` omits it and `Country()` agrees; (b) `Country()`'s traversal check becomes lexical (a single verbatim component cannot traverse), so symlinked entries are accepted everywhere.
- **Evidence:** `catalog.py:23-41` vs `country.py:2701-2704`. Seen 2026-10-19 on a scratch copy that symlinked `countries/` entries.
- **Recommendation:** (a). It keeps the "config tree is the reviewed source" invariant: an override should move the ROOT (`LSMS_COUNTRIES_ROOT`), not graft entries. It is also the conservative choice for path-escape semantics. Either option is a few lines plus a test, with no data change.

## 8.B08-framework-api

# Batch B08-framework-api

### #1230 — Org-key parsing: decide lexical storage and numeric-key compatibility
Verdict: needs-decision/none, tier C. Item 2 (sequential formatter/dictionary composition) landed in PR #1372 (`26803b81050d`, ancestor of `e5fdd2fa8`) and the body already says so (narrowed 2026-11-09). Item 1 is unchanged on development: `lsms_library/local_tools.py:3319` `df_from_orgfile(..., to_numeric=True)` still applies `_to_numeric` to every column (`:3382-3384`), so Uganda's declared `comm_frame_ea/comm7` parses `int64`. No ruling in `isa-decisions.org`. No write needed; the C item for the human:
- **Question:** should `#+key:` stay selection-only (adding an explicit lexical-key opt-in), or also declare that the key column is stored as a string?
- **Options:** (a) keep selection-only, add an opt-in such as a `#+key: <col> :lexical` modifier or a per-call flag; (b) make a declared key string-typed, which breaks the #928 contract (`test_df_from_orgfile_still_ignores_the_keyword`, numeric `Code`); (c) a corpus-wide lexical policy for all-digit columns, which would also touch genuine numeric inputs.
- **Evidence:** the 344-file census (comments 4-5) found 216 all-digit columns and two declarations (Uganda numeric, no leading zeros; Niger textual). The leading-zero losses (Peru Region/Code 9 of 25, Serbia `proizvod` 94 of 100, GhanaSPS `unit09/Code` 8 of 44) are all in *undeclared* tables, so (a) or (b) would not fix them. No served-data error has been shown.
- **Recommendation:** (a). It keeps #928's tests. File the undeclared leading-zero tables as a separate migration issue only if a served lookup is shown to miss. Note that any parser edit is `get_dataframe`-adjacent and therefore a corpus-wide re-warm.
No write needed (narrowed 2026-11-09; no merged work since).

### #1079 — DECISION: a declared NON-empty categorical table lacking the requested value column still falls through to a lower level
Verdict: needs-decision/none, tier C. Nothing has merged since it was filed (2026-10-19). The behaviour is live on development. In `lsms_library/local_tools.py:3203-3213` a `KeyError` from `df_data_grabber` is re-raised unless the table is declared empty, and the outer `except (FileNotFoundError, KeyError, ValueError): continue` then moves to the next rung. It is still pinned as report-only (`tests/test_categorical_mapping_empty_table.py:215`). No write needed. C item:
- **Question:** when a level declares `foo` with rows but lacks the requested value column, should resolution raise rather than fall through, possibly to the cross-country level?
- **Options:** (a) raise a distinct error (a sibling of `EmptyCategoricalTableError`) once the table is declared at that level; (c) warn and fall through.
- **Evidence:** same class as #1036 and #674, and it breaks #953's whole-table override. No live instance has been measured. The 77 call sites need an AST census of literal value column against the declaring level's columns before choosing.
- **Recommendation:** (a), after the census. It mirrors #1036's ruling for empty tables, and #953 already makes the cross-country level reachable, so falling through is a silent override. Cost: `get_categorical_mapping` is build-path code, so measure the fingerprint impact.

### #1080 — DECISION: the read-time categorical merge treats an empty declared table as a silent override (and can lose global rows)
Verdict: needs-decision/none, tier C. Nothing has merged since it was filed. Still pinned as report-only (`tests/test_categorical_mapping_empty_table.py:292`, `:303`). The code paths are unchanged: `load_categorical_level` (`local_tools.py:2891`), `_row_union_categorical` (`country.py:705`) and `_merge_categorical_tables` (`country.py:809`). No write needed. C item:
- **Question:** should a read-time level with a declared-empty (or all-null) table raise `EmptyCategoricalTableError`, or skip it and warn?
- **Options:** (a) raise. The blast radius is every table of that country, because the level is loaded whole. (b) Skip the empty table, so the global table survives, and warn. (c) Leave as is.
- **Evidence:** the census found 0 live instances. The failure modes are: a non-additive empty country table disables the global one; a bare-`#+name:` additive table loses every global row; an empty wave table replaces the merged table via `dict.update`.
- **Recommendation:** (b), skip and warn, at read time. It has no live instance to break, cannot take a whole country down over one table, and is read-path (expected 0 hashes moved; confirm by probe). Keep the build-time cascade's raise from #1036 as it is.

### #1116 — #600 residual: override records take their note and validation from `_OVERRIDES`, over a hand edit to the file
Verdict: needs-decision/none, tier C. Unchanged on development. `scripts/backfill_wave_provenance.py:419` (path 4a) and `:441-443` (4b) still write `note=ov["note"]` and `validation=ov.get("validation")`. `_resolve_not_wb` (`:542`, note at `:579-580`) still re-derives a self-written `not-a-wb-dataset` note from `_COUNTRY_CATALOG`. The later script commits (`519437658`, `d0f8bca09`, `fffb406c1`, `968117510`) changed held/latest/subdir handling and none of these fields. No write needed. C item:
- **Question:** for the five override waves and for self-stamped not-a-WB records, which copy is authoritative for note/validation: the script table or the file?
- **Options:** (a) the script table is authoritative; document "edit `_OVERRIDES`, not SOURCE.org" in the script docstring and CONTRIBUTING; (b) refuse the re-stamp when the file's value differs from the table's (the same class as the r1 "may not overwrite a recorded fact" guard); (c) the file wins and the table seeds only an absent value.
- **Evidence:** every overwrite already appears in the review artifact's `changed` table (old → new), so neither path is silent, and the live dry run has 0 instances.
- **Recommendation:** (b). It matches the #600 invariant that a re-stamp never erases what a human established, it is source-only, and it costs no cache hash (the script is not in any fingerprint).

### #794 — Format118 mojibake: design paired decode/crosswalk migration for strategy-B countries
Verdict: needs-decision/none, tier C. Narrowed 2026-10-25 after PR #981 (`1ea13f2db`) and #1105 (`e87a551e8`). No migration has merged since. `Burkina_Faso/_/CONTENTS.org:1606-1608` still says "Strategy B ... is UNCHANGED for this country", and `:1620-1623` says the 2018-19 format-118 double encoding (`Viande de b<U+00C5><U+0093>uf`) "is deliberately NOT re-keyed". #1226's Senegal 2018-19 community prices (`30b19860b`) deliberately keep the mojibake-keyed `u`/`harmonize_food` crosswalks load-bearing. That is consistent with this issue's "preserve live matches", but it adds one more consumer the migration must re-key together. Idiosyncrasy named: Burkina 2014 is cp1252 in a format-113 file (re-decoded by #805), while 2018-19 is UTF-8 in format 118 read as latin-1, so `_decode_mojibake` fixes only the latter. No write needed. C item:
- **Question:** migrate Benin, Burkina Faso, Senegal and Togo from strategy B (crosswalk keyed on the damage) to strategy A (`_decode_mojibake` hook, crosswalk re-keyed clean), and in which order?
- **Options:** (a) migrate all four, one country per PR, re-keying `harmonize_food`, `u` and community-price consumers in the same commit; (b) migrate only the countries with a live leak, today none after #805 (Burkina 2018-19 row 303 now matches); (c) keep strategy B and add a test that every damaged key still matches.
- **Evidence:** a census lower bound of 278,777 rows across the six-country cohort (comment 1 and comment 3 correction). The served leak scan finds 0 leaking `j`/`u` after #805/#981. Each migration is a whole-country re-warm (wave `mapping.py` plus country `.org`).
- **Recommendation:** (c) now, as a cheap guard that turns the fragility into a test, and (a) only when a country is next touched for food work. Never transcode org files blindly.

### #926 — Curated encodings: finish live format118 keys and the paired transcoding policy
Verdict: open/ask, tier C. Narrowed 2026-10-25 after PR #1105 (`e87a551e8`). Nothing has merged since. Its one live item, Burkina's format-118 double-encoded key and the paired migration, is owned by #794 by the issue's own text. The other residual ("inventory the still-load-bearing curated keys and ISO-8859 units.org inputs") can be done from source, and was done here:
- `GhanaLSS/_/ghanalss.py:584` (wave `categorical_mapping.org`, `harmonize_food`) and `:1137`/`:1225` (country `agric_crop`/`agric_animal`) read **UTF-8** org files with `encoding='ISO-8859-1'`. Today their tables are pure ASCII; the only non-ASCII bytes are `§` in prose (1987-88 lines 262/266, 1988-89 line 335). This is harmless now, and a latent landmine the day a table gains a non-ASCII label.
- `GhanaLSS/2005-06/_/food_acquired.py:58` (that wave's org is ASCII).
- `Senegal/2018-19/_/food_acquired.py:27` reads ISO-8859 `units.org`. That script is not the API path (per the #794 comments, the `encoding=` scripts are dead on every build path).
- `GhanaSPS/_/units.org` and `Senegal/_/units.org` are genuinely ISO-8859 files.
Draft for the parent to post (ask the maintainer before closing):
> Proposed: fold this into #794 and close it. The one live item, Burkina's format118 double-encoded key, is #794's paired migration. The inventory this issue asked for, from source at `e5fdd2fa8`:
> - Live ISO-8859-1 readers of UTF-8 org files: `GhanaLSS/_/ghanalss.py:584,1137,1225` and `GhanaLSS/2005-06/_/food_acquired.py:58`. Their tables are ASCII today, so nothing is mis-decoded; the risk is latent.
> - `Senegal/2018-19/_/food_acquired.py:27`: a dead script.
> - Genuinely ISO-8859 files: `GhanaSPS/_/units.org`, `Senegal/_/units.org`.
>
> If you agree, I will add one line to #794 ("when migrating, switch the GhanaLSS ISO-8859-1 org reads to UTF-8 or make them encoding-agnostic") and close this as superseded. -- Sue
QUEUE: #926 | GhanaLSS | drop `encoding='ISO-8859-1'` on the three `ghanalss.py` org reads, whose files are UTF-8; tables are ASCII so output is identical | `lsms_library/countries/GhanaLSS/_/ghanalss.py` | one country (GhanaLSS country module => every GhanaLSS table re-warms) | assert byte-identical label dicts before and after (pure parse test, no build)

### #787 — labels=: decide ambiguous targets, curate variants and finish numeric-target examples
Verdict: needs-decision/none, tier C. Narrowed 2026-10-25 after PR #1029 (`fb2f2ce8e`). The only label work merged since is `c033c0504` (2026-11-04: label-guide and skill text after the Uganda rulings, #833), which settles none of the three items. On development:
- Item 1 is still live. `lsms_library/country.py:4066` `_relabel_j`'s docstring still says a key with more than one distinct target "is served last-row-wins".
- Item 3's count discrepancy is still present: `docs/guide/label-selection.md:100` says 42,206 rows and `:122` says 42,240 (`CLAUDE.md:708` repeats 42,206). Which is right is unverified-data; it would be settled by `Country('Malawi').food_acquired(labels='2004-05').attrs['labels_unmatched']`.
No write needed. C item:
- **Question:** when one Preferred Label maps to several distinct non-missing targets, should `_relabel_j` raise rather than keep the last row?
- **Options:** (a) raise a structured error (Contract-B-degradable as `LabelUnavailableError`? or a plain `KeyError`, since a conflicting table is malformed); (b) keep last-row-wins and report the conflicts in `attrs` with a warning, as `labels_unmatched` does; (c) leave as is.
- **Evidence:** conflicting keys per the issue discussion: Niger 153, Senegal 134, Burkina 72, plus Uganda Cassava flour. These are qualified historical counts. The aggregate-labels skill treats a malformed table as a plain `KeyError` that must stay loud.
- **Recommendation:** (b) first, because it is read-path, costs no hash and is reversible, with a `LSMS_LABELS_STRICT` escalation to raise. Curating the conflicting rows (item 2) then removes the warnings country by country.

### #279 — Feature: bespoke label schemes without modifying the package tree
Verdict: needs-decision/narrow, tier C. No out-of-tree scheme mechanism has merged. The body's cheapest option no longer exists: `d3b3c18b3` (#682/#685, an ancestor of `e5fdd2fa8`) gave the `labels=` **dict** form the meaning `{target: variant}` (`lsms_library/country.py:500-513`), and it raises `TypeError` on a non-string value, so `labels={Preferred Label: bucket}` cannot be added as written. #436 (closed 2026-06-15) landed `LSMS_COUNTRIES_ROOT`. That allows an out-of-tree *copy of the whole config tree*, a workaround rather than the per-analysis scheme asked for. The worked example still lives in-tree (`EthiopiaRHS/_/categorical_mapping.org:207`, `EthiopiaRHS/_/bellemare_etal13.yml`).
New title: `labels=: accept an out-of-tree {Preferred Label: bucket} scheme (the dict form is taken)`
New body:
> **Resolved so far:** nothing implemented. Two facts changed the options: #436 landed `LSMS_COUNTRIES_ROOT` (`countries_dir` in config), which lets a downstream study point the library at a modified copy of the whole config tree. #682/#685 (`d3b3c18b3`) gave `labels=<dict>` the meaning `{target: variant}` (`lsms_library/country.py:500-513`; a non-string value raises `TypeError`). So the original option 1, a bare `{PL: bucket}` dict, would now collide with a landed API.
>
> **Remaining (Tier C: choose, then implement):** let `food_expenditures` / `food_quantities` / `food_acquired` take an analysis-specific `Preferred Label -> bucket` scheme that lives outside the package. Options:
> 1. `labels=LabelScheme({...})` or `labels=pd.Series(...)`: a typed object, distinguishable from the `{target: variant}` dict; `_relabel_j` dispatches on the type.
> 2. `labels=<path>`: a YAML/CSV `{PL: bucket}` file resolved from CWD or `$LSMS_LABELS_DIR`.
> 3. A user registry (`~/.config/lsms_library/labels/<name>.yml`) consulted before the in-package columns.
> 4. Document `LSMS_COUNTRIES_ROOT` as the supported route and close this issue.
>
> Contract to keep: `reaggregate=True` semantics unchanged; the unmatched-label report (`attrs['labels_unmatched']`, `LabelPassThroughWarning`, `LSMS_LABELS_STRICT`, #787) applies to an external scheme exactly as to an in-package column; `Feature` Contract B unchanged. Worked example to migrate: `EthiopiaRHS/_/bellemare_etal13.yml` and the `Bellemare-etal13 Label` column (`EthiopiaRHS/_/categorical_mapping.org:207`). Any implementation is read-path (`_relabel_j` runs after the cache read), so it should move no cache hash; confirm by probe. -- Sue
C item: **Question:** which of options 1-4? **Evidence:** above. **Recommendation:** option 1, which is typed, testable and read-path only, and needs no filesystem lookup; option 4 is the zero-cost fallback.

### #226 — feat: principled auto-aggregation script for harmonize_food's Aggregate Label column
Verdict: open/none, tier C. Ruled in scope on 2026-10-23 (@ligon: "in scope for the ISA campaign. It will be designed and built after the defect queue drains, with a design note here first"). No generic script or CLI has merged. One component exists. PR #940 (`3c23299b5d`) added `SkunkWorks/aggregation/ghana_gate.py`, the β-spread gate, together with the validation/partition pipeline. Its ledger (`.coder/ledger/ghanalss-aggregate-recuration.md`) says "the beta-spread gate had no prior implementation ... GH #226 tracks the script". When this is built, reuse those modules (prior art) rather than re-deriving them. No write needed: the issue is ruled and deferred, and its body is accurate.

### #939 — Check whether partition ranking is stable in min_goods before relying on the aggregation selection
Verdict: open/narrow, tier C. The 2026-09-17 sequencing comment said to wait for #937's crosswalk reconciliation. That has happened: PR #940 (`3c23299b5d`, ancestor of `e5fdd2fa8`) superseded #937 and re-curated GhanaLSS Aggregate on the #935 crosswalk. It kept `min_goods=1` and explicitly did not attempt this check (`.coder/ledger/ghanalss-aggregate-recuration.md:70-73`, `:125`). So the check now bears on the *served* partition. `min_goods` is threaded (`SkunkWorks/aggregation/ghana_validation.py:136,150,179,305`). The sweep has not been run; it needs data and compute.
New title: `GhanaLSS Aggregate partition (#940): check ranking stability across min_goods`
New body:
> **Status (2026-11-12):** the blocker in the 2026-09-17 comment is cleared. PR #940 (`3c23299b5d`) superseded #937, re-curated GhanaLSS `Aggregate` on the #935 crosswalk, and selected the partition now served in `GhanaLSS/_/categorical_mapping.org`. It scored held-out households at `min_goods=1` and explicitly deferred this check (`.coder/ledger/ghanalss-aggregate-recuration.md:70-73`).
>
> **Remaining:** re-run the Ghana validation (`SkunkWorks/aggregation/ghana_validation.py`, where `min_goods` is threaded through `evaluation_arguments` / `run_comparison`) at `min_goods` ∈ {1, 3, 5, 9 (= the fit cutoff `floor(0.1·J)+1`)}. Compare the **ranking** of candidate partitions, not only fit levels.
> - Invariant ranking: say so in the GhanaLSS CONTENTS / ledger with the sweep as evidence.
> - Ranking moves: record which buckets depend on thinly scored households before the partition is used in published work, and bring it back as a decision.
>
> Caveats unchanged: `SkunkWorks/` is outside CI; the fixtures use fewer than ten goods, so they cannot see a cutoff regression. Background: ligon/CFEDemands#11 (sd of `w` 0.70 fitting vs 2.15 when scored on one good). -- Sue

### #1068 — DECISION: native-unit modes treat U_UNKNOWN as a pooled pseudo-unit
Verdict: needs-decision/none, tier C (then A). No fix has merged. `lsms_library/transformations.py:3733-3734` (`unitvalue`) is still `Expenditure / Quantity` with no `U_UNKNOWN` exclusion (`U_UNKNOWN` is defined at `:4148`). The surface has **grown** since filing: `e978c3dd4` (GH #1234, `map_index`: a missing `u` becomes `U_UNKNOWN`) and `51b073e58` ("Panama unitvalue pin moves +390 ... blank-unit rows kept as Unknown") added Panama to the exposed countries. No write needed; the issue text is accurate (an optional comment could add the Panama +390). C item:
- **Question:** in `food_prices(units='unitvalue')` and `food_quantities(units='units')`, what happens to rows with `u='Unknown'`?
- **Options:** (a) exclude them and count them in `attrs`, which is what the kg modes effectively do; (b) serve them as NaN; (c) keep them and document it.
- **Evidence:** EthiopiaRHS `Unknown` unitvalue rows went 0 → 46 under #1044c; Panama +390 under #1234. Niger has 0 by accident of its data. The change is read-time `transformations.py`, so no build fingerprint moves, except GhanaLSS through `nutrition.py`'s import closure (measure).
- **Recommendation:** (a), decided together with #1129. Both are "never mint a number on a unit that is absent or is money".

### #1129 — `food_prices(units='unitvalue')` serves exactly 1.0 for every `u='Value'` row
Verdict: needs-decision/none, tier C (then A/B). No fix has merged. The `unitvalue` branch (`transformations.py:3733-3734`) still divides currency by currency. `_drop_unpriceable` keeps its separate `currency` bucket (`_CURRENCY_DENOMINATED_UNITS`, `:961`) for the `kg*` modes only (docstring `:288-320`). `CLAUDE.md`'s derived-tables section still documents the tautology ("gives 1 = 'Kwacha per Kwacha' for `u='Value'` rows"). No write needed. C item:
- **Question:** drop `u ∈ _CURRENCY_DENOMINATED_UNITS` rows from `unitvalue` as unpriceable, or keep the constant and document it?
- **Options:** (a) drop them and tally them in `attrs['price_rows_dropped']['currency']`, as `kgvalue` already does. GLSS1-3 `unitvalue` then becomes empty, consistent with their `correctly-empty` / `empty-pending-machinery` verdicts. (b) Keep them and register the identity in the docstring and the derivations registry.
- **Evidence:** 1,107,464 of 1,676,093 GhanaLSS `unitvalue` rows are exactly 1.0 (std 0 in every wave). This is the same shape the #770 ruling removed from `kgvalue` ("empty food prices is better than a made up constant"). Panama has not been measured (unverified-data: `Country('Panama').food_prices(units='unitvalue')`, count `u=='Value'`).
- **Recommendation:** (a). It reuses the existing `currency` bucket, and CLAUDE.md's derived-tables bullet would need its "gives 1" sentence corrected in the same PR.
QUEUE (after the #1068/#1129 ruling): #1068+#1129 | cross-country | in `food_prices_from_acquired`'s `unitvalue` branch (and `food_quantities` `units='units'` for Unknown), drop `U_UNKNOWN` and currency rows into `attrs['price_rows_dropped']` buckets | `lsms_library/transformations.py`, `CLAUDE.md`, `tests/test_currency_denominated_units.py` | none (read-time), except the GhanaLSS country hash via `nutrition.py`'s import closure | unit tests on synthetic frames; measure GhanaLSS/Panama/EthiopiaRHS row deltas on a warm private root

### #828 — food_expenditures(basis='total'): choose and implement missing nonpurchase-value disclosure
Verdict: needs-decision/none, tier C. Narrowed 2026-10-25 after PR #1029 (`fb2f2ce8e`). Nothing has merged since. `lsms_library/transformations.py:3308` `food_expenditures_from_acquired` still passes a `basis='total'` sum through. Where the instrument recorded no own-production or in-kind value, that sum is silently purchased-only, and the only related guard is "valuation requires basis='total'" (`:3415`). No write needed. C item:
- **Question:** when a country/wave has no non-purchase `Expenditure`, what does `basis='total'` do?
- **Options:** (a) raise a typed refusal (`UnitsUnavailableError`-style, so Contract B in `Feature` drops the country with one warning); (b) warn and add structured `attrs` (per-wave share of non-purchase rows with a value) and serve the frame; (c) point users to `valuation=` (e.g. `median_price_valuation`) and refuse only when no valuation is given.
- **Evidence:** the `units=` half (Contract B) is done in #1029. #913 proposes a source-coverage warning for GhanaLSS 1991-92 own production (option C there), and both should share one mechanism. GhanaAHIES/Peru/Nepal were not in the historical warm count.
- **Recommendation:** (b) with an `LSMS_*_STRICT`-style escalation. It keeps the read path non-destructive like the other guards, and #913 can reuse the same `attrs` key.

### #830 — Extend monetary scale diagnostics beyond isolated neighbor jumps
Verdict: open/none, tier C (enhancement). The first increment is on development: `lsms_library/coverage_matrix.py:709` `monetary_scale_breaks`, wired into grading at `:1034`, landed per the v0.15.0 reconciliation (PR #998). Nothing has touched `coverage_matrix.py` since 2026-10-19 except #781's verdict work (`a38f3d9ae`, `27c0f8248`). The four detection gaps in the body still apply as written. No write needed.

### #845 — Price provenance: is Price elicited or derived for Panama/GhanaLSS/Guatemala/Serbia; explain Uganda's ~5% gap; decide on a read-time diagnostic
Verdict: partial/split, tier C (plus a B residual). Fixes 1-2 landed in PR #1029 (`a141803a7`). Item (c) was characterised by PR #1415 (`724f9bd603`, an ancestor): `Uganda/_/CONTENTS.org:1384-1400` finds round 50-UGX prices, diffuse across households, over-representing fractional quantities, with the mechanism unidentified. Sue's 2026-11-04 comment proposes stopping there. The body, narrowed 2026-10-17, still lists (c) as open, and it omits the Panama finding from comment 1, which is still live: `Panama/2008/_/food_acquired.py:184-185` sums `Quantity`/`Expenditure` per key but serves `Price = _g['Price'].mean()`. Since #1251 (`675e544d53`) stopped the incidental frame-wide re-derivation, 8 purchased 2008 rows serve a mean price −9% to +51% off the ratio of sums. That is a script fix of a different kind (Tier B, Panama only), so split it.
**Parent, new title:** `Price provenance: record elicited-vs-derived Price for Panama/GhanaLSS/Guatemala/Serbia; decide on a read-time diagnostic`
**Parent, new body:**
> **Resolved so far:** PR #1029 (`a141803a7`): the `units=` docstring says `food_acquired.Price` is whatever the source recorded, and Uganda's raw/served agreement table is in `Uganda/_/CONTENTS.org`. PR #1415 (`724f9bd603`): Uganda's 2009-10 / 2011-12 residual (item c) is characterised in `Uganda/_/CONTENTS.org` ("That residual, characterised"): round prices (98% multiples of 50 UGX), diffuse across households and EAs, over-representing fractional quantities, consistent with quantity edits for at most a fifth of 2009-10. Mechanism unidentified; no further ISA work planned. Uganda is the only ISA country whose `food_acquired` carries `Price`. The Panama 2008 mean-price defect found in review is split to #NEW.
>
> **Remaining:**
> - (b) Record `Price` provenance in `CONTENTS.org` for Panama, GhanaLSS, Guatemala and Serbia: elicited, back-calculated by the statistics office, or built by the library. Same-day agreement shares: Panama 100% (partly an artefact, see #NEW), GhanaLSS 100% (produced rows only; probably the registered `8h-farmgate` derivation, unconfirmed), Guatemala 97.7%, Serbia 88.5%.
> - (a) **DECISION:** an optional read-time diagnostic that warns once when `unitprice` and `unitvalue` agree on more than 90% of a country's rows. Build it or drop it. The Uganda evidence supports building it; it is read-path and costs no cache hash.
>
> -- Sue

**Split, residual issue:**
Title: `Panama 2008 food_acquired: Price is a mean of row prices beside summed Quantity/Expenditure`
Tier: B
Body:
> Split from #845 (comment of 2026-10-23, the #1234 / PR #1251 review).
>
> `Panama/2008/_/food_acquired.py:184-185` groups by `(t, i, j, u, s)`, sums `Quantity` and `Expenditure` (`min_count=1`), and sets `out['Price'] = _g['Price'].mean()`. Where a key has more than one row, the served `Price` is a mean of per-row prices, not the ratio of the sums served on the same row. Before PR #1251 (`675e544d53`) this was hidden: three minted-`'unit'` rows collided with genuine `'unit'` keys, and the resulting additive collapse in `_normalize_dataframe_index` re-derived `Price` for every row. After #1251, 8 purchased 2008 rows serve the mean price, −9% to +51% of the ratio of sums (review measurement). The served definition depended on whether a duplicate key existed anywhere in the frame.
>
> **Fix:** either compute `Price` as `Expenditure / Quantity` after the groupby where `Price` was itself reported as that ratio, or keep the mean and register it as a derivation (`Panama::food_acquired::…`), after checking in the 2008 instrument whether `Price` is elicited. Panama-only re-warm (wave script plus that country's L2). Verify: the count of multi-row keys and the per-row before/after `Price` delta on a private root; add a pin to the existing Panama `unitvalue`/`unitprice` tests.
>
> -- Sue

**Comment for the parent:** "Split the Panama 2008 mean-price script defect to #NEW (Tier B). Narrowed this issue to items (a) and (b); item (c) is characterised in `Uganda/_/CONTENTS.org` (PR #1415). -- Sue"
QUEUE: #NEW (split from #845) | Panama | serve `Price` as ratio of sums (or register the mean) in `2008/_/food_acquired.py` | `lsms_library/countries/Panama/2008/_/food_acquired.py`, `Panama/_/CONTENTS.org`, a test | one country (Panama) | private-root rebuild of Panama `food_acquired` 2008; count of changed `Price` rows (expect 8 purchased)


## 8.B09a-ghana-food

# Batch B09a — GhanaLSS / GhanaSPS food (reviewer: Claude Opus 5.5 fork, read-only, base e5fdd2fa8)

### #140 — [umbrella] Rebuild GhanaSPS: registration, data refresh, feature gap
Verdict: partial/narrow, tier B (residual mixes B wiring, C feature direction, D acquisition). The body (2026-04) is stale on most load-bearing claims, as its own 2026-08-20 comment already says. Landed: `data_scheme.yml` registers food_acquired, household_roster, individual_education, sample, housing, plot_features, livestock, crop_production, plot_labor (`lsms_library/countries/GhanaSPS/_/data_scheme.yml:4,26,70,134,158,225,344,470,604`); PRs #673 (`b63f49e2a`, sample), #677 (`ad951c91a`, roster; merged via audit/ghana-correctness, ancestor of e5fdd2fa8), #752 (`8725bed2f`), #760 (`9c21ab89d`), #761 (`b9888b000`), #768 (`853ce4969`). Wave-3 public-only modules acquired 2026-08-19 (`GhanaSPS/2017-18/Documentation/SOURCE.org`, SECONDARY_SOURCE lines). Idiosyncrasy (CONTENTS.org:478ff): wave 3 is a MIXED-VINTAGE directory — 30 of our files carry extra pre-anonymisation variables (sub-district geography, free text, consent flags) relative to Dataverse v1.2; and v is right-anchored `FPrimary[-6:-3]` because FPrimary is not fixed-width. Remaining: cluster_features/Region (`market='Region'` raises "cluster_features returned no data", CONTENTS.org:2147-2168), anthropometry/assets/nonfood/shocks feature gap, 2013-14 LICENSE.org absent, wave-2/3 weights not published (producer-side, not actionable).
Draft (narrow):
New title: GhanaSPS: wire cluster_features/Region and finish the remaining feature gap (anthropometry, assets, nonfood)
New body:
> **Resolved so far:** registration and nine tables — #673 (`b63f49e2a`, `sample`, 334 EAs per wave), #677 (`ad951c91a`, `household_roster` + `household_characteristics`), #752 (`8725bed2f`, `plot_features`), #760 (`9c21ab89d`, `livestock`), #761 (`b9888b000`, `crop_production`), #768 (`853ce4969`, `plot_labor`); `food_acquired`, `individual_education` and `housing` are registered in `GhanaSPS/_/data_scheme.yml`. The ten public-only wave-3 modules were acquired from Dataverse v1.2 on 2026-08-19 (`2017-18/Documentation/SOURCE.org`). Settled questions (see comments 3-5): wave 1 is WB 2534 as published; waves 2/3 publish no sampling weight; `ppweight3` is a per-person expansion weight; the wave-1 licence clause is WB boilerplate.
>
> **Remaining:**
> 1. `cluster_features` is not declared, so `market='Region'` raises "cluster_features returned no data for any wave" for the food-derived tables and `household_characteristics` (`GhanaSPS/_/CONTENTS.org`, "Region-market dependency remains missing"). Wire a source-backed Region per wave; do not infer it from `strata`, and record wave 2's source geography gap rather than fabricating a national market.
> 2. Feature gap, ranked by the 2026-08-20 inventory: `anthropometry` (all three waves, whole household), then `assets`, `nonfood_expenditures`. Shocks are `not-asked` at household level and `asked-not-distributed` at community level (no community `.dta` in any wave): an acquisition target, not wiring.
> 3. Provenance: `2013-14/Documentation/` has no `LICENSE.org`; no GhanaSPS `SOURCE.org` records `PROVENANCE_VALIDATION`.
>
> Out of scope here: `plot_labor.source` taxonomy (#1121). Tier B for item 1; items 2-3 are feature/provenance work. -- Sue
QUEUE: #140 | GhanaSPS | declare cluster_features per wave with a source-backed Region (2009-10 id1/region; 2017-18 region; 2013-14 document the gap) | GhanaSPS/{wave}/_/data_info.yml, GhanaSPS/_/data_scheme.yml, CONTENTS.org | one country (data_scheme.yml is in every GhanaSPS table hash) | `Country('GhanaSPS').food_expenditures(market='Region')` returns rows per wave; check Region values against the questionnaire's region codes

### #562 — GhanaLSS Phase 3: community_prices feature + inverse price->quantity imputation (split from #109)
Verdict: partial/split, tier C. Phase 3a landed: #762 (`f653104a8`, "community_prices from each wave's own price survey (#562 phase 3a)"); `GhanaLSS/_/data_scheme.yml:19,161` declares `community_prices: (t, v, j, u, obs)`; CONTENTS.org:4700ff documents it ("Phase 3b ... is NOT wired here"). Phase 3b (inverse price->quantity imputation) is unbuilt (no inverse imputation in `transformations.py`), and its four open questions are still unanswered. Since the issue was filed, #1132 (`4c630652c`) closed GLSS1/2 `food_prices` as `correctly-empty` (`.coder/coverage/absent_verdicts.csv:35-36`) on the argument that a 3b fill would re-serve `community_prices` under another name; 1991-92 stays `empty-pending-machinery` (:37). Idiosyncrasy: GhanaLSS keeps three vendor observations per item under an extra `obs` level, so its `community_prices` departs from Malawi/Nigeria's `(t, v, j, u)` grain (CONTENTS.org:4735ff). The residual is a design decision, a different kind of work from 3a, so split it.
Draft (split): parent comment, then close #562.
> Phase 3a is done: #762 (`f653104a8`) built `community_prices` from each wave's own price survey (`GhanaLSS/_/data_scheme.yml:161`, index `(t, v, j, u, obs)`; six of seven waves, 2005-06 ships no price file). Phase 3b, the inverse price-to-quantity imputation, was never built and still needs its design questions answered, so it moves to #NEW. Closing this as 3a complete. -- Sue
Residual issue — Title: GhanaLSS: decide whether to impute Quantity from community prices (former #562 phase 3b). Tier C.
> Split from #562, whose phase 3a (`community_prices`, #762 `f653104a8`) is done.
>
> **Question.** For waves that record food expenditure but not quantity, should the derived `food_quantities` path fill `Quantity = Expenditure / price` from a hierarchical median-price ladder over `community_prices`, in native units?
>
> **What has changed since #562 was filed.** #1132 (`4c630652c`) closed GLSS1/2 `food_prices` as `correctly-empty` (`.coder/coverage/absent_verdicts.csv:35-36`): those waves' only price is the cluster price already served as `community_prices`, so an imputed household quantity would make `food_prices` re-serve it under another name. 1991-92 stays `empty-pending-machinery` (:37) because `units='kgprice'` serves GLSS3's Section 8H price. #770 Step 2 (a `source=` argument for the `u='Value'` kg factor) and #782 (item labels) are the related machinery.
>
> **Decide before implementing:** (1) derived `food_quantities` path only, `food_acquired` untouched? (2) an imputed-vs-reported marker — today that means a registered derivation (`Derivation` key), per the derived-values doctrine; (3) which waves, given the GLSS1/2 close; (4) any external baseline to check against, or shape + sanity only.
>
> -- Sue
C-item (for the human):
- Question: should GhanaLSS impute household food Quantity from community prices on the derived `food_quantities` path, and in which waves?
- Options: (a) build it for every value-only wave, as a registered derivation; (b) build it only for 1991-92 and later, keeping the GLSS1/2 `correctly-empty` close; (c) do not build it, and close 3b.
- Evidence: 3a is served (#762). The #1132 verdict rows say an imputation for GLSS1/2 re-serves `community_prices`. No 3b code exists.
- Recommendation: (b) or (c). Under (a), the GLSS1/2 verdict rows would have to be withdrawn first.

### #913 — DECISION: GhanaLSS 1991-92 own production -- value the 28% of rows carrying both factors (A), define u='All' as a whole-harvest construction with its own key (B), or keep NA plus a basis='total' source-coverage warning (C)
Verdict: needs-decision/close (as duplicate), tier C. Nothing has landed for 1991-92 since the 2026-10-14 narrowing: `GhanaLSS/_/ghanalss.py:652` still reads `FARMGATE_WAVES = ('1998-99', '2005-06', '2012-13', '2016-17')`, and CONTENTS.org:1361-1365 still says 1991-92 is "deliberately NOT wired". The same decision is filed again, newer and more actionable, as #1130 (read the GLSS3 8H unit codes, decide `All`, extend `8h-farmgate`). The "warning half of C" is #828 (`basis='total'` nonpurchase-value disclosure), whose body already says to coordinate with #913. Two open issues for one decision means neither says what is left. Idiosyncrasy: 1991-92's modal own-produce unit is `All` (94,039 of 287,712 rows; CONTENTS.org:2499 notes the drift from 93,349), and only 28% of produced rows carry both factors.
Draft (close as duplicate; move the options into #1130, see the #1130 draft):
> Closing as a duplicate. The decision this issue asks for (what `u='All'` means for GLSS3 own production, and whether 1991-92 is valued) is now carried by #1130, which has the next step (read the GLSS3 Section 8H unit codes). Options A/B/C from this issue are copied into #1130's body. The basis='total' warning (the dispatchable half of option C) is #828. Nothing has changed for 1991-92 since #916 (`f959ff644`): `GhanaLSS/_/ghanalss.py:652` still excludes it from `FARMGATE_WAVES`. -- Sue

### #1130 — GhanaLSS 1991-92 own-produce valuation: decide the `All` unit, then extend `8h-farmgate` to GLSS3
Verdict: needs-decision/narrow, tier C then B. Unchanged on development: `ghanalss.py:652` excludes 1991-92; `.coder/coverage/absent_verdicts.csv:37` still holds the 1991-92 `empty-pending-machinery` row. No merged PR references #1130 after it was filed (timeline: only #1132, which filed it). Narrow only to absorb #913's options so one issue carries the decision. Related evidence found: in Section 9 Part B, the library already reads unit code 22 `All` as "the whole lot" (CONTENTS.org:5101-5106, `GhanaLSS::crop_production::9b-sale-amount-per-unit`). That is a different section, so it is not proof for 8H.
Draft (narrow): title kept.
New body:
> **Resolved so far:** #916 (`f959ff644`) values own production in GLSS4-7 through `GhanaLSS::food_acquired::8h-farmgate` (Q × P). This issue now also carries the decision formerly filed as #913 (closed as a duplicate).
>
> **What.** GLSS3 Section 8H (`1991-92/Documentation/pdf/G3QPartB.pdf` p.9) records, per home-produced food, a quantity ("NO. OF UNITS", q3-12), a unit code (q13), and "For how much could you sell one unit of … now?" (q14). In the built `food_acquired` (1991-92, 287,712 produced rows), `Price` is non-null on 287,570, `Quantity` on 80,419, both on 80,277, and `Expenditure` on 0. `food_prices(units='kgprice')` serves 28,754 rows, but the default `kgvalue` is empty because `ghanalss.FARMGATE_WAVES` (`GhanaLSS/_/ghanalss.py:652`) excludes 1991-92. The dominant unit is `All` (94,039 rows).
>
> **Decision (from #913):** (A) value the ~28% of rows carrying both factors and leave the rest NA. That serves a "total" that is about 72% purchased-only, which is the failure #913 was filed about. (B) Define `u='All'` as a whole-harvest valuation: a different construction from Q × P, so it gets its own registry key. (C) Keep NA and rely on the `basis='total'` disclosure (#828). Evidence to read first: the GLSS3 8H unit-code list. For comparison, Section 9 Part B code 22 `All` means "the whole lot" (`GhanaLSS/_/CONTENTS.org`, "Section 9 Part B records a sale PRICE").
>
> **Then** extend `8h-farmgate` to 1991-92 for the countable units, and retire the 1991-92 `empty-pending-machinery` row (`.coder/coverage/absent_verdicts.csv:37`) in the same PR, so that `tests/test_coverage_matrix.py::test_ghanalss_food_prices_grades_against_its_verdicts` reports `sane`.
>
> **Tier:** DECISION, then B. Cost: a GhanaLSS re-warm (the derivation lives in `ghanalss.py`). -- Sue
C-item (for the human):
- Question: what does GLSS3 Section 8H unit `All` mean, and is 1991-92 own production valued?
- Options: A (value the 28%), B (a whole-harvest key for `All`), C (keep NA plus the #828 disclosure).
- Evidence: `ghanalss.py:652`; CONTENTS.org:1361-1365 and 2499; 9B code 22 = "whole lot" (CONTENTS.org:5101).
- Recommendation: first read the G3QPartB 8H code list (a C4 check, one PDF). Then B for `All` plus Q × P for countable units, or C. Avoid A.

### #956 — GhanaLSS wave-level harmonize_food carries an Aggregate Label column nothing can read, and it disagrees with the country table on 407 of 962 rows
Verdict: open/none, tier B (config cleanup that needs curation first). Unchanged since filing. All seven wave tables still have the column: `GhanaLSS/{1987-88,...,2016-17}/_/categorical_mapping.org`, `harmonize_food` header `| Preferred Label | Aggregate Label | ...`. The only edits since 2026-09-18 are #926's encoding fix (`21a8e9e07`). The read path is unchanged: `Country._relabel_j` reads `Country.categorical_mapping['harmonize_food']` (country.py:4078-4089), and no GhanaLSS script reads `Aggregate`. No merged PR references #956. Idiosyncrasy: GhanaLSS keeps a per-wave food code→label table per instrument section (`Code_12A`, `Code_9b`, `Code_8h` columns), so the wave level legitimately holds decoding config, and only the aggregation column is misplaced. Step 1 (reconcile 407 disagreements into the country table) is curation and needs a human judgement per item. Steps 2-3 (drop the column, one GhanaLSS re-warm, since wave `.org` files are hashed inputs) are mechanical after it.
Draft: none. The issue still describes exactly the remaining work. Optional comment if wanted:
> Re-checked 2026-11-12 against `e5fdd2fa8`: all seven wave `harmonize_food` tables still carry `Aggregate Label`, and nothing reads it (`Country._relabel_j` uses the country-level table only). Still open as written. -- Sue

### #1048 — GhanaLSS 1998-99 (GLSS4): the questionnaire prints two conflicting 8H unit-code lists (codes 10/11/17); verify which one the data use
Verdict: open/none, tier B (diagnose, then fix). No merged work since filing: no PR cross-references it, and nothing in `GhanaLSS/_/unit_labels.org` or `GhanaLSS/1998-99/` has changed since the #1042 branch commits of 2026-10-18 (`60f0d4e7b`, `7b20e8c31`). CONTENTS.org (Section 8H unit lists, about lines 2058-2072) still records both printed lists and "Not fixed here". Idiosyncrasy, quoted from it: "chicken and eggs (page 8.16) were coded on the list that has `Single/Whole 11`", and neither list prints `All`, which the data decode on 2,180 egg and 289 chicken rows. `unverified-data`: step 1 needs the `.dta` value labels on the 8H unit variable. Verify with `get_dataframe('GhanaLSS/1998-99/Data/<8H file>.dta')` value labels for the unit column, plus the median Expenditure/Quantity of codes 10/11/17 by item, in a private data root.
Draft: none; the body is current.
QUEUE: #1048 | GhanaLSS | read the 1998-99 8H unit-variable value labels; if they follow p.8.17 for meat/poultry/dairy items and p.8.15 elsewhere, decode 1998-99 unit codes per item-page instead of with one table | GhanaLSS/_/unit_labels.org (or a 1998-99 wave table), GhanaLSS/1998-99/_/ mapping or food_acquired script, CONTENTS.org | one country (GhanaLSS re-warm) | before/after rows, kg, kg_factor_sources, food_prices rows by code 10/11/17 × item; food_acquired rows and Expenditure unchanged

### #1072 — GhanaLSS nutrition ignores _/no_kg_factor.yml and weighs counted eggs at 0.50 kg; make the declaration a hashed input of nutrition
Verdict: open/none, tier B. Unchanged. `GhanaLSS/_/nutrition.py:180` is still `q = food_quantities_from_acquired(fa, units='kgs')`, with no `no_kg=`. `_BUILD_INPUT_SUFFIXES` (`country.py:1173`) still excludes `.yml`, so editing `no_kg_factor.yml` leaves `var/nutrition.parquet` stale. CLAUDE.md states the same ("That script calls the transform without `no_kg` (or `unit_kg`), so `nutrition` does not see the declaration"). No merged PR references #1072 after #1076, which filed it. Idiosyncrasy: `nutrition.py` already pre-filters `NON_PHYSICAL_UNITS` (`value`/`all`/`none`) before deriving, because the `u='Value'` ratio used to mint 0.49 kg per cedi (`nutrition.py:35-60`), so the no-kg routing goes beside that filter.
Draft: none; the body is current and has the fix.
QUEUE: #1072 | GhanaLSS | pass `no_kg=Country('GhanaLSS')._no_kg_factor_cells()` in `_quantities`; add a by-name, present-only `no_kg_factor.yml` hash part in `Country._table_cache_hash`, gated to the tables that read it (option A) | GhanaLSS/_/nutrition.py, lsms_library/country.py (_table_cache_hash) | one country (option A, measured 17/17 GhanaLSS tables; reject option B, corpus-wide) | egg kg before/after per wave (expected 1,077.85 / 515.75 / 31.97 kg); stash-probe that only GhanaLSS hashes move; a test that editing the .yml grades nutrition stale

### #1073 — DECISION: GhanaLSS egg units priced like one egg (All, Nut, Finger, Stick, Barrel, Bunch) get ~1 kg each -- extend the no-kg declaration to every egg unit?
Verdict: needs-decision/ask, tier C. Unchanged: `GhanaLSS/_/no_kg_factor.yml` declares Egg on `[Single, Piece, Ball, Pair, Dozen, Crate]` only, and its `evidence` still says "Container and non-count units ('All', 'Basket', 'Heap', ...) are not declared here". That is the line the issue asks to correct, whichever option is chosen. No ruling in isa-decisions.org. Idiosyncrasy: egg `Kilogram` rows price at the per-egg price (counts mislabelled as kg), so the `(t, Egg)` baseline the item rung divides by is itself broken (yml `reason`). Side note for #1454: the same yml header ("Labels are the ones food_kg_factors sees ... before _finalize_result") is one of the two passages CLAUDE.md calls stale.
C-item (for the human):
- Question: should every GhanaLSS egg unit be declared no-kg, or only `All` plus the five small units?
- Options: (i) `- item: Egg` with no `units` (every unit; `metric` still serves Kg/Pound/Gram/Liter/Milliliter, 537.28 kg), removing 24,899.95 item-rung kg and 169.17 u-pooled kg; (ii) declare `All`, `Nut`, `Finger`, `Stick`, `Barrel`, `Bunch` (-24,892.02 kg); (iii) rule that `All` is a measure (no change).
- Evidence: issue body (measured at the #1042 branch head); yml `reason` on the broken baseline. Read-time only, so no cache cost for `food_quantities`/`food_prices`. `nutrition` follows only after #1072.
- Recommendation: (i). The baseline is broken for the item as a whole, and (ii) leaves future unseen egg units exposed. Either way, fix the `evidence` line.

### #1082 — GhanaLSS 1987-88 / 1988-89 declare FOODCD twice in one categorical_mapping.org; the cascade and the read-time merge read different copies
Verdict: open/none, tier B (latent; filed as "Tier C (latent)", but the fix is specified and needs no design choice). Unchanged. `GhanaLSS/1987-88/_/categorical_mapping.org:6,74` and `GhanaLSS/1988-89/_/categorical_mapping.org:5,73` both still carry `#+name: FOODCD` twice. The level conflict check (`local_tools.py:2911-2917`) still catches a name across FILES of a level only. `df_from_orgfile` still warns and reads the first copy (`local_tools.py:3351`). Idiosyncrasy: GLSS1/2 record food by Section 12A (purchases) and 12B (home production), each with its own code list (Y12A 61 rows, Y12B 52 rows), so two FOODCD tables are genuine instrument facts that only need distinct names. Latent because nothing requests `FOODCD` by name and the table has no `Preferred Label` column.
Draft: none.
QUEUE: #1082 | GhanaLSS (+framework guard) | rename to FOODCD_12A / FOODCD_12B after a questionnaire check; make `all_dfs_from_orgfile` (and `df_from_orgfile`) raise on a within-file duplicate `#+name:` | GhanaLSS/1987-88/_/categorical_mapping.org, GhanaLSS/1988-89/_/categorical_mapping.org, lsms_library/local_tools.py, CONTENTS.org | one country for the rename (GhanaLSS .org is a build input); the reader change is unmeasured, so stash-probe it (the org readers may sit in a fingerprinted closure) | a test that a within-file duplicate raises; a corpus sweep confirming these are the only duplicates (the issue measured 145 levels)

### #1083 — GhanaLSS 1991-92 (GLSS3): the food recall window varies by stratum (urban 30 d, rural 14 d) -- stamp it per row so period= can convert the wave
Verdict: open/none, tier B. Unchanged. `GhanaLSS/_/recall.yml:40-47` still records 1991-92 as `within_wave_variation: stratum` with no `recall_days`/`n_asks`. `recall.py:715-730` still refuses such a wave (`PeriodUnavailableError`), and no per-row exposure column exists. The 2026-11-09 recall commits (`a9eae4428`, `5c24b6f9c`, `cb92ebe5f`) and #1671/#1672 do not add one. For the coordinator's §4c: the archived `refs/archive/2026-11-11/fix/851-recall-period` commit subjects describe the option-A ask-shortfall disclosure, not a per-row window, so that branch does not resolve this issue. Idiosyncrasy (CONTENTS.org "GLSS3 is stratified: 8 rural visits, 11 urban"): one shared form with eleven date boxes for both strata, so the window must come from the cover-page stratum, never from counting visits. The diary is urban-only.
Draft: none; the body is current and specifies the work.
QUEUE: #1083 | GhanaLSS 1991-92 | stamp RecallDays/Asks per row from GLSS3's own stratum (cover page read in the wave script); carry the group-constant column through the visit sum; teach `recall.to_period` to prefer a per-row exposure | GhanaLSS/1991-92/_/food_acquired.py (or the wave module), lsms_library/data_info.yml (optional column), lsms_library/recall.py, transformations derived-food sum | one country for the column (GhanaLSS 1991-92 wave and L2-country food_acquired rebuild once); recall.py/transformations read-path = none, but stash-probe it | `food_expenditures(period=...)` converts 1991-92; urban/rural exposure histogram on attrs['period']; 103,821 rows unchanged otherwise

### #1533 — GhanaLSS 2005-06: 413 Value-unit food rows sit 100x below their cell
Verdict: open/none, tier B (diagnose first). Filed 2026-11-05 from #1493's low-tail screen (#1544 `167d566d0` shipped the screen, which names the rows but does not change them). No later work references #1533. The cause is still unmeasured: `unverified-data`. Verify with `quantity_reports(country='GhanaLSS', table='food_acquired')` filtered to `direction == 'low'`, `t == '2005-06'`, `u == 'Value'`, then compare each household's other 2005-06 Value rows; a whole household at about 1/10,000 scale would point to a new-cedi entry. Idiosyncrasy: GLSS5 is in OLD cedi (GHC). CONTENTS.org contradicts itself on the ratio: line 2888 says "100:1 redenomination", line 3122 says "1 GHS = 10,000 GHC". 10,000:1 is the actual July 2007 rate, so line 2888 should be corrected in the same PR. The issue's "100x below" and median 3 against p10 2,000 (~667x) match neither ratio cleanly, so do not assume a redenomination slip.
Draft: none. The issue states the remaining work (measure the cause). Optional comment:
> Re-checked 2026-11-12 against `e5fdd2fa8`: nothing has landed since #1544 (`167d566d0`), which names these rows without changing them. While here, `GhanaLSS/_/CONTENTS.org` gives the 2007 redenomination as 100:1 at line 2888 and as 10,000:1 at line 3122. The second is correct; fix the first in whichever PR diagnoses this. -- Sue


## 8.B09b-ghana-roster-ag

# Batch B09b — GhanaLSS / GhanaSPS roster, agriculture and residuals

Worker: fork of the reconciliation agent (Claude Opus 5.5). Base `e5fdd2fa8`. No Ghana code or config edits merged since 2026-10-20 apart from `GhanaLSS/_/CONTENTS.org` (#1648 `9514be02e`), `GhanaLSS/_/sampling.yml` (#1677 `d6044e280`) and both countries' `recall.yml` (#1271 `ef93a9e09`, #1506 `df8faa75f`); none of the four post-base merges (#1690/#1694/#1695/#1693) touches GhanaLSS or GhanaSPS.

### #1100 — Ghana: parent-or-parent-in-law code is served Parent [1, 0, consanguineal] (2,962 people) -- the #698 'X or X-in-law' form, relabelled upstream
Verdict: open/none, tier B. Nothing merged. On `e5fdd2fa8` the relabel is unchanged: `GhanaLSS/_/categorical_mapping.org:173-174` (`Parent/Parentlaw`, `Parent/Parent-In-Law` -> Preferred `Parent`) and `GhanaSPS/_/categorical_mapping.org:1013-1015` (three GSPS spellings -> `Parent`); `lsms_library/categorical_mapping/kinship.yml:671-676` carries the #698 NOTE saying the fix needs a new Ghana Preferred Label. The tuple is not a pending decision: the 2026-10-18 affinity-only ruling makes an "X or X-in-law" code `[1, 0, null]`. Idiosyncrasy named: the code is ONE survey code in GLSS5-7 and in every GSPS wave, with the spelling drifting across GSPS W1/W2/W3 (`kinship.yml:659-668`). Contradiction to report: `GhanaLSS/_/CONTENTS.org:3405-3410` still says the code is "filed as [1, 0, consanguineal] like =Parent=" -- written 2026-09-07, before the ruling; the fixer should update it. The one sub-choice the body leaves is the `Comparable Label`: GLSS1/2's separate `Father/mother in-law` already pools into Comparable `Parent` (`categorical_mapping.org:175`), and `tests/test_finalize_order_797.py:150-163` pins Comparable `Parent` -> consanguineal, so keeping Comparable `Parent` for the new label is consistent with existing pooling.
No write needed: the body is current and exact.
QUEUE: #1100 | GhanaLSS + GhanaSPS | new Preferred Label `Parent/parent-in-law` for the conflating codes in both country `relationship` tables + `kinship.yml` key `[1, 0, null]`; keep Comparable `Parent`; correct GhanaLSS CONTENTS.org:3405-3410 | `GhanaLSS/_/categorical_mapping.org`, `GhanaSPS/_/categorical_mapping.org`, `categorical_mapping/kinship.yml`, both CONTENTS.org, a kinship test | two-country re-warm (country `.org` files are build inputs; `kinship.yml` is read-path, free) | private cold build of both `household_roster`s: the 2,962 rows move consanguineal -> NA Affinity, nothing else moves; Comparable view unchanged

### #1101 — GhanaLSS GLSS3/4 code 6 'Relative of head or head's spouse' is served [null, null, consanguineal] (2,977 people)
Verdict: open/none, tier B. Nothing merged. `GhanaLSS/1991-92/_/categorical_mapping.org:54,66` and `GhanaLSS/1998-99/_/categorical_mapping.org:55,67` still decode code 6 to `Other relative`, which `kinship.yml` files `[null, null, consanguineal]`. The target tuple is ruled (CLAUDE.md kinship paragraph, GH #698/#797: a "relative of head *or spouse*" is `[null, null, null]`). Idiosyncrasy named: GLSS3 and GLSS4 use one nine-code form (GhanaLSS CONTENTS.org "Trap 2", 3249-3330; GLSS3 was decoded through GLSS1's scheme until 2026-09-07), and the country `relationship` table is a harmonizing table whose Comparable view folds siblings into `Other relative`, so a new label needs its own Comparable row.
No write needed: the body is current.
QUEUE: #1101 | GhanaLSS | decode GLSS3/4 code 6 to a distinct label (e.g. `Relative of head or spouse`) in both wave tables, add the country `relationship` row (Comparable `Other relative`) and a `kinship.yml` key `[null, null, null]` | `GhanaLSS/1991-92/_/categorical_mapping.org`, `GhanaLSS/1998-99/_/categorical_mapping.org`, `GhanaLSS/_/categorical_mapping.org`, `categorical_mapping/kinship.yml`, CONTENTS.org Trap 2 | one country (GhanaLSS) re-warm; bundle with #1100 | private cold `household_roster`: 1,211 + 1,766 rows move consanguineal -> NA, row count unchanged, no unknown-label warning

### #1108 — GhanaLSS `Fertilizer` (1,091 rows with a quantity) has no product identity, so it gets no N share
Verdict: needs-decision/ask, tier C. Nothing merged since #1112 (`931934b12`) filed it. `transformations.py:6169-6195` (`_NITROGEN_CONTENT`) now has `'inorganic fertilizer': 0.20` from Uganda's class vocabulary and Niger's `npk 15-15-15` (#1445, `084f6e06a`), but no key matching GhanaLSS's bare `Fertilizer`, so `nitrogen_kg(GhanaLSS)` stays empty. Idiosyncrasy named: GLSS1/2 Part D asks only "fertilizer", a CATEGORY that the wave docstrings say "must not be pooled" with product labels (`GhanaLSS/1987-88/_/plot_inputs.py:45-50`, CONTENTS.org:5195-5199); but Part D has a SEPARATE `Manure` block (`plot_inputs.py:91`, `Y09D1C.DAT`), which is evidence that `Fertilizer` means non-manure (most likely chemical) fertilizer.
C item (draft for the human):
- **Question.** GhanaLSS GLSS1/2 `plot_inputs` records only `Fertilizer` (1,091 rows with a quantity). Give it a nominal nitrogen share, or refuse it on the record?
- **Options.** (A) A documented refusal like the organic one: it stays unmatched, is named in `NutrientCoverageWarning`, and `nitrogen_kg(GhanaLSS)` stays empty. (B) Serve it at the existing `inorganic fertilizer` class share, 0.20, through a Ghana alias, with a citation for applying a class share to an unnamed product. (C) Leave it as it is, an unexplained gap.
- **Evidence.** The questionnaire separates manure (Q `MANU*`, `Y09D1C`) from fertilizer (`FERT*`, `Y09D1B`), so (B)'s "inorganic" reading has some instrument support. No product, blend or N content is recorded. Uganda's 0.20 is itself a nominal class value. The quantity unit (`FERTQNTU`) has not been checked for kg convertibility (unverified-data: read `Y09D1B.DAT` through `get_dataframe` and tabulate `FERTQNTU`).
- **Recommendation.** (A), with the manure/fertilizer split recorded as the reason (B) remains possible. A 1987-89 Ghanaian product mix cannot be inferred from a 2010s Ugandan class value. If the maintainer prefers (B), first establish that the unit is kg-convertible.
-- Sue

### #1121 — plot_labor.source: decide GhanaSPS/GhanaLSS taxonomy and containment
Verdict: needs-decision/none, tier C. The body was narrowed 2026-10-25 after PR #1186 (`6213e7935`, Mali casing only). No merged work since touches Ghana `plot_labor.source`; none of the four post-base merges touches Ghana. Confirmed current. Idiosyncrasy named: GhanaSPS's source vocabulary DRIFTS within the country. 2009-10's `casual` + `permanent` are the later waves' `hired`, and 2009-10's `family` is broader, absorbing own and exchanged labour because that instrument has no "yourself" question (GhanaSPS CONTENTS.org:2052-2060). GhanaLSS GLSS1/2's `exchange` is deliberately NOT equated with GhanaSPS `communal` (nnoboa unverified; GhanaLSS CONTENTS.org:5343-5347).
C item (draft for the human):
- **Question.** Should `plot_labor.source` get a cross-country taxonomy, or explicit containment semantics, for GhanaSPS's seven values and GhanaLSS's `exchange`/`hired`?
- **Options.** (A) Keep the native values and publish a containment table, like `harmonize_stage` for `stage`. It would record `casual`+`permanent` ⊆ `hired` and 2009-10 `family` ⊇ `self`+`family`+part of `communal`, and serve no fold. (B) Add a coarse canonical `source` column (household / hired / exchange-communal / other) beside the native one. (C) No change; document only.
- **Evidence.** GhanaSPS CONTENTS.org:2046-2060 already argues "neither vocabulary can be dropped without inventing data", and stage containment is tabulated that way. Whether nnoboa = `communal` = `exchange` is unchecked (GhanaLSS CONTENTS.org:5343).
- **Recommendation.** (A): it matches the `stage` precedent and invents nothing. Under (B), an `exchange` = `communal` equivalence would first need the instrument check.
-- Sue

### #1122 — Ghana: three stale registration comments deferred to country cache rebuilds
Verdict: open/none, tier A (comment-only residual). Narrowed 2026-11-09 after #1648 (`9514be02e`); nothing merged since touches the three files. All three stale comments are present at `e5fdd2fa8`: `GhanaLSS/1987-88/_/plot_labor.py:14`, `GhanaLSS/1988-89/_/plot_labor.py:14` ("``plot_labor`` has no canonical ``index_info``"), and `GhanaSPS/_/data_scheme.yml:609-610` ("absent from `index_info`"). All three are false since `lsms_library/data_info.yml:157-170` registers `plot_labor`. One-line confirmation; no write needed. They are hashed build inputs, so the only sensible carrier is a re-warm that is happening anyway: the GhanaLSS one required by #1101/#1141/#1145/#1142 and the GhanaSPS one required by #1100.

### #1131 — Re-check the GhanaLSS `food_security` 1987-88 and 1988-89 `unsure` verdicts: the GLSS1 questionnaire scans are readable as images
Verdict: open/none, tier B (as filed; a coverage-adjudication document task, not code). Nothing merged. `.coder/coverage/absent_verdicts.csv:2-3` still holds both `unsure` rows (`C4-notrun`). The file was last touched by `c0f15b640` (Mali, 2026-10-30). Idiosyncrasy named: the 1988-89 questionnaire PDF is GLSS1's scan republished, with all 78 page images identical and the cover one filled-in household (GhanaLSS CONTENTS.org:2502, "Two facts"). GLSS2's own form is not shipped, so 1988-89's C4 can only run through the joint BID's section list. No write needed. Tractable as a document task, by an agent that can read the page images (pypdf extraction, as done for the #781 `food_prices` verdicts). It is not a code fix, so it is not in the code queue.

### #1138 — #1008 residual b: GhanaLSS GLSS1/2 `labor` keeps 20 null-`v` rows for ids with no cover, roster or food record
Verdict: open/none, tier B. Nothing has merged since #1151 (`d63895606`) filed it. On `e5fdd2fa8` the exclusion is deliberate and documented: `GhanaLSS/1988-89/_/mapping.py:172-178` and `GhanaLSS/1987-88/_/mapping.py:178-183` do not recover one-module households, and CONTENTS.org:5704-5710 records "=labor= still serves 1 (1987-88) and 19 (1988-89) null-=v= rows ... Open; GH #1008 residual". Idiosyncrasy named: GLSS1/2 ids are `HID = CLUST*100 + NH`, and the cover page `Y00A` has defects in both directions. It omits interviewed households (four now recovered) and carries data-less ones (#1139). A household that appears in one module only cannot be told from a mis-keyed id. No write needed.
QUEUE: #1138 | GhanaLSS | diagnose cluster 2198 (15 HIDs, `Y05B1.DAT` only) and HID 112516: compare NH sets with clusters whose households lack `Y05B1` records and check PID overlap with `Y01A`. Then either re-key at source in the wave `mapping.py`, or drop with a logged count | `GhanaLSS/1987-88/_/mapping.py`, `GhanaLSS/1988-89/_/mapping.py` (or the labor wave script), CONTENTS.org:5704-5710 | one country (GhanaLSS; a wave `mapping.py` is in every table's input hash) | read-only diagnosis first (private root, `get_dataframe` on `Y05B1/Y00A/Y01A.DAT`), then a private cold `labor` build: 20 null-`v` rows go to 0 (re-keyed or counted)

### #1139 — #1008 residual c: GhanaLSS GLSS1/2 `sample` serves 13 data-less cover-page households; 114518 may duplicate the recovered 114519
Verdict: open/none, tier B. Nothing has merged since #1151 (`d63895606`). CONTENTS.org:5715-5736 records all 13 data-less `Y00A` rows as "not resolved here (GH #1139)". Idiosyncrasy named: CONTENTS.org adds a lead the issue body lacks. 1988-89's 260605 is the one data-less `Y00A` row and also the one household `Y00C` lacks, while `Y00C`/`Y00B` carry the recovered 220629 that `Y00A` lacks: a one-for-one swap. 260605's cover row is also blank on `RELIGION`/`LANGHD`/`LANGR`/`INTRPRET` and is dated 23/3/89. So 260605 may be 220629's own cover record mis-keyed, which would be a second possible double-count, with `v` 2606 vs 2206. The body's "decide the same for the other 12" already covers it, so no write is needed. The fixer should read CONTENTS.org:5726-5736 first. Impact is on household counts only: the weight is a synthesised 1.0 and no food rows are involved.
QUEUE: #1139 | GhanaLSS | compare the cover fields of 114518 with 114519, and of 260605 with 220629 (`HHSAME`, `RELIGION`, `LANGHD`, interviewer, dates). Re-key a proven mis-keyed cover, or drop the data-less phantoms from `sample` with a count | `GhanaLSS/1987-88/_/mapping.py`, `GhanaLSS/1988-89/_/mapping.py`, `data_info.yml` `sample` | one country (GhanaLSS) | private read-only diagnosis, then a cold `sample` build: household count per wave and per cluster before/after; no `v` or food change. Do it in the same session as #1138

### #1141 — #1008 residual e: GhanaLSS `inputs_produced_farmgate` returns ungated 8H rows
Verdict: open/none, tier B (filed as C). Nothing has merged since #1151 (`d63895606`). On `e5fdd2fa8`, `GhanaLSS/_/ghanalss.py:736-770` still returns every raw 8H record, with no screener. The wave scripts gate at `1998-99/_/food_acquired.py:110` (`s8hq1 == 1`), `2005-06/_/food_acquired.py:133` and `2012-13/_/food_acquired.py:94` (`== 'yes'`), and 2016-17 at `s8hq1 != 2` (#1140 ruling). Idiosyncrasy named: in every gated wave `s8hq1`/`s8hq2` are screeners, the visit columns start at `q3`, and `s8hq3` is the 2nd visit (CONTENTS.org:1462). It is not really a design question: #1140 settled the gate rule, and #1145 item 4 already names this exact fix and bundles its cost.
Optional duplicate-close draft (coordinator's call; the alternative is to leave both open with #1141 depending on #1145):
> Closing as a duplicate of #1145, whose item 4 is this fix ("Also apply each wave's screener in `ghanalss.inputs_produced_farmgate` (#1141), so that `derivation_inputs` joins to the served rows") and which already bundles the one GhanaLSS re-warm. The gate to apply per wave is the one #1145 decides: `!= 'no'` wherever its measurement finds blank-screener rows with answers, as #1140 ruled for 2016-17. On development `e5fdd2fa8`, `ghanalss.py:736-770` is still ungated. -- Sue
QUEUE: (with #1145) #1141 | GhanaLSS | in `inputs_produced_farmgate`, apply the same per-wave screener the wave script applies, so the registry's inputs equal the stamped rows | `GhanaLSS/_/ghanalss.py` | one country (GhanaLSS, every table: `ghanalss.py` is the country module) | `derivation_inputs` for `8h-farmgate` joins 1:1 to the stamped produced rows in each of the four waves (2016-17: 65,609 of 934,584 raw rows pass, per the issue)

### #1142 — GhanaLSS 1991-92 `sample` omits 29 interviewed households, so their 142 roster rows serve a NaN `v`
Verdict: open/ask, tier B with one embedded sub-decision. Nothing has merged since #1151 (`d63895606`) filed it. `GhanaLSS/1991-92/_/data_info.yml` still declares `sample:` twice, at line 13 and line 71, both on `POV_GH.DTA`. `load_yaml` is a `yaml.SafeLoader` subclass (`yaml_utils.py:12`, used at `country.py:1184`), so the later block silently wins. Idiosyncrasy named: GLSS3 is self-weighting (constant `weight` 1.0, GSS-documented) and `clust` IS the EA. `POV_GH.DTA` holds 4,523 households, while `S0A`/`S1` hold 4,552, the stated response count (99.72%). **Trap for the fixer, not in the issue body:** the dead first block (line 13, `cluster_features`-shaped) carries the CURRENT explanation of the varying take (EA vs workload, Report Appendix 1, Poverty Profile Appendix 4). The live block (line 71) carries the superseded "Unresolved from anything shipped" paragraph. Delete the first block's YAML body, but move its comment into the surviving block in place of the stale paragraph.
C sub-item (draft for the human):
- **Question.** When the 29 households are recovered from `S0A.DTA` (which carries `clust`, so `v` is direct), how are `Rural` and `strata` filled? `POV_GH.DTA`'s `loc2`/`region` are absent for them.
- **Options.** (A) Broadcast `loc2`/`region` from the household's cluster: both are EA-level design attributes in a self-weighting design, so this is a derivation to register. (B) Read them from `S0A` if it carries a region/locality code (unverified-data: list `S0A.DTA` columns in a private read). (C) Recover `v` only and leave `Rural`/`strata` NA, which Site B will report.
- **Recommendation.** First check (B). Failing that, (A) registered as a derivation: the cluster is the EA, so the values are cluster-invariant by design, and the audit should confirm that on the 4,523 present households first.
-- Sue
QUEUE: #1142 | GhanaLSS | recover the 29 `S0A` households into 1991-92 `sample` with `v=clust` (and `Rural`/`strata` per the sub-decision); delete the duplicate `sample:` block, keeping its comment | `GhanaLSS/1991-92/_/data_info.yml` (+ possibly `1991-92/_/mapping.py`), CONTENTS.org | one country (GhanaLSS; a wave `data_info.yml` moves every GhanaLSS L2-country hash) | private cold build: `sample` 1991-92 4,523 -> 4,552 households; `household_roster` NaN-`v` rows 142 -> 0; Site I report clears

### #1145 — GhanaLSS 1998-99 / 2005-06 / 2012-13 8H screener: measure blank-screener rows with real answers, and align to 'drop only No' (#1140 ruling)
Verdict: open/none, tier B (possible silent row loss; size unmeasured). Nothing has merged since the issue was filed (2026-10-20, after #1151 `d63895606`). `e5fdd2fa8` still keeps only "yes": `1998-99/_/food_acquired.py:110` (`s8hq1 == 1`), `2005-06/_/food_acquired.py:133` and `2012-13/_/food_acquired.py:94` (`== 'yes'`). GhanaLSS CONTENTS.org:5560-5572 records the 2016-17 `!= 2` rule and says "The sibling waves still drop their blank screeners; measuring them is GH #1145". Idiosyncrasy named: `s8hq1` is asked per (household, item) row. In 2016-17 a `== 1` gate dropped 39 blank-screener rows that carried real consumption (red team r2), which is what the ruling corrected. No write needed: the body is current. Its item 4 is #1141.
QUEUE: #1145 (+#1141) | GhanaLSS | measure blank-screener rows with answers per wave; where nonzero, change the gate to `!= 'no'` / `!= 2`; apply the same gate in `ghanalss.inputs_produced_farmgate` | `GhanaLSS/1998-99/_/food_acquired.py`, `2005-06/_/food_acquired.py`, `2012-13/_/food_acquired.py`, `GhanaLSS/_/ghanalss.py`, CONTENTS.org | one country (GhanaLSS full re-warm; `ghanalss.py` is the country module) | per wave: rows/households/ΣQ/ΣE of blank-screener rows with answers (read-only, private root); then private cold `food_acquired`, `food_quantities(units='kgs')`, `nutrition` before/after; `derivation_inputs` joins the stamped rows

## Batch B09b bundle note
#1100 (GhanaLSS + GhanaSPS), #1101, #1122, #1138, #1139, #1141, #1142 and #1145 each cost a GhanaLSS re-warm, and #1100 and #1122 a GhanaSPS one too. One GhanaLSS+GhanaSPS fix PR, or a short series landed together before a single re-warm, would pay that cost once. #1108 and #1121 are Tier C questions for the human; #1131 is a document-reading task.


## 8.B10a-nonisa-labels

# Batch B10a — non-ISA labels (worker fork, Opus 5.5)

### #438 — Non-ISA item stack: finish source-backed label foundations and module-specific extensions
Verdict: needs-decision/narrow, tier C. Narrowed 2026-10-25. Nothing merged since touches the remaining design note or Guinea-Bissau crop extraction (`git log --since=2026-10-25 e5fdd2fa8 -- lsms_library/countries/Guinea-Bissau/` shows only strata, GPS-area docs, recall and community-price work). One body line is stale: "Code21 authority remains #1187". #1187 closed 2026-11-03 via `6f7c23d79` (ancestor of `e5fdd2fa8`): Burkina 2018-19 `s16bq01` slot 21 now maps to `Arachide` in `harmonize_seed_crop`, recorded as DATA-VALIDATED, not as a source binding. Guinea-Bissau's CONTENTS.org says nothing about crop extraction; its `data_scheme.yml:149,175` declares crop_production/plot_inputs with `plot = "{field_no}_{parcel_no}"`.

New title: Non-ISA item stack: finish source-backed label foundations and module-specific extensions (unchanged)

New body (replace only the "Completed west-country scope" paragraph's second sentence; everything else unchanged):
> Burkina's native slot identity (#1189), gift/purchase routing (#1219), maize/cowpea labels (#1203), carried sold unit (#1270) and the 2018-19 code-21 groundnut-seed mapping (#1187, `6f7c23d79`, data-validated rather than source-bound) are landed. EHCVM community-price wiring is separately #1226. Do not rerun a blanket Burkina structural rollout.

Add to "Resolved so far": `- #1187 closed 2026-11-03 by 6f7c23d79 (Burkina 2018-19 input code 21 -> Arachide seed; not this issue's PR, but it removes a dependency).`
Comment to post: "Refreshed: #1187 (Burkina code 21) closed 2026-11-03 by 6f7c23d79, so it is no longer a pending authority here. The design note is still owed under the 2026-10-23 ruling; nothing merged since 2026-10-25 touches Guinea-Bissau crop extraction or the remaining harmonize_input/seed-crop foundations. -- Sue"

C item (for the human): Question: when should the #438 design note be written, given the defect queue has not drained? Options: (a) keep it parked until the Tier A/B queue empties (the 2026-10-23 ruling); (b) write the design note now and build later. Evidence: nothing landed since 10-25; EPAR `crop_category1/2` crosswalk still needs the #833 screen first. Recommendation: (a). The ruling stands and no new evidence changes it.

### #439 — Exploit LSMS+ individual/intra-household (male vs female) disaggregation — new feature direction
Verdict: needs-decision/none, tier C. Nothing merged. On `e5fdd2fa8`, `rg 'individual_assets|financial_inclusion|decision_making'` finds no table, and `Cambodia/_/data_scheme.yml` still declares only sample/cluster_features/household_roster/individual_education/plot_features/housing/interview_date/food_acquired. The 2026-10-23 ruling (design note first, after the defect queue) governs. No write needed. The body still describes the remaining work, and the EPAR comment (2026-09-09) records the `season` grain gap that #862 now owns.
C item: Question: same timing question as #438. Should the design note wait for the defect queue? Options: wait (the ruling), or write the design note now and fold #862 (`plot_cultivation`) into it. Evidence: no change since 10-23; #862 is the reserved per-season home (`lsms_library/data_info.yml:586`). Recommendation: keep the ruling. When the note is written, write it jointly with #862 so the `(t,i,plot,season,pid,role)` grain is settled once.

### #552 — WB microdata coverage gaps (June 2026 audit): EHCVM-2 ×4 + classic-LSMS, tiered by obtainability — post-0.8.0
Verdict: partial/split, tier D. Tier 1 acquisition is done. PR #1506 (`df8faa75f`, ancestor of `e5fdd2fa8`) holds and registers 2021-22 for Benin (`8099c0051`), CotedIvoire (`96ad1a0d8`), Guinea-Bissau (`fe35f4a1c`) and Togo (`783bc045e`). None is wired: each `2021-22/_/data_info.yml` declares only `Country`/`Wave`, and `Benin/_/CONTENTS.org:688` says "held and registered, not wired". Wiring is a different kind of work, so split it off. The rest stays acquisition:
- Jamaica, Ecuador and Vietnam need a person.
- `.coder/coverage/blocked_sources.csv` was never committed. `lsms_library/coverage_matrix.py:422` reads that path.
- The ISA next-round audit.

Kyrgyz has already moved to #975, where the data are held and only config is missing. "No new LSMS-ISA panel rounds are published yet" is stale: Malawi IHS-VI (8507) and IHPS-V (8520) are catalogued and tracked by #1275. Idiosyncrasy (`Benin/_/CONTENTS.org:694-700`): 2021-22 is "a panel by cluster" (all 670 grappes recur; 6,994 of 8,032 `(grappe, menage)` keys recur). `s00_me` carries `PanelHH`. A recurring key is not shown to be the same household (Burkina renumbers, #1245).

**Split — new issue.** Title: Wire EHCVM-2 2021-22 for Benin, CotedIvoire, Guinea-Bissau and Togo (held since #1506). Tier B.
Body:
> Split from #552. The four 2021-22 EHCVM-2 waves are held and registered (PR #1506, `df8faa75f`: Benin 6272 `8099c0051`, CotedIvoire 6273 `96ad1a0d8`, Guinea-Bissau 6274 `fe35f4a1c`, Togo 6279 `783bc045e`). No table is served: each `2021-22/_/data_info.yml` declares only `Country`/`Wave`.
>
> Wire them table by table, using Niger, Mali and Senegal 2021-22 as the template (`sample`, `household_roster`, `cluster_features`, `food_acquired`, `shocks`, … as each country's 2018-19 wave declares). Per country, read `_/CONTENTS.org` first. Benin's 2026-11-04 entry records that 2021-22 is a panel by cluster, that `s00_me` carries `PanelHH`, and that a recurring `(grappe, menage)` is not shown to be the same household (Burkina renumbers its households, #1245). It also records that Benin's 2018-19 shock-code table must not be reused unread, because Niger's and Senegal's 2021-22 code 3 means something else.
>
> Panel links (`updated_ids`) are a production panel-ID change and need their own ruling. Weights follow the EHCVM convention. Downstream: #1598 (own-country NSU food weights for Togo, Benin, CotedIvoire 2021-22) and #1556 (shocks purchases/stock ruling in unfinished wave integrations) wait on this.
>
> Cache: a new wave's `data_info.yml` and wave module move only that country's hashes (whole-country re-warm). -- Sue

**Narrow #552.** New title: WB microdata acquisition gaps: Jamaica, Ecuador, Vietnam (external) and the late-2026 ISA re-audit.
New body:
> **Resolved so far:** PR #1506 (`df8faa75f`) holds and registers EHCVM-2 2021-22 for Benin (6272), CotedIvoire (6273), Guinea-Bissau (6274) and Togo (6279). Wiring them is split to #NEW. Kyrgyz Republic is not an acquisition gap: its four waves are held and only config is missing (#975). The repository-blind-spot finding (South Africa GHS, Armenia ILCS) is carried by #597.
>
> **Remaining (Tier D, needs a person):**
> - **Jamaica**: JSLC, annual ~1988–2000+. WB 2367 etc. redirect to STATIN/PIOJ; access needs Jamaican-government permission.
> - **Ecuador**: ECV 1995 (2692) and ECV 1998 (2693) redirect to INEC.
> - **Vietnam**: VLSS 1992-93/1997-98 and VHLSS 2002/2004/2006 (2694). GSO property, fees $200–$2000.
> - **Record them.** Add these three, with `probe` URL and `blocker`, to `.coder/coverage/blocked_sources.csv`. `lsms_library/coverage_matrix.py:422` reads that file, but it was never committed (Nepal and Albania 1996 belong there too, per the 2026-07-12 comment).
> - **Re-run the ISA next-round audit** (`data_access.discover_waves(<country>)` for every ISA/EHCVM country). The June note that no new rounds were published is stale: Malawi IHS-VI (8507) and IHPS-V 2024 (8520) are now catalogued (#1275). Still unchecked: Ethiopia ESS6, Tanzania NPS 2026, Uganda 2021-22/2023-24 and EHCVM wave 3.
>
> The obtainability tiering (Tier 1 WB-direct / Tier 2 NSO-redirect / Tier 3 licensed) and the credential notes in the original audit still apply. -- Sue

Parent comment: "Split the wiring of the four held EHCVM-2 waves into #NEW (Tier B). This issue now tracks only the external acquisitions, the missing `blocked_sources.csv` and the ISA re-audit. -- Sue"
QUEUE: (the split issue) #NEW | Benin/CotedIvoire/Guinea-Bissau/Togo | wire `sample`/`household_roster`/`cluster_features` first from the Niger/Senegal 2021-22 template, then food | `countries/{C}/2021-22/_/data_info.yml`, `{C}/2021-22/_/mapping.py`, `{C}/_/data_scheme.yml` | one country each (whole-country re-warm) | cold build of each new table in a private root, `scan.py` restricted to the four countries, CONTENTS panel note

### #698 — Kinship: reconcile upstream follow-ups after the affinity-only ruling landed
Verdict: open/none, tier B. Narrowed 2026-11-10, and nothing has merged since: `git log 16c3f4d9c..e5fdd2fa8 -- lsms_library/categorical_mapping/kinship.yml lsms_library/countries/Ghana* lsms_library/countries/Kosovo tests/test_kinship*` is empty. No write needed. Note for the coordinator: every residual named in the body has its own issue (#1100, #1101 Ghana relabels; #1102 DECISION; #1103 Kosovo codes). The umbrella can close as "split" once #1100/#1101 are adjudicated, or now if Sue prefers the split form. Closing it now loses nothing.

### #1084 — Recall research: finish remaining non-ISA questionnaire evidence
Verdict: open/none, tier C (bounded document reading). Narrowed 2026-11-10 against `4381d701f`. #1671 (`1c1ef12da4`) and #1672 (`ba03bf85a1`) are both ancestors of that base, so they were already counted. Nothing has merged since: `git log 16c3f4d9c..e5fdd2fa8 -- .coder/coverage/food_recall.csv '*/recall.yml'` is empty. `.coder/coverage/food_recall.csv` still says `basis=not-recorded` for the nine cells the 2026-11-04 comment names: Cambodia 2019-20, EthiopiaRHS 1989/1994a/1994b/1995/1997, and GhanaSPS 2009-10/2013-14/2017-18. Every one of them has a questionnaire held in `Documentation/` as a DVC sidecar, so what remains is reading, not acquisition. Useful to state in the issue next time it is touched. Optional comment: "Each of the nine remaining cells has a household questionnaire held (Cambodia `cambodia_lsms_plus_questionnaire_english_public.pdf`; EthiopiaRHS `ERHS_hhquestionnaire_<wave>.pdf`; GhanaSPS Part A/B 2009-10, ISSER_Yale Part A/B WAVE II 2013-14, `HH_Questionnaire_23_10.docx` 2017-18). The remaining work is reading them, not acquiring them. -- Sue"
Archived branch `fix/851-recall-period`: it does not bear on #1084. These cells need questionnaire evidence, not `period=` code; the coordinator owns the branch question.

### #1102 — DECISION: kinship conflations of the #698 shape outside its wording (Timor-Leste Adopted/stepchild, Cambodia Adopted/Foster, Guyana Servant/Other, bare 'Other')
Verdict: needs-decision/ask, tier C. Unchanged on `e5fdd2fa8`. The four entries in `lsms_library/categorical_mapping/kinship.yml`:
- `:441` `Adopted/Stepchild: [-1, 0, step]`
- `:450` `"Adopted Child/Foster Child": [-1, 0, foster]`
- `:463` `"Servant/Employee/Other Non-Relative": [0, 0, servant]`
- `:176` `Other: [0, 0, unrelated]`

The precedent `"Adopted/Foster/Stepchild": [-1, 0, null]` is at `:677`.
C item:
- Question: should the #698 null-component rule extend to conflations outside its wording?
- Options, per label:
  - (a) Adopted/stepchild (Timor-Leste, 246 people) and Adopted/Foster (Cambodia, 9) → `[-1, 0, null]`, matching Ghana's `:677`; or keep `step`/`foster`.
  - (b) Guyana "Servant/Employee/Other non-relative" (81) → keep `servant`, or `[0, 0, null]`.
  - (c) bare `Other` (79, six country-waves) → `[null, null, null]`, matching the 2026-09-07 rule for undetermined labels; or keep `[0, 0, unrelated]`.
- Evidence: the counts were verified by red-team r1 (body). Bare `Other` is an unevidenced guess: it is the only one of the four with no instrument wording behind it.
- Cost: read-time only. `_expand_kinship` runs in `_finalize_result`, so no build fingerprint moves.
- Recommendation, for the human to decide: (c) is the closest to the existing null rule. (a) is consistent with `:677`. (b) is a genuine judgement, best left to the questionnaire's wording.

### #1103 — Kosovo 2000 kinship: truncated labels 'Other re' (71) and 'Niece/Ne' (455) need the code list
Verdict: open/narrow, tier B. No merged change. The code list the issue asks for is already in the repo. `lsms_library/countries/Kosovo/2000/_/data_info.yml:31-46` maps `s1a_q03` codes 1–15 to full labels:
- 5 `Niece/Nephew`
- 8 `Son/Daughter-in-law`
- 9 `Brother/Sister-in-law`
- 11 `Father/Mother-in-law`
- 12 `Other relative`
- 15 `Other`

It was added in `ad0da2c85` (Nov 2025). The held questionnaire `Kosovo/2000/Documentation/hheng.pdf.dvc` can confirm it.

Yet `lsms_library/categorical_mapping/kinship.yml:424-433` ("Kosovo variants (truncated .dta labels)") still keys `Niece/Ne`, `Other Re`, `Son/Daug`, `Father/M`, `Brother/`. That suggests the numeric-keyed mapping never fires (the CLAUDE.md gotcha: Stata value labels arrive as string categoricals, so integer YAML keys do not match). If so, the defect is larger than the title. Each in-law label truncates to the same 8 characters as its blood twin:
- "Son/Daughter-in-law"[:8] = "Son/Daughter"[:8] = `Son/Daug`
- likewise `Father/M` and `Brother/`

Codes 8, 9 and 11 would then be served consanguineal, which is why "Kosovo has no in-law code". This is unverified-data. To verify, read the Kosovo 2000 roster `.dta` through `get_dataframe` and cross-tab `s1a_q03` codes against labels. Kosovo's `CONTENTS.org` has no kinship entry. Add one.

New title: Kosovo 2000 kinship: truncated .dta labels merge in-law codes into blood labels; serve s1a_q03 by its code list
New body:
> **Resolved so far:** nothing merged. The code list this issue asked for is already in `countries/Kosovo/2000/_/data_info.yml:31-46` (codes 1–15, from `ad0da2c85`), and the English household questionnaire is held (`Kosovo/2000/Documentation/hheng.pdf.dvc`).
>
> **Finding (residual of #698, red-team r1 counts):** `Other re` (71 people) is filed `[0, 0, unrelated]`, and `Niece/Ne` (455) is filed `[-1, 2, consanguineal]` (every other niece/nephew key is Distance 1). Both come from the "Kosovo variants (truncated .dta labels)" block in `categorical_mapping/kinship.yml:424-433`.
>
> **Why it is larger than two labels (unverified-data):** the served labels are 8-character Stata value labels, so the integer-keyed mapping in `data_info.yml` apparently never fires. Truncation then merges three in-law codes into their blood twins: 8 `Son/Daughter-in-law` → `Son/Daug`, 9 `Brother/Sister-in-law` → `Brother/`, 11 `Father/Mother-in-law` → `Father/M`. All three are filed consanguineal. "Kosovo has no in-law code" is this artefact, not the instrument.
>
> **Remaining:**
> 1. Confirm by reading the roster `.dta` `s1a_q03` codes against its value labels.
> 2. Serve `Relationship` from the code, either with string keys matching the truncated labels where they are unambiguous, or by reading codes, so that codes 5/8/9/11/12 reach the full labels, which `kinship.yml` already holds (`Niece/Nephew`, `Son/Daughter-In-Law`, `Brother/Sister-In-Law`, `Father/Mother-In-Law`, `Other Relative`).
> 3. Delete the dead truncated keys.
> 4. Check the code list against `hheng.pdf`.
> 5. Record the idiosyncrasy in `Kosovo/_/CONTENTS.org`.
>
> Cache: a wave `data_info.yml` edit is a Kosovo-only re-warm. A `kinship.yml` edit is read-time. -- Sue

QUEUE: #1103 | Kosovo | make `s1a_q03` reach its code list (string keys or code read) so in-laws are served affinal and `Niece/Ne`/`Other Re` disappear | `countries/Kosovo/2000/_/data_info.yml`, `categorical_mapping/kinship.yml`, `Kosovo/_/CONTENTS.org`, a test in `tests/test_kinship*` | one country (Kosovo re-warm; kinship.yml is read-time) | cross-tab codes vs served Relationship, check the Affinity counts before and after, confirm against `hheng.pdf`

### #804 — Tajikistan 1999 household_roster: code 14 served as 'lodger' but the questionnaire prints 14 = Other (13 = Lodger never occurs); 13 people, needs the 1999 BID codebook
Verdict: needs-acquisition/none, tier D (one document). Nothing merged. `lsms_library/countries/Tajikistan/1999/_/data_info.yml:42` still maps `14.0: lodger`. `Tajikistan/_/CONTENTS.org:446,518-519` records the gate. `1999/Documentation/` holds `HH012007-00.pdf`, `com01.pdf` and `fem01.pdf`, but not the BID `tajbid.pdf` (WB download 11687). The 2026-09-18 comment says the BID downloads with the configured key through the plain catalog URL, not through `get_data_file`. Idiosyncrasy: `SSEC1.DTA` `s1q3` has codes and no value labels, and the instrument splits parent-in-law into 6 (wife-side) and 7 (husband-side), the list whose copy caused #800. No write needed; the body is current. Once the BID is archived and read, the fix is a one-line, Tajikistan-only `data_info.yml` edit (14 → Other, or a documented recode).

### #737 — DECISION: EthiopiaRHS TLU -- relabel the *lvs5 files to t='1989' and apply the library's tlu() (A), or serve IFPRI's scalar under TLU with a convention marker (B)
Verdict: needs-decision/ask, tier C. Unruled: `isa-decisions.org` has no TLU entry.
- **Served column unchanged.** `EthiopiaRHS/_/data_scheme.yml:70` still reads `TLU: float # Livestock Standard Units (tropical livestock units)`.
- **No mule/camel factor yet.** `_TLU_FACTORS` (`lsms_library/transformations.py:6628-6639`) still has none; the body's `:4849-4860` reference is stale.
- **The "silently zero" sub-point is now disclosed.** `952eb819d` (2026-11-07, ancestor of `e5fdd2fa8`, `tests/test_gh1485_tlu_coverage.py`) added `TLUCoverageWarning`, so positive heads with no factor warn rather than vanishing.

Idiosyncrasy (EthiopiaRHS `CONTENTS.org`, via #738): the `1999/*lvs5.tab` files are the 1989 IFPRI round, misfiled and served at `t='1999'`. `lsu80` is an exact identity (R²=1 per village): cattle 0.83, sheep/goats 0.20, horses/mules/camels 1.00.

C item:
- **Question:** A (relabel the `*lvs5` files to `t='1989'`, build the `(t,i,animal)` roster per #738, apply `tlu()`) or B (keep IFPRI's scalar under `TLU`, with a convention marker)?
- **Options and costs:** A gives EthiopiaRHS the canonical roster shape and the library's 0.70 scale, and it changes which wave the rows sit in. Rows move from 1999 to 1989, and the real R5 numbers (`livval5`/`lsu5`) still have no roster. B is cheaper but leaves a TLU about 19% (cattle) to 2× (small ruminants) off the scale `Feature('livestock')` pools it with.
- **Evidence:** the body's tables. EPAR's 27 do-files use cattle 0.5 and sheep/goats 0.1, unsourced (2026-09-09 comment).
- **Note:** neither option settles 2004 (`lsu6`) or 2009 (`tlu7`), which have only aggregates.
- **Recommendation:** A for 1989, plus a convention marker on 1994–2009, because the t-relabel is a correctness fix in its own right.

Optional refresh of the narrowed body's "Remaining" line: "transformations.py:6628-6639 has no mule/camel factor; since 952eb819d such heads raise `TLUCoverageWarning` instead of contributing silently zero."

### #1044 — EthiopiaRHS food units: float-stringified codes 31.0/83.0/95.0 leak inferred kg after 1989, and 1989 serves raw unit codes
Verdict: partial/narrow, tier B. PR #1067 (merge `277226e56`, ancestor of `e5fdd2fa8`) fixed the first half:
- integer codes after 1989;
- 1989 code 0 → `Unknown`;
- missing 1994+ units → `Unknown`.

Files touched: `countries/EthiopiaRHS/_/ethiopiarhs.py` and `categorical_mapping.org`. Tests: `tests/test_ethiopiarhs_units_1044.py` and the `tests/test_u_code_leak.py` update. Nothing on EthiopiaRHS units since then. The body still lists both halves, so narrow it.

Idiosyncrasy (`EthiopiaRHS/_/CONTENTS.org:550-570`): `Codeunit1.SPS` recodes `q16_5d`, the unit of a 1994 R1 CREDIT in-kind loan, not `food89`. It does share the quintal-first codes 1–5 and 8 with 1989's lists, so it can seed part of a 1989 decode. Which 1989 instrument list the data were entered on is open.

New title: EthiopiaRHS 1989 food: decode food89 unit codes (1989 still serves raw unit tokens, so it has no kg baseline)
New body:
> **Resolved so far:** PR #1067 (`277226e56`) serves integer unit codes after 1989 (no more `31.0`/`83.0`/`95.0` float tokens), 1989 code 0 as `Unknown`, and missing 1994+ units as `Unknown` (+212 rows). Pinned by `tests/test_ethiopiarhs_units_1044.py` and `tests/test_u_code_leak.py`.
>
> **Remaining (Tier B):** 1989's `food89.unitcode` is unlabelled in the data, so `u` carries the raw code. Code `1` is kilograms per the code table, but it is served as the token `'1'`, so 1989 has no kg baseline and every row's kg factor source is `none` (2,068 rows at filing; unverified-data now). Decode the 1989 codes:
> 1. First test the unified D8/D9 data-entry hypothesis against prices and items.
> 2. `Codeunit1.SPS` (in each 1994+ `Documentation/`) is a partial crosswalk for codes 1–5 and 8. It was written for the 1994 R1 credit-section variable `q16_5d`, not for `food89`, so treat it as corroboration, not authority.
>
> Report before/after kg totals and `attrs['kg_factor_sources']`. Evidence and the 1989 instrument's code lists are in the GH #1044 section at the end of `EthiopiaRHS/_/CONTENTS.org`.
>
> Related: #1061 (1994a round-1 labels, the same `harmonize_unit` table) and #1068 (`Unknown` pooled as a unit in native modes). Cost: one EthiopiaRHS re-warm. -- Sue

### #1061 — EthiopiaRHS 1994a: unit codes 40/76/80-82/85/86 are the round-1 scheme but are decoded with 1997's labels (238 rows)
Verdict: open/none, tier B. Nothing merged. `EthiopiaRHS/_/categorical_mapping.org:357-360` keeps `harmonize_unit` single-scheme: `:398` `40 | Big mada`, `:439` `82 | Efigne`. `CONTENTS.org:556-558` correction (a) records that 1994a's own value labels differ. The body is accurate, so no write is needed.
QUEUE: #1061 | EthiopiaRHS | recode 1994a units through `Codeunit1unit1234.SPS` (or a 1994a column in `harmonize_unit`) before decoding; leave 1994b/1995 on the unified scheme unless a document says otherwise | `countries/EthiopiaRHS/_/categorical_mapping.org` and/or `ethiopiarhs.py`, `tests/test_ethiopiarhs_units_1044.py` | one country (EthiopiaRHS re-warm; country `.org` and module are build inputs) | per-code row counts before/after for 1994a, kg factor movement on affected `(j,u)` cells, and a check that 1994b/1995/1997 are unchanged
Pair it with #1044's 1989 decode in one EthiopiaRHS re-warm.


## 8.B10b-nonisa-wiring

# Batch B10b: non-ISA wiring (458 468 560 567 699 701 702 706 975 977 1009)

### #458 — EthiopiaRHS plot_features: extend to R2–R4 (follow-up to #278)
Verdict: open/none, tier B (wiring). No merged work since filing. `lsms_library/countries/EthiopiaRHS/_/data_scheme.yml:201-205` still records 1994b/1995/1997 as deferred, and only `1994a/_/data_info.yml` declares `plot_features`. Idiosyncrasy: R2 and R4's plot sections are multi-file and need the script path, R3 needs a composite `plot_id` formatter, and R5–R7 ship only household-by-crop aggregates, so R2–R4 is the whole residual (`slurm_logs/DESIGN_erhs_plot_features_2026-05-20.org`).
Draft: none. The body still states the remaining work. Optional one-line comment: "Still open on development e5fdd2fa8: data_scheme.yml:201-205 lists R2 (multi-file, script path), R3 (composite plot_id formatter) and R4 (multi-file) as deferred. -- Sue"
QUEUE: #458 | EthiopiaRHS | wire plot_features for 1994b/1995/1997 from the raw R2–R4 plot sections (R3 by YAML with a plot_id formatter; R2/R4 by wave scripts) | EthiopiaRHS/{1994b,1995,1997}/_/data_info.yml (+ plot_features.py for R2/R4), _/ethiopiarhs.py if the formatter is shared, _/data_scheme.yml comment | one country (any _/ethiopiarhs.py or data_scheme.yml edit re-warms EthiopiaRHS) | cold build of EthiopiaRHS plot_features under a private root; Site I on plot_id; v-join coverage per wave

### #468 — EthiopiaRHS crop_production: wire deferred 1999 (R5) wave (packed-hhid keyspace)
Verdict: open/narrow, tier B, depends on #737. No merged work since filing. `EthiopiaRHS/_/data_scheme.yml:127-131` still defers 1999, and `1999/_/data_info.yml` declares no `crop_production`.
Contradiction with the body: the issue explains `area_output_99.tab`'s packed `hhid` as "the documented ERHS 1999 dual-keyspace problem (same one that makes 1999 livestock/income use the packed-hhid scheme)". `CONTENTS.org:83-106` and `:593-602` (WRONG WAVE, 2026-08-24, WAITING; #738 / `1990988e4`) found that those `lvs5`/`inc5` files are **1989-round data**. The packed `Region*100000 + PA*1000 + hh` id is the documented 1989 signature, while the real R5 livestock file keys on `(paid, within-PA hhid)` across 19 PAs. The premise therefore no longer holds, and wiring option 1 (a crosswalk) could put 1989 data at `t='1999'` a third time. The body should be narrowed to put the round check first.

New title: EthiopiaRHS crop_production 1999 (R5): establish which round area_output_99 is, then wire or retire it

New body:
> **Narrowed 2026-11-12 (Sue).** Resolved so far: none. crop_production still covers 1994a, 1994b, 1995, 1997, 2004 and 2009 (`EthiopiaRHS/_/data_scheme.yml:127-131`); 1999 is deferred.
>
> **Why the original premise needs re-checking first.** This issue said `_dataverse_archive/area_output_99.tab` keys on `paid` plus a packed `hhid` (max 1300001), "the same one that makes 1999 livestock/income use the packed-hhid scheme". Since then the `*lvs5.tab` / `*inc5.tab` files wired to 1999 have been shown to be **1989-round data** (`EthiopiaRHS/_/CONTENTS.org`, "WRONG WAVE", 2026-08-24; #738, #737 pending). The packed `Region*100000 + PA*1000 + hh` id is the documented 1989 signature, and the genuine R5 livestock file keys on `(paid, within-PA hhid)` across the 19 panel PAs. The zero overlap with the 1999 `[q1c, hh]` sample keyspace is therefore not by itself evidence of a 1999 dual keyspace. The CONTENTS.org section warns against citing raw-key overlap of 0 as evidence.
>
> **Remaining work, in order:**
> 1. Establish the round of `area_output_99.tab` the way the WRONG WAVE section did: SPSS `_date` range, PA set (the seven 1989 PAs versus the 19 of the 1994+ panel), and household overlap with the 1989 roster versus the R5 `roster.tab`.
> 2. If it is R5: build a crosswalk from its key to the 1999 `[q1c, hh]` composite (check whether `panel_ids` covers R5) and wire `1999/_/data_info.yml` `crop_production` so `v` joins. Do not serve a v-less wave; the table-level `_no_v_join` cannot exempt one wave.
> 3. If it is 1989 data: record that in CONTENTS.org, do not wire it at t='1999', and fold the question into #737.
>
> Discovery script: `slurm_logs/2026-06-15_ehcvm_parity/disc_crop_keyspace.py`. Parent: #438 / PR #467.
>
> -- Sue

### #560 — Cambodia nutrition: obtain + wire the Food Conversion Table (SMILING/ASEANFOODS) — split from #108
Verdict: needs-acquisition/narrow (pointer only), tier D. PR #561 (2026-06-20) did the food side and nothing since. `nutrition` is not in `Cambodia/_/data_scheme.yml`. The `FCT ID` column of `harmonize_food` (`Cambodia/_/categorical_mapping.org:108`, noted at `:95` and `:177`) is still a blank scaffold, and `CONTENTS.org:378-382` keeps the TODO. The body's file pointer is stale: `_/food_items.org` was retired into `categorical_mapping.org` (`CONTENTS.org:71`). Idiosyncrasy: that table's `2019-20` column keeps source mojibake and a zero-width space verbatim because it is a string join key. Map FCT codes against `Code`, not against the label text.

New title: (unchanged) Cambodia nutrition: obtain + wire the Food Conversion Table (SMILING/ASEANFOODS) — split from #108

New body: keep the original body unchanged, with this header above it:
> **Narrowed 2026-11-12 (Sue).** Resolved so far: PR #561 (food side only). Still blocked on acquiring the FCT. **Pointer correction:** the 64-item table with the blank `FCT ID` scaffold is now the `harmonize_food` table in `Cambodia/_/categorical_mapping.org` (the standalone `_/food_items.org` was retired). Its `2019-20` column holds source mojibake verbatim on purpose because it is a join key, so key FCT codes on the numeric `Code` column. Remaining steps 1–3 below are unchanged; step 3 registers `nutrition` in `Cambodia/_/data_scheme.yml`. -- Sue

### #567 — India housing: decode numeric Roof/Floor codes (1-5) then harmonize (split from #170)
Verdict: open/narrow (pointer), tier B (labelling). Nothing has merged. `India/1997-98/_/data_info.yml:85-86` still maps `Roof: v03a05` / `Floor: v03a06` with no mapping or formatter. `India/_/categorical_mapping.org` holds only `harmonize_education` (`:22`). India's CONTENTS.org has no housing entry. Step 1 may already be possible without new acquisition: the household questionnaire `India/1997-98/Documentation/slc-hh97.pdf` and the DDI `ddi-documentation-english_microdata-276.pdf` are held under DVC (since 2025-08-08). Neither has been read for section 3A (unverified-data: open `slc-hh97.pdf` section 3A through `data_access.get_data_file` in an authorised session).

New title: (unchanged)

New body: keep the original body unchanged, with this header above it:
> **Narrowed 2026-11-12 (Sue).** Resolved so far: none. `India/1997-98/_/data_info.yml:85-86` still serves `v03a05` / `v03a06` as bare numeric codes. **Pointer for step 1:** the household questionnaire (`India/1997-98/Documentation/slc-hh97.pdf`) and the DDI (`ddi-documentation-english_microdata-276.pdf`) are already held under DVC. Read section 3A's printed roof/floor codes there before treating this as a data gap. If the codes are printed, step 2 is a `mapping:` in `data_info.yml` plus `Roof`/`Floor` tables per `canonical_housing_labels.org`. -- Sue
QUEUE: #567 | India | decode v03a05/v03a06 from the held questionnaire's section 3A, then add Roof/Floor tables | India/1997-98/_/data_info.yml (mapping:), India/_/categorical_mapping.org | one country (categorical_mapping.org is a build input; data_info.yml is per wave) | read slc-hh97.pdf §3A; then a cold India housing build with values in canonical_housing_labels vocabulary

### #699 — Wire Peru 1990 tables after the XPORT reader repair
Verdict: open/none, tier B (wiring). Narrowed 2026-10-15 and no merged work since. The XPORT reader fix is PR #990 (`010b84ba0`; `4855c09df`). `Peru/1990/` still has only `Data/` and `Documentation/`, with no `_/`. Idiosyncrasy (`Peru/_/CONTENTS.org:126-199`): 1990 is Metropolitan Lima only and re-visits the 1985-86 Lima panel rather than drawing afresh, so it must not be pooled as a national round. The `_i`/`_d` monetary-suffix blocker from #701 applies here too.
Draft: none. The body states the remaining scope.

### #701 — Peru: wire ENNIV 1991 — the closest of the three unwired waves to a quick win
Verdict: open/none, tier B (wiring). No merged work since PR #691. `Peru/1991/` has no `_/`. The body is still exact. Idiosyncrasy (`CONTENTS.org:202-251`, `:346-372`): the catalog universe says "Población nacional", but its coverage list names neither Selva nor Costa Rural, and the 1994 sampling text ("las áreas no estudiadas en 1991") corroborates partial coverage. The 1994 decodes do not transfer.
Draft: none.
QUEUE: #701 | Peru | wire 1991 non-monetary tables first (household_roster from N01 with its own code list, sample with a documented cluster choice, interview_date from dayi/moi); leave expenditure until the _i/_d meaning is documented | Peru/1991/_/data_info.yml (new), Peru/_/categorical_mapping.org (1991-specific tables), Peru/_/CONTENTS.org | one country (Peru) | cold Peru build in a private root; kinship labels resolve against the 1991 value labels; v cluster choice cited from documentation, not inferred

### #702 — Afghanistan: `_/` config exists but declares no `Data Scheme:` — two waves of data unreachable
Verdict: open/none, tier B (registry/wiring). Nothing has merged. `Afghanistan/_/` still has `CONTENTS.org`, `Makefile`, `afghanistan.py` and `population.yml`, with no `data_scheme.yml` and no wave `_/` directories. The only later touch is `064398379` (2026-11-09), a dead-Make-target cleanup. CONTENTS.org still does not say that the country is deliberately unstarted.
Draft: none. The body's ask stands. The cheapest tractable close is the "say so in CONTENTS.org" option.
QUEUE: #702 | Afghanistan | add a CONTENTS.org paragraph recording that no table is wired and why (or start a data_scheme.yml) | Afghanistan/_/CONTENTS.org | none (CONTENTS.org is excluded from the hash) | doc review only

### #706 — CotedIvoire 1985-89: four unwired community-questionnaire files (COMM85-88.DAT), rural-only
Verdict: needs-acquisition/none, tier D. PR #708 (2026-08-22) fixed only the reader. `CotedIvoire/_/CONTENTS.org:155` still says "community module: available but NOT wired". `:262-268` forbids labelling `C02Q01`/`S01Q01` as `Population` until the instrument is read. The blocker, the 1996 CILSS Basic Information Document (catalog 83, download 15551), is still not held. The only BID under `Documentation/` is the 2018-19 EHCVM one. Idiosyncrasy: the instrument is rural-only (2 of 182 urban clusters), the two designs (CnnQnn 1985-87, SnnQnn 1987-89) share no variable mapping, and the questionnaire scans have no text layer.
Draft: none. The body states the remaining work and its order.

### #975 — Kyrgyz Republic: four waves of data (1993, 1996, 1997, 1998) and no _/ config at all
Verdict: open/narrow (correction), tier D (bootstrap). Nothing has merged, and `countries/Kyrgyz Republic/` still has no `_/`. Correction: the body says the bootstrap "needs the questionnaires", but every wave's `Documentation/` already DVC-tracks instrument PDFs (1993 `kyradult`/`kyrchild`/`kyrcomm`; 1996 `kyr96fhh_V2`/`kyr96com`; 1997 `kyr14a_V2`/`kyr14b`/`com97`; 1998 `hh98_V2`/`com98`).

New title: (unchanged)

New body: keep the original body unchanged, with this header above it:
> **Narrowed 2026-11-12 (Sue).** Resolved so far: none; there is still no `_/` directory. **Correction:** the instruments are already held. Each wave's `Documentation/` tracks the household (and community) questionnaire PDFs under DVC (1993 `kyradult.pdf`/`kyrchild.pdf`/`kyrcomm.pdf`, 1996 `kyr96fhh_V2.pdf`, 1997 `kyr14a_V2.pdf`/`kyr14b.pdf`, 1998 `hh98_V2.pdf`). So this is a bootstrap with documents in hand: `household_roster` and `sample` first, a CONTENTS.org with the format-105 `2^333` missing-code note, and the 1998 `SECT_06.DTA` `fprimary` row decided from the instrument. -- Sue

### #977 — Panama 1997 and Serbia & Montenegro 2002: sample weights 100% null -- questionnaire check (C4) before calling it not-asked
Verdict: open/none, tier B. Nothing has merged, and `absent_verdicts.csv` has no row for either. The Panama 1997 wave config asserts "No household expansion weight is present in this wave" (`Panama/1997/_/data_info.yml:3-7`) with no evidence recorded, and `Panama/_/CONTENTS.org:519-522` says the ENV-97 weighting text was never examined. Panama 1997 holds `pn97hhq.pdf`, `caratula.pdf` and `Formulario_Precios.pdf` for C4. A point for the fixer to quote, not decide: `Serbia and Montenegro/_/CONTENTS.org:269-273` calls 2002's weight-only shape consistent with the library convention, while CLAUDE.md says pre-refreshment waves carry the same value in both columns. 2002's null `panel_weight` may therefore be a wiring `todo`, not `not-asked`.
Draft: none. The body states the work.
QUEUE: #977 | Panama, Serbia and Montenegro | run C1–C4; record a verdict and quote it in CONTENTS.org; if Serbia 2002 is the base wave, wire panel_weight = pobder per convention | .coder/coverage/absent_verdicts.csv, Panama/_/CONTENTS.org, Serbia and Montenegro/_/CONTENTS.org, possibly Serbia and Montenegro/2002/_/data_info.yml | none for verdict/docs; one country if data_info.yml changes | read pn97hhq.pdf / caratula.pdf; cold sample() build if wired

### #1009 — Panama: Region-market food variants lack a geography lookup
Verdict: open/none, tier A (cross-country assembly: Panama is dropped from `Feature(...)(market='Region')`). No fix has merged. Panama still declares no `cluster_features` or `Region`, and `country.py:3352-3356` (`_market_lookup`) still raises `RuntimeError("cluster_features returned no data for any wave.")`. That raise comes before the 2026-11-03 keep-unlabelled-`m` path, so the newer behaviour does not reach Panama (by source reading; unverified-data: `Country('Panama').food_expenditures(market='Region')` in a private root). Idiosyncrasy (`Panama/_/CONTENTS.org:115-128`, `:1085-1105`): ENV-97 separates Planning Regions, survey Domains and ad hoc Collection Regions, so none can be silently substituted for another. `panama.regional_data` (`panama.py:65`) registers nothing.
Draft: none. The body's acceptance list is current.
QUEUE: #1009 | Panama | after choosing a source-backed Region per wave (planning region, documented per wave), add Region to sample (HH-level) or declare cluster_features with it; document unsupported waves | Panama/{1997,2003,2008}/_/data_info.yml, Panama/_/data_scheme.yml, Panama/_/panama.py (if a formatter is needed), Panama/_/CONTENTS.org | one country (Panama) | private-root Country + Feature market='Region' for food_expenditures/food_prices/food_quantities; household and cluster coverage; default market unchanged


## 9. Limits

- Source and GitHub reading only: no builds, imports, `dvc`, or data reads. Every row-count claim inherited from an issue body or report is the body's, not re-measured; rows say `unverified-data` where a conclusion depends on data, with the command.
- "Resolved" was granted once (#778), and only with a merge SHA that is an ancestor of `e5fdd2fa8` plus path:line on development. Absence of a repro was never used as evidence.
- Path-based matching covered the four post-base merges; earlier merged work was matched by timeline cross-references, `#N` mentions in PR bodies and commit messages since 2026-09-01, and keyword search. An omnibus PR that touches an issue without naming it or its paths could still be missed for issues last narrowed before 2026-11-10.
- Workers were forks of the same model; their drafts were spot-checked (all closes and splits re-read; SHAs and path:lines checked mechanically), not independently red-teamed. Treat each `narrow` body as a draft to read before pasting.
