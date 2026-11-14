# v0.17.0 closing-keyword audit

Prepared 2026-11-12 by a read-only release-preparation agent (Claude Opus 5.5) for Sue.
Range: `v0.16.0` (`097283459f80016b0f00a51b814a0cb964a42789`, the current `origin/master`) to
`development` `e5fdd2fa831c4d03a247d264be7f339da459e6b0`.

## Result

| verdict | count |
|---|---|
| **CLOSE** (genuinely resolved, still open, needs a `Closes #N` line) | **0** |
| NOT-RESOLVED (open, residual work stated in the issue's current text) | 57 |
| UNSURE | 0 |
| excluded: in the pending 2026-11-12 batch (listed below, not graded) | 14 |
| already closed after v0.16.0, by hand (release-notes list): 179 fixed on development, 5 closed on a maintainer ruling with a related PR, 2 not planned, 2 duplicates, 3 whose fix was already in v0.16.0 | 191 |

**No open issue qualifies for a `Closes` line today.** Every open issue touched by work merged
since v0.16.0 has been narrowed after that work landed, and each one's current body or
latest comment states what remains (quoted in `CLOSES.tsv`). The repo's practice since
v0.16.0 has been to close resolved issues by hand on development merge (191 such closures,
all by `ligon`), so the master merge has nothing left to auto-close. The `Closes` list in
`MERGE_PR_BODY.md` is empty until the batch lands; re-run this audit then.

## Method

1. **PRs.** `gh pr list --state merged --base development --search "merged:>=2026-10-21"`
   returned 368 PRs. Each merge commit was classified by ancestry: 13 are already in
   `v0.16.0`, and **355 are new** (`merge-base --is-ancestor <sha> e5fdd2fa8` true, and not
   an ancestor of `v0.16.0`). The brief's figure was "about 314"; the measured count is 355.
   It includes 19 PRs whose merge commits do not say "Merge pull request" (integration and
   squash merges, e.g. #1680-#1687). A separate search for merged PRs on other bases found only
   #1168 (the v0.16.0 release merge itself).
2. **Candidates** were the union of four sources:
   - closing keywords in the 355 PR titles and bodies, plus GitHub's `closingIssuesReferences`;
   - issue numbers in branch names (`fix/<n>-…` and similar);
   - `#N` in commit subjects and full commit messages in `v0.16.0..e5fdd2fa8`;
   - GraphQL over all 147 open issues: cross-references from new merged PRs, and a last-comment
     scan for "landed", "merged", "fixed by" and similar.

   That gave 63 open candidates. The other 84 open issues are mentioned by no new PR, branch or
   commit message.
3. **Grading.** Each of the 57 non-excluded candidates was read from its current text: the
   head of the body, its "Remaining", "Still open" and checkbox lines, and its latest comment.
   CLOSE required every listed item to map to a new merge SHA. None did.
   - Spot checks at `e5fdd2fa8` confirmed three residuals:
     - #1451: `_EXACT_METRIC_LABELS` is still in `lsms_library/ehcvm.py`.
     - #1247: Iraq and Timor-Leste have no `sampling.yml`, and Albania's has no `panel:` block.
     - #1122: the stale comment is still at `GhanaLSS/1987-88/_/plot_labor.py:14`.
4. **Already closed.** GraphQL over closed issues gave each one's `closedAt`, `stateReason` and
   `ClosedEvent` closer.
   - Of 239 closed after the tag, 47 were closed by #1168 (v0.16.0's own `Closes` list) and #1099
     by a commit already in v0.16.0. All 48 are excluded.
   - All 191 remaining were closed by hand (no closer). They are listed below with their
     resolving PR, chosen in this order: closing keyword, then branch name, then cross-reference,
     then the closing comment.

## Auto-close hazards checked

- **Commit messages.** GitHub closes an issue when a *commit message* containing `Closes`,
  `Fixes` or `Resolves #N` reaches the default branch, and it does not parse negation.
  - `v0.16.0..e5fdd2fa8` has 59 such (issue, commit) pairs. **Every referenced issue is already
    closed**, so the master merge will close nothing by accident today.
  - Re-run after the batch lands:
    `git log --format='%B' master..development | grep -iE '(close[sd]?|fix(e[sd])?|resolve[sd]?):?\s+#[0-9]+'`.
- **#1483.** PR #1695's body carries a closing keyword for issue 1483. PR-body keywords fire only when that PR merges
  into the default branch, and #1695 merged into `development`, so this will not fire. That is
  correct, because #1483 was narrowed afterwards and remains open.
- **#1287.** PRs #1313, #1344 and #1349 contain the negated phrase "does not close" followed by the 1287 reference. These are PR bodies, so
  they are harmless here. **Do not paste such negated phrases into `MERGE_PR_BODY.md`**:
  GitHub would close issue 1287.
- **Default branch** is `master` (`gh repo view --json defaultBranchRef`), so `Closes` lines in
  the development → master PR body will fire. The local `origin/HEAD -> origin/development` is a
  mirror artefact.

## Excluded: in the pending 2026-11-12 batch (add `Closes` lines after it lands, if resolved)

- #1604 (OPEN): Recover Malawi 2024 food with traceable IHS5 factor borrowing
- #1275 (OPEN): Malawi 2024-25: acquire IHS-VI (WB 8507) and IHPS-V 2024 (WB 8520), a new wave
- #1697 (OPEN): Wire EHCVM-2 2021-22 for Benin, CotedIvoire, Guinea-Bissau and Togo (held since #1506)
- #552 (OPEN): WB microdata acquisition gaps: Jamaica, Ecuador, Vietnam (external) and the late-2026 ISA 
- #587 (OPEN): Resolve remaining historical coverage-drop cells and Tanzania fielding authority
- #1509 (OPEN): food_kg_factors pools self-valued non-purchased rows into the kg inference
- #1068 (OPEN): DECISION: native-unit modes treat U_UNKNOWN as a pooled pseudo-unit (food_prices(units='un
- #1129 (OPEN): `food_prices(units='unitvalue')` serves exactly 1.0 for every `u='Value'` row: 1,107,464 G
- #1560 (OPEN): Sold kilograms are recorded and unread in Niger and Mali
- #1571 (OPEN): Niger sold kilograms: resolve field meaning and state/eligibility policy
- #1707 (OPEN): Mali 2018-19: 3,357 constructed rice rows (q3 = 1.4 kg x household size) are served as pur
- #1708 (OPEN): Niger 2018-19: last-purchase quantity s07bq07a looks computed on 14.2% of rows (26% of ser
- #1536 (OPEN): Uganda ea in 2009-10: fill the 569 NA rows from the household's label in another wave?
- #1519 (OPEN): Senegal: decide treatment of near-zero quantities already present in released data

## NOT-RESOLVED (57): residual work stated in the issue's current text

Full evidence and partial-PR SHAs are in `CLOSES.tsv`. Every listed PR is an ancestor of
`e5fdd2fa8`.

| # | title | what remains (from the issue) |
|---|---|---|
| #438 | Non-ISA item stack: finish source-backed label foundations and modu... | Body "## Remaining work": post-defect-queue design note owed under the 2026-10-23 ruling; Sue 2026-11-12: "nothing merged since 2026-10-25 touches Guinea-Bissau crop extraction or the remaining harmonize_input/seed-crop foundat... |
| #449 | Tanzania W1–W3 geovariables: choose the W2 variant and grain, then ... | #1514 holds the W1-W3 geovariables zips, "not wired"; remaining: choose the W2 household file and grain, then wire a native extractor through the r_hhid bridge. |
| #647 | Uganda 2019-20 PART-harvest slots (5A1): ruled status quo; ask UBOS... | Ruled status quo 2026-11-04; remaining Tier D "needs a person": ask UBOS/World Bank whether 5A1 Q6 is incremental or cumulative; correct stale CONTENTS.org:3097-3098 line. |
| #791 | UEMOA PHMECV as a source: ERI-ESI 2018 studies and the community fi... | #1506 holds EHCVM-2 from the WB catalog instead; "Remaining (UEMOA-only)": Benin grant discriminating test; _find_stata_zip cannot see UEMOA .rar archives. |
| #821 | Mali 2021-22 community prices: obtain the community questionnaire t... | #1356 wired Mali 2018-19; "Remaining (C4): Mali's own 2021 community questionnaire" (not on WB 6275 / IHSN 12257). |
| #824 | Sold-unit valuation: Burkina 2014/2021-22 sale wiring, Nigeria W1/W... | Many partial PRs (#1270 #1272 #1326 #1355 #1392 #1466 ...); "Remaining: 3. Ethiopia: residual export cards (W2/W3/W4/W5) and held W1/W2 grams / W1 BIRR Decimal questions". Umbrella. |
| #833 | Food/crop identity: Uganda's five unruled processing-state families... | Sue 2026-11-04: "What remains: Uganda's Aggregate column (memo); Ethiopia's comparison, which needs a sold unit first" (Tanzania's part could close; umbrella stays open). Uganda Beans/Peas/Rice/Ginger/Soybean unruled. |
| #837 | Cluster disagreements: decide Ethiopia's shipped GPS conflicts and ... | Tanzania District ruled 2026-11-03 (left as served); "Remaining, each needing its own explicit scientific decision": Ethiopia shipped GPS conflicts (38/72/72 EAs W2/W3/W5) and Tanzania household-grain Rural. |
| #845 | Price provenance: record elicited-vs-derived Price for Panama/Ghana... | Narrowed 2026-11-12 to items (a) and (b) (elicited-vs-derived Price record for Panama/GhanaLSS/Guatemala/Serbia; decide a read-time diagnostic); Panama defect split to #1700. |
| #862 | Schema proposal: plot_cultivation, a per-season plot decision-maker... | Schema proposal; ruled in scope 2026-10-23 "designed and built after the defect queue drains, with a design note here first". #1658 explicitly keeps the name plot_cultivation reserved for it. |
| #905 | Vectorized min_count reducers: 10 remaining Python-callback sites (... | "Remaining sites (source at e5fdd2fa8; each still a Python lambda s: s.sum(min_count=1) callback)": 10 sites in GhanaSPS, Cambodia, Serbia, Guatemala, GhanaLSS, Iraq, Albania + Tanzania multirow branch. #1349 body: "do not clos... |
| #1000 | Uganda FCT row 4003 (yellow sweet potato): acquire Kidmose et al. 2... | Status quo ruled 2026-11-04; "Remaining (acquisition)": obtain Kidmose et al. 2007 full text and the HarvestPlus electronic FCT. |
| #1045 | Calendar parser: finish Phases 2–4, stamp/replay coverage and the t... | Tier A reconciliation 2026-11-12: "## Remaining work" migrate remaining producers through the shared parser (Phases 2-4) + open time-of-day question for the maintainer. |
| #1084 | Recall research: finish remaining non-ISA questionnaire evidence | Sue 2026-11-04: "Left: Cambodia 2019-20, EthiopiaRHS x5, GhanaSPS x3, whose questionnaires were not read." |
| #1118 | plot_inputs: replace the minted `Manquant` unit with `Unknown` in s... | "Remaining (Tier B)": [ ] wave scripts still mint Manquant in countries lacking a Manquant->Unknown row; [ ] GhanaLSS cost-only rows' u. |
| #1119 | plot_inputs crop axis: Guinea-Bissau seed crops, Senegal maize/cowp... | "Remaining (Tier B)": [ ] Guinea-Bissau 2018-19 seed crops all '(not crop-specific)'; [ ] Senegal maize/cowpea spelling; [ ] GhanaLSS j='Farm-level' migration. |
| #1121 | plot_labor.source: decide GhanaSPS/GhanaLSS taxonomy and containment | "Remaining work": design/document a cross-country labour-source taxonomy or containment semantics for GhanaSPS/GhanaLSS (Tier C). |
| #1122 | Ghana: three stale registration comments deferred to country cache ... | Three stale hashed comments deferred to each country's next authorized cache rebuild; verified still present at e5fdd2fa8 (GhanaLSS/1987-88/_/plot_labor.py:14). #1648 did only the non-hashed docs part. |
| #1134 | Sampling design tags: add structural tags with their first evidence... | #1541 added design_tag not-stated (Albania 2008); "this issue now tracks the remaining structural-tag implementation", one tag at a time. |
| #1135 | Cluster identity: verify complete-mover and physical-cluster measur... | #1677 delivered the warning text; "This issue remains open for the measurement questions below" (complete-mover and physical-cluster measurements). |
| #1148 | Household recall exposure: adjudicate passage-status evidence befor... | "## Current remaining work - 2026-11-10": adjudicate passage-status evidence (Burkina 2014; GhanaLSS follow-up) before normalization; #1671 prerequisite done. |
| #1179 | Mali EAC-I 2014-15 community prices: 129 positive-priced entries wi... | "Remaining: obtain producer instructions or a released correction" for the 129 unresolved unit codes; #1515 did Poids only. |
| #1226 | EHCVM community prices: bridge Guinea-Bissau 94, one variety conven... | Three unchecked boxes ruled 2026-11-04: bridge Guinea-Bissau product 94; one variety convention; Burkina tomato tin (re-measure after #1452). |
| #1228 | Malawi crop_production: seasonal crop "Other" is mostly cowpea, whi... | #1523 did 2010-11 only; ruling 2026-11-12 "Join D, single-text only" for 2016-17/2019-20 is not yet implemented. |
| #1230 | Org-key parsing: decide lexical storage and numeric-key compatibility | "## Remaining work: Org-key storage policy" - decision open; red team found a tested-contract constraint (#928 tests). |
| #1235 | CotedIvoire 2018-19 plot_inputs: diagnose the modal quantity 350 (3... | Body: "no fix merged"; checklist of four unchecked diagnostic steps plus a ruling on the modal quantity 350. |
| #1247 | Site C: add evidence-backed boundary records for Iraq, Albania and ... | Remaining: Iraq, Albania and Timor-Leste boundary records. Verified at e5fdd2fa8: Iraq and Timor-Leste have no sampling.yml; Albania's has no panel: block. |
| #1276 | Burkina 2021-22: relocate 6224 into Data/Panel, acquire 6277 into D... | "Remaining work": wire food/housing/assets/shocks/s08a/s20a/individu/agriculture to both components; 6277 not acquired (Sue 2026-11-05). |
| #1281 | Declared-wave KeyErrors: corpus exposure inventory and audit of pre... | #1300/#1599 landed; "Remaining scope": corpus exposure inventory and audit of pre-#1599 warm caches. |
| #1287 | Country compilers: two non-ISA concatenators still skip a declared ... | "Remaining (source reading at e5fdd2fa8)": GhanaLSS community_prices and Senegal plot_features concatenators can still skip a declared wave. Keyword hits in #1313/#1344/#1349 are negations ("does not close" + the 1287 reference). |
| #1288 | make backend: remaining caller exposure and undeclared-input freshness | "## Current residuals - 2026-11-12": broader real (country, table) exposure; failure paths outside the direct opted-in builder; undeclared-input freshness. |
| #1436 | Crop production: Mali 2017 condition authority and remaining 2021–2... | "Actual residuals": [ ] Mali 2017-18 s7fq13b condition authority; unbuilt 2021-22 waves (six take Quantity_kg when wired). |
| #1437 | Burkina 2021-22 EHCVM: harvest quantity s16cq16a is blank for finis... | Mali half done by #1515; "Burkina's half waits until its 2021-22 crop_production is wired" (#1436/#1276). |
| #1441 | Region/District: remaining source gaps and attributable code-map au... | "Remaining work": [ ] Albania Region coverage 2008/2012 (+2003/2004); Uganda 2013-14 code-to-name map not built. |
| #1442 | Burkina 2014 food: land the ruled C05/C06 values (`autocons`, `cade... | "Remaining: land commit d81358091 (archived at refs/archive/2026-11-11/...; it is not on development)", held for #1509 (in pending batch). |
| #1451 | Serve EHCVM metric units with their recorded size (185 rows stay ba... | No PR addresses it; 185 rows still bare. Verified: _EXACT_METRIC_LABELS still in lsms_library/ehcvm.py at e5fdd2fa8 (the body's stated fix removes it). |
| #1454 | EHCVM sized-unit small follow-ups from #1224 | #1647 fixed the fingerprint item; "Still open: Senegal's size synonym ruling, Togo's own code-98 authority, bare-to-sized u_kg fallback, ambiguous numeric/code-prefixed metric labels". |
| #1460 | yield_kg: resolve held land-route area authority after merged routi... | Sue 2026-11-12: "Benin, Togo and Guinea-Bissau remain held pending #1531"; Nigeria residual split to #1701. |
| #1482 | Tonne-dominated plot-input quantities: adjudicate quantity basis; S... | #1618 added screening; "Remaining work: inspect the Tonne-dominated quantity cells ... and decide whether any documented correction is justified". |
| #1483 | EHCVM household inputs: validate the Burkina/Mali routes publicly a... | PR #1695 body carries a closing keyword for 1483, but the issue was narrowed after it (Sue 2026-11-12): [ ] public serving validation of the three #1687 routes; [ ] reconcile Niger/Senegal 2021-22 documentation authority. #1695 merged to deve... |
| #1485 | Anonymous fertilizer-rate routing: decide automatic recovery policy... | "Remaining work": [ ] decide whether/how the default anonymous parcel route should recover rates. |
| #1497 | Tanzania 2019-20 split-offs: decide the first-cover-row fallback wh... | "Remaining (decision)": how _map_with_head_tracking treats a family whose former head is absent or no longer head (20 fallback pairs). |
| #1518 | Nigeria W1-W3 harvest/yield coverage: integrate verified W3 helper ... | Three unchecked items (W3 adoption, W1/W2 evidence, final API correspondence); ruling 2026-11-12 (wire ag_conv_w3 with #1502) not yet implemented. |
| #1525 | Uganda 2019-20: Cassava codes 1081 and 1082 do not behave as their ... | Body after #1672: "Keep open: obtain the attributable food roster/questionnaire GUID + version/export specification or producer erratum". |
| #1529 | Niger 2011-12: resolve section 3E crop-cycle area semantics and yie... | 3C (#1632) and 3B/3D (#1658) served; "Remaining: section 3E crop-cycle area (as03eq08)". |
| #1530 | Niger: Oignon frais falls to 4 rows in 2021-22 | #1648/#1675 record evidence only; "The useful remaining discriminator is a full gate/omission roster or a version-bound fielded SurveySolutions questionnaire". |
| #1531 | EHCVM GPS plot area: resolve units and export fields in Togo, Benin... | "Resolved so far (documentation only; no runtime change)" #1649/#1693; "Remaining work: establish the relevant export/cleaning relationship or choose an explicit source policy". |
| #1533 | GhanaLSS 2005-06: 413 Value-unit food rows sit 100x below their cell | Measured by #1544's low-tail screen; body: "The class is measured, the cause is not." No fix merged. |
| #1537 | Nigeria: generic Pepper food-price label lacks an Aggregate mapping | #1631 did Groundnuts/Maize only; "Remaining: generic Pepper still lacks an approved Aggregate mapping"; ruling 2026-11-12 "Check codes, then Fresh" not yet implemented. |
| #1552 | Nigeria W1/W2: resolve native cross-plot sale repeats with source e... | "## Remaining work": obtain fielded A3/export-custodian evidence; "No contact or deduplication has been authorized". |
| #1555 | Niger 2018-19: crop kilograms need release and calibration authority | "## Remaining work": establish fielded-CAPI/export bridge and authoritative kg-per-unit calibration. |
| #1556 | Shocks: apply purchases/stock ruling in unfinished wave integrations | "Remaining: apply and verify the ruling when the unfinished wave integrations land" (incl. Malawi 2024 branch, in pending batch). |
| #1559 | Site Q cannot see a survey kilogram wrong against its own quantity | #1582 landed the stated-mass screen; "Remaining: units with no stated mass cannot fire (67.7% of Niger's survey kilograms)"; measurement in progress 2026-11-12. |
| #1562 | EHCVM 2018-19 coping order: hooks list strategies by code | Ruling 2026-11-12 ("Positions = respondent order", set column for every shocks country) is not yet implemented; export-value contract still to establish. |
| #1565 | EHCVM 2021-22 leavers: decide roster membership for departed person... | "Resolved so far (evidence only, no behaviour change)"; roster-membership decision for departed persons still open. |
| #1570 | Ethiopia crop sales: W1 section 9 is short, W1 trees have a sale an... | "Still open: the section-9 coverage discrepancy, section-12 Q3 harvest, value-only responses and grams/Birr handling". |
| #1574 | Mali 2017-18 strata serves EAC-I design codes | "Remaining: obtain an authoritative complete 51-code mapping and verify its association with the released strate before wiring labels". |

## Already closed after v0.16.0, resolved by work merged only to development (191)

These need no keyword. They are for the release notes. The full table, with closing dates and
the source of each resolver, is `ALREADY_CLOSED.tsv`. Resolver precedence: closing keyword in a
new PR, then a new PR named in the closing comment, then branch name, then cross-reference.
#1170 (the backlog inventory PR) is never counted as a resolver.

### fixed on development (179)

- #218 Refresh generated skill references to retired food helpers -- #1659 (952690833)
- #381 Food derivation: clear terminal diagnostic for registered but unwired tables -- #1678 (bb5f4cca5)
- #592 cluster_features: 18 waves across 6 countries have zero GPS (Latitude/Longitude) — will be -- #1262 (7f6a10c97)
- #637 Tanzania seed grain: decide native observations and Improved aggregation -- #1430 (5dcbe95f1)
- #650 Documentation: land minimum standard, automatic fetching and verified backfills -- #1685 (83ddab2e8)
- #689 Uganda: `cluster_features` is at PARISH grain from 2013-14 — coarser than the sampling clu -- #1472 (e1c1b9905)
- #709 Docs: 1.7498e100 is Stata's pre-108 double missing code -- append the correction block to  -- #1660 (333d047bb)
- #771 Uganda/Ethiopia nutrition.py: Ethiopia has no FCT csv and hits the USDA API on EVERY build -- #1375 (82362dce0)
- #812 Investigate remaining EHCVM allocation and strata gaps after Guinea-Bissau/Togo documentat -- #1572 (111b64ed7)
- #820 Ethiopia crop_production reads ESS §11 (field crops) only; the §12 tree/fruit/root module  -- #1257 (f6aaee234)
- #822 Mali 2014-15 community_prices: restore readings 1-2 and declare native observation slots -- #1180 (94098ff45)
- #823 Niger 2018-19 ships an unwired cluster-grain community price file (s05_co_ner2018.dta, 504 -- #1227 (683665128)
- #827 Malawi community_prices: u and NumberOfUnits jointly blank on 38,506 of 82,635 rows (46%)  -- #1239 (7f1d69988)
- #839 Correct Mali's stale food-acquired config comment; Tanzania's quantity note is already res -- #1231 (856d9ddbf)
- #843 Nigeria crop_production has no sale price in W3-W5 (0 of 37,315 rows): secta3ii is hh-crop -- #1473 (ec3f4a370)
- #844 Nigeria asks a farmgate valuation of the whole harvest (sa3q18, 76% of rows) that no table -- #1419 (d47b296a2)
- #846 Tanzania: record a not-asked capability for food price and own-consumption value (Section  -- #1308 (033669be3); #1410 (a39ad3200)
- #849 Uganda: wire reported kg factors in the remaining five waves after preserving native produ -- #1207 (79a374e0b)
- #851 Recall Phase II: review and integrate approved period= conversion and exposure disclosure -- #1671 (1c1ef12da); #1672 (ba03bf85a)
- #855 Malawi: _livestock_block never reads ag_r07 ("owned exactly 12 months ago"), which is pres -- #1421 (e42fb8812)
- #856 Malawi: measure the harvest-side 'other (specify)' free-text residue per wave before decid -- #1229 (fc56812a6)
- #860 Nigeria: no factors for heaps/ridges/stands areas; EPAR's two grids disagree by a median o -- #1508 (fe62137f1)
- #864 Burkina 2014 food: adjudicate passage-1 quantity meaning and unread in-kind values -- #1431 (f6ed07ae9)
- #881 DECISION: Nigeria W4 purchased row -- identity derivation labelling the 30-day window (A)  -- #1429 (55b558ef2)
- #882 DECISION: Niger 2011-12 people_last7days dummies -- keep as served (A), re-point wage_work -- #1417 (24cc79875)
- #894 panel_ids: convert Malawi and Nigeria from the cache-parquet path to committed _/panel_ids -- #1265 (9105d5d90)
- #932 Development CI: enforce actual data validation with a compatible required gate -- #1680 (e2710db21)
- #978 Nigeria W5: wire the acquired v02 household geovariables into cluster_features -- #1335 (318e216fc)
- #1006 Uganda derived-table tests skip successful builds and retain obsolete index/dtype assertio -- #1191 (7e365fec2)
- #1032 DVC writes countries_root()/.dvc/tmp/btime when the DVCFileSystem is built: the last runti -- #1683 (d3d42e9e7)
- #1033 Nigeria W4 food unit labels carry invented W3 size prefixes; W4's size columns (s7bq2c/s7b -- #1508 (fe62137f1)
- #1034 Nigeria W3 post-planting unit codes 131/151/152/161 are labelled in the planting .dta but  -- #1172 (5f2d198db)
- #1043 Ethiopia crop_production tags a missing harvest unit as 'Other (Specify)' (ethiopia.py:182 -- #1324 (24e2cf629)
- #1051 Degraded-build grading (#675) residuals: outage crashes the build, single-file probe, read -- #1663 (389694ee1); #1665 (5d525ce50); #1666 (03acc1d8d); #1669 (20c85655a); +1 more
- #1055 DECISION: Ethiopia 2021-22 interview_date dates the post-harvest agriculture visit (1,475  -- #1412 (e574ff316)
- #1057 DECISION: Burkina Faso 2021-22 cluster_features -- serve the 2018-19 cluster GPS for the 5 -- #1465 (033988095)
- #1058 Burkina Faso 2021 plot_features: expose 4,180 reported GPS pairs on 7,350 retained parcels -- #1194 (6b3e67e0d)
- #1059 Burkina Faso cluster_features District: 50 cross-wave spelling variants (mapping table) an -- #1242 (50fbf9e92)
- #1060 Burkina Faso 2014: province varies within 94 of 900 zd in the source (pre-existing cluster -- #1465 (033988095)
- #1062 Uganda: Makefile recipes rewrite sibling var/ tables without a hash (fct by nutrition, ear -- #1173 (801e47978)
- #1063 EHCVM household_roster row order differs between builds of the same commit (Benin, Guinea- -- #1273 (3450bd6c2)
- #1066 assume_cache_fresh: define and restore stored grain-audit replay -- #1674 (50a927d69)
- #1071 Correct script-wave LSMS_NO_CACHE read-gate documentation (code repairs landed) -- #1687 (16c3f4d9c); #1690 (1e549e96a)
- #1074 DECISION: Malawi rows served u=Unknown keep a shipped code-23 survey_kg (29 Mango rows at  -- #1428 (1f36bca40)
- #1075 Malawi leftover unit codes: 2010-11 suffixed codes (16b/16a/12a/21b/12b/03a, 13 rows) and  -- #1428 (1f36bca40)
- #1077 Label selection: handle nonfinite keys and prevent missing-input label fabrication -- #1182 (b42b3bf68)
- #1078 Uganda food cache: confirmed producer race writes nine missing values as zero under curren -- #1197 (9cce8c88a)
- #1081 Derived food/roster methods: reviewed categorical-error preservation awaits merge -- #1195 (cb99555a3)
- #1087 DECISION: how to serve a second source record that disagrees with the first (Niger communi -- #1527 (997f33c8a)
- #1088 Niger CS07: recognize the stored 9999.9 missing-quantity code without changing Price -- #1176 (c598082ee)
- #1090 Niger `cs07q01` "Riz" decodes to `j = 'Autre Riz local'` for both of its varieties -- #1426 (c90d9c825)
- #1091 Retire duplicate country community-price builders in Mali, Niger and Tanzania -- #1199 (609d9bac8); #1200 (51944a0c5); #1201 (2c9b519d7)
- #1095 Nigeria community_prices: land recorded-unit recovery and decide future unplaced-label ret -- #1508 (fe62137f1)
- #1096 DECISION: Nigeria interview_date serves visit dates for households that were not interview -- #1508 (fe62137f1)
- #1106 Malawi `NPK Fertilizer` is Chitowe 23:21:0+4S but is served at the shared map's NPK 17-17- -- #1446 (7e43ce2a4)
- #1109 Malawi organic fertilizer: restore reported quantities and units; retain missing amounts w -- #1175 (ec067f800)
- #1110 `fertilizer_rate`'s default `on='parcel'` mis-joins Malawi and empties Ethiopia; its docst -- #1333 (2ae987e7b)
- #1111 Malawi applied fertilizer: correct timing-derived units while retaining enumerator kg tota -- #1238 (ee1fc7d44)
- #1120 Plot tables: implement approved source-frame, singleton-v and unknown_plot repairs -- #1534 (3aefbbedf)
- #1123 Niger ECVMA plot_inputs: preserve the 73/45 colliding native seed-slot lines -- #1336 (5a4b2ac62)
- #1127 Ethiopia 2021-22: acquired ESPS-5 v03_M and corrected provenance -- #1211 (725d66b5c); #1284 (6d8d696b6)
- #1133 Uganda: harmonise `v` (`comm`) across the survey's two renderings (int32 8-digit in 2005-0 -- #1232 (4593442d3)
- #1144 Nigeria food_acquired j is decoded wave-blind: W1 'Other vegetables' served as 'Leaves', ' -- #1178 (989c7abb0)
- #1146 Test-side follow-ups to #1113: baseline generator pins partial parquets; Nigeria sold-unit -- #1367 (88c7b540e)
- #1149 add_wave: on re-acquisition, place each zip member at its existing sidecar's path (nested  -- #1211 (725d66b5c)
- #1150 plot_features: serve the WB plot geovariables (elevation, slope, wetness, distance to dwel -- #1352 (373af66f0)
- #1171 Ethiopia nutrition: diagnose the legacy food_acquired Make prerequisite and unused FCT tar -- #1371 (dc23e3206); #1375 (82362dce0); #1376 (1864a9bd1)
- #1174 Malawi plot_inputs: determine reported use for 437 nonorganic rows whose missing quantitie -- #1523 (14c0f5790)
- #1177 Nigeria: verify inherited W4-PH maize-shell and tinned-milk qualifiers against its instrum -- #1346 (96d9b3970)
- #1183 Ethiopia plot_inputs: diagnose missing seed-purchase answers before recoding them as No/ze -- #1425 (98ac1b27e)
- #1184 Ethiopia W2 plot_inputs: diagnose all-missing holder/crop seed-purchase quantities summed  -- #1330 (d0ed41726)
- #1187 Burkina Faso 2018-19: establish the source meaning of unlabelled agricultural input code21 -- #1427 (bc2e15f4d)
- #1188 Burkina Faso 2018-19 plot_inputs: gift/donation fields are served as purchase fields -- #1219 (7080ad52e)
- #1190 Malawi plot_inputs: preserve seed purchase-unit identity before aggregating or joining pla -- #1694 (7f1a9c954)
- #1196 Cold/warm index StringDtype drift: Niger food_acquired and Uganda cluster_features -- #1212 (1d3c035bd)
- #1198 Uganda crop_production: preserve distinct production records before pairing quantities wit -- #1202 (019b7d05e)
- #1204 Uganda crop_production: merge the reviewed requested-wave assembly repair -- #1217 (8d02d0881)
- #1205 Uganda yield_kg: merge the reviewed parcel-area repair and coverage correction -- #1216 (76556b07b)
- #1209 Malawi: country food_acquired Make builder expects retired wave columns -- #1213 (954c18ca4)
- #1210 Malawi: invalid food wave request evicts native caches before raising -- #1214 (26b8f1982)
- #1215 Constructed empty tables: implement the approved declaration-order rule -- #1220 (a3aad0d43)
- #1218 Constructed empty tables ignore declared types of country-local optional fields -- #1221 (326b6e3de)
- #1224 EHCVM food_acquired: the served u drops the unit SIZE for 60-88% of consumed rows in all e -- #1458 (98c6069d2)
- #1225 _coerce_datetime_column raises on an empty Series with a MultiIndex (swallowed by both dty -- #1302 (3f77cf3ca)
- #1234 map_index mints u='unit' for a missing unit (the #1041 sentinel is 'Unknown') -- #1251 (675e544d5)
- #1237 Malawi Makefile: per-wave rules omit the wave script prerequisite, so the make backend nev -- #1258 (ff3962e23)
- #1240 Burkina Faso panel_ids.json: 1,088 of 3,132 2018-19 targets lack the '0' separator, so a t -- #1249 (1f2b2bead)
- #1243 Burkina Faso: v reuses 1-585 for 2014 zd and EHCVM grappe (different frames); no sampling. -- #1246 (3c41a3717)
- #1244 Burkina household namespaces: review the fix and complete the independent-sample census -- #1322 (a19390fcc)
- #1245 Resolved: single-pass id_walk restores the 80 Burkina panel links -- #1282 (c3674e863)
- #1250 Uganda plot_labor: a MISSING use-flag is served as 'answered No' (PersonDays=0): ~5,200 fa -- #1255 (3c0434d07)
- #1252 Ethiopia harmonize_crop: codes 19 and 86 change meaning across waves (W1/W2 CACTUS served  -- #1274 (6c99c6554)
- #1253 Ethiopia crop_production §11 sales: all-blank groups served as 0 (bare .sum()), 'No' overw -- #1274 (6c99c6554)
- #1254 median_price_valuation: default item_keys=('j',) pools prices across waves (and the nation -- #1399 (d0acdf610)
- #1256 Ethiopia W4/W5 crop_production: permanent-crop rows in §11 use a fixed 12-month window, se -- #1274 (6c99c6554)
- #1259 Site Q: the p90 reference includes zero quantities, so 'did not harvest' rows collapse it  -- #1266 (eff9e25de)
- #1260 Malawi IHPS panel EAs serve no GPS: household geovariables ARE released (2939 HouseholdGeo -- #1279 (8a8278a2a)
- #1261 Nigeria: account for W3 geo-only household outside the cover/roster frame -- #1416 (931376950)
- #1264 Nigeria panel crosswalk: 328 targets not in household_roster, 99 W3->W2 links to absent hh -- #1449 (3067c0352)
- #1267 Mali 2014-15 tree crops: crop_production.Quantity is the NUMBER OF HARVEST MONTHS (s3bq09) -- #1272 (e6ed24ee5)
- #1277 Malawi panel provenance corrected: held files belong to catalog 3819 v06 -- #1286 (09f8af262)
- #1278 Malawi 2019-20 data_info.yml: 'function: cs_i' is a no-op (the recognised key is 'mapping: -- #1523 (14c0f5790)
- #1283 Provenance: secondary held/latest release records implemented -- #1286 (09f8af262); #1293 (eea68586e)
- #1290 Uganda housing 2015-16: reconcile three unsupported keys after recorded-link repairs -- #1415 (724f9bd60)
- #1291 Uganda nonfood IDs: finish other-table rendering check after merged fix -- #1325 (169b0d337)
- #1292 Nigeria individual_education: one post-harvest file read for both rounds serves 56,967 dup -- #1309 (21069ab20); #1318 (188adf03f); #1328 (32db08452); #1332 (d08eac030)
- #1298 Malawi plot_inputs seed rows stay at label grain (two crop codes sharing a label share one -- #1323 (3e2f22f69)
- #1304 Ethiopia quintal regression calls unsupported sum on an Arrow boolean mask -- #1305 (6a8660461)
- #1331 Fix Nigeria source-test oracle import under checkout-only PYTHONPATH -- #1332 (d08eac030)
- #1401 DECISION: Uganda anthropometry zero-month age and repeated-measurement selection -- #1415 (724f9bd60)
- #1432 Tests failing on development: Malawi crop_production (17), Uganda test_sample (4), Mali co -- #1481 (3934879c3)
- #1433 Nigeria crop_production keep-first dedup at (t,i,plot_id,crop) discards distinct sales and -- #1550 (07174098d)
- #1434 Uganda 2015-16 shocks: inner merge on gsec1.hh drops 2 agriculture-cover-only households -- #1478 (5d90d405b)
- #1435 Malawi livestock: sum reducer serves blank HeadCount/HeadAcquired/HeadSold cells as 0 -- #1486 (e92ae6b0f)
- #1439 cfe demands.main fails: food_expenditures(market=) returns (i,t,m,j,s), cfe expects (i,t,m -- #1612 (1fb02d5b2)
- #1440 Pakistan market= raises on a cold data root until cluster_features has been built -- #1647 (6d9563f7a)
- #1443 Ethiopia documentation follow-ups: W2/W3 agriculture-only households, BID acquisition, cal -- #1506 (df8faa75f)
- #1444 DECISION: Nigeria -- should interview status (HHTrack / interview_result) gate D1 roster a -- #1508 (fe62137f1)
- #1445 EHCVM NPK 15/15 is served at NPK 17-17-17's 0.17 N; Malawi fertilizer_rate note is stale -- #1527 (997f33c8a)
- #1447 Ethiopia: panel_ids YAML blocks disagree with the committed JSON, and LSMS_BUILD_BACKEND=m -- #1470 (fdf309523)
- #1448 Nigeria sampling.yml notes quote cover overlaps that disagree with the #1264 crosswalk -- #1550 (07174098d)
- #1450 Kg factors: #1224 makes Mali 2014-15 and Burkina 2014 worse; fix before the next release -- #1474 (56085be48)
- #1452 Wire approved EHCVM survey unit weights for 2018-19 and 2021-22 -- #1597 (0c4004872)
- #1453 Mali community_prices spelling does not join household sized u (#1224 Mali half) -- #1515 (ad44c8d46)
- #1455 LSMS_NO_CACHE=1 on a YAML-path food_acquired writes a degraded parquet graded fresh -- #1504 (4646a0fd2)
- #1456 Feature(labels=...) returns a frame with no country level when every country degrades -- #1490 (c37e5e70a)
- #1457 Multi-plot household-crop sales are still unserved in Malawi, Mali 2017-18, Niger, Tanzani -- #1690 (1e549e96a)
- #1459 Uganda: one design-PSU label across all eight waves (comm vs ea) -- #1534 (3aefbbedf)
- #1461 Ethiopia crop_production: 639 repeated native groups need production_obs -- #1477 (cbcf85f8f)
- #1462 Niger: Condition_sold and the dry-season harvest modules are unread -- #1527 (997f33c8a)
- #1463 Soft drop of a dfs: sub-df does not catch an unreachable remote (EndpointConnectionError) -- #1646 (af91b3f71)
- #1464 Burkina 2014 valuation frame loses v on 6,841 rows that food_acquired has -- #1522 (19db1a068)
- #1467 Ethiopia crosswalk: link 48 W3 households the BID counts as ESS1; record Panel II and W2-W -- #1558 (45f713df9)
- #1468 LSMS_BUILD_BACKEND=make without make on PATH reruns panel_ids.py and rewrites committed JS -- #1643 (a63306951)
- #1469 crop_diversity(weight='count') counts a Farm-level row as an occurrence -- #1491 (153d18b47)
- #1475 Crop transforms do not group on country, so pooled frames sum across countries -- #1545 (0abbb1723)
- #1476 harvest_kg silently deletes null-plot_id rows (Mali tree rows, 185,908 kg) -- #1507 (b95e927f3)
- #1480 test_uganda_invariance skips silently on a partly built root, so it can check 3 of 53 entr -- #1609 (c780562b2)
- #1487 A wave's kilograms depend on which other waves were requested: 10 of 50 cells differ by mo -- #1511 (807393d6e)
- #1489 Senegal 2018-19 food_prices has values up to 1.79e22 on bottle units -- #1496 (cf5a4bbf6)
- #1493 Nothing screens a near-zero Quantity or food_prices: Senegal's quantities near 1e-19 put 1 -- #1544 (167d566d0)
- #1494 Mali EHCVM 2021–22: namespace raw household IDs and preserve approved panel links -- #1638 (dd40aae55)
- #1495 Farm-level rows that carry a harvest: data_info.yml says such a row holds a sale -- #1644 (11714b185)
- #1499 _normalize_dataframe_index concatenates a string-typed additive column on a repeated key -- #1636 (5695e0855)
- #1500 Senegal 2018-19: 21 food_acquired rows serve NaN on the declared u -- #1641 (6863a6bf6)
- #1501 LSMS_NO_CACHE: route the L2-wave write gate in Wave.grab_data through _no_cache at the nex -- #1642 (29de1acf3)
- #1503 Test order: fixtures repoint countries_root without clearing derivation_records, giving 17 -- #1521 (84c0c3e4e)
- #1510 _collect_file_paths_from_block skips file: members with a per-file override, so a new rele -- #1601 (f51ccf213)
- #1512 Four small follow-ups from #1487's review -- #1542 (ae965b29a)
- #1513 Mali 2018-19 household j never receives harmonize_food: labels carry seven leading spaces -- #1587 (2a888bcea)
- #1516 household_characteristics reads a build failure as absence -- #1589 (efd817667)
- #1517 Uganda yield_kg on a Feature frame gives 14,520 rows against 27,755 on a Country frame -- #1545 (0abbb1723); #1600 (3b28d293f)
- #1520 Country.wave_folder_map is empty until Country.waves has been read -- #1628 (d82ecf6e3)
- #1524 A guard raised from a by-path mapping.py can become an empty derived table under parallel  -- #1589 (efd817667)
- #1526 demand-estimation skill: the recipe raises 'y must have index (i,t,m,j)' because of the s  -- #1612 (1fb02d5b2)
- #1528 Burkina Faso and Mali crop_production: Condition_sold is recorded and not read -- #1605 (bcd77200f)
- #1532 Strict-mode errors that subclass RuntimeError may be swallowed by the food dispatch -- #1589 (efd817667)
- #1535 Feature('crop_production') cannot to_parquet: harvest_month mixes float and string -- #1608 (72197c8ee)
- #1540 EHCVM crop_production: condition helpers now live in eight files -- #1633 (5cd21347d)
- #1546 _join_v_from_sample swallows ValueError at INFO, so a sample failure serves tables without -- #1589 (efd817667)
- #1547 NoCredentialsError is outside the #515 handler -- #1629 (bee011377)
- #1549 Stale counts in framework docstrings after Nigeria #1433 -- #1626 (a799ce58d)
- #1554 Niger 2011-12 onion dates and region investigation documented -- #1657 (86077a65f)
- #1561 CotedIvoire 2018-19 shocks: decode Stata .d not-concerned answers -- #1661 (53b0c26c9)
- #1569 Site Q does not screen Quantity_sold or sale price -- #1650 (42a25fabc)
- #1573 Validate and integrate approved Burkina 2014 strata labels -- #1690 (1e549e96a)
- #1575 Wire documented region × milieu sample strata for Benin, Togo, Guinea-Bissau and Côte d’Iv -- #1693 (e5fdd2fa8)
- #1576 Mali Region: fold Taoudenit to Taoudenit with its accent, and drop the extra strata pair -- #1611 (ff3276096)
- #1578 derivations.yml accepts a duplicate key silently -- #1624 (bb828626f)
- #1585 conversion._interview_dates fails on `Interview start`: seven countries convert numeraire= -- #1595 (65b1730f0)
- #1588 Feature: a country served without v wins the shape filter and costs a sibling its rows -- #1619 (631bb6d84)
- #1590 Prevent failed waves and corrupt crosswalks from becoming trusted cached data -- #1599 (c964c6c4f)
- #1591 _parallel_waves: an exception class only the child imported reads as a dead worker -- #1625 (ebb87135c)
- #1592 market= with sample missing warns of a fallback, then raises KeyError -- #1623 (b8346cf1f)
- #1594 Mali 2014-15 food_expenditures(numeraire=FX) is about 70x the later waves -- #1634 (fdec032ef)
- #1602 Prewarm misses sidecars for bare Data-relative source paths -- #1610 (1ce4f525b)
- #1603 Unqualified shipped crop factors lack their documented pooled-country warning -- #1613 (2df9fb9bd)
- #1606 Diagnostics: distinguish native-cache and finalized dtypes for canonical overrides -- #1627 (b284112ed)
- #1616 tlu() pools distinct households when supplied countries share local (t,i) keys -- #1620 (8a35e1c22)

### closed on maintainer ruling (listed PR delivered related code or docs) (5)

- #660 Ethiopia: establish the release-side cause of the agriculture/household-cover discrepancy -- #1412 (e574ff316)
- #1056 Ethiopia interview_date serves 33 device-clock dates years outside fieldwork (W4 15, W5 18 -- #1413 (62f6aaa99)
- #1297 Uganda parcel recovery: decide how to treat a unique complete candidate with an incomplete -- #1338 (1542a886f); #1350 (123fef6af); #1358 (c4ae83cfb)
- #1357 DECISION: Ethiopia W3 agriculture rows have field identity but no recorded household ident -- #1359 (af0a53ee0); #1365 (8578ec314); #1371 (dc23e3206); #1374 (0b1effb22); +13 more
- #1360 Niger crop_production: resolve Make/default ordering and index mismatch -- #1477 (cbcf85f8f)

### ruling: not planned (2)

- #1206 Campaign validation: finish exact numeric and object-type saved-result reassessment -- #1208 (529161c4e)
- #1551 Nigeria #843: attach W3-W5 sales by the sale's own crop code? -- ruled "Keep Farm-level" 2026-11-12, no code change

### duplicate (NOT_PLANNED) (2)

- #913 DECISION: GhanaLSS 1991-92 own production -- value the 28% of rows carrying both factors ( -- duplicate of #1130
- #1089 Mali community_prices reads only relevé 3 of three -- duplicate of #822 (related: #1180 (94098ff45))

### fix already in v0.16.0 (3)

- #562 GhanaLSS Phase 3: community_prices feature + inverse price->quantity imputation (split fro -- via #762 (f653104a8, in v0.16.0); closed 2026-11-12, Phase 3b split to #1699
- #778 Tests that rebuild in the SHARED root under LSMS_NO_CACHE still open the #866 delete-then- -- via #1030 (dbab68d8c), #1162 (5519de059), both in v0.16.0; closed 2026-11-12
- #798 Interview-date coverage: refresh final-source raw-cache and served-API matrix -- via #1025 (f8e14a7c2), #1105 (e87a551e8), both in v0.16.0; closed into #1438 on 2026-11-12

