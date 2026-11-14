
# GH #1562 coping-strategy audit (stream 1562-coping-audit, read-only)

Base: development `f138a3d61c203f892148592675a7ef72e160a322`. **No commits, no worktree, no tracked file edited. Branch head: none.** All Python ran in co_carleton burst jobs on private, initially empty data roots (`tools/burst-env.sh`). Hash probes edited job-local `cp -a` copies of the base checkout only.

Rulings this first step serves (`isa-decisions.org`, quoted):

> **2026-11-12: Coping-strategy contract, GH1562 (ANSWERED).** "Positions = respondent order (Recommended)". HowCoped0..2 mean the household's REPORTED order (1st/2nd/3rd) and are filled only where an instrument records an order (Uganda s16q04a/b/c, ...). Every shocks country also gets an UNORDERED set column of chosen strategies, so "which strategies" composes everywhere. EHCVM 2018-19 fills the set only (not positions) until its order is proven from export documentation. First step: audit every shocks country's coping semantics. Canonical data_info.yml shocks columns change (a new set column; position semantics stated).

> **2026-11-12: EHCVM coping order, GH1562.** "...EHCVM's code order is therefore not composable even if documented."

## 1. What each instrument records

17 countries, 52 served logical waves; Nigeria serves 8 `t` from 4 post-harvest files. Per-wave variables, labels, questionnaire page and verdicts are in `out/per_wave_instrument.tsv`; counts are in `out/per_wave_measured.tsv`.

| Country | Waves | What the instrument records | Served positions = reported order today? | Set buildable from what reaches the build? |
|---|---|---|---|---|
| Uganda | 2005-06 … 2019-20 (8) | Up to 3, ranked. Questionnaires: "UP TO 3 ANSWERS WITH RANK". 2005-06 and 2009-10 rest on variable labels ("first/second/third", "1st/2nd/3rd"). | **Yes.** Filler 40 "Nothing else" (2013-14, 2015-16) and, apparently, 96 "Other (specify)" (2010-11, 2011-12) are served as strategies. | Yes, after fillers are dropped |
| Malawi | 2004-05, 2010-11, 2013-14, 2016-17, 2019-20 | Up to 3 "BY ORDER OF IMPORTANCE" in every held questionnaire. | **Yes.** 2004-05 serves filler 99 "No other response given" on 27,046 rows. | Yes, after the filler is dropped |
| Ethiopia | 2011-12 … 2021-22 (5) | Up to 3 "BY ORDER OF IMPORTANCE" in all five questionnaires. | **Yes.** W1 serves unlabelled codes as "0.0", "21.0" and similar. | Yes |
| Tanzania | 2008-09 … 2020-21 (6) | Harmonised panel file (UPD): 3 slots headed 1ST/2ND/3RD. 2019-20 and 2020-21: 2 slots, 1ST/2ND. | **Yes** from 2012-13. 2008-09 and 2010-11 carry no coping: 0 of 10,241 and 0 of 10,216 experienced rows. | Yes; empty in rounds 1-2 |
| Iraq | 2012 | Up to 3 "BY ORDER OF IMPORTANCE" (Part 3, p19). | **Yes** | Yes |
| Nigeria | 4 PH files → 8 `t` | W1–W3: up to **4**, ranked (columns 1st…4th). W4: up to 4, "SELECT ALL THAT APPLY". | W1–W3 slots 1–3: yes. **W4: no**, no order is asked for. **The 4th slot is not wired.** | **No**: the 4th slot is lost on 274 / 165 / 150 / 31 rows (W1–W4) |
| Burkina 2014, Mali 2014-15 and 2017-18, Niger 2011-12 and 2014-15 | 5 | Up to 3 "par ordre d'importance", columns 1ère/2ème/3ème. Burkina 2014 rests on its labels "1re/2e/3e stratégie". | **Yes.** "Aucune stratégie"/"Nothing" fills later slots; "Manquant" (99) or "99.0" is served as a label. | Yes, after fillers and missing codes are dropped |
| **EHCVM 2018-19**: Benin, Burkina, CIV, GNB, Mali, Niger, Senegal, Togo | 8 | Paper form: up to 3 "par ordre d'importance". CIV CAPI: "CHOIX MULTIPLE: ORDONNÉ". **The release** is a per-option grid `s14q05__k` with values {0,1,3}; Mali and Senegal also have 2s, only on code 26. The meaning of 3 is not established. | **No.** The hooks list options in code order. Value order and code order disagree on Benin 326, Burkina 156, CIV 331, GNB 165, Mali 315, Niger 249, Senegal 80 and Togo 81 rows, which reproduces the issue's counts. | Today yes: at most 3 positives, Benin at most 2. **No once the positions are emptied**: the set must be written at build time |
| **EHCVM 2021-22**: Benin, Burkina, CIV, Mali, Niger, Senegal, Togo | 7 | Rank per option (Première/Deuxième/Troisième/Non). Burkina: unlabelled raw ranks. Code 25 "Autre" is labelled Oui/Non, so its rank is not in the file, except in Burkina. | **Yes**, except code 25. Its slot is inferred beside another strategy on Benin 13 and Niger 17 rows; 2 Niger rows are placed first. | Yes (0 rows choose more than 3) |
| Guatemala | 2000 | **One action** per (household, problem): 1 DETALLE row per (hogar,item) in all 11,685. Label "accion de solucion tomada". | n/a: a single answer, served as HowCoped0 | Yes |
| Liberia | 2018-19 | Up to 3 **forest products** "by order of importance", asked only if forest products were used (S17_4). Not a coping-strategy question. | Ordered, but of products. Filler "no other product" on 342 rows. | Yes, as products |
| Albania | 2012 | No coping question (psu, hh, M6D_Q00, M6D_Q01, year flags). | n/a | Nothing |

**The answer to the ruling's premise.** Every non-EHCVM instrument except Nigeria W4 asks for an order. For Uganda 2005-06/2009-10 and Burkina 2014 the evidence is the variable labels. EHCVM 2021-22 records a rank per option. Only **EHCVM 2018-19** (the release grid) and **Nigeria W4** ("select all that apply") lack an order. Guatemala and Albania have nothing to order.

No slot instrument is code-sorted (`out/extra2.json`). Among rows with 2 or more named codes, the share whose codes ascend left to right is 0.25–0.83, never about 1.0. Nigeria W4 is 0.63: it is not code-sorted, but no order was requested.

## 2. Findings beyond the contract (pre-existing; not fixed)

**F1. Guinea-Bissau 2018-19: codes 18–25 are very probably mislabelled.**
- The hook's `COPING_LABELS` and the release's variable labels use the French 26-option list (18 = "Louer/mettre ses terres en gages", 25 = "Autre stratégie").
- The Portuguese questionnaire (`gnb_ehcvm1_qnr_household_excel_vague1.xlsx`, sheet S14_Chocs) has 25 options and no "Louer…": 18 "Venda das existências de provisões", 19 "…pesca", 20 "Venda de gado", 21 "Confiar os filhos…", 22 "…atividades espirituais", 25 "**Nenhuma estratégia**". Codes 23–24 were not extracted.
- The data side with the questionnaire. Per-mille share of all positive cells, the other seven files against GNB:
  - code 20 (French "pêche"): others 0–2, GNB 27;
  - code 21 (French "Vente de bétail"): others 9–149, GNB 3;
  - code 25 (French "Autre"): others 6–35, GNB 137 on 1,540 rows, never beside another option. The others' code 26 "Aucune" is 105–313 per mille and also never sits beside another option, apart from Mali and Senegal.
- Under the shifted alignment, GNB 20 → bétail (27) and 25 → aucune (137) fall inside the others' ranges.
- `Guinea-Bissau/_/CONTENTS.org` says only: "The existing coping-code-25 interpretation is not adjudicated by this food-effects change."

**F2. Nigeria.**
- (a) Each post-harvest `sect15a` block is served under both quarters of its wave: 22,550 rows for 11,275 household-shocks. The two copies are identical on every measured aggregate; I made no row-level comparison.
  - The mechanism is documented: `2010-11/_/data_info.yml:5` says "This file is read once per ROUND (both quarters of the folder map to it)", and `Nigeria/_/CONTENTS.org:923` says a YAML block "reads one file and is stamped with every round of its directory".
  - `individual_education` in the same file pins `t: '2011Q1'`. The `assets` block has the same unpinned shape.
  - No CONTENTS passage says shocks is meant to be served under the post-planting quarter.
- (b) The 4th slot is not wired: `s15aq5d` in W1–W3, `s15aq7_4` in W4.
- (c) W4 asks "SELECT ALL THAT APPLY" (ph_w4_household_quest.pdf p62).
- (d) Unlabelled codes are served as "31.0", "41.0", "51.0" (W2, 20 cells).

**F3. Filler, terminator and missing codes are served as strategies.**
- Uganda code 40 "Nothing else": 1,035 slot cells in 2013-14 and 1,065 in 2015-16. "Nothing else" fills all three slots on 55 and 41 rows.
- Uganda code 96 in 2010-11 and 2011-12, in filler patterns: ('k','96','96') on 165 and 131 rows, ('96','96','96') on 129 and 72. That gives 569 and 442 served rows that repeat a label. Nothing documents 96 as a filler.
- Malawi 2004-05 code 99 "No other response given": 27,046 rows. "Dont know": 185 cells.
- Liberia "no other product": 342 rows.
- "Aucune"/"Nothing" after a strategy in Burkina 2014, Mali 2014-15/2017-18 and Niger 2011-12/2014-15. This is the large "none+strat" count in §4.
- "Manquant": Mali 2014-15 177 cells, Niger 2014-15 207. "99.0": Niger 2011-12 317, Uganda 2009-10 1. Ethiopia W1 numeric strings: 10.

**F4. Shocks without any follow-up answer are not served, a framework property.** Rows whose non-index columns are all NA are dropped, so a served shocks table holds only experienced shocks that carry a follow-up. Measured as experienced → served (`out/extra3.json`):

| Wave | Experienced | Served |
|---|---|---|
| Malawi 2004-05 | 49,038 | 28,425 |
| Malawi 2019-20 | 44,757 | 21,024 |
| Ethiopia W1 | 3,630 | 3,552 |
| Ethiopia W5 | 6,593 | 5,823 |
| Nigeria W1 | 2,428 | 2,202 |
| Nigeria W2 | 2,992 | 2,882 |
| Nigeria W3 | 2,321 | 2,321 |
| Nigeria W4 | 3,870 | 3,870 |
| Tanzania R1 | 10,241 | 7,391 |
| Tanzania R2 | 10,216 | 7,455 |
| Tanzania R3 | 10,741 | 8,247 |
| Tanzania R4 | 8,526 | 6,722 |

- These instruments ask follow-ups only of the 3 most significant shocks, and EHCVM 2021-22 only of the last 12 months, which is "with the maintainer".
- **Liberia is the extreme case.** The always-asked follow-ups (S17_3, S17_4) are not wired, so 2,164 of 3,522 experienced shocks vanish. 1,358 are served; S17_4 = no on 2,172.
- **So a HowCoped0 fill of about 1.000 is partly by construction.**

**F5. Tanzania 2008-09 and 2010-11 coping is 100% NA in the UPD release** (0/10,241 and 0/10,216), and Site B warns. CONTENTS records an original-R1-form read: "In =R SHOCKS=, B4/D4/E9 establish a past-five-year severe-event question and follow-ups limited to the three worst events. The inspected R question regions are questions1–5 … the bounded K/R inspection is not a whole-form negative keyword/fielding verdict." Whether coping was asked in R1/R2 is not established here.

**F6. Uganda 2005-06's relabel map folds two code pairs into one label.** Formal and informal borrowing both become "Obtained credit"; crop choice and technology both become "Changed cropping practices". That gives 14 served rows with a repeated label the source does not repeat.

**F7. "None" beside a real strategy.**
- Ethiopia W1–W3: 260 / 388 / 446 rows.
- Iraq: 412.
- Malawi: 257–530.
- Uganda 2018-19 and 2019-20: 69 and 70.
- Mali 2018-19: code 26 positive beside others on 444 rows.
- EHCVM 2021-22, "Aucune" ranked beside others: Benin 27, CIV 56, Mali 118, Senegal 73, Togo 16.

**F8. Contradiction with `Guatemala/_/CONTENTS.org`.** It says "shocks — implementable, not yet wired (2026-06-07) … =shocks= is not yet wired". Shocks is wired (`2000/_/shocks.py`) and serves 11,752 rows.

**F9. Variable labels in Mali 2014-15 and Niger 2014-15** say "…après le 1er/2ème/3ème **CHOC**". The questionnaires (EACI2014 MEN P2 p14; ECVMA2 MEN P1 p61) head the same columns 1ère/2ème/3ème **stratégie**.

## 3. Idiosyncrasies per country (from CONTENTS.org and the configs)

- **Uganda.**
  - `shocks: !make`.
  - #805: cp1252 labels are decoded ("…etc…)").
  - #1434: 2015-16 uses AGSEC1 as a fallback.
  - 2005-06 has a hand relabel map (F6).
  - 2013-14 adds code 40.
  - 14,459 rows; the ruling log records 14,457 → 14,459 (#1478).
  - 1 NaN `Shock` row in 2005-06 (NullIndexKeyWarning).
- **Malawi.**
  - 2016-17 and 2019-20 use the Cross_Sectional file and `cs_i` ids.
  - IHS2 Module AB has no food-effect questions.
  - 2024-25 is unwired.
  - The 2004-05 collapse destroys 291 rows (GrainCollapseWarning).
  - Coping equals the source (CONTENTS 2026-11-06).
- **Ethiopia.**
  - #875: one builder.
  - CONTENTS: "The =Shock= index is *not harmonized across waves*". The same is true of the coping labels: W4/W5 say "19. DID NOT DO ANYTHING", W1–W3 "Did Not Do Anything".
  - `Occurrence` exists in W1–W3 only.
- **Tanzania.**
  - 2008-15 is one folder with four rounds; `i = r_hhid` (#637).
  - 2019-20 and 2020-21 have YAML blocks, but `materialize: make` scripts serve, reading `hh_r04_1/2`.
  - R1/R2 have no coping (F5).
  - The back-casting design is in CONTENTS.
- **Iraq.** The 23-shock cross-product is filtered on q2001. CONTENTS flags its own q2005 claim as false. 2006-07 is unwired.
- **Nigeria.**
  - No shocks passage in CONTENTS.
  - Folders are read once per round (F2a).
  - The W1–W3 labels call the coping question a "consequence".
- **Liberia.** The NHFS covers a forest-proximate universe. Its slots are forest products (F4).
- **Guatemala.** A single `accion` per problem; HowCoped1/2 are omitted by design. 1,885 shocks appear only in RESUMEN and have no action. CONTENTS is stale (F8).
- **Albania.** The 2012 table is occurrence-only. The 2003/04/05/08 modules exist and are unwired, per the data_scheme correction.
- **EHCVM 2018-19.**
  - The 14.04 layout differs by vague (#1556, `ShockLayoutError`).
  - Benin's hook stops at 2. CONTENTS: "no household with more than 2 strategies marked", which the measurement confirms.
  - CIV's hook is in the **country module** `cotedivoire.py` and drops HowCoped2 when that column is all-null.
  - GNB has 25 options (F1).
  - Togo 2018 asks recall over "12 derniers mois" (issue body; not rechecked).
  - Prior evidence, consistent with this audit: `isa-sm-20261103/reports/1562-civ2018-coping-structure-20261109/REPORT.md` and `isa-sm-20261103/reports/review-ehcvm-shocks/r7b_cope18.txt`.
- **EHCVM 2021-22.**
  - Code 25 is Oui/Non and its rank is inferred (#1556). It stands alone in every Mali, Senegal, CIV and Togo row.
  - Only rows with follow-ups are served, for example Benin 4,756 of 10,982.
  - Burkina's file is unlabelled raw ranks.
  - CIV keys are damaged (GrainCollapseWarning, 44 rows destroyed).

## 4. Measurements (cold, private roots)

**Jobs.**
- Served table: 41271540–3; label classes: 41271695–6.
- Released source files: 41271544, 41271545, and 41271713 for the GNB rerun.
- Targeted checks: 41271728 (`extra.json`), code order 41271832 (`extra2.json`), experienced vs served 41272014 (`extra3.json`).
- Questionnaires: 41271697 (`out/qnr/`).
- Hash probes: 41271547, 41271759 and 41271888.

**Column meanings.**
- *fill*: non-null share among served rows (see F4).
- *dup*: served rows that repeat a label.
- *set*: distinct named strategies per row, with filler, missing and "none" labels excluded; n:rows.
- *none*: the only answer is did-nothing/aucune.
- *none+strat*: did-nothing beside a strategy. In the pre-EHCVM French forms this is mostly "Aucune/Nothing" used as a filler.

| Country | t | rows | fill0 | fill1 | fill2 | dup | set size | none | none+strat |
|---|---|---|---|---|---|---|---|---|---|
| Albania | 2012 | 1532 | - | - | - | 0 | (no coping) | | |
| Benin | 2018-19 | 12305 | 1.000 | 0.203 | 0.000 | 0 | 0:1956 1:7846 2:2503 | 1956 | 1 |
| Benin | 2021-22 | 4756 | 1.000 | 0.319 | 0.071 | 0 | 0:890 1:2373 2:1160 3:333 | 890 | 27 |
| Burkina_Faso | 2014 | 11596 | 0.996 | 0.384 | 0.096 | 89 | 0:2098 1:5141 2:3447 3:910 | 2054 | 162 |
| Burkina_Faso | 2018-19 | 8766 | 1.000 | 0.168 | 0.000 | 0 | 0:1078 1:6218 2:1470 | 1077 | 0 |
| Burkina_Faso | 2021-22 | 4475 | 1.000 | 0.250 | 0.057 | 0 | 0:1366 1:1989 2:866 3:254 | 1366 | 0 |
| CotedIvoire | 2018-19 | 13778 | 1.000 | 0.160 | 0.000 | 0 | 0:2137 1:9444 2:2197 | 2137 | 0 |
| CotedIvoire | 2021-22 | 7528 | 0.994 | 0.286 | 0.077 | 0 | 0:1833 1:3591 2:1530 3:574 | 1787 | 56 |
| Ethiopia | 2011-12 | 3553 | 0.978 | 0.562 | 0.301 | 91 | 0:491 1:1233 2:1004 3:825 | 427 | 260 |
| Ethiopia | 2013-14 | 3128 | 0.980 | 0.495 | 0.201 | 95 | 0:572 1:1249 2:947 3:360 | 530 | 388 |
| Ethiopia | 2015-16 | 5541 | 0.996 | 0.527 | 0.221 | 79 | 0:854 1:2062 2:1651 3:974 | 836 | 446 |
| Ethiopia | 2018-19 | 3736 | 1.000 | 0.308 | 0.119 | 0 | 0:940 1:1646 2:704 3:446 | 940 | 0 |
| Ethiopia | 2021-22 | 5823 | 1.000 | 0.327 | 0.125 | 0 | 0:953 1:2966 2:1179 3:725 | 953 | 1 |
| Guatemala | 2000 | 11752 | 0.840 | - | - | 0 | 0:4834 1:6918 | 2949 | 0 |
| Guinea-Bissau | 2018-19 | 9487 | 1.000 | 0.176 | 0.001 | 0 | 1:7813 2:1669 3:5 (code 25 counted as a strategy; F1 says 1,540 are "none") | — | 0 |
| Iraq | 2012 | 8812 | 1.000 | 0.550 | 0.360 | 56 | 0:1779 1:2323 2:1884 3:2826 | 1779 | 412 |
| Liberia | 2018-19 | 1358 | 1.000 | 0.995 | 0.993 | 236 | 1:243 2:109 3:1006 | 0 | 0 |
| Malawi | 2004-05 | 28389 | 1.000 | 1.000 | 1.000 | 22146 | 0:9532 1:12885 2:4745 3:1227 | 9407 | 327 |
| Malawi | 2010-11 | 17422 | 0.999 | 0.096 | 0.027 | 259 | 0:8459 1:7798 2:1007 3:158 | 8448 | 530 |
| Malawi | 2013-14 | 9952 | 0.999 | 0.351 | 0.055 | 13 | 0:1975 1:4716 2:2761 3:500 | 1969 | 266 |
| Malawi | 2016-17 | 27904 | 1.000 | 0.255 | 0.074 | 0 | 0:6233 1:14791 2:4847 3:2033 | 6233 | 257 |
| Malawi | 2019-20 | 21024 | 1.000 | 0.183 | 0.023 | 0 | 0:5990 1:11525 2:3077 3:432 | 5990 | 380 |
| Mali | 2014-15 | 5837 | 1.000 | 0.883 | 0.783 | 353 | 0:728 1:725 2:967 3:3417 | 683 | 1379 |
| Mali | 2017-18 | 6216 | 1.000 | 1.000 | 1.000 | 1024 | 0:251 1:773 2:1077 3:4115 | 251 | 1846 |
| Mali | 2018-19 | 9837 | 1.000 | 0.207 | 0.004 | 0 | 0:1922 1:6279 2:1636 | 1922 | 444 |
| Mali | 2021-22 | 4884 | 1.000 | 0.238 | 0.056 | 0 | 0:1040 1:2756 2:861 3:227 | 1040 | 118 |
| Niger | 2011-12 | 4298 | 1.000 | 0.664 | 0.389 | 374 | 0:1149 1:1588 2:770 3:791 | 1139 | 1534 |
| Niger | 2014-15 | 3781 | 1.000 | 0.802 | 0.538 | 176 | 0:735 1:1127 2:880 3:1039 | 733 | 1743 |
| Niger | 2018-19 | 6551 | 1.000 | 0.173 | 0.000 | 0 | 0:922 1:4498 2:1131 | 922 | 0 |
| Niger | 2021-22 | 2876 | 1.000 | 0.353 | 0.073 | 0 | 0:457 1:1405 2:804 3:210 | 457 | 0 |
| Nigeria | 2010Q3 = 2011Q1 | 2200 ×2 | 0.978 | 0.434 | 0.236 | 55 | 0:426 1:918 2:415 3:441 | 426 | 46 |
| Nigeria | 2012Q3 = 2013Q1 | 2882 ×2 | 1.000 | 0.408 | 0.176 | 0 | 0:803 1:1011 2:596 3:472 | 802 | 133 |
| Nigeria | 2015Q3 = 2016Q1 | 2323 ×2 | 1.000 | 0.434 | 0.190 | 0 | 0:540 1:821 2:538 3:424 | 540 | 64 |
| Nigeria | 2018Q3 = 2019Q1 | 3870 ×2 | 1.000 | 0.201 | 0.069 | 0 | 0:2246 1:846 2:513 3:265 | 2246 | 0 |
| Senegal | 2018-19 | 6131 | 1.000 | 0.081 | 0.000 | 0 | 0:2038 1:3634 2:459 | 2038 | 37 |
| Senegal | 2021-22 | 4182 | 1.000 | 0.257 | 0.052 | 0 | 0:1228 1:1935 2:816 3:203 | 1227 | 73 |
| Tanzania | 2008-09 | 7391 | 0 | 0 | 0 | 0 | 0:7391 | 0 | 0 |
| Tanzania | 2010-11 | 7455 | 0 | 0 | 0 | 0 | 0:7455 | 0 | 0 |
| Tanzania | 2012-13 | 8247 | 1.000 | 0.043 | 0.004 | 7 | 0:4428 1:3496 2:301 3:22 | 4425 | 37 |
| Tanzania | 2014-15 | 6722 | 1.000 | 0.064 | 0.000 | 0 | 0:3699 1:2635 2:388 | 3699 | 41 |
| Tanzania | 2019-20 | 864 | 1.000 | 0.065 | (2 slots) | 0 | 0:377 1:435 2:52 | 377 | 4 |
| Tanzania | 2020-21 | 5067 | 1.000 | 0.069 | (2 slots) | 0 | 0:2436 1:2280 2:351 | 2436 | 0 |
| Togo | 2018 | 7230 | 1.000 | 0.083 | 0.000 | 0 | 0:1307 1:5324 2:599 | 1307 | 0 |
| Togo | 2021-22 | 2666 | 1.000 | 0.262 | 0.034 | 0 | 0:829 1:1150 2:601 3:86 | 829 | 16 |
| Uganda | 2005-06 | 3644 | 0.902 | 0.461 | 0.143 | 14 | 0:223 1:1800 2:1188 3:433 | 0 | 0 |
| Uganda | 2009-10 | 2667 | 0.975 | 0.434 | 0.123 | 19 | 0:55 1:1476 2:821 3:315 | 0 | 0 |
| Uganda | 2010-11 | 1541 | 0.996 | 0.914 | 0.861 | 569 | 0:7 1:356 2:421 3:757 (96 counted as a strategy) | 0 | 0 |
| Uganda | 2011-12 | 1288 | 0.995 | 0.911 | 0.886 | 442 | 0:6 1:286 2:297 3:699 (same) | 0 | 0 |
| Uganda | 2013-14 | 1572 | 1.000 | 1.000 | 1.000 | 654 | 0:55 1:390 2:575 3:552 | 0 | 0 |
| Uganda | 2015-16 | 1023 | 1.000 | 1.000 | 1.000 | 334 | 0:41 1:277 2:452 3:253 | 0 | 0 |
| Uganda | 2018-19 | 1472 | 1.000 | 0.457 | 0.148 | 0 | 0:220 1:614 2:454 3:184 | 220 | 69 |
| Uganda | 2019-20 | 1252 | 1.000 | 0.373 | 0.131 | 0 | 0:226 1:598 2:295 3:133 | 226 | 70 |

**What does not reach the build.**
- Nigeria's 4th slot (274 / 165 / 150 / 31 rows).
- Text-only "other, specify" columns: Ethiopia W4/W5 `s9q04_os` (22 and 24 rows), Malawi 2010-11 `hh_u04*_os` (407 on a), Nigeria W4 `s15aq7_os` (14), Malawi 2004-05 `ab07*oth`.

Elsewhere the served slots equal the source slots. EHCVM 2018-19 has at most 3 positives (Mali 43 rows, Senegal 1, GNB 5; Benin at most 2). EHCVM 2021-22 has at most 3 chosen.

**Delimiters.**
- No served coping label in any country contains `+`, which is `derivations.MULTI_SEP`.
- Commas occur in Malawi (7 labels), Burkina (2), Liberia (3) and Uganda (1), so a comma join (Albania's `Years`) would be ambiguous.
- The longest joined set is about 300 characters.

**Invalid fields.** The `none_like*` fields in `out/served/*.json` and `out/raw/*.json` came from a regex that matched "friends". Use `out/served2/` instead.

## 5. Options and measured cost

**Method.** Census (`tools/hash_census.py` + `hashdiff.py`) of base against job-local edited copies. Totals: 42 btf, 38 fwimp, 555 L2-country, 2,321 L2-wave. The positive controls moved. Outputs are in `out/hash/`, `out/hash2/` and `out/hash3/`.

| Probe | btf | fwimp | tables | waves | Where |
|---|---|---|---|---|---|
| Canonical `data_info.yml`: the §6 entry, set `optional: true` | 0 | 0 | 0 | 0 | none |
| Same with `required: true` | 0 | 0 | 0 | 0 | none (17 schema tests fail, §7) |
| Read-time hook called from `Country._finalize_result` | 0 | 0 | 0 | 0 | none |
| Label-class registry as `.yml`, country or global | 0 | 0 | 0 | 0 | none |
| … as a table in `Uganda/_/categorical_mapping.org` | 0 | 0 | 28 | 224 | all of Uganda |
| … as a new global `categorical_mapping/*.org` | 0 | 0 | 555 | 2,291 | corpus |
| New YAML `derived:` kind (`_DERIVED_TRANSFORMERS`) | **42** | 0 | 555 | 2,291 | corpus cold |
| New unreferenced function in `ehcvm.py` | 0 | 0 | 0 | 0 | none |
| No-op in imported `ehcvm.community_price_slots` | 0 | 0 | 92 | 92 | Benin, Burkina, GNB, Senegal |
| Benin 2018-19 `mapping.shocks` | 0 | 0 | 22 | 22 | all Benin tables |
| Iraq `2012/_/2012.py` | 0 | 0 | 10 | 10 | |
| Uganda `2019-20/_/shocks.py` | 0 | 0 | 28 | 28 | |
| Tanzania `_/shocks.py` (country script) | 0 | 0 | 25 | **150** | every Tanzania wave-table |
| Ethiopia `2011-12/_/data_info.yml` (add a myvar) | 0 | 0 | 25 | 25 | |
| Malawi `_/data_scheme.yml` (declare the set) | 0 | 0 | 26 | 0 | L2-country only |
| Niger `_/derivations.yml` | 0 | 0 | 0 | 0 | none |
| **Option (i)**: every shocks wave's input, 16 countries | 0 | 0 | **343** | **1,405** | 16 countries; only 16 of the 343 are `shocks` |
| **Option (iii)**: EHCVM 2018-19 hooks (7 `mapping.py` + `cotedivoire.py`) + GNB `data_scheme.yml` (HowCoped0..2 optional) | 0 | 0 | **184** | **289** | Benin 22/22, Burkina 25/25, CIV 21/**126**, GNB 22/22, Mali 24/24, Niger 25/25, Senegal 23/23, Togo 22/22 |

**No country `data_scheme.yml` declaration is needed** for a read-time column.
- Uganda's `shocks: !make` declares no columns and serves ten.
- `feature._harmonize_country_frame` drops only all-NaN columns (`feature.py:868-877`); it does not select by declaration.
- Declaring the set in all 16 schemes would cost their 343 L2-country hashes and 0 wave hashes, by the Malawi mechanism: a re-aggregate, not a cold rebuild.

**(i) Build-time everywhere.**
- 343 / 1,405 hashes: every table of 16 countries re-warms cold, because wave `mapping.py`, `data_info.yml` and `.py` scripts are in every table's input hash.
- YAML-only waves need a formatting function plus a myvar, at the same cost. Do not use a shared `derived:` kind.
- What it buys: a set that is complete, including Nigeria's 4th slot.

**(ii) Read-time from the build output.**
- 0 hashes, with the label classes in `.yml` or canonical `data_info.yml`.
- Incomplete for Nigeria today.
- Impossible for EHCVM 2018-19 once positions are emptied.
- Inherits F1 and F6.

**(iii) Mixed.**
- EHCVM 2018-19 at build time, inside the hook edit the ruling requires anyway: 184 / 289.
- CIV's 126 wave hashes come from its country-module hook; moving that hook to `CotedIvoire/2018-19/_/mapping.py` keeps future edits local.
- Optionally Nigeria (4th slot + round pin), about 24 / 192 more.
- Read-time elsewhere at 0. F1 can ride inside GNB's edit.

**What "set only, positions empty" requires for EHCVM 2018-19, under any option:**
1. The 8 hooks write the set from `s14q05__k >= 1` and leave the positions NA.
2. Site B issues per-wave NullReadWarnings for HowCoped0/1 (and HowCoped2) in Benin, Burkina, CIV, Mali, Niger, Senegal and Togo 2018-19. These are honest per-wave reports, like Ethiopia's `Occurrence`; do not use `optional:` to silence one wave.
3. **GNB's only shocks wave is 2018-19.** Its HowCoped0..2 must be optional or undeclared (country-grain, legitimate), or `tests/test_table_structure.py::test_no_fully_null_columns[Guinea-Bissau/shocks]` fails. This edit is in the (iii) probe.
4. `tests/test_shocks_extension_food_effects.py:92` (`('HowCoped2' in out) == (country != 'Benin')` on the 2018-19 hooks) must change. The 2021-22 wave tests pin slots and code-25 counts and change only if Q4 does.

## 6. Draft canonical `data_info.yml` shocks entry

This replaces the three bare `HowCoped*` entries (file `scripts/draft_shocks_columns.yml`). It moved 0 hashes, and the schema tests give 307 passed, the same as base.
- The probe key was the placeholder `HowCopedSet`.
- Sentences marked **[contingent on Qn]** presume an answer the user has not given.

```yaml
    HowCoped0:
      type: str
      note: |
        The coping strategy the household reported FIRST for this shock
        (ruling 2026-11-12, "Coping-strategy contract, GH1562": positions are
        the household's REPORTED order).  Filled only where the instrument
        records an order: a 1st/2nd/3rd battery (Uganda s16q04a/b/c, Malawi
        hh_u04a/b/c, ...) or an explicit rank per option (EHCVM 2021-22
        s14bq05__N).  NA where the instrument records WHICH strategies but not
        their order (EHCVM 2018-19 s14q05__N): such a wave fills <SET> only.
        Never filled by listing options in code order.  [contingent on Q4:
        "never shifted to close a gap" -- today's 2021-22 hooks do place code
        25 in an inferred slot.]  Values are the wave's own labels; they are
        not harmonized across countries or waves.
    HowCoped1:
      type: str
      note: |
        The strategy reported SECOND; the same rule as HowCoped0.  A recorded
        gap (HowCoped0 NA, HowCoped1 filled) is served as recorded.
    HowCoped2:
      type: str
      note: |
        The strategy reported THIRD; the same rule as HowCoped0.
    <SET>:
      type: str
      optional: true
      note: |
        UNORDERED set of every coping strategy the household reported for this
        shock (ruling 2026-11-12, GH1562), so "which strategies" composes
        across every shocks country whether or not its instrument records an
        order.  Representation: the distinct labels, sorted, joined with
        derivations.MULTI_SEP ('+'); a plain string, so it hashes, survives
        parquet and the grain audit, and splits back with
        str.split('+').explode().  Includes every chosen option, beyond the
        third where the instrument records more.  [contingent on Q3: slot
        fillers ("Nothing else", "No other response given") and missing
        codes are never members.]  [contingent on Q5: whether an explicit
        "did nothing" answer is a member.]  NA where no strategy is recorded
        (not asked, no follow-up).  Test membership by splitting, never by
        substring.
```

**Representation, measured on synthetic frames** (`scripts/repr_demo.py` → `out/hash/repr_demo.txt`).
- **A list column:**
  - raises `TypeError: unhashable type: 'list'` in `duplicated()`, `nunique()` and `groupby().nunique()`;
  - is stringified by the canonical `type: str` cast to `"['Relied on savings', 'Sold livestock']"`;
  - comes back from parquet as numpy arrays;
  - has no `_SCHEME_DTYPE_MAP` type.
- **A `'+'`-joined string:**
  - passes `duplicated`, `nunique`, `_audit_index_collapse`, `most_complete_row` and the parquet round trip;
  - splits back to long form;
  - can reuse `derivations.union_keys` ('b+a' ∪ 'a+c' → 'a+b+c').
  - `str.contains` is a substring test and must not be used for membership.

**Name candidates** (the user decides): `HowCopedSet`, `CopingStrategies`, `CopingSet`.

## 7. Hash census and tests

- **Census.** No branch, so no head census; the probes are in §5, all against base `f138a3d61`. The only btf move (42/42) is the `derived:` kind, a placement recommended against. Global-`.org` and `derived:` moves are corpus-wide by construction.
- **Tests** (job 41271547, `-m "not requires_s3"`): `test_schema_consistency.py`, `test_declared_spellings.py`, `test_feature_vocabulary.py`.
  - Base: 307 passed, 7 deselected.
  - Draft entry: 307 passed.
  - `required: true`: 17 failed (`TestRequiredColumns::test_required_columns_present[<C>:shocks]`, every shocks country), 307 passed.
  - Outputs: `out/hash/pytest-*.txt`.
- **Not run.** The S3-gated shocks tests; nothing was changed for them to test.
- **Tooling.** `pypdf==4.3.1` and `xlrd==2.0.1` were installed `--target` into a job-local directory only (job 41271697). Questionnaire copies and their full text (80 MB) are in `private/1562-coping-audit/qnr_docs/`; Sue to keep or clean.

## 8. Questions for the user

1. Option (i) (343 / 1,405), or (iii) (184 / 289, plus Nigeria about 24 / 192 if its 4th slot counts)?
2. Nigeria: is W4 set-only? Does the 4th slot go to the set only, or also to a `HowCoped3`? Should the round-pin fix (F2a) ride along?
3. Do positions keep recorded fillers and missing codes, or serve NA in their place? Is Uganda's repeated 96 a filler?
4. EHCVM 2021-22 code 25: keep the inferred slot as a registered derivation (it is unregistered today), or put it in the set only? Inferred on Benin 13 and Niger 17 rows beside another strategy.
5. Is "did nothing / aucune stratégie" a set member, including beside real strategies (F7)?
6. Guatemala's single action and Liberia's forest products: do they stay in HowCoped*/the set?
7. May the GNB 18–25 relabel follow the questionnaire over the released French variable labels, inside the 2018-19 hook change?
8. Is cross-wave or cross-country label harmonization in scope for the set?
9. The name of the set column.

## 9. Issue status and drafted residuals

**#1562: partial.** The audit (step 1) is done; implementation awaits Q1–Q9.

- **[B] GNB 2018-19 coping codes 18–25 follow the French list, not the Portuguese instrument.**
  - The xlsx S14_Chocs has 25 options with 25 = "Nenhuma estratégia".
  - Code 25 is positive on 1,540 rows and never beside another option.
  - Codes 20/21/25 fit the shifted alignment and not the French one.
  - Decide the relabel basis; extract codes 23–24 from the form.
- **[B] Nigeria shocks are served under both quarters of each wave.** `sect15a` blocks have no round pin; 22,550 rows for 11,275 household-shocks. Pin `t` as `individual_education` does, and check `assets`, which has the same shape.
- **[C] Nigeria's 4th coping slot is unwired.** `s15aq5d` (W1–W3) and `s15aq7_4` (W4) are filled on 274 / 165 / 150 / 31 rows. W4 asks "select all".
- **[C] Liberia 2018-19 drops 2,164 of 3,522 experienced shocks.** The always-asked follow-ups S17_3/S17_4 are unwired, and the slots are forest products.
- **[C] Filler and missing codes are served as strategies:** Uganda 40 and 96, Malawi 99 / "Dont know", Liberia "no other product", Mali/Niger "Manquant", and unlabelled numeric strings. Could ride with #1562 (Q3).
- **[C] Tanzania R1/R2 coping is all NA in the UPD release.** Establish from the NPS Y1/Y2 instruments whether it was asked, then adjudicate the cells.
- **[docs] CONTENTS corrections** (not made: read-only stream):
  - Guatemala's stale "not yet wired" (F8).
  - The Mali/Niger 2014-15 "CHOC" variable labels (F9).
  - The Uganda 2005-06 relabel folds (F6).
  - GNB F1.

## Files

- `out/per_wave_instrument.tsv`, `out/per_wave_measured.tsv`.
- `out/served/`, `out/served2/`, `out/raw/`.
- `out/extra.json`, `out/extra2.json`, `out/extra3.json`.
- `out/qnr/`, `out/hash/`, `out/hash2/`, `out/hash3/`.
- `scripts/`, `logs/`.

