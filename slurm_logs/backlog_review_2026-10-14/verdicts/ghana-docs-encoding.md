VERDICT: pass-with-fixes -- every served-data, config, test and cache claim reproduces exactly on a cold private build; the only defects are two one-word count slips in the new EHCVM prose.

Reviewer: red-team, job 39923193, branch fix/ghana-docs-encoding @ 3973e9094, merge-base 2ed85b49b against origin/development 88707ddb0.

## 1. Refutations (claim -> what I ran -> result)

- HELD: "28 / 3 C1 cells in the two GhanaLSS org files, 0 after". `grep -c -P '[\x{0080}-\x{009F}]'` on base worktree: 28 / 3; on branch: 0 / 0. Edited lines keep identical character lengths (org alignment intact). Only non-ASCII left in GhanaLSS/_/categorical_mapping.org is the two pre-existing `Ê` cells the triage record already names as a different artefact (lines 1655, 1763).
- HELD: "nonfood_expenditures.j 25,585 rows / 27 distinct / all 2016-17; community_prices.j and .Description 927 each". pyarrow read of the seeded pre-fix parquets: 25,585 / 27 / ['2016-17']; 927 / 927, 3 distinct, 2016-17. Seeded hash b25abf97... == base `_table_cache_hash`, so the seed is development's output.
- HELD: "rows unchanged 977,193 / 660,168; j C1 -> 0; cold". `lsms-library cache clear --country GhanaLSS` against MY root (0 parquets left), then `Country('GhanaLSS').nonfood_expenditures()` / `.community_prices()` on the branch: 977,193 rows (2005-06 244,349; 2012-13 442,927; 2016-17 289,917), index unique, j C1 0; 660,168 rows, index unique, j C1 0, Description C1 0. `grain_reports()` 0, `null_read_reports()` 0, `null_index_reports()` 0 for both. Written parquets carry the branch's expected hash (dc8bfd79..., 389c9f75...). `find lsms_library/countries -name '*.parquet'` = 0 throughout.
- HELD (advisor's extra check): the ASCII repair creates NO new duplicate Preferred Label. Parsed both tables from `git show 2ed85b49b:` and HEAD: nonfood_items 687 rows, 0 duplicate Preferred Labels before and after, per-wave entries 250 / 251 / 508 all injective; harmonize_price_item 643 rows, 37 duplicate Preferred Labels before AND after (the deliberate folds CONTENTS.org already documents, unchanged).
- HELD: "g7sec9a.dta is format 118 and mixes cp1252 with ASCII: 41,803 Women\x92s vs 97,531 Women's; 0x92 411,886 / 0x96 418,085 / 0x94 241,757; UTF-8 curly quotes 0". Counted on the blob via its .dvc md5 (955,515,953 bytes, header `<stata_dta>`): identical to every digit; `(1/4\x94)` 13,947 vs ASCII `(1/4")` 0.
- HELD: "cache cost: full GhanaLSS re-warm, no other country". `Country._table_cache_hash(t, waves)` base vs branch: GhanaLSS 21/21 moved; GhanaSPS 0/13 moved; Togo 0/22 moved. Guinea-Bissau 21/21 moved too, but that is a CONFOUND: `git diff --stat 2ed85b49b..origin/development -- Guinea-Bissau` shows development added 52 lines to GB's categorical_mapping.org (PR #971) after the merge-base; the branch touches only GB's CONTENTS.org. Togo and GhanaSPS are the clean CONTENTS-only controls and moved 0, which is the evidence that CONTENTS.org is excluded from the hash (`_ORG_HASH_SKIP = {"CONTENTS.org"}`, country.py:741).
- HELD: new tests fail on development. Copied the test into a base worktree (origin/development) and ran it there: 4 failed (3 meaningful: the Preferred-Label scan and both whole-file GhanaLSS asserts; `test_scanner_sees_the_known_tables` is tautological with the fix since it asserts post-fix strings). On the branch: 4 passed.
- HELD: the scanner is not vacuous. My own independent parser (blocks of consecutive `|` lines, header = first row) over the same 66 files: base 31 offending Preferred Label cells, all GhanaLSS (28 + 3); branch 0. The test's own `_preferred_label_cells` yields 15,322 cells across 60 files on the branch; mine 15,298 (its extra 24 are header rows of blank-line-separated follow-on tables -- harmless).
- HELD: GhanaSPS "5,008 -> 5,007; mean 3.771 -> 3.769; both households absent; no residence-fallback UserWarning". `Country('GhanaSPS').household_characteristics()`: 2009-10 5,007 / 2013-14 4,762 / 2017-18 5,661; roster households 5,009 / 4,774 / 5,669; wave-1 mean size 3.769 (2013-14 3.418, 2017-18 3.346, both as written); 107254027 absent, 107268086 absent, 105144071 present; wave-1 MonthsAway 18,889/18,889 non-null, max 12; only NullReadWarning (GhanaSPS/sample, pre-existing) and ResourceWarning fired.
- HELD: every NEW raw-file figure in the GhanaSPS §2 rewrite, checked on S1D.dta via get_dataframe: 18,889 rows / 5,009 hh; s1d_27 non-null 9,956 (52.7%), missing 8,933, explicit zeros 8,840, no negatives, the single >12 value is 15 at hhno 105144071 hhmid 2, s1d_2=2 (spouse), age 48; 9 missing-s1d_27 members in 5 households with any Q28-Q33; s1d_28 non-null 18; after fillna(0).clip(12): 17,773 zeros, 14 twelves, 1,116 non-zero; per-EA (id3): 334 EAs, 48 at 0%, 128 at 100%, 17 in the OPEN 25-75% interval (20 closed); share non-zero among recorded 95.9% in the <=25% EAs falling to 4.2-6.3% in the >75% EAs; 107254027 one member with null age (5 null ages in the wave); 107268086 three members all 12, the ONLY all-12 household; 2,188 / 2,537 / 284 household missingness split. Line refs data_info.yml:63 and :142 and mapping.py:74 (`fillna(0).clip(upper=12)`) are correct.
- HELD: EHCVM allocation. `Country(C).sample(waves=[w])`, log cluster weight, between-strata variance share, ratio = hh share / weight share: Guinea-Bissau 5,351 hh / 450 clusters / 9 strata / within-cluster weight sd 0 / 76.80% / SAB 0.651 ... Bolama-Bijagos 3.243, every hh and cluster count in the doc's table matches; Togo 6,171 / 540 / 6 / 66.51% / Lome 0.54 (1,034 hh, 98 cl) ... Savanes 1.90 (1,144 / 96), Plateaux 0.85, Maritime 0.89, Centrale 1.45, Kara 1.55; Benin 8,012 / 670 / 12 / 31.35% / Zou 0.65 (432/36) ... Donga 1.77 (633/53), Mono 1.57, Plateau 1.28; CotedIvoire 12,992 / 1,084 / 33 / 36.54% / Abidjan 0.33 ... Folon 6.17. `strata: s00q01` is at line 16 of all four data_info.yml; `grep -c strata` = 0 in all ten Senegal/Mali/Niger/Burkina EHCVM-era data_info.yml files.
- HELD: the test docstring's named C1 cells elsewhere: Mali 2, Niger 2, Burkina_Faso 3, Uganda 4, Togo/2018/_/notes.org 2 (prose); GhanaSPS/_/units.org and Senegal/_/units.org are ISO-8859 per `file`.
- HELD: clean merge onto current development: `git merge-tree --write-tree origin/development HEAD` exit 0 (PR #971's GB hunk has landed; no conflict with the new GB subsection).
- REFUTED (prose, trivial): Guinea-Bissau CONTENTS.org "Seven of nine regions get ~42-50 clusters" -- the branch's own table shows EIGHT (48, 44, 48, 50, 42, 42, 42, 42; only SAB at 92 is outside). Togo CONTENTS.org "Five of six regions get 72-98 clusters each" -- ALL six are in 72-98 (98, 96, 82, 72, 96, 96).

## 2. Claimed vs measured

| claim | claimed | measured |
|---|---|---|
| C1 cells, two GhanaLSS org files, before / after | 28+3 / 0+0 | 28+3 / 0+0 |
| nonfood_expenditures rows | 977,193 | 977,193 |
| nonfood_expenditures j C1 rows / distinct, before -> after | 25,585 / 27 -> 0 | 25,585 / 27 -> 0 |
| community_prices rows | 660,168 | 660,168 |
| community_prices j, Description C1 before -> after | 927, 927 -> 0, 0 | 927, 927 -> 0, 0 |
| Preferred Label injectivity per wave | 250 / 251 / 508 | 250 / 251 / 508, no new dups |
| blob: Women\x92s / Women's | 41,803 / 97,531 | 41,803 / 97,531 |
| blob bytes 0x92 / 0x96 / 0x94; UTF-8 curly | 411,886 / 418,085 / 241,757; 0 | same; 0 |
| reader: s9aname values with C1 (report) | 390,140 of 7,077,839 | not re-run (7M-row read; blob counts above corroborate the mechanism) |
| GhanaLSS hashes moved | all | 21/21; GhanaSPS 0/13; Togo 0/22 |
| GhanaSPS wave-1 hc households / mean size | 5,007 / 3.769 | 5,007 / 3.769 |
| GhanaSPS hc 2013-14, 2017-18 mean size | 3.418, 3.346 | 3.418, 3.346 |
| s1d_27 non-null / missing / zeros / twelves after hook | 9,956 / 8,933 / 17,773 / 14 | same |
| EAs 0% / 100% / 25-75% | 48 / 128 / 17 | 48 / 128 / 17 (open interval) |
| GB between-strata share; ratio range | 76.8%; 0.65-3.24 | 76.80%; 0.651-3.243 |
| GB Oio / Gabu / Bafata ratios | 0.77 / 0.87 / 1.07 | 0.765 / 0.865 / 1.065 (half-up rounding; held) |
| Togo share; range | 66.5%; 0.54-1.90 | 66.51%; 0.54-1.90 |
| Benin share; range | 31.4%; 0.65-1.77 | 31.35%; 0.65-1.77 |
| CotedIvoire share; range | 36.5%; 0.33-6.17 | 36.54%; 0.33-6.17 |
| GB regions in 42-50 cluster band | "seven of nine" | eight of nine |
| Togo regions in 72-98 cluster band | "five of six" | six of six |

## 3. Regression

`OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 .venv/bin/python -m pytest tests/test_ghanalss_*.py tests/test_no_c1_controls_in_curated_labels.py tests/test_categorical_mapping_resolution.py tests/test_schema_consistency.py tests/test_feature_canonical_index.py tests/test_build_transform_hash.py -n 16 --dist=loadfile -q`:

    595 passed, 1 skipped, 170 warnings in 707.65s (0:11:47)

The skip is `test_categorical_mapping_resolution.py` needing `LSMS_EXPECTED_ROOT` (pre-existing). Disclosure: this run exceeded the harness's 600 s foreground cap and was auto-detached; I read the completed log file (exit 0), it was not abandoned. Cold GhanaLSS build of the two touched tables: 43 s + 23 s. No scan.py run: no `Feature()` surface is touched (config labels + docs only).

## 4. Scope findings

- `git diff --stat $(merge-base)..HEAD` = exactly the 9 files the task names: 4 EHCVM CONTENTS.org, GhanaLSS CONTENTS.org + two categorical_mapping.org, GhanaSPS CONTENTS.org, one new test. No baseline / golden / blessed / lockfile / dependency edit; no assertion weakened, skipped or xfailed.
- Caution for readers: a raw `git diff origin/development..HEAD` shows 46 files including apparently "reverted" pins in `test_crop_kg_factor.py`, `test_gh842_uganda_harvest_unit.py`, `test_guinea_bissau_cluster_features.py`, `test_uganda_sold_unit.py`. Those are development-side commits the branch predates (GH #848 etc.), NOT branch edits; the merge-base diff is the 9 files only.
- `Label_2016-17` (also edited) is informational: the wave script decodes `Code_2016-17 -> Preferred Label` (nonfood_expenditures.py:99-103) and nothing keys on the Label columns (`rg Label_20 countries/GhanaLSS --type py` = 0), so editing it alongside Preferred Label cannot break a join.
- Report drafted residuals for #926 (Mali/Niger/Burkina/Uganda key-column C1 cells, the two ISO-8859 units.org files, the re-derivation transcoding rule) and #812 (Senegal/Mali/Niger/Burkina wire no `strata`); declared #880 fully resolved. All three are consistent with what I measured.
- Writes: all parquet writes landed under my private LSMS_DATA_DIR (which now holds 16 countries' `sample` tables from the canonical-index / build-hash tests and my allocation script, not the 3 seeded). In-tree parquets 0; `git status --short` empty at the end; base worktree removed.

## 5. Residual risks and what a human must decide

- ASCII (`'`, `-`, `"`) was chosen over the typographic characters the source intended (U+2019, U+2013, U+201D). Low stakes, consistent with the file's convention, and recorded as a decision in CONTENTS.org -- but it is a labelling choice a maintainer may want to confirm. Anyone with a downstream join on the OLD served j strings (27 labels, 2016-17 only) will see those rows re-keyed; no analysis is known to depend on them.
- The new test covers Preferred Label columns corpus-wide and the two GhanaLSS files in full; the Mali/Niger/Burkina key-column C1 cells remain live and load-bearing until GH #794/#805 re-key them (drafted as the #926 residual).
- Whether the missing `strata` wiring in Senegal/Mali/Niger/Burkina EHCVM waves gets its own issue (report's decision_for_human; I recommend yes: it blocks the same allocation check in ten waves).

## 6. Recommended follow-up commits (pass-with-fixes)

1. `lsms_library/countries/Guinea-Bissau/_/CONTENTS.org`, line beginning "(Seven of nine regions get ~42-50 clusters": change to "Eight of nine regions get 42-50 clusters regardless of size (SAB, at 92, is the exception)".
2. `lsms_library/countries/Togo/_/CONTENTS.org`, sentence "Five of six regions get 72-98 clusters each regardless of size.": change to "All six regions get 72-98 clusters each regardless of size."
3. Optional, same files: GhanaSPS CONTENTS.org "17 in the 25-75% middle" -> "17 strictly between 25% and 75%" (20 if the bounds are inclusive).
