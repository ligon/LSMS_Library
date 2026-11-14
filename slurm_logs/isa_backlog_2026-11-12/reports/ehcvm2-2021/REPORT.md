# EHCVM-2 2021-22 wiring restored onto development: Togo, Benin, Cote d'Ivoire (GH #552)

Agent: fix agent, Claude Opus 5.5 (claude-opus-5-5), dispatched by Sue (job 41201840, n0035.savio4).
Worktree `/local/job41201840/worktrees/ehcvm2-2021`, branch `feature/552-ehcvm2-2021-wiring-20261112`.
Start `67525c607` (= `refs/archive/2026-11-11/isa/wire-togo-2021-20261104`), development `e5fdd2fa8`.
Ruling: @ligon 2026-11-12, "Go: restore and wire" (`.sucoder/isa-decisions.org` on `docs/isa-sweep-20261103`).
Guinea-Bissau is not in this dispatch and nothing here touches it, except that `tests/test_sample.py` still exempts its unwired 2021-22 wave.

## Verdict

READY for a development PR, with questions for the maintainer (strata, below) and the residuals listed at the end.
No existing wave's served rows, weights or household ids change; the derived kilogram tables of the older waves move, as the ruling expects, and the movement is quantified below.

## Commits (all on the branch, all published to the mirror by the hook)

| commit | what |
|---|---|
| `2f64852bd` | merge development `e5fdd2fa8` into the Togo branch |
| `65277f627` | Togo 2021-22 shocks on the #1556 ruling; strata note |
| `20e03aba1` | merge `refs/archive/2026-11-11/isa/wire-benin-2021-20261104` (`48a34b22b`) |
| `5b477d671` | Benin 2021-22 shocks on the ruling; strata note |
| `2fec14b84` | merge `refs/archive/2026-11-11/isa/wire-civ-2021-20261104` (`7a4030042`) |
| `38791d17b` | Cote d'Ivoire 2021-22 shocks on the ruling; strata note |
| `03f438a54` | three development strata tests follow the wiring; CONTENTS.org records the re-measurement |
| `df1df60ed` | Togo strata test notes the out-of-scope residual; CIV shocks count wording |

Head: `df1df60edc089920ab02ad445226a4764c9feb12` (== `origin/feature/552-ehcvm2-2021-wiring-20261112` on the mirror). Nothing pushed to GitHub.

## Named idiosyncrasies (read from each `_/CONTENTS.org`, TODO / WAITING / LOGBOOK included)

### Togo
1. Wave labels are `2018` (sources in `2018/Data1/`, not `Data/`, which holds only `_forEthan` extracts) and `2021-22` (flat, `tgo2021` names).
2. **Lome Commune / Grand Lome, preserved.** The June 2021 BID p.10 says Lome Commune respondents "are from rural areas"; the 2023 retrospective Table 1a gives Grand Lome 98 urban / 0 rural clusters. Region code 6 is `Lomé commune` in 2018 and `Grand Lomé` in 2021-22 on the same 98 grappes; each wave serves its own label, so a cross-wave group on `Region`/`strata` splits it. Nothing reconciles them.
3. **Same-cluster revisit, preserved.** All 540 grappes recur, with region, prefecture, milieu and the GPS point identical. 5,441 of 6,462 (grappe, menage) keys recur, but 1,397 (`panel` = 2) sit at the wrong-household baseline. So no link is served: `i` is `E2_<grappe>0<menage>`, so the ids cannot meet. Three panel flags disagree (`PanelHH` 4,377, `panel` 4,044, `s00q07d` 3,938).
4. `s07bq03a` is rescaled by region (0.9143-1.6725; Grand Lome is two factors by row). This is WAITING with the maintainer. It causes 11,952 phantom purchases on `Jamais` rows and `Expenditure` NA on 35.3% of purchased rows. The pooled kg factors are not contaminated.
5. The unit code book carries two label sets (`s07bq03b` and `s07bq07b`), so `LastPurchaseUnitCode` restores 70,581 matches. Codes 659/660/662 are bare in 2018 and labelled in 2021-22 (#1557).
6. Section 9 item codes moved (53 of 131), so the wave has its own `nonfood_items` table.
7. In `s14b`, d is the herd, e the stock and f the purchases; all six columns are asked in both vagues here, while 2018 has five columns in vague 1. Column 25 `Autre stratégie` is Oui/Non. Follow-ups are asked only for the last 12 months (2,666 of 7,229 shocks served), so cross-wave incidence reads as a false 63% fall.
8. `s12` ships owned items only (33,619 rows) where 2018 ships the full grid. `s12q09` has the #1566 off-grid block.
9. `crop_production` has Togo's shape (`s16cq16a` filled on 10,325 of 10,877 rows). `s16d` repeats records exactly (1,111). The harvest-state list has eight codes; `Tubercule`/`Gousse`/`Régime` are WAITING outside the #1436 fold.
10. `poids` takes two values per grappe (by `panel`) and no longitudinal weight is shipped, so `panel_weight` = `poids`.
11. The catalog swaps the titles of the vague-1 and vague-2 questionnaire PDFs.
12. `plot_inputs` 2018 serves the gift (`s16bq05`) under `Purchased` (WAITING).

### Benin
1. The 2018-19 universe statement is local (DDI p.3): Littoral is urban-only, with 23 department x milieu strata. The 2021-22 weights file ships its own `strat` (24 cells), not read.
2. `i` is `E2_<grappe><menage:03d>`. `PanelHH` = Panel sits at real-link level (head sex 94.8% on 6,878), unlike Togo and CIV; a link is plausible but not served, pending a maintainer decision.
3. `ehcvm_individu.age` was set from 2018-19 for panel members (do not test links on it); the roster serves the raw answers.
4. `s07bq03a` is not rescaled (98.95% on the quarter grid; 6 `Jamais` residuals).
5. Two unit label sets (`102 Alvéole`, `207 Chaîne en bois`). Local units 200-230 are bundled under moved codes. `tongolo avec 1/2 brassée` take 2018-19's sized labels through YAML.
6. `Piment` bundle: WAITING on a ruling.
7. `crop_production` has Mali's shape (`s16cq16a` on 381 of 8,934), so #1437's harvest-from-dispositions is applied to Benin; that reach is put to the maintainer. Four `Tonne` rows read as kg, and Site Q names them.
8. The files are grids. `s01`/`s04a` list 6,138 departed people, who are left out. `s16aq47` holds two scales (about 10,000x on 16.7% of parcels).
9. No grappe-grain price file exists in 2021-22, so `community_prices` stays 2018-19 only. `HowCoped2` is new (337 rows).

### Cote d'Ivoire (CotedIvoire)
1. 2018-19 is under `Data/Menage/` with upper-case `CIV2018`; 2021-22 is flat and lower case. The library also holds four CILSS waves (1985-89).
2. The cover holds 728 non-interviewed households (`s00q08` = Non rempli), so `sample` takes the weights file as primary and every merge with the cover is `merge_how: left`.
3. `PanelHH`/`panel`-flagged keys sit at the baseline. The link (07F1/07F2) is asked but not distributed, so none is served.
4. `s14b` keys are damaged (junk `vague` values, keys missing `menage`, repeated blocks). `GrainCollapseWarning` fires on purpose.
5. `s07bq03a` has off-grid H/L rows (inferred imputation / NSU conversion). WAITING.
6. Crop codes 56-60 are unlabelled (56 very likely cashew, 57 banana/plantain) and served as numbers. One 5,000 `Tonne` rubber row is 37.7% of `harvest_kg`.
7. `s16aq47` mixes units. `plot_labor` is asked but not distributed (verdict recorded). 38 households lost their input codes.
8. 2021-22 region labels are lower case on the same codes; `mapping.strata` upper-cases them. 2018-19 stores `Non-concerné` as Stata `.d` (#1561's scoped map). `numind` differs from `membres__id` on 86 heads.

### Contradictions with CONTENTS.org (quoted, now annotated in place with dated notes)
- Togo: "=s14bq04d= is the herd here, so =AffectedConsumption= reads =s14bq04e=, the food stock". Benin's text is the same. Both are superseded by the ruling.
- CIV: "*WAITING: the column is not the same variable across the waves.*" This is answered: development rewired 2018-19 to `s14q04f`/`s14q04e`, and this branch rewires 2021-22 to `s14bq04f`/`s14bq04e`.
- All three 2021-22 YAML comments said "GH #812: region only, as 2018[-19]". This became false when GH #1575 made 2018 region x milieu.
- CIV's archive hand-edited the GENERATED `_/population.yml`, so the generator reverted it. The note is now carried into its source org file.
- The archived kg totals do not reproduce exactly, though rows moved, medians and rungs do (see below).
- Development's `ehcvm.py` comment "Benin, Togo, Guinea-Bissau and Cote d'Ivoire serve the region alone (`strata: s00q01`)" has been stale for 2018 since #1575. It was not edited.

## Conflict resolutions
| file | resolution | why |
|---|---|---|
| `lsms_library/ehcvm.py` (Togo merge) | union: development's NSU / strata / crop-condition / #1454 code kept verbatim; the Togo EHCVM-2 crop section appended byte for byte; `__all__` extended on its own lines; module-level `union_keys` import kept | every development behaviour preserved; `git diff 67525c607` removes no branch line |
| `ehcvm.py` (Benin merge) | `__all__` only; Benin's `DISPOSITIONS`, `harvest_totals`, `derive_hh_crop_harvest_total` added | |
| `ehcvm.py` (CIV merge, **no textual conflict**) | git silently kept two definitions of `harvest_totals` and `derive_hh_crop_harvest_total`, Benin's and CIV's. The duplicate was caught with an `ast` scan. Benin's copy is kept: it is the same rule line for line, plus a `.astype(bool)` on the plot mask. CIV's `crop_production.py` imports `DISPOSITIONS as DISPOSITION_VARIABLES` | a later definition silently wins |
| `{Togo,Benin,CotedIvoire}/_/derivations.yml` | union, no duplicate key | |
| `{...}/_/CONTENTS.org` | union, archived 2021-22 text first (dated 2026-11-04), development's #1561/#1575/#1531 sections after; stale pointers fixed; a dated 2026-11-12 section at the end of each | |
| `.coder/coverage/food_recall.csv` | development's file (new `n_asks_by_column` column) plus each country's 2021-22 row with an empty value; 20 `recall.yml` regenerated by `scripts/promote_recall_records.py` at each merge | the CSV is the source (#851) |
| `docs/guide/recall.md`, `tests/test_recall_record.py` | "**58 of 70**" cells; 70 records (test name no longer carries a count) | `test_the_guide_states_the_live_count` |
| `population.yml` / `slurm_logs/POPULATION_STATEMENTS_2026-07-21.org` | regenerated with `scripts/promote_population_records.py`; byte-identical after CIV's note moved into the source | |
| `tests/test_sample.py` | Benin and CIV both leave `PARTIALLY_WIRED_SAMPLE` (only Guinea-Bissau remains) | |
| `tests/test_sampling.py` | both stable-boundary rows kept | |

## The ruling, applied

| ruling item | done |
|---|---|
| new waves served with their own weights | `weight` = `panel_weight` = `poids` (`ehcvm_ponderations_*2021`), one row per household; no NA, all positive, mean 1.0 after the read-time normalisation; 1,053 / 1,007 / 2,054 distinct values (Togo / Benin / CIV) |
| no existing wave's weights or panel ids change | every older wave of `sample` (and of every other non-derived table) is frame-equal to base, same `i` sets; none of the three is a panel country (no `panel_ids.json` / `updated_ids.json`), and the 2021-22 ids are `E2_`-prefixed so they cannot meet 2018's |
| kg pool moves, disclosed not fixed | quantified below; nothing in `transformations.py`'s estimator was changed (the only edit there is the archived `Très grand` size word) |
| Togo Lome / Grand Lome discrepancy and same-cluster revisit preserved | served as each wave records it; `_/sampling.yml` declares 2021-22 `stable`; Site C stays silent; `v` = bare `grappe` in both waves |
| order sample, roster, cluster_features, food, shocks, assets | all wired as archived; shocks re-pointed to the #1556 ruling; assets' `s12q09` checked (#1566); food NSU (#1598) left as a residual (see below) |

### Shocks (#1556's ruling), the one semantic change made to the archived wiring
All three 2021-22 files label `s14bq04d` "l'Effectif du cheptel", `e` "le Stock produits alimentaires" and `f` "les Achats produits alimentaires", filled on the same answered rows in both vagues (Togo 2,666; Benin 4,756; CIV 7,588 follow-up rows). They now read `AffectedConsumption: s14bq04f`, `AffectedFoodStock: s14bq04e`, as Niger/Mali/Senegal/Burkina 2021-22 do. `test_gh1556_ehcvm_shocks_2021.py::test_food_effects_read_purchases_and_stock_separately` discovers the three and passes. Affected (Augmente/Diminue) on f / e / d: Togo 1,858 / 1,811 / 870; Benin 3,777 / 3,668 / 1,872; CIV 5,294 / 4,867 / 1,228. The coping hooks were not changed: no row has a non-contiguous rank, and Togo's and CIV's `Autre stratégie` never sits beside another strategy (Benin's hook is already the core waves' rule). CIV's served `shocks` collapse warning now reads 44 rows / 23 tuples destroyed (+38 NaN-key rows), where the archive's table says 42 / 20. The served table is still 7,528 rows, as the archive records, so no further row is lost. The difference is in how many repeated pairs disagree, since a pair can agree on the stock and differ on the purchases. Not traced row by row.

### Strata: kept region-only in 2021-22, a question for the maintainer
GH #1575 rewired 2018 / 2018-19 `strata` to region (department) x milieu and scoped each `<C>::sample::region-milieu-stratum` entry to that wave alone ("no 2021 integration"; `test_gh1575_togo...` pins `waves == ['2018']`). The archived 2021-22 wiring is region only. Measured on the candidate, the labels served per wave are:

| country | 2018/2018-19 | 2021-22 | shared |
|---|---|---|---|
| Togo | 11 | 6 | 0 |
| Benin | 23 | 12 | 0 |
| CIV | 66 | 33 | 0 |

So a cross-wave group on `strata` splits every stratum. Three development tests pinned "2021-22 has no `sample`" and failed (base: pass). They are narrowed in `03f438a54`, and do not hide the split: the gh812 Feature test checks milieu only on the entry's `waves` and adds the converse, that no row outside them carries the stratum key. Aligning is small per country but is a scope the ruling did not grant, and each country has its own wrinkle: Benin's `strat` is 24 cells against the DDI's 23, CIV's labels are lower case, and Togo has the Lome relabel.

### Assets: `s12q09` (#1566), checked, nothing changed
`assets.Value` is `s12q09` in all three.

| wave | positive | off the 25 grid | non-integer |
|---|---|---|---|
| Togo 2021-22 | 33,354 | 1,245 (3.7%) | 1,239 (`1000.0000298`, `1071.4286`, `1166.6667`) |
| Togo 2018 | 30,337 | 4 | 0 |
| CIV 2021-22 | 74,058 | 3,066 (4.1%; vague 1 4.8%, vague 2 3.5%) | 0 (`2,374`, `2,502`, ...) |
| CIV 2018-19 | 61,682 | 10 | 0 |
| Benin 2021-22 | 40,220 | 25 (one is 9,999) | 0 |
| Benin 2018-19 | 37,551 | 1 | 0 |

Togo's off-grid block is concentrated on `Radio simple/Radiocassette`: 1,054 of 2,270 rows (46%), the same in both vagues. It is not a per-unit value: only 94 of its 1,245 rows have `s12q03` > 1. CIV's block is a different shape, spread over items (`Téléphone portable` 581 of 11,995, `Ventilateur sur pied` 353, `Radio` 199). Unexplained in both, recorded in CONTENTS.org.

### Food: own-country NSU (#1598) not wired, and why
The archive wires none. Header inventory (`get_data_file`, private L1):
- **Togo** `ehcvm_nsu_tgo2021.dta`: 29,685 raw market weighings (`q110a` g, `poids` kg, `poids_moyen`), unit codes up to 664 (Togo's local ones), with a market stratum `s00q06`.
- **Benin** has four tables: `_ben2021` (department x milieu), `_nat_` (3,477 keys, `(p 50) poids_kg`), `_dep_` and `_com_`.
- **CIV** `ehcvm_nsu_civ2021.dta`: 53,078 raw weighings in grams, with an `ipoids` imputation, `unite_corr`/`taille_corr` corrections and an `id_2018`. This is not a key-to-weight table.

So each country needs its own column choice and 2018 applicability assessment, which is #1598's acceptance, not a small completion. These waves' kilograms come from the price-ratio ladder, as their 2018 waves' do.

## Validation (all burst jobs: `-A co_carleton -p savio4_htc -q carleton_htc4_normal -c 8 --mem=64G`; private `LSMS_DATA_DIR`/`XDG_CACHE_HOME`/`TMPDIR`/`MPLCONFIGDIR`/`LSMS_ISSUES_LOG` under `/local/job$ID`; identity and private root asserted; BLAS pinned; `LSMS_BUILD_WORKERS=1`; clones `--shared` from the mirror at full SHAs; venv by `bin/savio_venv.sh mount`)

Scripts: `scripts/` in this directory. Job outputs: `jobs/<label>-<id>/`.

Commands (from `scripts/`; `CAND` the head under test, `BASE=e5fdd2fa831c4d03a247d264be7f339da459e6b0`):

```sh
sbatch -J tests-cand  --export=ALL,SHA=38791d17b495afb8389e2be693cc41725fa58e67,LABEL=tests-cand tests.sbatch        # 41235848
sbatch -J tests-base  --export=ALL,SHA=$BASE,LABEL=tests-base tests.sbatch                                            # 41235849
sbatch -J builds      --export=ALL,SHA=38791d17b495afb8389e2be693cc41725fa58e67,BASE=$BASE,LABEL=builds,COUNTRIES="Togo Benin CotedIvoire" build.sbatch   # 41235873
sbatch -J twide-cand  --export=ALL,SHA=03f438a54bf7f8cf71285e35085e458877476ff4,LABEL=twide-cand tests_wide.sbatch    # 41236416
sbatch -J twide-base  --export=ALL,SHA=$BASE,LABEL=twide-base tests_wide.sbatch                                       # 41236417
sbatch -J scan        --export=ALL,SHA=03f438a54bf7f8cf71285e35085e458877476ff4,BASE=$BASE,LABEL=scan scan.sbatch     # 41236418
sbatch -J static-cand --export=ALL,SHA=df1df60edc089920ab02ad445226a4764c9feb12,LABEL=static-cand static_full.sbatch  # 41236819
sbatch -J static-base --export=ALL,SHA=$BASE,LABEL=static-base static_full.sbatch                                     # 41236820
```

pytest inside each: `$PY -m pytest -n 8 --dist=loadfile -q -rfEs -p no:cacheprovider <files>` (output to a file), then `tests/test_schema_consistency.py -k "Togo or Benin or CotedIvoire"`. The static jobs run `tests/ -m "not requires_s3"`. Light source reads on n0035 (shock labels, NSU headers, `s12`, `derivation_inputs`) used a private root under `/local/job41201840/ehcvm2priv/`.

| job | what | result |
|---|---|---|
| 41235848 `tests-cand` (38791d17b) | targeted: wave tests, every `tests/` file named Togo/Benin/CIV/EHCVM, sample, sampling, recall*, build_transform_hash, ehcvm_7day, + shocks/size-pool/population/derivations/... | 2,839 passed, 85 skipped, 4 xfailed, **3 failed** (the strata pins below); schema `-k Togo or Benin or CotedIvoire` 26 passed |
| 41235849 `tests-base` (e5fdd2fa8) | same selection on base | 2,644 passed, 88 skipped, 4 xfailed, 0 failed; schema 26 passed |
| 41236416 `twide-cand` (03f438a54) | 125 files: every `tests/*.py` whose text names togo/benin/cotedivoire/ivoire/ehcvm + the list above + code-prefixed units, u-code leak, C1 controls, kinship hygiene, null index, weight normalisation, kg-subset slice, wave provenance | **6,121 passed, 115 skipped, 8 xfailed, 0 failed**; schema 26 passed |
| 41236417 `twide-base` (e5fdd2fa8) | same, on base | 5,921 passed, 120 skipped, 8 xfailed, 0 failed |
| 41235873 `builds` | config-only hash probe both sides; cold build of every `data_scheme` table + the four derived tables, base and candidate, each in its own private root; compare; `s12q09`; Feature check | below |
| 41236819 `static-cand` (df1df60ed) / 41236820 `static-base` | all of `tests/` with `-m "not requires_s3"` (catches suites that scan every wave script) | **15,168 passed, 35 skipped, 4 xfailed, 0 failed** / base 15,109 passed, 35 skipped, 0 failed |
| 41236418 `scan` | `bench/feature_audit/scan.py --features canonical --countries Togo Benin CotedIvoire`, base and candidate | 16 features, 0 skips; 7 `fail` records in both (pre-existing `assembly_nonempty`, identical); 3 new `warn` fingerprints, all `PopulationHeterogeneityWarning` (3 waves without a population statement: the new 2021-22 records state none); one gone (Togo `shocks` `HowCoped2` constant: now filled by 2021-22) |

Skips are not passes: no test in the three wave files, `test_gh1556_ehcvm_shocks_2021.py` or `test_gh812_ehcvm_strata.py` was skipped in `twide-cand`. Their source-gated tiers ran against `get_dataframe` reads. The candidate-only skips that disappear are `test_sample.py`'s "Togo/Benin/CotedIvoire has 1 expansion-scale wave(s)": those now compare two waves and pass. In-tree parquets after every job: none.

## Base vs candidate, cold (job 41235873; base e5fdd2fa8, candidate 38791d17b, whose build inputs equal 03f438a54's)

"Older equal" = every wave other than 2021-22 is equal to base on base's columns, row for row after sorting by the declared index, with identical household-id sets per wave. Columns new on the candidate are listed separately with their NA share on the older waves; dtype changes separately from values.

### Togo

| table | base rows | cand rows | 2021-22 rows | older equal | new columns (NA on older) | dtype change |
|---|---|---|---|---|---|---|
| assets | 31,462 | 65,081 | 33,619 | yes |  |  |
| cluster_features | 540 | 1,080 | 540 | yes |  |  |
| crop_production | 11,563 | 23,112 | 11,549 | yes | Unit_sold (100%), Derivation (100%) | plot_id str->string |
| food_acquired | 112,596 | 248,810 | 136,214 | yes |  |  |
| food_expenditures | 66,043 | 134,634 | 68,591 | yes |  |  |
| food_prices | 67,434 | 137,183 | 69,749 | moves (kg pool), below |  |  |
| food_quantities | 110,742 | 244,710 | 133,968 | moves (kg pool), below |  |  |
| food_security | 6,163 | 12,618 | 6,455 | yes |  |  |
| household_characteristics | 6,146 | 12,603 | 6,457 | yes |  |  |
| household_roster | 27,480 | 56,295 | 28,815 | yes |  |  |
| housing | 6,171 | 12,633 | 6,462 | yes |  |  |
| individual_education | 27,481 | 56,296 | 28,815 | yes |  |  |
| interview_date | 18,493 | 37,878 | 19,385 | yes |  |  |
| livestock | 6,181 | 11,540 | 5,359 | yes |  | i str->string |
| nonfood_expenditures | 108,445 | 219,435 | 110,990 | yes |  |  |
| people_last7days | 25,282 | 54,097 | 28,815 | yes |  |  |
| plot_features | 9,553 | 18,735 | 9,182 | yes |  |  |
| plot_inputs | 13,730 | 29,772 | 16,042 | yes | Unit_purchased (100%), Received_as_gift (100%), Quantity_received_as_gift (100%), Unit_received_as_gift (100%) |  |
| plot_labor | 15,052 | 30,191 | 15,139 | yes |  |  |
| sample | 6,171 | 12,633 | 6,462 | yes |  |  |
| shocks | 7,230 | 9,896 | 2,666 | yes |  |  |
| subjective_well_being | 6,120 | 12,549 | 6,429 | yes |  |  |

### Benin

| table | base rows | cand rows | 2021-22 rows | older equal | new columns (NA on older) | dtype change |
|---|---|---|---|---|---|---|
| assets | 38,191 | 78,526 | 40,335 | yes |  |  |
| cluster_features | 670 | 1,340 | 670 | yes |  |  |
| community_prices | 222,296 | 222,296 | 0 | yes |  |  |
| crop_production | 9,056 | 18,374 | 9,318 | yes | Unit_sold (100%), Derivation (100%) | plot_id str->string |
| food_acquired | 188,349 | 393,306 | 204,957 | yes |  |  |
| food_expenditures | 132,341 | 271,639 | 139,298 | yes |  |  |
| food_prices | 133,112 | 273,676 | 140,563 | moves (kg pool), below |  |  |
| food_quantities | 187,338 | 390,741 | 203,403 | moves (kg pool), below |  |  |
| food_security | 8,011 | 16,028 | 8,017 | yes |  |  |
| household_characteristics | 7,991 | 16,009 | 8,018 | yes |  |  |
| household_roster | 42,343 | 85,003 | 42,660 | yes |  |  |
| housing | 8,012 | 16,044 | 8,032 | yes |  |  |
| individual_education | 42,343 | 85,003 | 42,660 | yes |  |  |
| interview_date | 24,035 | 48,129 | 24,094 | yes |  |  |
| livestock | 7,334 | 13,750 | 6,416 | yes |  | i str->string |
| people_last7days | 35,042 | 77,702 | 42,660 | yes |  |  |
| plot_features | 7,588 | 15,142 | 7,554 | yes |  |  |
| plot_inputs | 10,605 | 21,973 | 11,368 | yes | Unit_purchased (100%), Received_as_gift (100%), Quantity_received_as_gift (100%), Unit_received_as_gift (100%) |  |
| plot_labor | 10,738 | 20,992 | 10,254 | yes |  |  |
| sample | 8,012 | 16,044 | 8,032 | yes |  |  |
| shocks | 12,305 | 17,061 | 4,756 | yes | HowCoped2 (100%) |  |
| subjective_well_being | 7,953 | 15,868 | 7,915 | yes |  |  |

### CotedIvoire

| table | base rows | cand rows | 2021-22 rows | older equal | new columns (NA on older) | dtype change |
|---|---|---|---|---|---|---|
| assets | 66,857 | 143,737 | 76,880 | yes |  |  |
| cluster_features | 1,484 | 2,565 | 1,081 | yes |  |  |
| crop_production | 22,216 | 44,892 | 22,676 | yes | Unit_sold (100%), Derivation (100%) | plot_id str->string |
| food_acquired | 296,945 | 602,427 | 305,482 | yes |  |  |
| food_expenditures | 182,891 | 374,917 | 192,026 | yes |  |  |
| food_prices | 182,890 | 375,003 | 192,113 | moves (kg pool), below |  |  |
| food_quantities | 296,945 | 602,292 | 305,347 | moves (kg pool), below |  |  |
| food_security | 12,960 | 25,921 | 12,961 | yes |  |  |
| household_characteristics | 19,367 | 32,327 | 12,960 | yes |  |  |
| household_roster | 111,788 | 176,279 | 64,491 | yes |  |  |
| housing | 12,992 | 25,957 | 12,965 | yes |  |  |
| individual_education | 79,677 | 144,164 | 64,487 | yes |  |  |
| interview_date | 38,973 | 77,867 | 38,894 | yes |  |  |
| livestock | 4,711 | 8,930 | 4,219 | yes |  | i str->string |
| people_last7days | 60,617 | 125,108 | 64,491 | yes |  |  |
| plot_features | 14,417 | 29,637 | 15,220 | yes |  | plot_id str->string |
| plot_inputs | 11,259 | 25,907 | 14,648 | yes | Unit_purchased (100%), Received_as_gift (100%), Quantity_received_as_gift (100%), Unit_received_as_gift (100%) |  |
| plot_labor | 22,264 | 22,264 | 0 | yes |  |  |
| sample | 19,380 | 32,345 | 12,965 | yes |  |  |
| shocks | 13,778 | 21,306 | 7,528 | yes | HowCoped2 (100%) |  |
| subjective_well_being | 12,912 | 25,840 | 12,928 | yes |  |  |

dtype `str`->`string` is the pandas default string dtype widening to `StringDtype` where the 2021-22 frame brings one; values are equal.

## 2021-22, per country (candidate)

| | Togo | Benin | CotedIvoire |
|---|---|---|---|
| sample households / v NA / strata labels | 6,462 / 0 / 6 | 8,032 / 0 / 12 | 12,965 / 0 / 33 |
| weight NA / <=0 / distinct | 0 / 0 / 1,053 | 0 / 0 / 1,007 | 0 / 0 / 2,054 |
| Rural | Rural 4,003, Urban 2,459 | Rural 4,080, Urban 3,952 | Rural 7,722, Urban 5,243 |
| cluster_features clusters | 540 | 670 | 1,081 |
| roster members / households | 28,815 / 6,462 | 42,660 / 8,032 | 64,491 / 12,965 |
| roster Sex | F 15,093, M 13,722 | F 21,815, M 20,845 | F 32,830, M 31,661 |
| roster Age NA / max / >100 | 2 / 106 / 2 | 2 / 106 / 2 | 1 / 105 / 6 |
| roster Generation NA (null by design) | 782 | 1,092 | 2,317 |
| MonthsSpent 12 / 0 | 28,453 / 362 | 41,886 / 774 | 63,009 / 1,482 |
| food_acquired rows (dup keys) | 136,214 (0) | 204,957 (0) | 305,482 (0) |
| shocks rows (dup keys) | 2,666 (0) | 4,756 (0) | 7,528 (0) |
| assets rows (dup keys) | 33,619 (958) | 40,335 (1871) | 76,880 (2088) |
| crop_production rows (dup keys) | 11,549 (0) | 9,318 (0) | 22,676 (0) |
| plot_features rows (dup keys) | 9,182 (0) | 7,554 (0) | 15,220 (0) |
| plot_inputs rows (dup keys) | 16,042 (0) | 11,368 (0) | 14,648 (0) |
| plot_labor rows (dup keys) | 15,139 (0) | 10,254 (0) | 0 (not served) |
| livestock rows (dup keys) | 5,359 (0) | 6,416 (0) | 4,219 (0) |
| housing rows (dup keys) | 6,462 (0) | 8,032 (0) | 12,965 (0) |
| interview_date rows (dup keys) | 19,385 (0) | 24,094 (0) | 38,894 (0) |
| food_security rows (dup keys) | 6,455 (0) | 8,017 (0) | 12,961 (0) |
| people_last7days rows (dup keys) | 28,815 (0) | 42,660 (0) | 64,491 (0) |
| individual_education rows (dup keys) | 28,815 (0) | 42,660 (0) | 64,487 (0) |
| nonfood_expenditures rows (dup keys) | 110,990 (0) | not declared | not declared |

`assets` "dup keys" is the existing grain of the table, not a new defect. 2018 / 2018-19 serve duplicated `(i, t, j)` tuples too (`harmonize_assets` folds several items into one `j`, e.g. `Furniture`): Togo base 1,192, Benin 1,861, CIV 2,169. The 2021-22 waves follow the same rule. `plot_labor` has no CIV 2021-22 rows (asked, not distributed) and `community_prices` no Benin 2021-22 rows (no `s05_co`), both as archived.

Warnings on the new waves, by class (candidate minus base): Site Q `QuantityImplausibleWarning` on each wave's crop, food, plot and livestock tables. Togo 2021-22 `food_acquired` has 737 near-zero quantities (`0.00175 Tas (Petit)`), which are the residuals of its region-rescaled `s07bq03a` (WAITING). CIV 2021-22 `GrainCollapseWarning` fires on `shocks` and `plot_inputs` on purpose, as archived. `StatedVsInferredKgWarning` names more stated labels the pooled inference contradicts: Togo 1 -> 3, Benin 6 -> 9, CIV 5 -> 7 (from each build's own warning). `NullReadWarning` on Benin `people_last7days` 2021-22 names the four labour columns (`farm_hrs`, `SB_hrs`, `wage_hrs`, `Industry`). They are 100% null in every wave of all three countries, the earlier waves included (measured on the saved frames; declared for schema parity), so it is not new. Nothing raised under the default levers.

## Kilogram movement on the older waves (GH #1487 pooling; disclosed, not fixed)

Each older wave read alone (`waves=[w]`, so `attrs` describe that wave only), base then candidate:

| | Togo 2018 | Benin 2018-19 | CIV 2018-19 |
|---|---|---|---|
| `food_quantities(units='kgs')` rows (both) | 110,740 | 187,336 | 296,928 |
| rows whose quantity moves | 91,261 | 126,788 | 128,245 |
| median ratio among moved (p10, p90) | 1.152 (0.951, 1.262) | 1.222 (0.917, 1.734) | 1.012 (0.70, 1.50) |
| kg on rows served `u = kg` | 63,577 -> 66,944 (+5.3%) | 164,440 -> 182,615 (+11.1%) | 358,449 -> 359,122 (+0.2%) |
| rows switching kg <-> native `u` | 2 | 2 (+1 row in `food_prices`) | 17 |
| `food_prices` rows moving, median ratio | 53,091, 0.868 | 92,147, 0.818 | 78,498, 0.988 |
| `food_prices` sum | 90.8M -> 82.6M | 158.4M -> 131.3M | 144.66M -> 141.75M |
| rung `item_unit` | 14,087 -> 14,450 | 35,589 -> 65,116 | 95,949 -> 111,868 |
| rung `item_unit_tight` | 0 -> 0 | 160 -> 438 | 0 -> 6,933 |
| rung `unit` | 88,132 -> 87,771 | 132,190 -> 102,387 | 173,867 -> 151,032 |
| rung `none` / `metric` | 33 -> 31 / 10,344 | 155 -> 153 / 20,255 | 48 -> 31 / 27,081 |

The new waves' own kilograms come from the same ladder. 2021-22 rungs: Togo `metric` 11,559, `item_unit` 16,474, `unit` 108,174, `none` 7; Benin 25,831 / 85,075 (+435 tight) / 93,583 / 33; CIV 36,496 / 121,869 (+6,374 tight) / 140,730 / 13. Kg on `u = kg` rows: Togo 70,300, Benin 209,400, CIV 331,383.

Against the archived CONTENTS.org figures (measured 2026-11-04 on the old base), the rows moved, medians, price sums and rungs reproduce exactly. The kg totals do not: Togo 63,624 -> 66,987, Benin 168,465 -> 186,631, CIV 358,536 -> 359,170. Their sum's definition is not recorded, so the difference was not traced; it is noted in each file. Native `Quantity` and `Expenditure` of the older waves (`food_acquired`, `food_expenditures`) do not move. `harvest_kg` was not re-measured.

## Cache scope (config-only probe, job 41235873; base e5fdd2fa8 vs 38791d17b)

| | moved / probed |
|---|---|
| `Country._table_cache_hash` | 86 / 544: Togo 22 of 22, Benin 22 of 22, CotedIvoire 21 of 21, **GhanaLSS 21 of 21** |
| `Wave._input_hash` | 361 / 2,258: Togo 44/44, Benin 44/44, CotedIvoire 126/126, GhanaLSS 147/147 |
| `build_transforms_fingerprint` | 0 / 40 |

Niger, Mali, Senegal, Burkina Faso and Guinea-Bissau: 0. The ehcvm.py additions (Togo's crop section and Benin's harvest helpers) reach no other country's fingerprint, despite the #1454 mapper-closure hashing. GhanaLSS moves because the archived `Très grand` was added to `transformations._UNIT_SIZE_WORDS`, under `food_quantities_from_acquired`, which `GhanaLSS/_/nutrition.py` imports. The archive's claim that no GhanaLSS label carries the word, so its values do not change, was not re-verified here. The commits after 38791d17b change only CONTENTS.org and tests, which are not build inputs. No shared cache was touched; the re-warm is the four countries above, deferred to the bundled campaign.

## Feature assembly (job 41235873, candidate, fresh private root)

`Feature('sample')(['Togo','Benin','CotedIvoire','Senegal'])` returns 75,298 rows, index `(country, i, t)`, with no duplicate. `household_roster` returns 447,227 rows on `(country, t, v, i, pid)`, with no duplicate. Each country's slice equals its own `Country()` frame (`assert_frame_equal`, all four, both features). Counts by wave: Togo 2018 6,171 / 2021-22 6,462; Benin 8,012 / 8,032; CIV 12,992 / 12,965 (plus the four CILSS waves); Senegal 7,156 / 7,120. `attrs` carry `population`, `recall`, `sampling` and `derivations` for the kept countries. Warnings: one `PopulationHeterogeneityWarning` per call (the 2021-22 records state no universe), and CIV 1988-89's existing roster `GrainCollapseWarning`. No `ClusterIdentityWarning`, since the boundaries are declared `stable`.

## Issues

| issue | effect of this branch | draft text |
|---|---|---|
| #552 | partly resolves (Tier 1, three of four EHCVM-2 waves) | "Togo (6279), Benin (6272) and Cote d'Ivoire (6273) 2021-22 are wired by #PR (restored from the 2026-11-04 archive onto development `e5fdd2fa8`): every table their earlier wave serves, under `E2_` ids with no cross-wave link, with their own weights, and with older waves frame-equal. Remaining under this issue: Guinea-Bissau 2021-22 (6274, held, unwired), and the external Tier 2/3 acquisitions. Keep open." |
| #1556 | partly resolves (the three held branches) | "The held Togo, Benin and Cote d'Ivoire 2021-22 wirings now apply the ruling: `AffectedConsumption` = `s14bq04f` (purchases), `AffectedFoodStock` = `s14bq04e` (stock), verified against each file's own labels (d = cheptel, e = stock, f = achats, all six asked in both vagues). `test_gh1556_ehcvm_shocks_2021`'s static YAML test discovers all seven wired 2021-22 waves and passes. Its source-gated class covers the four core countries; the three new waves' source oracles are in their own wave tests, which ran and passed. Remaining: Guinea-Bissau 2021-22 and the Malawi 2024 branch. Narrow, keep open." |
| #1566 | check done, nothing fixed | "Checked on #PR. `assets.Value` is `s12q09`. Togo 2021-22: 1,245 of 33,354 positive values (3.7%) are off the 25 grid, 1,239 of them non-integer (`1000.0000298`, `1071.4286`, `1166.6667`). 1,054 are `Radio simple/Radiocassette` (46% of its rows); the share is the same in both vagues; it is not a per-unit value (94 of 1,245 have quantity > 1). Cote d'Ivoire 2021-22 has its own block: 3,066 of 74,058 (4.1%), integers, spread over items, 4.8% in vague 1 and 3.5% in vague 2. Benin 2021-22: 25 of 40,220. All 2018 waves: about 0. Unexplained; served as filed. Suggest retitling to Togo and Cote d'Ivoire. Keep open." |
| #1557 | evidence only | "Togo 2021-22's `ehcvm_nsu_tgo2021.dta` labels `uniteID` 659 `Main` on Banane douce / Plantain (177 rows), 660 `Quartier` on six meats (76), 662 `Tête` on Manioc / Igname / Taro (130). Those are the goods 2018's CONTENTS gives for the same bare codes, which supports one code book across the waves. Re-decoding 2018 still moves 2018's served `u` and is the maintainer's call. Unchanged by #PR." |
| #1598 | unchanged; inventory added | "Not wired by #PR; these waves' kg still come from the ladder. Inventory of the held files: Togo has raw market weighings (29,685 rows; `q110a` g, `poids` kg, `poids_moyen`; unit codes to 664; market stratum `s00q06`). Benin has four key tables (`_ben2021` department x milieu, `_nat_` 3,477 keys `(p 50) poids_kg`, `_dep_`, `_com_`). Cote d'Ivoire has raw weighings (53,078 rows, grams, `ipoids` imputation, `unite_corr` / `taille_corr`, `id_2018`), not a key table. Each needs its own construction; keep open, sequenced after #PR." |
| #791 | narrows the motivation | "The four EHCVM-V2.0 waves this issue lists as missing are held from the World Bank catalog (#1506), and three are now wired (#PR). So the UEMOA source's EHCVM-2 motivation is down to Guinea-Bissau, which is also WB-held. Its value is now ERI-ESI and the un-jittered-GPS claim. On GPS: in all three wired 2021-22 waves the cover's `GPS__Latitude/Longitude` is one point per grappe, equal to the WB 2018 grappe fix to the last digit. So the WB 2021-22 points are the 2018 points, and they say nothing about whether UEMOA's are displaced. Keep open." |

None is closed by this branch.

## Residuals

1. **Guinea-Bissau 2021-22** (6274): held, unwired, not in this dispatch. `tests/test_sample.py` still exempts it.
2. **Strata alignment for 2021-22** (question 1 for the maintainer): region-only against the earlier wave's region x milieu, with no label shared. For Benin, choose between the documented construction and the weights file's own `strat`.
3. **#1598 own-country NSU** for all three. The files are raw weighings (Togo, CIV) or a set of key tables (Benin); see the inventory above.
4. **`derivations.call_inputs` does not refuse a wave outside the entry's `waves` when one is named.** `Country(c).derivation_inputs('<c>::sample::region-milieu-stratum', wave='2021-22')` returns 6,462 / 8,032 / 13,693 cover answers for a wave whose rows carry no key; before this wiring it raised `KeyError`. Framework-level and not patched (`ehcvm.inputs_region_milieu_stratum` is shared with the four core countries, whose hashes must not move).
5. **Survey-kg `Quantity_kg` on 2021-22 `crop_production`** (`s16cq16c` / `s16dq05c`) for Benin and CIV: the rung landed on development after the archive base (Mali's and Niger's scripts show the column).
6. #1557 decode of Togo 2018's 659/660/662 (evidence above); #1566 (above); Togo `s07bq03a` rescale and CIV off-grid `s07bq03a` (both WAITING); Togo `Tubercule/Gousse/Régime` outside the #1436 fold; Benin `Piment` bundle; the 2018 `plot_inputs` gift-under-`Purchased` defect in all three; CIV crop codes 56-60; the Benin #1437 reach (harvest from dispositions); the Benin panel link (`PanelHH` sits at real-link level), which is a decision.
7. Development's `ehcvm.py` comment that Benin, Togo, Guinea-Bissau and Cote d'Ivoire "serve the region alone" has been stale for 2018 since #1575. It was left alone.
8. CIV `shocks` collapse count 42 / 20 -> 44 / 23 (served rows unchanged at 7,528; not traced row by row); the archived kg totals' definition (not traced).

## Unverified limits

- GhanaLSS's 21 moved hashes: whether its served values stay unchanged under `Très grand` was not rebuilt here.
- `harvest_kg` on the older waves was not re-measured; `crop_production` rows are frame-equal, so only the cross-wave `harvest_kg_factors` pool could move it.
- The Feature check covered `sample` and `household_roster` over the three countries plus Senegal; other features only through `scan.py` (three countries, canonical grid).
- Frame equality is on base's columns after sorting by the declared index, under the default (warn) levers. Strict levers (`LSMS_GRAIN_STRICT`, `LSMS_READ_STRICT`, `LSMS_QUANTITY_STRICT`) were not run. CIV 2021-22 `shocks` and `plot_inputs` would fail under `LSMS_GRAIN_STRICT=1` by design, and every wave's Site Q firings would fail under `LSMS_QUANTITY_STRICT=1`.
- Warm-cache reads, `assume_cache_fresh`, `LSMS_BUILD_BACKEND=make` and Windows were not exercised; every build here was cold in a private root.
- The data-gated (`requires_s3`) suite was run on 125 files, not the whole of `tests/`; the non-data suite was run whole (15,168 passed). Development CI and the required `pr-data-validation` gate are the remaining gates.
- The archive's own measurements that this branch did not re-run (panel-link benchmarks, `s07bq03a` factor tables, harvest-sum tests, UEMOA `depan` checks) are carried as recorded on 2026-11-04.

## Draft PR body

```markdown
## EHCVM-2 2021-22: wire Togo, Benin and Cote d'Ivoire (#552)

Ruling @ligon 2026-11-12, "Go: restore and wire". This restores the three archived wirings (`refs/archive/2026-11-11/isa/wire-{togo,benin,civ}-2021-20261104`, 2026-11-04) onto current development and brings them under its rulings. Guinea-Bissau is not included.

### What it serves
Every table each country's earlier EHCVM wave serves, for 2021-22, with these exceptions as archived: Cote d'Ivoire has no `plot_labor` (asked, not distributed), and Benin has no `community_prices` (no `s05_co`). Ids are `E2_`-prefixed, so they cannot meet the earlier wave's: no cross-wave household link is served, which is a decision for the maintainer. `v` is the bare `grappe`, since the clusters are the same (`_/sampling.yml`: `stable`). Weights are each wave's own `poids` (`weight` = `panel_weight`; no longitudinal weight is shipped).

### What changed from the archive, because development moved
- **Shocks, on the #1556/#1561 ruling.** In all three waves, `AffectedConsumption` = `s14bq04f` (purchases) and `AffectedFoodStock` = `s14bq04e` (stock), checked against each file's own labels.
- **`ehcvm.py`.** The Benin and CIV refs each added `harvest_totals` / `derive_hh_crop_harvest_total`, and git kept both silently; Benin's identical copy is kept.
- **Generated records.** `food_recall.csv` (#851) and the population source are merged as sources and regenerated; the recall count is now "58 of 70".
- **Tests.** Three development strata tests pinned "2021-22 has no `sample`" and are narrowed to the entry's `waves`.

### Unchanged, measured (cold, private roots, base `e5fdd2fa8`)
- Every older wave of every non-derived table is frame-equal to base, with the same household ids.
- New optional columns are NA on the older waves.
- No weight and no panel id of an existing wave changes.

### Moves, disclosed (GH #1487 pooling)
The older waves' inferred kilograms move. Kilograms on rows served `u = kg` go Togo 2018 +5.3%, Benin 2018-19 +11.1% and CIV 2018-19 +0.2%. `food_prices` medians among moved rows are 0.87, 0.82 and 0.99. Native `Quantity` / `Expenditure` do not move.

### Cache
- `Country._table_cache_hash`: Togo 22/22, Benin 22/22, CotedIvoire 21/21, and GhanaLSS 21/21. GhanaLSS moves because of `Très grand` in `_UNIT_SIZE_WORDS`, through `nutrition.py`.
- `build_transforms_fingerprint`: 0/40.
- Niger, Mali, Senegal, Burkina and Guinea-Bissau: 0.
- No shared cache was touched.

### Questions for the maintainer
1. **Strata.** 2021-22 keeps region-only strata: GH #1575 was ruled for the earlier wave alone. The waves now share no strata label (Togo 11 vs 6, Benin 23 vs 12, CIV 66 vs 33). Should 2021-22 be aligned, and for Benin, through the documented construction or the weights file's own `strat`?
2. **Benin panel link.** Benin's `PanelHH` sits at real-link level (head sex 94.8%). Should Benin become a panel country? Togo and CIV do not qualify.

### Tests
On 125 data-gated test files (`tests/*.py` naming these countries or EHCVM, plus sample, sampling, recall, hashes, units, kinship, weights, provenance), base passes 5,921, and this branch 6,121 passed, 0 failed (jobs 41236417 / 41236416). All of `tests/` with `-m "not requires_s3"`: 15,168 passed, 0 failed (base 15,109; jobs 41236819 / 41236820). `scan.py` over the three countries finds no new fail.

Residuals: #1598 (own-country NSU), #1557, #1566 (checked: Togo 3.7%, CIV 4.1% of `s12q09` off the 25 grid), Guinea-Bissau 2021-22, and an out-of-scope `derivation_inputs` wave that is not refused.

Report: `burst/isa-sm-20261112/reports/ehcvm2-2021/REPORT.md`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
```
