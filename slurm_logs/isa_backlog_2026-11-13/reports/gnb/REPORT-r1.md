
## 1. Commits (branch `feature/gnb-labels-2021-20261113`, base `f138a3d61`)

| SHA | what |
|---|---|
| `23556bae61bfd39d7cfba5f2fc2baeee92c204b3` | Phase A: `harmonize_input`, `harmonize_seed_crop`, country `u_kg` table, Djagatu/Beringela, 94→95, `Unknown` unit fill, stale comments |
| `bbf58ecc57d1297ea894768375632781478a8232` | 2021-22 `sample`, plus `_/sampling.yml`, strata aliases and registry scope |
| `7ebac5aaf89f26e046ea4f1fe7a7db62096d1bc0` | 2021-22 `household_roster`, plus `kinship.yml` `Sobrinho(A)` |
| `ef14c9d89bea185d179ed47da10905ac97d3c6a0` | 2021-22 `cluster_features` |
| `a46d708667fd9eff7da5a141dc0b932610e80582` | test: scope the 2018-19 `cluster_features` pins to that wave |
| `3a7520ce34193b8ed302ed0684a0f428d6f8b94a` | 2021-22 `shocks` |
| `acb3e8a4476b819735dc73db61e38884b9f495ad` | 2021-22 `assets` |
| `180307740ac517016bf941248729711a96ca24e4` | config tests for Phase A; `sampling.yml` quotes the BID |
| `fd61454259dfba084fadd9b70f29d2f3393a0292` | CONTENTS.org entries; strata `validated_against` |
| `5fcd64d4a74f6f4b1a6fc4f992e821043e66436e` | roster: keep `MonthsSpent` as `int64` (a two-wave read had widened 2018-19 to `float64`) |
| `55e39b9c97b0188e2fbb885dae2e60e7f8d94cb6` | test re-pins (listed in §5) |
| `c35c95eaf4dfd4c70474e7ba0c63110aa47d4bcc` | CONTENTS food evidence, rounding rule, nitrogen-mass counts; gift-question comments (comments only) |

## 2. Idiosyncrasies found

**Guinea-Bissau 2018-19**
- The s16b file ships only applied rows (5,534 of 5,534), and none has a missing unit, so `Manquant` was served on 0 rows. The fill is now `Unknown`.
- Seed code 11 is spelled `Sementes de  milho preto`, with a double space; the lookup folds whitespace.
- The s16b unit labels are Portuguese (`Grama`, `Quilograma`, `Tonelada`, `Litro`, `Carroça`, `Outras`), and none is in `KNOWN_METRIC`.
- **`Purchased` / `Quantity_purchased` read `s16bq05` / `s16bq07a`, which are the gift question.** The variable label is "16B.05. Avez-vous obtenu [INTRANT] sous forme de cadeau ou de don?", and `s16bq06` names the giver; the purchase is `s16bq08` / `s16bq09a`. This is the same defect Togo 2018 and Benin 2018-19 record. Recorded, not changed.
- **`s07bq07a` is filled on every `Nunca` row (23,204) and every `Há mais de 30 dias` row (11,458), with `s07bq08` empty there.** That is Niger 2018-19's shape. No served value passes through it.
- `Djagatu` appears both as a crop (s16cq04 code 31) and as a food (s07bq01 / s05q01 code 145). Its one `harmonize_food` row serves all three tables.
- CONTENTS.org's "No questionnaire is held", "one wave", "Product 94 … not established" and #1445 passages are now bracket-dated.

**Guinea-Bissau 2021-22**
- **Ids:** 4,893 of the 5,351 (grappe, menage) keys recur; `s00q07f1`/`f2` record the 2018 grappe and household; `PanelHH` flags 4,897 panel households. `i` is `E2_`-prefixed and no link is served.
- **Labels:** value labels are Portuguese and mostly clean UTF-8; variable labels are French. `s01q00aa` spells "não" as `nÃo` (double-encoded, one byte lost). The coping rank labels are French.
- **Region spellings:** `Bafatá`, `Gabú` and `Bolama/Bijagós` are new and fold onto 2018-19's labels. All 450 grappes recur and keep their region and milieu.
- **Weights:** `hhweight` is labelled " Ponderation panel", is one row per household, equals `ehcvm_welfare.hhweight` on 5,351 of 5,351, and takes two values in 240 grappes (one per `panelHH`). It is served as both `weight` and `panel_weight`, as the precedent waves do.
- **The weights file's own `strate` (17 codes) is exactly region × milieu, one-to-one** with the served construction (unlike Benin's `strat`, which is department × vague). It is recorded and not served.
- **GPS:** `GPS__Latitude_m` is one point per grappe, but unlike Togo's and Benin's **it is not 2018-19's fix**. Only 2 of 439 grappes coincide. The rural median distance is 1.02 km, with 21 grappes over 10 km and 4 over 20 km; urban median 0.49 km, max 39.4 km. That exceeds even two independent WB displacements of one point. It is served as recorded (#791).
- **Leavers:** 341 people have left (dropped, as `ehcvm_individu` does); 246 of them still carry a sex. `Sobrinho(a)` is new (6,186 rows).
- **Shocks:** 4,417 of 10,842 reported shocks carry follow-ups. Column 25 uses the `nonoui` label set (34 Oui). The `d`/`e`/`f` effect columns are herd, stock and purchases by this file's own labels.
- **Assets:** 43 of 45 item labels are 2018-19's; codes 25 and 30 are respelled. 14 owned lines record no number (29,727 owned, 29,713 served).

## 3. Measurements

### Phase A

Base `f138a3d61` against candidate `23556bae6`, cold, private roots, burst job 41271716; compare job 41272254. Re-verified at head in job 41272255 on the 2018-19 slice. Frames are kept under `private/gnb/frames-*`.

| check (from the brief) | base | candidate |
|---|---|---|
| `nitrogen_kg` matched rows | 0 | **293** (Urea 190, Phosphate 57, NPK 46) over 188 households; 257 carry a mass (Quilograma 244, Grama 13). The nitrogen total was not measured. |
| `seed_kg` | 0 rows | 0 rows. Vacuous: no input label is `Seed` (Q3). |
| `plot_inputs` | 5,534 rows | 5,534 rows, unique, **frame-equal after the expected relabel of `input`/`crop` only** |
| `Feature('plot_inputs')(['Guinea-Bissau'])` | 5,534, unique | 5,534, unique, relabel-equal |
| `u == 'Manquant'` per wave | 0 | 0 |
| `crop_production` | 12,173 rows (`GrainCollapseWarning`: 11 rows destroyed) | 12,188, no warning; only the 250 → 202 `Jaxatu` + 63 `Aubergine` rows differ |
| `harvest_kg` | 730 | 731 |
| `community_prices` | 80,202 | 80,202, relabel-equal |
| `food_acquired`, `food_quantities`, `food_prices`, `food_expenditures` | – | relabel-equal |
| every other table | – | plain frame-equal |

"Nothing else moves" in `community_prices` needs restating. Besides the 696 product-94 rows, the Djagatu relabel moves 1,356 `community_prices` rows and 2,990 `food_acquired` rows from `Aubergine,` to `Jaxatu`.

### Phase B, served 2021-22 (cold, job 41272010, at `180307740`)

| table | 2021-22 rows | build warnings |
|---|---|---|
| `sample` | 5,351 | none |
| `household_roster` | 41,160 | none |
| `cluster_features` | 450 | none |
| `shocks` | 4,417 | none |
| `assets` | 29,713 | none |

The `assets` index is non-unique in both waves; that is the pre-existing #1710.

### Food gate

The isa-sm-20261112 `s07bq03a` check was patched for Portuguese labels and run with an on-grid extension (jobs 41271502, 41271717, 41271796, 41271889, 41272010, 41272061).
- **The five known patterns are absent.** No regional factor (q3/q4 mode 1.0 in all 9 regions), no H/L rows, no per-capita block, no rows without `vague`. The controls work: Mali 2018-19 rice (73.9% at its mode vs 7.8% under the null) and Senegal 2021-22 salt (34% vs 3.7%).
- **But every positive 2021-22 `s07bq03a` (128,229) and `s07bq07a` (96,262) is an even integer, in every unit.** `s07bq04`/`s07bq05` are 99.9% even. By comparison:
  - 2018-19: 38.6% of positive q3 even, 4.5% halves, `Kg` mode 1.
  - Senegal 2021-22: 6.66% halves.
  - The same 2021-22 release holds halves in its agriculture sections (s16b, s16c).
- **3,945 "consumed: yes" rows have q3 = 0** (none in 2018-19). 2,739 of them carry a purchase value, and none has any `depan` in `ehcvm_conso`.
- **The fitted rule is 2 × round-half-up(x/2), which cannot be inverted.** It maps 1, 2 and 2.5 all to 2. Applied to 2018-19's q3 it reproduces 2021-22's distribution; the other candidate rules do not. A second reader (pyreadstat) sees Stata `int8` storage.
- Neither the BID nor the EHCVM-2 enumerator manual documents it, and the WB DDI `dataProcessing` is empty.
- **Food was stopped here, as the brief requires.**

### Hash census (`tools/hash_census.py` + `hashdiff.py`)

| run | btf | fwimp | table | wave | errors |
|---|---|---|---|---|---|
| base → `5fcd64d4a` (job 41272256) | 0/42 | 0/38 | 22/555, all Guinea-Bissau | 44/2,321, all Guinea-Bissau | 0 |
| base → `c35c95eaf` (job 41272668) | 0/42 | 0/38 | 22/555, all Guinea-Bissau | 44/2,321, all Guinea-Bissau | 0 |

Nothing moves outside Guinea-Bissau. The shared `lsms_library/categorical_mapping/kinship.yml` edit is read-path and moves no hash. No cross-wave kg-pool movement occurs, because 2021-22 food is not wired.

### scan.py restricted to Guinea-Bissau (at `180307740`, before the `MonthsSpent` fix)

47 records: 35 warn, 5 pass, 7 fail. All 7 fails are `labels='Aggregate'` or `units='kgprice'/'unitprice'` degrades, because GNB curates neither an Aggregate column nor `Price`. I attributed them by reading their kwargs; I did not run a base scan.

## 4. Tests

| run | files | result |
|---|---|---|
| `55e39b9c9`, job 41272302 | 36 files: every test file naming Guinea-Bissau, plus `test_gnb_2021_wave`, `test_gnb_2018_input_labels`, `test_ehcvm_7day`, `test_derivations`, `test_schema_consistency`, `test_build_transform_hash`, `test_kinship_yml_hygiene`, `test_categorical_mapping_resolution`, `test_sample`, `test_sampling`, `test_population`, `test_recall_period`, `test_food_kg_size_pool`, `test_gh1556_ehcvm_shocks_2021`, `test_panel_ids_persistence`, `test_feature_attrs_carriers` | 3,307 passed, 87 skipped, 4 xfailed, 0 failed |
| `c35c95eaf`, job 41272669 | all of `tests/` with `-m "not requires_s3 and not slow"` | 15,515 passed, 35 skipped, 4 xfailed, 0 failed |

Command: `pytest -n $SLURM_CPUS_ON_NODE --dist=loadfile -p no:cacheprovider`, in private cold roots. Output is in `out/pytest-final.txt` and `out/pytest-wide.txt`.

The tests that `c35c95eaf` touches are comments and CONTENTS only.

Not run: fresh-process warm reads, `assume_cache_fresh`, the strict levers (`LSMS_*_STRICT`), the native-unit modes, and the `requires_s3` tests outside the 36 files.

## 5. Re-pins and shared records

**Re-pins, each with its reason:**
- `test_gh1575_guineabissau…`: the strata registry scope now covers both waves, and the default-scope inputs call covers both.
- `test_gh812_ehcvm_strata`: GNB added to `DOCUMENTED_SCOPE` and `DOCUMENTED_2021`, and the "2021 still unwired" pin is retired.
- `test_guinea_bissau_cluster_features`: the fixture is scoped to 2018-19.
- `tests/fixtures/ehcvm_condition_originals.json`: the GNB `crop_production.py` sha256 moves because of the brief's stale-comment fix (comments only).
- New tests: `test_gnb_2021_wave.py`, `test_gnb_2018_input_labels.py`, and a 94→95 bridge test.

**Left for Sue to do once at integration:**
- `tests/test_sample.py` `PARTIALLY_WIRED_SAMPLE` still lists Guinea-Bissau 2021-22. The test passes either way.
- `.coder/coverage/latest.csv` will need the regrade.
- `population.yml` already has a 2021-22 `not-stated` record.
- No recall record is needed, since food is not wired.

## 6. Issues

| issue | status |
|---|---|
| #1107 | **closes** (DAP is left raw, per #1107's own instruction; tracked by #1696) |
| #1539 | **closes** |
| #1119 | partial: GNB half done; Senegal and GhanaLSS remain |
| #1226 | partial: GNB product-94 done; the variety convention and the Burkina tin remain |
| #1483 | GNB item done via #1107 |
| #1118 | partial: GNB site done (0 rows moved); Benin, Togo, Burkina ×2, Senegal, CIV and GhanaLSS remain |
| #1697 | partial: GNB `sample`/`roster`/`cluster_features`/`shocks`/`assets` wired; food held (Q1) |
| #1556 | GNB 2021-22 adopts purchases = `s14bq04f`, stock = `s14bq04e` |
| #1566 | measured for GNB: 4 of 29,407 off the 25 grid, nothing changed |
| #1565 | GNB leavers dropped de facto (341), as Benin |
| #1598 | not wired for GNB (it would need an `ehcvm.py` edit); moot while food is held |

**Drafted residuals:**
- **(R1, tier C)** "Guinea-Bissau 2021-22 food_acquired: released s07b quantities are rounded to even integers". Body: the evidence above, with options from Q1.
- **(R2, tier B)** "Guinea-Bissau 2018-19 plot_inputs: Purchased reads the gift question s16bq05; the purchase is s16bq08/s16bq09a".
- **(R3, tier B)** "Guinea-Bissau: plot_inputs Tonelada/Litro have no mass; food Litro is not metric".
- **(R4, tier B)** "Guinea-Bissau 2021-22: unwired tables (individual_education, interview_date, housing, food_security, subjective_well_being, plot_*, crop_production, livestock, people_last7days)".
- **(R5, #791)** "GNB 2021-22 cover GPS is not 2018-19's fix (rural 21 grappes >10 km)".
- **For whoever wires food later:** 182 item codes (renumbered from code 12); unit codes 107, 122 and 126 are labelled differently in `s07bq03b` and `s07bq07b`; the size `Inteiro` is missing from `_UNIT_SIZE_WORDS`, a `transformations.py` edit that re-warms GhanaLSS, Uganda and Ethiopia; the NSU file shape (4,419 rows, region × milieu × product × unit × size).

## 7. Questions for the user

1. **Guinea-Bissau 2021-22 food.** Every released quantity is rounded to an even integer, small consumption is zeroed, and this is undocumented and cannot be inverted. Expenditure inherits the error too, because q3 and q7a are rounded separately. An unverified hypothesis: Stata's `round(q, 2)` rounds to a multiple of 2, so a call meant as "2 decimals" would produce exactly this. Options, no choice made:
   - (a) leave it unwired, which is the current state;
   - (b) serve it as released, under a registered key that names the rounding;
   - (c) seek an unrounded release from WB or UEMOA (#791).
2. **DAP catch-all.** The brief offered "as Togo (DAP) or as Other". I left it raw and unmatched, per #1107, until #1696 is ruled. Confirm?
3. **Seeds.** Relabel the seed rows `input = 'Seed'` so `seed_kg` reads them? Today it is vacuous.
4. **Strata.** The 2021-22 strata ruling names Togo, Benin and CIV; I applied it to GNB per the brief. Confirm? Its `strate` agrees one-to-one.
5. **GPS.** Keep serving the 2021-22 cover point, or withhold it pending #791?
6. **2018-19 `Purchased`.** Correct the gift/purchase mix-up (R2)? That is a served-value change.

## 8. Disclosures

- **Two accidental `python3` no-ops on n0035:** `python3 - 2>/dev/null` with empty stdin, and `python3 -c 'print(1)'`. Neither imported `lsms_library` or read data. All other Python ran in burst jobs.
- **Kept under `private/gnb/`** (mode 700): `frames-41271716`, `frames-41272011` and `frames-41272255`, about 22 MB of microdata-derived frames, plus `docs/` (the 2021-22 BIDs and questionnaires).
- **`out/` is not aggregates only.** The discover JSON holds three interview timestamps and some free-text answers with a count of 1. The dump JSONs carry a Site Q warning that names one household id.
- **SHAs per measurement:** census and verify at `5fcd64d4a`; tests at `55e39b9c9`; scan at `180307740`; final census and wide tests at `c35c95eaf`.
- **Text reused:** the GNB English BID text came from the isa-sm-20261112 docsearch conversion.

---
