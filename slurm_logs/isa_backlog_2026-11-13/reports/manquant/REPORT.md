
# Stream manquant: #1118 residual (Togo, Senegal, Cote d'Ivoire), the Togo 2018 gift fix, and Senegal #1119 / #1287

Worktree `/local/job41201840/worktrees/manquant`, branch `fix/1118-manquant-residual-20261113`, base `f138a3d61c203f892148592675a7ef72e160a322`.

**Branch head: `fc40960d27b800045984735a38c68cf91c9b2194`.** Every commit printed `SUCODER: published`.

## 1. Commits

| # | SHA | What |
|---|---|---|
| 1 | `408d02a2417772b96c7c539e92120b7e4dc0db4c` | **Togo.** 2018 `plot_inputs` reads the purchase (`s16bq08`/`s16bq09a`/`s16bq09b`). The gift answers (`s16bq05`/`s16bq07a`/`s16bq07b`) go into their own columns. A missing unit is `Unknown`. Also updates CONTENTS and the `data_scheme.yml` comment, adds a test, and moves the GH #962 sweep pin. |
| 2 | `6d4477c9d0cc7698ad43bed379bb366fdd2d4b7b` | **Senegal.** `Unknown` fill (#1118), maize/cowpea `harmonize_seed_crop` labels (#1119-SEN), and a refusing `_/plot_features.py` (#1287-SEN). Also CONTENTS, the scheme comment, two test pins and two new tests. |
| 3 | `f001cfbd0aac3d67ca66cd76c1ed891179245172` | **Cote d'Ivoire.** `Unknown` fill in both waves. Also CONTENTS, the scheme comment and a new test. |
| 4 | `4ab20ca27e537ff06fb7d0425e62f14de3c18918` | **Togo, comments only.** The grappe 101 / menage 9 note had the gift and the purchase reversed (§2). |
| 5 | `fcda613b742cc6080c26b810c4679cc087793cfa` | **Tests only.** Arrow-safe `u` counts in my two new tests, and a re-pin of the Senegal digests in `test_gh1500`. |
| 6 | `fc40960d27b800045984735a38c68cf91c9b2194` | **CONTENTS only.** Puts the measured hash cost and the #1287 route findings into the three files, instead of pointing at this report. |

There is one code commit per country (1-3). Commits 4-6 are follow-ups that the verification jobs surfaced after publishing. I did not amend anything.

## 2. Idiosyncrasies named, per country

**Togo.** I read `_/CONTENTS.org` in full, and both `plot_inputs.py` docstrings.
- **Source location.** The 2018 module lives in `2018/Data1/`; `Data/` holds only the undocumented `_forEthan` extracts.
- **Grain.** EHCVM inputs are household × input grain. `crop` is the only level that separates the seed slots, so `harmonize_seed_crop` must stay injective, and the GH #323 uniqueness guard runs before `to_parquet`.
- **The gift defect was already recorded.** The "2021-22 wired / plot_inputs" entry carried it as WAITING; it is now DONE, pointing to a new 2026-11-13 section.
- **Hollow lines.** The 3 hollow 2018 lines are dropped by `dropna(how='all')` (`TOGO_ROWS = 13730`). They are blank in both flags, so the hollow set does not change. Measured.
- **The 2018 file's own Stata labels.** `s16bq05` "Avez-vous obtenu [INTRANT] sous forme de cadeau ou de don?", `s16bq08` "Avez-vous acheté [INTRANT]?". `s16bq09b` has a 12-answer card; `s16bq03b` and `s16bq07b` have 5.
- **New, added to CONTENTS: gift and purchase are not exclusive.** 79 lines say both. `s16bq07a` is filled exactly where `s16bq05` = Oui, and `s16bq09a` exactly where `s16bq08` = Oui.
- **Contradiction with the existing notes, corrected.** The GH #323 notes call grappe 101 / menage 9's colliding lines "16 Charrette market-purchased" and "3 Charrette own-production". The file says the reverse:
  - the 16 Charrette came from `Autre paysan ou ménage` as a gift (`s16bq05` Oui, `s16bq07a` 16, `s16bq08` Non);
  - the 3 Charrette were bought at `Marché/Boutique` (`s16bq08` Oui, `s16bq09c` 6,000 FCFA).

  The notes had read the gift flag as the purchase. I corrected the wave-script comment and the test. I left the same parenthetical in `_/categorical_mapping.org` (lines 200-201) and recorded that in CONTENTS: that file is hashed into every wave's `Wave._input_hash`.

**Senegal.** I read `_/CONTENTS.org` in full, and the docstrings of `2018-19/_/plot_inputs.py`, `_/s16b_2021.py`, `2021-22/_/plot_inputs.py`, `_/plot_features.py` and `2018-19/_/crop_production.py`.
- **Mojibake crosswalks.** The `u` and `j` crosswalks for s07b are keyed on mojibake, and that is load-bearing. I touched neither. The s16b and s16c labels are clean: `s16cq04` gives `Maïs` / `Niébé`. The served join (0 -> 723) proves the `ï` I wrote matches byte for byte.
- **2021-22 shares the map.** `_/s16b_2021.py` reads the same `harmonize_seed_crop`, so 941 2021-22 seed rows move with 2018-19's. A grep confirms only the two `plot_inputs` drivers read the table.
- **The `Manquant` fill never fired.** The GH #1123 entry already recorded that no applied line lacks a unit. Confirmed: 0 in both waves.
- **No 2021-22 `crop_production`.** The join gain can only be measured on 2018-19.
- **The 2018-19 gift defect is recorded, but not ruled** for Senegal. Question 1.
- **`plot_features` has no `$(VAR_DIR)` Make rule**, but it does have a country builder.

**Cote d'Ivoire.** I read `_/CONTENTS.org` in full.
- **File layout.** 2018-19 uses the upper-case `CIV` tag and `Data/Menage/`.
- **The 350 placeholder.** 350 is the modal 2018-19 input quantity (39.5% of lines) and is unadjudicated. Untouched.
- **The 38 damaged 2021-22 households.** They collide on 32 keys with a `GrainCollapseWarning`. The `u` relabel cannot create a new key, because `Unknown` was served on no 2021-22 row before.
- **The 2018-19 gift defect is recorded as WAITING, but not ruled.** Question 1.
- **New, the unlisted site.** `2021-22/_/plot_inputs.py`, written for GH #1697 after the #1118 inventory, minted `Manquant` on 4 served rows. Fixed and recorded.

## 3. Measurements

Everything below is cold, in private empty roots. Paths are under `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant/`.

| what | job | output |
|---|---|---|
| base frames | 41275616 | `out/base-probe/` |
| raw source probe | 41275616 | `out/probe_raw_base.json` |
| candidate frames at `f001cfbd0` | 41275888 | `out/cand-f001cfbd0aac/` |
| base vs candidate comparison | 41275888 | `out/compare_f001cfbd0aac.json` |
| Senegal join, base | 41275746 | `out/senjoin_base.json` |
| Senegal join, candidate | 41275888 | `out/senjoin_cand_f001cfbd0aac.json` |
| Togo 101009 and Feature dtype probe | 41276006 | `out/probe2.json` |
| broken-wave repro | 41275889 | `out/repro/` |
| make route, direct run and census at the head | 41276506 | `out/final2-fc40960d27b8/` |

### #1118: `u == 'Manquant'`, per wave

| cell | applied lines | blank `s16bq03b` | `Manquant` at base | candidate `Manquant` / `Unknown` |
|---|---|---|---|---|
| Togo 2018 | 13,733 | 12 (3 of them hollow) | 9 | 0 / 9 |
| Togo 2021-22 | 16,042 | 1 | 0 (1 `Unknown` already) | 0 / 1 |
| Senegal 2018-19 | 9,916 | 0 | 0 | 0 / 0 |
| Senegal 2021-22 | 9,368 | 0 | 0 | 0 / 0 |
| CIV 2018-19 | 11,259 | 0 | 0 | 0 / 0 |
| CIV 2021-22 | 14,704 | 4 | **4** | 0 / 4 |

- **No raw answer to keep.** None of the six `s16bq03b` value sets has a missing-marker label, so Niger's keep-the-raw-answer case does not arise.
- **Rows conserved.** Togo 29,772; Senegal 19,284; CIV 25,907. After reading base `Manquant` as `Unknown`, the key sets are identical.
- **Kilograms unchanged.** `seed_kg` and `nitrogen_kg` are `assert_frame_equal` base vs candidate in all three countries.

### Togo 2018 gift and purchase (13,730 served lines)

| column | base | candidate |
|---|---|---|
| `Purchased` True / False | 804 / 12,926 (from `s16bq05`) | 7,128 / 6,602 (from `s16bq08`) |
| `Quantity_purchased` non-null | 804 | 7,128 |
| `Received_as_gift` True / False | NA | 804 / 12,926 |
| `Quantity_received_as_gift` non-null | NA | 804 |
| `Unit_purchased` | NA | 7,122 (`Kg` 4,076, `Litre` 1,898, `Sac(50kg)` 685, …) |
| `Unit_received_as_gift` | NA | 803 |

- **Cells changed:** `Purchased` 7,774; `Quantity_purchased` 7,833; `Received_as_gift` 13,730; `Quantity_received_as_gift` 804; `Unit_purchased` 7,122; `Unit_received_as_gift` 803.
- **The new gift columns equal the old purchase columns row for row**, so nothing served before is lost.
- **Unchanged:** `Quantity`, and every 2021-22 cell (0 changed).
- **Gift × purchase cross-tab:** Non/Non 5,877; Non/Oui 7,049; Oui/Non 725; Oui/Oui 79. So 725 lines that were never bought had been served as purchases.
- **Unit vocabulary.** Unit columns are served in the `u` vocabulary (`ehcvm.served_unit_labels`, as 2021-22 does).

### Senegal

**#1119 relabel.** 2,136 rows move: 735 maize and 460 cowpea in 2018-19, 555 and 386 in 2021-22. No other cell moves.

**#1119 join, 2018-19.** Seed rows that have a `crop_production` row for the same `(t, i, crop)`, measured on the served candidate:

| crop | seed rows | with a harvest row, before -> after |
|---|---|---|
| maize | 735 | 0 -> 723 |
| cowpea | 460 | 0 -> 448 |

`crop_production`, `plot_features`, `seed_kg` and `nitrogen_kg` are frame-equal to base.

**#1287.** I replaced `2021-22/_/plot_features.py` with a failing script, in a job-local clone with a fresh root:

| route | base | candidate |
|---|---|---|
| `Country('Senegal').plot_features()`, default backend | raises `CalledProcessError` from `make ../2021-22/_/plot_features.parquet` | same |
| `LSMS_BUILD_BACKEND=make` | raises `RuntimeError: Make build failed … refusing cached output and alternate/Python fallbacks` (GH #1288) | same |
| `python plot_features.py` run directly, with the 2021-22 wave parquet removed | exits 0 and writes `var/plot_features.parquet` with **2018-19 only (7,767 rows)** | raises `FileNotFoundError: Senegal plot_features: wave 2021-22 declares plot_features but its parquet was not built …` and writes nothing |

- **Who reached the skip.** Both Country routes already raised at base, so only the direct run reached the silent skip. That answers the `unverified-data` question: the default route did not reach it.
- **The new script's success path also ran on real data,** and matched base:
  - healthy `LSMS_BUILD_BACKEND=make` build: 13,824 rows, base == candidate. The log shows the country target succeeding. I infer it ran the concatenator through the Makefile's generic `%.parquet: %.py senegal.py` rule on `_/plot_features.parquet`; I did not instrument that.
  - default build: 13,824 rows, base == candidate.
  - direct run on a healthy root with only the var parquet removed: 13,879 rows (pre-finalize), base == candidate.

### Country vs Feature (candidate `plot_inputs`)

To compare, I dropped the sentinel levels Feature adds (`plot_id` = `Farm-level`, `season`, `input_obs`, `currency`) and renamed `j` to `crop`.
- **Togo (29,772 rows) and CIV (25,907):** index and every value column equal.
- **Senegal (19,284):** equal except seven `*_unlisted` flag columns of the 2021-22 adapter. Country serves them as `object` None/False; Feature serves `string` `<NA>`/`'False'`. **Base shows the same difference**, so this branch did not cause it. Question 3.

## 4. Hash census

Run with `tools/hash_census.py` and `tools/hashdiff.py`, base `f138a3d61` -> head `fc40960d2`; output `out/hashdiff-fc40960d27b8.json`.

| kind | moved / total | which |
|---|---|---|
| `build_transforms_fingerprint` | **0 / 42** | none |
| `framework_imports_fingerprint` | **0 / 38** | none |
| `Country._table_cache_hash` | 66 / 555 | CotedIvoire 21/21, Senegal 23/23, Togo 22/22 |
| `Wave._input_hash` | 110 / 2,321 | CotedIvoire 42 (2018-19 and 2021-22; the four 1985-89 waves 0), Senegal 46 (both waves), Togo 22 (2018 only; 2021-22 0) |

- **Nothing moves outside the three countries.**
- **Senegal `fwimp` does not move**, even though `_/plot_features.py` now imports `Country` and `paths.data_root`. `Country` is a class with no `__code__`, and `lsms_library.paths` is in `_EXCLUDED_MODULES` (`_build_registry.py:99`).
- **Togo 2021-22 wave hashes do not move.** That is deliberate: I did not edit its docstring or the Togo org comment (R3, question 2).

## 5. Tests

All runs used `-n <cpus> --dist=loadfile -p no:cacheprovider`, the BLAS pins and `LSMS_BUILD_WORKERS=1`, each in a cold private root.

| run | job | files | result |
|---|---|---|---|
| full suite at `4ab20ca27` | 41276062 | 65 (`out/pytest_files.txt`) | 3 failed, 4,483 passed, 102 skipped, 8 xfailed |
| rerun at `fcda613b7` | 41276378 | the 3 that failed, plus `test_senegal_seed_crop_labels.py` and `test_senegal_plot_concatenator_refusal.py` | 75 passed |
| extra files at `fc40960d2` | 41276506 | 7 (below) | 317 passed, 8 skipped (Uganda/Malawi `crop_production` not warm) |

- **The 65 files** are the 62 test files naming Togo, Senegal or CotedIvoire, plus `test_schema_consistency.py`, `test_build_transform_hash.py` and `test_derivations.py`. Output: `out/pytest-cand-4ab20ca27.txt`.
- **The 3 failures:**
  - `test_gh1500_senegal_missing_food_unit.py` pins the sha256 of Senegal's `_/data_scheme.yml` and `_/categorical_mapping.org`, both of which this branch edits. Re-pinned with a note in `fcda613b7`. Neither edit is on the `food_acquired` `u` path that test guards.
  - Two of my new tests called `.sum()` on an Arrow array. Fixed in `fcda613b7`.
- **The 7 extra files** came from a broader case-insensitive grep the first selection missed: `test_863_transforms.py`, `test_age_handler.py`, `test_benin_2021_wave.py`, `test_burkina_faso_plot_inputs_identity.py`, `test_gh323_ethiopia_config.py`, `test_gh323_grain_contract.py`, `test_metric_decimal_comma.py`.
- **Not re-run:** the full 65-file suite at the head. Between `4ab20ca27` and `fc40960d2` only test files and `CONTENTS.org` changed (`git diff --name-only`), and the changed tests were rerun.
- **No in-tree parquet** appeared in any job.

**New tests:**
- `test_togo_2018_plot_inputs_acquisition.py`: an AST check of the bindings, plus a `requires_s3` oracle on the `.dta` codes.
- `test_senegal_seed_crop_labels.py`: the label cells, injectivity, and a `requires_s3` pin of the 723 / 448 join.
- `test_senegal_plot_concatenator_refusal.py`: the twin of Burkina's #1287 test.
- `test_cotedivoire_plot_inputs_unit_sentinel.py`.

**Changed pins in shared files** (line ranges for merging with other streams):
- `test_962_unit_sentinel.py:762-764`: the Togo site now mints `Unknown`. The Benin, Burkina and Guinea-Bissau streams probably touch this file.
- `test_gh323_benin_togo.py:23-28` and `:136-137`: Togo comments only. The Benin stream probably edits this file.
- `test_ehcvm_plot_inputs_identity.py:178-180` and `:192-193`: Senegal codes 13 and 18.
- `test_senegal_1483_s16b.py:241-242` and `:281-282`.
- `test_gh1500_senegal_missing_food_unit.py:319-324`.

## 6. Issues

| issue | status | done here | still open |
|---|---|---|---|
| #1118 | partial | Togo, Senegal, Cote d'Ivoire (including the unlisted CIV 2021-22 site) | Benin, Burkina (2018-19 and `s16b_2021.py`), Guinea-Bissau (other streams); GhanaLSS cost-only rows |
| #1119 | partial | the Senegal half | Guinea-Bissau (other stream), GhanaLSS |
| #1287 | partial | the Senegal half | GhanaLSS `community_prices`, plus R1 |
| gift ruling "Fix all three now" | Togo third done | Togo | Benin and Guinea-Bissau (other streams) |

### Drafted residuals

**R1 (tier B): "Togo and Benin plot_features concatenators skip a declared wave (#1287 re-opened by the 2021-22 wiring)"**

> `Togo/_/plot_features.py` and `Benin/_/plot_features.py` loop over `WAVES = ['2018'|'2018-19', '2021-22']` with `except Exception: continue`. #1287 recorded them as "one listed wave … not at risk"; that became stale when GH #1697 wired 2021-22 on 2026-11-12.
>
> The same script in Senegal, run directly with one wave parquet absent, wrote the other wave alone and exited 0 (job 41275889). The Country routes raised earlier, in the wave build.
>
> Fix: the Burkina/Senegal form (a wave declares the table, so its parquet under `data_root()` must exist, else `FileNotFoundError` naming the wave), plus a synthetic test like `tests/test_senegal_plot_concatenator_refusal.py`. Cost: one re-warm each of Togo and Benin.

**R2 (tier B): "Senegal 2018-19 and Cote d'Ivoire 2018-19 plot_inputs: Purchased reads the gift question s16bq05"**

> This is the defect ruled for Guinea-Bissau, Togo and Benin on 2026-11-13.
>
> | | gift `s16bq05` Oui | purchase `s16bq08` Oui | cross-tab Non/Non, Non/Oui, Oui/Non, Oui/Oui |
> |---|---|---|---|
> | Senegal | 536 | 4,956 | 4,504 / 4,876 / 456 / 80 |
> | Cote d'Ivoire | 890 | 8,134 | 2,373 / 7,996 / 752 / 138 |
>
> Both countries' 2021-22 waves read the purchase, so the column means different things across waves. Template: commit `408d02a24`. `test_senegal_1483_s16b.py:629` pins the old Senegal binding. Needs a ruling.

**R3 (tier C): "Two stale Togo comments, left to avoid a 2021-22 re-warm"**

> The `Togo/2021-22/_/plot_inputs.py` docstring still says 2018 serves the gift question. The note above `harmonize_seed_crop` in `Togo/_/categorical_mapping.org` (lines 200-201) has 101009's gift and purchase reversed. Both files are hashed whole into the 2021-22 `Wave._input_hash`; fix them with the next real edit there. CONTENTS records both.

## 7. Questions for the user

1. **Gift fix for Senegal and Cote d'Ivoire.** Should the 2026-11-13 gift ruling also cover Senegal 2018-19 and Cote d'Ivoire 2018-19 (R2)? I did not widen it.
2. **The two stale Togo comments (R3).** Fix them now, at the cost of a Togo 2021-22 wave re-warm (22 hashes) for comments only? I left them and recorded them in CONTENTS.
3. **Senegal Feature flags.** `Feature('plot_inputs')` serves Senegal 2021-22's seven `*_unlisted` flags as strings (`'False'`), where Country serves objects (None/False). This predates the branch. File it?

---

