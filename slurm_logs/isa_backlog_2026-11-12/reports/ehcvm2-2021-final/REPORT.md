# EHCVM-2 2021-22 wiring, finished on the 2026-11-12 rulings: Togo, Benin, Cote d'Ivoire (addresses #1697)

Agent: fix agent, Claude Opus 5.5 (`claude-opus-5-5`), dispatched by Sue (job 41201840, n0035.savio4). No sub-agents.
- Worktree: `/local/job41201840/worktrees/ehcvm2-2021`.
- Branch: `feature/552-ehcvm2-2021-wiring-20261112`.
- Start: `df1df60edc089920ab02ad445226a4764c9feb12` (asserted).
- Base: development `e5fdd2fa831c4d03a247d264be7f339da459e6b0`.

Previous report: `../ehcvm2-2021/REPORT.md` (read in full). This report covers only what changed since then and what was measured.

## Verdict

READY for a development PR (body: `PR_BODY.md` here).
- The three rulings are applied: strata, harvest, and Benin's panel link as a pointer to #1705.
- Old waves are frame-equal to base.
- 2021-22 `sample` changes only in `strata` / `Derivation`.
- GhanaLSS serves identical values.
- Targeted tests pass on base and candidate.

One choice needs the maintainer's confirmation: Cote d'Ivoire 2021-22 upper-cases the region before the shared helper (below).

## Commits

| commit | what |
|---|---|
| `62a3ed5f1` | 2021-22 strata on the #1575 rule (all three countries). Harvest entries extend Mali's ruled derivation. Benin link points to #1705. Wiring references #552 -> #1697, Benin leavers -> #1565. Tests cover 2021-22. |
| `d4a0e6245` | `CONTENTS.org`: the strata measurements, Benin's `strat` cross-tab, dated superseding notes and issue pointers. Docs only; no build input. |
| `6538bc8b8` | After review: drops the stratum-count goldens from the wave tests (the #1575 ruling forbids them). The harvest `rule` texts say "extends Mali's". Each stratum entry's `validated_against` records the 2021-22 measurement. The Benin `strat` attribution is narrowed. Tests, `derivations.yml` and `CONTENTS.org` only; no build input. |

Head: `6538bc8b88c331835c9f78a316f3bf2092bae2b5`, published to the mirror by the hook (`origin/feature/552-ehcvm2-2021-wiring-20261112`). Nothing was pushed to GitHub, and no merge commit was made.

## Idiosyncrasies (read in each `_/CONTENTS.org`, sample/strata and 2021-22 sections)

**Togo**
- Region code 6 is `Lomé commune` in 2018 and `Grand Lomé` in 2021-22, on the same 98 grappes.
- Two documents disagree on Lomé: the June 2021 BID p.10 says "except Lome Commune where all respondents are from rural areas", while the 2023 Table 1a gives 98 urban / 0 rural clusters. Both covers hold only an Urbain Lomé cell.
- Sources are in `2018/Data1/`.
- Three panel flags disagree. `panel` = 1 is at real-link level (96.1% head sex) and `panel` = 2 (1,397) is at the baseline.

**Benin**
- `s00q01` is the department, and both waves spell all twelve in lower case, identically.
- Littoral is urban-only in both waves; the DDI states 23 strata.
- `PanelHH` = Panel is at real-link level (94.8% on 6,878).
- `ehcvm_individu.age` was set from 2018-19 for panel members.
- `s01` lists 6,138 people who have left.
- The 2021-22 weights file carries `strat`.

**Cote d'Ivoire**
- 23 of 33 regions are labelled in lower case in 2021-22, on 2018-19's codes; the wave's `Region` formatter upper-cases them.
- The cover holds 728 households that were not interviewed, so `sample` takes the weights file as primary, merged left.
- One 2018-19 grappe (648) holds two milieux, served literally.
- The link questions 07F1/07F2 are asked but not distributed.
- `s16cq16a` is empty on every finished crop row, which is Mali's shape, sharper.

## What changed

1. **Strata, "Same rule as #1575".**
   - All three 2021-22 waves use the 2018 spec, `[s00q01, s00q04, {mapping: strata_label}]`, with the shared `ehcvm.strata_label` and the shared `sample` stamp hook (`ehcvm.stamp_region_milieu_stratum`).
   - `'2021-22'` is added to each `<C>::sample::region-milieu-stratum` entry's `waves`. The `inputs` callable stays the earlier wave's `mapping:inputs_strata`; it reads each wave's own cover, as Senegal, Mali, Niger and Burkina do.
   - Rule and assumption text is updated. The pinned "Actual-row validation UNRUN" sentence about the original 2018 construction is kept.
   - **Togo** keeps each wave's region-6 label literally.
   - **Cote d'Ivoire** (a choice, flagged): the YAML names a wave-local cell formatter, `strata_label_upper_region`. It upper-cases `s00q01` and then calls `ehcvm.derive_region_milieu_stratum` unchanged, so the NA and refusal semantics are the helper's.
     - Why: without it, 23 regions are served as `poro Rural` beside 2018-19's `PORO Rural` and the same wave's `Region` `PORO`. That breaks the helper's documented invariant that the region part is `cluster_features.Region`, and splits 46 of 66 strata by case.
     - The name-bound `strata` formatter is removed. `mapping.Region` is unchanged.
     - The registry `rule` says the raw inputs give lower case.
   - **Benin**: `strat` is not read. It is compared in `CONTENTS.org` (see below).
   - **Tests.**
     - `test_gh1575_{togo,benin,cotedivoire}...`: invented frames, no data. Covered: scope `[earlier, '2021-22']`; scoped `call_inputs` visits both waves and reads each wave's own cover under its own `i` formatter; 2021-22 extraction conserves every other field; NA / blank / code refusal on the 2021-22 path; Togo's `Grand Lomé Urbain`; Cote d'Ivoire's fold.
     - `test_gh812_ehcvm_strata.py`: pins each documented country's scope (Guinea-Bissau stays `['2018-19']`; its 2021-22 is still unwired) and the 2021-22 declarations. Its Feature test asserts that both Togo waves are constructed, with the two Lomé labels each in its own wave.
     - The three wave tests: the oracles now build region + milieu from the codes; every row carries the key; new tests re-derive the served strata from `derivation_inputs` (Cote d'Ivoire with the fold: 13,693 inputs against 12,965 served).
2. **Harvest from dispositions** (Benin and Cote d'Ivoire): kept as wired.
   - Each `derivations.yml` entry now opens with an assumption that it extends `Mali::crop_production::harvest-from-dispositions`, citing the 2026-11-12 ruling and stating that #1437 stays open.
   - `CONTENTS.org` carries dated notes. No rule or value changes; `derivations.yml` is not a build input.
3. **Benin panel link**: not built. `sampling.yml`'s note, the wave's `mapping.py` / `data_info.yml` comments and `CONTENTS.org` point to the ruling and to #1705.
4. **Issue numbers.**
   - "GH #552" becomes "GH #1697" in the three countries' 2021-22 scripts, mappings, YAML, the country-level comments, `sampling.yml` and the tests.
   - Left alone: provenance `SOURCE.org`, the generated `population.yml` (whose "Registered ... (GH #552)" is the 2026-11-04 history), `ehcvm.py`, `coverage_matrix.py`, Guinea-Bissau, and the dated `CONTENTS.org` headings (`contents:` fields point at them). Each `CONTENTS.org` says the older "GH #552" is the issue as it stood then.
   - #1565 is now referenced for Benin's leavers. #1556, #791 and #1598 are stated per country.
5. **GhanaLSS**: checked cold; no served value changes (below).

## Measured

### `sample`, cold, three sides (burst job 41240821)
Each side was built in its own private root: base `e5fdd2fa8`, prev `df1df60ed`, cand `62a3ed5f1`. Compared by `scripts/compare_sample.py`; output in `jobs/samples-41240821/compare_sample.json`.

How "equal" was tested:
- Values: `assert_frame_equal` after sorting by `(t, i)`, with `check_dtype=False`. Object columns were cast to `string` before saving.
- dtypes: diffed separately from each side's served `dtypes` (`frames/<side>/<C>.json`). They are identical on all three sides for all three countries.
- Index: `(i, t)` on every side.
- `attrs`: the candidate's `derivations` summary keys the stratum across both waves, e.g. Togo 12,633 = 6,171 + 6,462 rows.

| country | wave | households | strata labels (prev -> cand) | NA | keyed | grappes with 2 strata | vs base | vs prev |
|---|---|---|---|---|---|---|---|---|
| Togo | 2018 | 6,171 | 11 -> 11 | 0 | 6,171 | 0 | equal, all columns | equal |
| Togo | 2021-22 | 6,462 | 6 -> **11** | 0 | 6,462 | 0 | (not in base) | `v`, `Rural`, `weight`, `panel_weight` equal; same `i` |
| Benin | 2018-19 | 8,012 | 23 -> 23 | 0 | 8,012 | 0 | equal | equal |
| Benin | 2021-22 | 8,032 | 12 -> **23** | 0 | 8,032 | 0 | | same four columns equal; same `i` |
| CIV | 1985-89 (4 waves) | 1,588 / 1,600 x3 | 5 -> 5 | 0 | 0 | 0 | equal | equal |
| CIV | 2018-19 | 12,992 | 66 -> 66 | 0 | 12,992 | 1 (648, as recorded) | equal | equal |
| CIV | 2021-22 | 12,965 | 33 -> **66** | 0 | 12,965 | 0 | | same four columns equal; same `i` |

- **Labels shared across the two EHCVM waves:** Togo 10 of 11 (`Lomé commune Urbain` is 2018-only and `Grand Lomé Urbain` 2021-22-only), Benin 23 of 23, Cote d'Ivoire 66 of 66.
- **Region and milieu parts:** in every country and both EHCVM waves, the region part equals `cluster_features.Region` and the milieu part matches `Rural` on every grappe.
- **Togo households per label, 2021-22 (2018):**
  - Grand Lomé Urbain 1,169 (Lomé commune Urbain 1,034);
  - Savanes Rural 935 (930); Plateaux Rural 887 (855); Kara Rural 804 (788); Maritime Rural 744 (719); Centrale Rural 633 (609);
  - Kara Urbain 348 (339); Plateaux Urbain 259 (246); Maritime Urbain 240 (227); Centrale Urbain 228 (210); Savanes Urbain 215 (214).

### Benin `strat`, a comparison only (`jobs/samples-41240821/benin_strat.json`)
- `ehcvm_ponderations_ben2021.dta` holds `strat` as `int16`, with no value labels and 24 codes.
- `strat` = 10 x department + `vague` on **8,032 of 8,032** households. With the milieu code in place of `vague` the identity holds on 3,965.
- It is constant within all 670 grappes.
- So **`strat` is department x visit (12 x 2), not region x milieu**. The restored wiring's YAML comment called it "24 region x milieu cells", which is wrong. `CONTENTS.org` and the previous PR body offered it as a 24-cell alternative to the DDI's 23 strata, which it is not. Corrected in `CONTENTS.org`; the YAML comment was already replaced in `62a3ed5f1`.
- Every constructed stratum spans its department's two codes, for example `littoral Urbain` = 81: 347 + 82: 347, and `atlantique Rural` = 31: 288 + 32: 276. Every code except Littoral's holds both milieux.
- The full 23-row table is in `Benin/_/CONTENTS.org`, 2026-11-12.
- `strat` by `PanelHH` (0 / 1): code ending 1: 696 / 3,317; ending 2: 447 / 3,572.

### Cache scope (config-only hash probe, same job)
Base `e5fdd2fa8` vs `62a3ed5f1`:
- `Country._table_cache_hash` moved 86 of 544: Togo 22/22, Benin 22/22, CotedIvoire 21/21, GhanaLSS 21/21.
- `Wave._input_hash` moved 361 of 2,258 (44 / 44 / 126 / 147).
- `build_transforms_fingerprint` moved 0 of 40.

This is the same scope as the restored wiring before this change: the strata wiring adds no country.

`d4a0e6245` and `6538bc8b8` touch only `CONTENTS.org`, `derivations.yml` and tests, none of which is a build input, so the `sample`, hash and GhanaLSS results measured on `62a3ed5f1` hold for the head.

### GhanaLSS, cold, base vs candidate (burst job 41240822)
Each side was built in its own private root, 12:40 wall each, peak RSS 5.8 GB. Compared by `scripts/compare_ghana.py`.

| table | rows (base = cand) | frame | `attrs` |
|---|---|---|---|
| `food_acquired` | 5,328,568 | exactly equal, same order and dtypes | equal |
| `food_quantities()` | 1,623,676 | exactly equal | equal |
| `food_prices()` | 560,023 | exactly equal | equal |
| `nutrition` | 34,798 | exactly equal | equal |

- **No served value changes.**
- Why: the only size word added to `_UNIT_SIZE_WORDS` is `Très grand`. Not one GhanaLSS `u` label, in `food_acquired` (60 distinct), `food_quantities` or `food_prices`, carries any size suffix the vocabulary recognises (0 rows), so the size-pool rule has nothing to pool there.
- The 21 moved GhanaLSS hashes are a re-warm that reproduces the same values.
- `nutrition`'s `make` subprocess inherited the private root: `GhanaLSS/var/nutrition.parquet` was written under each private root, and no parquet was written in-tree.

### Tests (burst jobs 41240823 and 41242293)
The targeted selection (`scripts/tests.sbatch`, 38 files) covers:
- every `tests/` file named Togo, Benin, Ivoire, CIV, EHCVM or Guinea-Bissau, including the GH1575 strata files, gh812 and the three 2021-22 wave files;
- `test_sample.py`, `test_sampling.py` and `test_recall*.py`;
- `test_build_transform_hash.py`;
- `test_derivations.py`, `test_gh218_derived_registrations.py` and `test_gh1437_mali_harvest_total.py`;
- `test_population.py` and `test_panel_ids_persistence.py`;
- the size-pool and shocks tests, `test_gh675_degraded_build.py`, `test_gh1093_declared_unit_consumers.py` and `test_feature_attrs_carriers.py`.

It was run with `pytest -n 8 --dist=loadfile`, BLAS pinned and `LSMS_BUILD_WORKERS=1`, in a fresh private root per side. Then `tests/test_schema_consistency.py -k "Togo or Benin or CotedIvoire"`.

| side | sha | targeted | schema |
|---|---|---|---|
| base | `e5fdd2fa8` | 2,627 passed, 86 skipped, 4 xfailed, **0 failed** (35 files; the three 2021-22 wave files do not exist on base) | 26 passed |
| cand | `62a3ed5f1` | 2,845 passed, 83 skipped, 4 xfailed, **0 failed** | 26 passed |
| final head | `6538bc8b8` | 2,845 passed, 83 skipped, 4 xfailed, **0 failed** | 26 passed |

Skips are Nepal (no microdata), `test_sample.py`'s "no non-null weights" cases and two case filters in `test_gh323_benin_togo.py`. No GH1575, gh812 or 2021-22 wave test was skipped, so their data-gated tiers ran against `get_dataframe` reads.

Also run config-only in fresh private roots on n0035: `test_gh1578_derivation_duplicates.py`, `test_1081_derived_categorical_errors.py` and `test_gh218` (119 passed). `test_ghanalss_visit_derivations.py` was not run; it is GhanaLSS-only and its values are unchanged.

In-tree parquets after every job: none (`intree_parquets*.txt` empty).

## Contradictions found (reported, not resolved)

1. **Benin `strat`.** The restored YAML comment says "24 region x milieu cells", and the earlier notes offered it as an alternative stratification. It measures as department x vague (above).
2. **#1705 and Togo.** #1705's body says Togo's and Cote d'Ivoire's "flagged keys sit at the wrong-household baseline". That holds for Cote d'Ivoire. Togo's `CONTENTS.org` measures `panel` = 1 at real-link level (96.1% head sex) and only `panel` = 2 (1,397) at the baseline. Togo's link is not ruled and none is served. The note is added to Togo `CONTENTS.org`.
3. **#1565 and Benin.** #1565 says the leavers decision "should precede" Benin 2021-22's `household_roster` step. The archived wiring already serves the roster without the 6,138 departed people, as `ehcvm_individu` does (the de facto Senegal treatment). Behaviour is unchanged and flagged in `CONTENTS.org`, the YAML header and the PR.
4. **`ehcvm.py` comment** (lines 796-797): "Benin, Togo, Guinea-Bissau and Cote d'Ivoire serve the region alone (`strata: s00q01`) and do not import this." This is now false for every wired wave. It was left alone so that `ehcvm.py` stays out of this diff's hash scope; fix it with the next `ehcvm.py` edit.

## Draft issue comments (not posted; no GitHub writes)

- **#1697**: "The Togo / Benin / Cote d'Ivoire 2021-22 wiring is in PR #___ (branch `feature/552-ehcvm2-2021-wiring-20261112`).
  - Strata follow the 2026-11-12 ruling, 'Same rule as #1575': region (department) x milieu by the shared helper. Labels: Togo 11 (10 shared with 2018; Lomé kept as each wave's own label), Benin 23 (all shared), Cote d'Ivoire 66 (all shared; the region is upper-cased first, as the wave's `Region` is).
  - Benin's weights-file `strat` is department x vague, recorded and not served.
  - Harvest-from-dispositions follows 'Extend Mali's derivation'.
  - Benin's link is #1705.
  - Older waves are frame-equal to base.
  - Remaining here: Guinea-Bissau 2021-22, and #1598 own-country NSU."
- **#552**: "Wiring is #1697 / PR #___; nothing here changes #552's acquisition scope."
- **#1556**: "The Togo, Benin and Cote d'Ivoire 2021-22 wirings in PR #___ read purchases `s14bq04f` and stock `s14bq04e`. Remaining: Guinea-Bissau 2021-22 and Malawi 2024-25."
- **#791**: "The three World Bank 2021-22 waves are wired (PR #___, #1697). The UEMOA scope here is unchanged."
- **#1565**: "Benin 2021-22's roster (PR #___) leaves out the 6,138 leavers, as `ehcvm_individu` does, pending this decision. Revisit Benin when it is ruled."
- **#1437**: "Benin and Cote d'Ivoire 2021-22 apply Mali's derivation on the 2026-11-12 ruling (the registry entries say so). The open questions here are unchanged."
- **#1705**: "The wiring PR keeps Benin's `E2_` ids unlinked and points here. Note: this issue's text on Togo conflicts with Togo `CONTENTS.org` (`panel` = 1 at real-link level)."

## Residuals
- #1598: own-country NSU weights for all three countries.
- Guinea-Bissau 2021-22.
- #1557.
- #1566 (checked previously).
- The survey-kg `Quantity_kg` on Benin and Cote d'Ivoire 2021-22 `crop_production`.
- Togo's link (not ruled).
- The `ehcvm.py` comment.
- #1565.
- The earlier report's residuals that this change does not touch, such as Togo `s07bq03a`, Cote d'Ivoire off-grid `s07bq03a`, crop codes 56-60 and the 2018 gift-under-`Purchased` defect.
- The framework residual that `call_inputs` does not refuse a named wave outside `waves` is unchanged, but is now moot for these three countries.

## Unverified limits
- The Cote d'Ivoire upper-case fold is a choice. It is consistent with the wave's `Region` and the helper's docstring, but it is not separately ruled.
- Not exercised here: strict levers, warm-cache reads, `assume_cache_fresh`, the make backend, and a whole-`tests/` data-gated run. The previous report's wider runs (on `03f438a54` and `df1df60ed`) cover the parts this change does not touch.
- The derived food tables of Togo, Benin and Cote d'Ivoire were not rebuilt: this change moves no food input, though it moves those countries' hashes.
- Only `sample` was frame-compared against `df1df60ed`. This change touches no other table's build input except comments and docstrings (the issue-number edits). The other 2021-22 tables rest on the previous report's cold comparisons and on the wave tests' oracles, which ran here.
- Light config-only pytest and registry loads ran on n0035 with no microdata reads (load average 20 there; nice'd, about 5-17 s each). The first runs shared one private root (`/local/job41201840/ehcvm2final-priv`), with `data_root()` asserted once rather than per invocation; the last two used fresh roots (`-priv2`, `-priv3`) with the assertion. Every data read and build ran in burst jobs with fresh private roots per side.
- Development CI and `pr-data-validation` remain the gates.

## Paths
- Scripts: `scripts/` (`samples.sbatch`, `build_sample.py`, `compare_sample.py`, `benin_strat.py`, `ghana.sbatch`, `build_ghana.py`, `compare_ghana.py`, `tests.sbatch`, `explore.*`, `hash_probe.py`, `hash_diff.py`, `common.sh`).
- Jobs: `jobs/<label>-<id>/`.
