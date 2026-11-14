# Served-wave corrections, final pass: red-team fixes and the #1708 revaluation

Fix agent **Claude Opus 5.5** (`claude-opus-5-5`), for Sue (scrum master, Slurm job 41201840), 2026-11-12. I used no sub-agents and made no GitHub writes. This file holds aggregates only: no microdata rows and no household ids. The full pytest logs and the pickled frames are in the mode-700 `private/served-corrections-final/`.

## Verdict

**Done; ready for the coordinator.** All six brief items are committed, and every requested check passes on the final code. Three points for decision or disclosure:

1. **"Every other row frame-equal" holds except for Niger 2021-22's kilogram modes.** 114 `food_quantities` (kgs) rows and 66 `food_prices` (kgvalue) rows move, because the inferred kg factors pool across waves (GH #1487). Niger 2021-22 `food_acquired` is unchanged.
2. **Two readings of the ruling's wording are coordinator decisions**, each behind one named constant:
   - **Region floor 1.** 831 rows rest on one genuine report and 964 on two. For those cells the held-out within-25% share is 43–49%.
   - **Revaluing the 2,787 flagged rows that #876 left NA.**
3. **New dependency.** Niger 2018-19 `food_acquired` now reads the cover `s00_me_ner2018.dta`. It is declared, and the table fails loudly when the file cannot be fetched.

- **Branch:** `fix/isa-served-wave-corrections-20261112`.
- **Head:** `e338fedbac29b283f987717136bbb8e4020f953b`. Job 3 tested `e12278688b5ca629e66ba8ac6f7ccc8baf3ba335`; the only later commit changes `Niger/_/derivations.yml` and `CONTENTS.org` text.
- **Base:** development `e5fdd2fa831c4d03a247d264be7f339da459e6b0`.
- **Start:** `3ae15b56ceb8093acaf75c8e24369dcd4d012aea` (asserted).
- **Commits:** the worktree's post-commit hook published each commit to the mirror (`SUCODER: published ...`), and every job cloned the mirror at a full SHA.

| # | commit | brief item | what |
|---|---|---|---|
| 1 | `942159c97` | 1 salt (F1) | The selector reads s07b alone; `household_size:` is removed |
| 2 | `2369864d9` | 2 tests (F2) | Served pins for Mali 2018-19 and the Niger flag; Uganda per-household guard |
| 2b | `777626c82` | 1 | `test_gh1500` Senegal 2021-22 digests re-pinned |
| 3 | `5f113252a` | 3 #1708 | Revaluation at a local median, registered, with tests |
| 5 | `c08f24c40` | 5 #1519 | Expenditure ruling recorded (CONTENTS, registry) |
| 6 | `cb3dcab73` | 6 CLAUDE.md | Site Q rows (in AGENTS.md, the target of the CLAUDE.md symlink) |
| 4 | `a81d8477a` | 4 premise (F3) | Niger CONTENTS and registry: premise corrected, revaluation recorded |
| 7 | `e12278688` | 3 | `test_ehcvm_7day` Niger 2018-19 total re-pinned; two new-test bugs fixed (the head job 3 tested) |
| 8 | `e338fedba` | 3, 4 | Registry and CONTENTS text: the full held-out table, the region-floor cost, the outage scope (neither file is a build input) |

## 1. Senegal 2021-22 salt selector (F1)

- **The rule.** Salt (code 139), in kg (100), size-free (0), with `s07bq03a / 0.0199997` within 1e-9 of a whole number n >= 1. No `hhsize` is read. The welfare file is read only by `inputs_salt_per_capita`, as evidence.
- **Same 100 records.** On the released file (job 41253614) the old and new masks are identical: 100 records. On all 100, n equals the welfare `hhsize`. No other product or unit meets the constant, and 2018-19 has 0.
- **Tolerance, from a measured gap.**
  - The selected records sit at most 3.6e-15 from a whole multiple.
  - The nearest of the other 194 positive salt-kg records sits 4.5e-5 away.
  - Every tolerance from 1e-12 to 1e-5 selects the same 100; 1e-4 adds 2 records and 1e-3 adds 101.
  - A typed value on a 0.01 kg grid sits at least 1.5e-5 away below 600 kg.
- **Outage proof** (job 41253953, fresh private root, `get_dataframe` patched to raise `FileNotFoundError` for `ehcvm_welfare_sen2021` only):
  - `food_acquired()` serves 436,515 rows (2018-19: 215,660; 2021-22: 220,855).
  - `food_expenditures()` serves 319,634 rows.
  - `food_acquired(waves=['2021-22'])` serves 220,855 rows.
  - The build asked for the file 0 times, and its blob is not in the private L1.
  - Positive control: the old branch head raises `FileNotFoundError` under the same patch (2 refused reads).
- **Data_info.** 2021-22 `data_info.yml` is development's again, byte for byte. The hash probe lists only the NSU table and s07b as declared sources.

## 2. Tests (F2)

`TestServed` (`requires_s3`) in `tests/test_gh1707_served_corrections.py` builds the served tables and pins:

- **Mali 2018-19:**
  - 157,856 rows;
  - 2,276 `Riz local Gambiaka`/`Kg` purchased rows, carrying 23,581.7 kg;
  - `derivation_inputs` returns the 3,357 block records, and only 1 block household still has a served rice-kg purchased row;
  - no row carries the deletion key.
- **Niger:**
  - exactly 15,014 flag rows, all purchased, all 2018-19;
  - the revaluation key on exactly the same 15,014 rows;
  - the registered function, run on `derivation_inputs`, reproduces the served Expenditure.

Synthetic tests cover:

- the Uganda per-household guard in `_ea_from_another_wave`;
- the salt rule, and that the salt build reads s07b alone;
- the revaluation function, its positional write, and its refusal of reordered rows.

## 3. Niger 2018-19 #1708: "Revalue at local median"

**Rule.**

- On each flagged purchased row, Expenditure = the served purchased Quantity × the median genuine (unflagged) `s07bq08 / s07bq07a`, for the same product (`s07bq01` code) × unit × size.
- The median is taken in the cluster (`grappe`) when the cell has at least 3 genuine reports there, otherwise in the region (cover `s00q01`) when it has at least 1, otherwise the row is NA.
- **Unit match.** A genuine report prices the cell of its own last-purchase unit and size. Units are compared through `ehcvm._unit_key`, which is #876's identity; nothing is converted. Codes and labels agree in both variables (measured).
- **One named gate.** Every flagged purchased row is revalued, including the 2,787 that #876 left NA.
- **Written by position.** The value is written after the shared hook, which is checked row for row (count, `s`, Quantity, #876 Expenditure, household); a mismatch raises `LocalMedianJoinError`.
- **Keys.** The rows carry the new key `Niger::food_acquired::flagged-purchase-local-median` and keep the #876 and flag keys. Quantity, `Quantity_kg` and consumed quantities are unchanged (verified).
- **Inputs.** The `inputs` callable returns all 107,269 records with `flagged`. The producer's `s07bq07a` and `s07bq08` are recoverable there.
- **New build input.** The cover is declared under `household_region:`. With the cover unfetchable, `food_acquired(waves=['2018-19'])` raises `FileNotFoundError` naming it, and `waves=['2021-22']` still serves. Not measured: an all-waves call, and the kilogram modes of a 2021-22 subset. Both read every wave, so both should raise.

**Floor.** Each genuine report was scored against the median of the *other* reports in its cell. Share of held-out prices within 25%:

| other reports | cluster median | region median |
|---|---|---|
| 1 | 67.1% | 64.6% |
| 2 | 63.6% | 67.4% |
| 3 | 71.7% | 69.1% |
| 4 | 69.7% | 70.7% |
| 5 | 73.5% | 70.8% |
| 10+ | 87.2% | 84.9% |

- The cluster floor is 3. The comparison is noisy below 5 and does not single out 3, but it permits it: from 3 on, the cluster median is within a point of the region's or better. Three is also the smallest median that one stray report cannot move.
- The region floor is 1: the ruling withholds only where there is "no genuine local price".

**Result** (cold, development against `5f113252a`):

| 2018-19 purchases | before | after |
|---|---|---|
| Expenditure, all (FCFA) | 53,351,991.10 | 52,486,790.70 (−1.6%) |
| on the 15,014 flagged rows | 13,890,402.00 (26.04%) | 13,025,201.60 (24.82%) |
| flagged rows with an Expenditure | 12,227 | 13,022 |

**Level of the median used**, out of 15,014 flagged rows: cluster 1,361 (9.1%), region 11,661 (77.7%), NA 1,992 (13.3%).

| flagged rows | cluster | region | NA |
|---|---|---|---|
| 12,227 previously valued | 1,107 | 9,705 | 1,415 |
| 2,787 previously NA | 254 | 1,956 | 577 |

**Sanity against the producer's reference values.** On the 10,812 rows valued both before and after, revalued unit value / producer unit value:

- median 1.00, and exactly equal on 27.5%;
- p10–p90 0.74–1.25, p5–p95 0.62–1.33;
- within 10%: 49.0%; within 25%: 74.9%; within 2×: 94.3%;
- at the cluster level p10–p90 is 0.71–1.22; at the region level 0.74–1.25.

**Other tables.**

- **`food_acquired`:** only `Derivation` (15,014 rows) and Expenditure (11,557 rows) change, all on 2018-19 rows that carry the new key. The index is the same, and 2021-22 is frame-equal.
- **`food_expenditures` and `food_prices(unitvalue)`, 2018-19:** 1,415 rows go, 2,210 appear, and 7,932 values change. 2021-22 is frame-equal.
- **`food_quantities(units='units')`:** frame-equal.
- **Kilogram modes, both waves.** The inferred factors pool across waves (GH #1487), so 2021-22 moves too:
  - `food_quantities` (kgs): 864 rows change in 2018-19 and 114 in 2021-22. The totals go from 202,924.9 to 202,852.9 kg and from 208,904.3 to 208,900.9 kg.
  - `food_prices` (kgvalue): 66 prices change in 2021-22.

**For the coordinator.**

- **Region floor 1.** 831 region-level rows rest on 1 report and 964 on 2. For reports whose cluster has fewer than 3 others, the region median's held-out within-25% share is 48.5% with 1 other report, 43.1% with 2, 49.8% with 3–4, 57.9% with 5–9 and 64.5% with 10+. A region floor of 3 would serve 3,787 rows NA instead of 1,992.
- **Gate set to False.** It would revalue only 12,227 rows; the wave total would be 49,528,911.28.
- **Non-integer `s07bq08`.** 167 genuine reports have one (the red team counted 162). They stay in the price pool. Under the exploration floors, 255–380 revalued rows read a cell that contains one; this was not measured at the shipped floors (cluster 3, region 1).

## 4. #1708 premise (F3)

`Niger/_/CONTENTS.org` and the flag entry now state:

- The flagged rows are where the questionnaire ENABLES `s07bq07a`: 15,263 of 15,264. Their values were reconstructed from a reference unit value.
- The 21,063 disabled-but-filled records are a separate set. They carry no `s07bq08`, feed no served value, and are not keyed.
- One record is in both sets.
- The first ruling's summary ("CAPI disables it there") described the disabled set.

No text calls the flagged rows disabled.

## 5. #1519

The ruling "GH1519 Expenditure on the NA rows" ("A value computed from a withheld quantity is withheld with it") is recorded in Senegal CONTENTS and in the near-zero registry entry, as its own assumption.

## 6. CLAUDE.md (AGENTS.md)

The Site Q lower-tail row and `food_prices` row keep the `997f33c8a` history. Each now states that since 2026-11-12 (GH #1519) the three Senegal quantities are served NA, that the 2018-19 `food_prices` maximum went from 1.79e22 to 479,452, and that the corpus counts were not re-measured.

## Verification

**Jobs.** All ran on `savio4_htc` with 8 cores and 64 GB, `--exclude=n0035.savio4`:

| job | node | what |
|---|---|---|
| 41253614 | n0080 | explore, base builds, base hash, base tests |
| 41253953 | n0045 | candidate builds, compare, outages, hash, candidate tests |
| 41254193 | n0034 | final-head pins, tests, hash |

**Roots and run settings.**

- Private roots were exported before the mount: `LSMS_DATA_DIR`, `XDG_CACHE_HOME`, `TMPDIR`, `MPLCONFIGDIR`, `LSMS_ISSUES_LOG`.
- Each step had its own `/local/job<ID>/priv-*` (mode 700). Every script asserted the import identity and that `data_root()` was private.
- BLAS was pinned to 1 thread, `LSMS_BUILD_WORKERS=1`, and pytest ran `-n 8 --dist=loadfile` with output to a file.

**Cold builds** (outer-merge diff and per-wave `assert_frame_equal`):

- **Mali:** 3,356 rows removed and 1 changed, in 2018-19 only.
- **Senegal:** 100 rows removed in 2021-22; 3 rows changed in 2018-19 (2 in Expenditure).
- **Uganda `sample` 2009-10:** `ea` and `Derivation` change on 514 rows. `cluster_features` and `household_characteristics` are frame-equal.
- **Niger:** as in §3.
- **Everything else is frame-equal**, except the Niger kilogram modes in 2021-22.

**`derivation_inputs` joins.**

- Mali 3,357 records (1 still served).
- Senegal salt 100 records; near-zero 3.
- Niger flag: 15,014 served keys, 0 without inputs.
- Niger revaluation: 15,014 served keys, 0 without inputs.
- Uganda: 514 served keys, 0 without inputs.

**Hash footprint**, development against the candidate:

| hash | moved | where |
|---|---|---|
| `_table_cache_hash` | 96 of 544 | Mali 23/23, Niger 24/24, Senegal 22/22, Uganda 27/27 |
| `Wave._input_hash` | 307 of 2,258 | Mali 23/92 and Niger 24/96 (2018-19 only), Senegal 44/44, Uganda 216/216 |
| build fingerprints | 0 of 41 | |

Nothing moved elsewhere. Job 3, development against `e12278688`, gives the identical footprint; candidate against `e12278688` moves 0 hashes. No build-path file has changed since `5f113252a`, so the cold builds, joins and outages above describe the head. Job 3 also read the pins from the candidate frames: Mali 157,856 rows, 2,276 rice-kg purchased rows and 23,581.7 kg; Niger 15,014 rows with each key.

**Tests.** The selection is every test file naming Mali, Niger, Senegal, Uganda or `ehcvm`, plus the drift and schema tests.

| run | result |
|---|---|
| development | 10,021 passed, 190 skipped, 8 xfailed, 0 failed (261 files) |
| candidate `5f113252a` | 10,068 passed, 3 failed: the expected `AFTER` pin and two bugs in tests this pass added, all fixed in `e12278688` |
| `e12278688` (final code) | **10,070 passed, 190 skipped, 8 xfailed, 0 failed** (262 files, 1,037 s) |

**Hygiene.**

- No in-tree parquet in any clone.
- Issues logs were private: 0 lines in every build root, 4 in each simulated-outage root, 240 in the test roots.
- No `dvc` CLI was run and no lock was removed; every source was read through `get_dataframe`.
- `config.yml` was readable and `LSMS_SKIP_AUTH` was unset.

**Rule slips, disclosed.** On n0035:

- Early on I read source files with `sed -n` several times. After that I used the Read tool.
- One command carried a stray no-op `python3 -c 1` (stderr discarded) before a `jq` call on my own results JSON. It imported nothing and read no data.
- I also ran `jq`, `grep`, `awk`, `sha256sum` and `git` there.
- I ran `awk` twice on the private pytest log. The second call also printed the Mali served test's traceback lines (code and error type, no data), as well as the synthetic salt test's.

## Files

All under `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/served-corrections-final/`:

- `PR_BODY.md` and this report.
- `scripts/`: scripts and sbatch files.
- `results/`:
  - `explore.json`, `build_*.json`, `compare.json`, `revaluation.json`, `inputs_joins.json`, `outage_*.json`;
  - `hash_*.json`, `hash_diff.json`;
  - `pytest_*_summary.txt`.

Private data to delete when this is closed: `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/served-corrections-final/`.
