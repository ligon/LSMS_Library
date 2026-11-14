# GH #587, final: red-team findings + the two newest rulings, Nigeria PP `community_prices`

Agent: Claude Opus 5.5 (`claude-opus-5-5`). No sub-agents.
- **Worktree:** `/local/job41201840/worktrees/587-mapping`, branch `feature/587-nigeria-pp-mapping-20261112`.
- **Start HEAD:** `e5940eaa944aeb0555b06f85017e791589f6eceb` (asserted). **Base:** `e5fdd2fa831c4d03a247d264be7f339da459e6b0`.

**Verdict: READY** for human review.
- **Head:** `9e74f2cf9b5961d2c6873f791967166e8eb40dad`.
- **PR body:** `reports/587-final/PR_BODY.md`.
- **One judgement to look at:** Semovita's Aggregate (§4).

## Commits (all auto-published to the mirror)

| SHA | what |
|---|---|
| `d0ba8ee2363372b4979ea16f9e98ad1b8b3615c5` | **Hashed edits, registry and tests.** W1 N bands: 3 new holds plus the unvalidated key. Semovita: harmonize_food code 15's 2012Q3 cell plus a label-only row. `u` rows 1020-1022. W5 Bottle/Can → `metric-size`. Red-team F3, F4, F6 (yml), F7. |
| `b64e1229f562c79b9bf5439743fd8a4906340b7f` | Test fix: two new pins read org cells as text, but the parser returns NA and a float Code. |
| `b330b570e4a2d1f48dad6ae2cb3bc842f875b544` | Prose: CONTENTS.org (rulings, N-band table, counts, Semovita, Owed list, F6); derivations.yml W5 ratios; ledger step 4. |
| `cad166d40d3b9600536945b5337dae9a77462465` | GH #1537 harmonize_food pins moved over the Semovita change. The fixture and its sha256 are unchanged, and each test also asserts the change's exact extent. |
| `4ce666e975e85f2c462b7e08d42931aeb770e54b` | CONTENTS.org: six 2026-11-12 GH587 entries, not seven (one word). |
| `9e74f2cf9b5961d2c6873f791967166e8eb40dad` | Prose. `pp-w1-metric-package-hold`'s rule still said g/ml N > 1 "are divided as usual"; now N 2-5 held, N >= 6 divided. The Unknown N > 50 note (0.005x divided, 0.99x undivided; divided only because the ruling names all 49) in the yml and CONTENTS.org. The unvalidated key's t-only selector stated. Ledger: `build_mapping.py` would revert MAPPING.org's two hand-edited rows. |

The cold builds ran at `d0ba8ee23`. Every later commit touches tests or prose only: CONTENTS.org, the
derivations.yml assumption text and the ledger. None of these is a build input.

## 1. Idiosyncrasies (named)

Read for this step:
- the CONTENTS.org "Post-planting community prices (GH #587)" section and the C8 decoder section;
- the harmonize_food and `u` prose in `categorical_mapping.org`;
- the red-team VERDICT and the previous REPORT / PR_BODY;
- all six 2026-11-12 GH587 decision-log entries.

1. **W1's PP unit card is its own** (101-104 metric). The bands key on the decoded base `u`, never on a code number.
2. **W1/W2 PP item codes follow their quarter's household numbering, except W2 code 15 = Semovita.** The
   2012Q3 household card has no code 15. Measured: 0 code-15 rows in every household food file after W1;
   W1 PP has 2,747 and W1 PH 2,464, all Bread.
3. **Household files record Semovita only as an other-specify recode.** It appears as
   `231. Semovita/semolina` (`item_os_cd` / `item_cd_alt`) under item_cd 23 "Other grains and flour":
   - W4 PP: 59 of 60 (the 60th is under 38);
   - W4 PH: 29 of 29;
   - W5 PP: 32 of 32;
   - NLSS 2018-19: 69 of 69.

   **This is new. It is added to CONTENTS.org and to the food prose in `categorical_mapping.org`.**
4. **The quarter resolver and the Code contract differ by design** (GH #1144). A label-only row is the
   established way to add a Preferred Label that only a quarter cell names (Groundnuts, Maize (shelled)).

No finding contradicts CONTENTS.org. Stale counts there are corrected: Pepper 837 → 808, and the served
`u` token counts.

## 2. W1 band table (N > 1; every measured count equals the red team's)

| class | N 2-5 | N 6-50 | N > 50 |
|---|---|---|---|
| Kg | 547 unvalidated | **208 held** (`pp-w1-kg-mid-count-hold`) | 128 held (`pp-w1-count-as-size-hold`) |
| non-metric | 1,432 unvalidated | 1,601 unvalidated | **241 held** (`pp-w1-nonmetric-count-as-size-hold`) |
| Unknown | 20 unvalidated | 26 unvalidated | 3 unvalidated |
| g | **61 held** (`pp-w1-metric-small-count-hold`) | 263 supported | 1,754 supported |
| ml | **3 held** (same key) | 44 supported | 328 supported |
| l | 183 supported | 153 supported (mixed) | 204 held (`pp-w1-count-as-size-hold`) |

Totals: newly held 513 = 208 + 241 + 64; unvalidated 3,629 (`pp-w1-number-of-units-unvalidated`);
supported 2,725 (`pp-w1-number-of-units`). Together they make the old key's 6,867.

The W1 count cells (F5) are 15,015 at N = 1, 7,199 at N > 1 and 105 with no count, 22,319 in all. Every
N is an integer.

## 3. Per-wave PP served / held (cold, candidate `d0ba8ee23`)

| PP t | served | keyed | held |
|---|---|---|---|
| 2010Q3 | 20,326 | 2,725 supported + 3,629 unvalidated | no-count 105; metric package 1,043; count as size 332; g/ml N 2-5 64; Kg N 6-50 208; non-metric N > 50 241 |
| 2012Q3 | 21,196 (72 Semovita) | -- | unrecorded size 209; item-hold 0 |
| 2015Q3 | 24,995 (18,365 / 6,630) | -- | unrecorded size 12,934 |
| 2018Q3 | 32,246 | 3,156 `pp-c2-metric-size` | -- |
| 2023Q3 | 29,938 | 4,619 `pp-c2-metric-size` (63 new) + 208 `pp-w5-metric-unit-quantity` | unrecorded size 23; `c2q2a` != 1: 60 |

- **Totals:** PP 128,701; `community_prices` 275,493, of which 146,792 PH.
- **PH:** the 146,792 PH rows are frame-equal to `e5940eaa9`.
- **PP keyed diff from `e5940eaa9`:**
  - 513 rows leave;
  - 135 arrive (72 Semovita; 63 Bottle/Can, now `cl`);
  - 3,629 change their key only;
  - 0 change `Price` or `Size`.
- **W5's 63 Bottle/Can, divided:** 0.96x the same round's `pp-c2-metric-size` item-unit medians (62
  matched, p10-p90 0.88-1.07, all within 2x), and 0.70x PH 2024Q1 (62, all within 2x).

## 4. Items and units

**W3 161 → Palm wine; W2 0 → dropped.** Both were already on the branch; confirmed. Code 161 serves 77
rows as Palm wine. Code 0 has 0 priced rows and stays in `PP_ITEM_HELD`.

**Semovita:**
- **Wiring:** code 15's 2012Q3 cell plus a label-only row, Preferred `Semovita` and Aggregate
  `Other grains and flour`. The Code contract still reads code 15 as Bread.
- **Source of the Aggregate:** the Nigeria household recode in §1.3.
- **Alternatives disclosed:**
  - GhanaLSS 2016-17 maps "Wheat semolina, suji" to `Wheat Flour` (`GhanaLSS/2016-17/_/categorical_mapping.org:38`, `:679`);
  - EHCVM keeps `Semoule de blé` as its own label (Mali `:391`, Senegal `:83`, Niger `:817`, Burkina `:318`);
  - Serbia's COICOP 0106 is "Wheat/Rye Flour and Semolina".
- **`labels='Aggregate'`:** Semovita maps to `Other grains and flour` (2012Q3: 20 + 72 = 92).
- **W2 PP Kg medians:** Semovita 450 Naira, Wheat flour 220, Other grains and flour 1,900.

**`u` tokens:**
- `Congo/Mudu`, `ml` and `Tomatoes cup` are codes 1020-1022, with no per-wave cell.
- They are served on 1,830 / 372 / 229 rows. The red team's 1,834 / 375 / 231 predate the holds.
- The 399 blank-unit PP rows stay `Unknown`.
- In the served frame, the only PP `u` outside the `u` Preferred Labels is `Unknown`. The only PP `j`
  outside harmonize_food is `Pepper` (808).

**Before editing the categorical_mapping table:**
- **Re-warm:** the Nigeria re-warm is already owed. The red team measured all 24 Nigeria
  `_table_cache_hash` values moving on this branch.
- **Other tables, cold against base:** `food_acquired` (595,725), `sample` (49,089), `household_roster`
  (287,630), `crop_production` (66,090), `plot_inputs` (96,620) and `plot_features` (56,403) are all
  frame-equal (`check_exact`), with equal attrs and identical warning and audit sets.
- **`food_acquired` `j`:** no row changes. No household file after W1 has a code-15 row, so the 2012Q3
  cell edit reaches no household row.

## 5. Verification

All jobs ran on `co_carleton` / `savio4_htc` with `--exclude=n0035`. Private roots were exported before
the mount, and the import identity and private `data_root` were asserted.

| job | SHA | what | result |
|---|---|---|---|
| 41250939 | `e5940eaa9` | `measure.py`: bands, W5 groups, W2 items, W3 161, Unknown, household codebook sweep | §2 |
| 41250997 | `e5940eaa9` | `measure2.py`: where the household files classify Semovita | §1.3 |
| 41251131 | `d0ba8ee23` | tests | stage 1: 2 failed (my test-reading bug), 277 passed; fixed in `b64e1229f` |
| 41251132 | base / `e5940eaa9` / `d0ba8ee23` | cold builds + `compare.py` | §3, §4; 0 in-tree parquets; clean trees |
| 41251275 | `b64e1229f` | registry / label checks (rerun after a name clash in my script: `ll.derivations` is a function) | all 4 constructions join and reproduce Price; 9 omissions unique, disjoint, attrs `rows: 0` |
| 41251198 | base / `b64e1229f` | targeted selection | base 1,370 / 79 skipped / 0 failed; cand 1,515 / 80 / **3 failed** (GH #1537 pins); fixed in `cad166d40` |
| 41251433, 41251685 | -- | cancelled by me (superseded heads) | -- |
| 41251929 | `9e74f2cf9` (final) | stage 1 only (the last commit is prose) | **308 passed, 1 skipped, 0 failed** |
| 41251687 | base / `4ce666e97` | stage 1 + targeted selection | stage 1: **308 passed, 1 skipped, 0 failed**. Base (45 files): **1,370 passed, 79 skipped, 0 failed**. Candidate (47 files): **1,518 passed, 80 skipped, 0 failed** |

The targeted selection is `test_gh587*`, every `test_*nigeria*`, `test_community_prices_variety`,
`test_derivations` (both drift directions), `test_build_transform_hash`,
`test_no_build_input_in_contents_org`, `test_categorical_mapping_resolution`, `test_gh1537*` and
`test_gh1578*`. It ran with `-n 4 --dist=loadfile`, BLAS pinned and `LSMS_BUILD_WORKERS=1`, and wrote its
output to files.

Scripts are in `reports/587-final/scripts/`. JSON, frames and test logs are in `private/587-final/`.

## 6. Must-state items (in the PR body and in CONTENTS.org's "Owed" list)

- **Pepper:** 808 rows (817 at `e5940eaa9`; 9 W1 Pepper rows are now held). Its Aggregate is GH #1537's ruling.
- **A full Nigeria re-warm.**
- **W2's 5 cl rows** are held by the no-size rule; no ruling names them.
- **The PH W3 Kg/l asymmetry** ("Measure PH, then align") is a follow-up for the next batch.
- **PH's blank-unit drop** is a follow-up.
- **The coverage regrade** is owed for the five PP cells; `make matrix` was not run.
- **Also stated:**
  - the one W5 gram row at about 12x;
  - the W3 31/42 overrides (827 rows);
  - `unit-unplaced` has no key (0 rows).

## 7. Disclosures

- **On n0035:**
  - Bash ran only `git` (add/commit/log/diff/show/status, plus read-only `rev-parse`,
    `branch --show-current` and `ls-tree`), read-only `grep`/`sed -n`/`awk`/`ls`/`tail`/`od`, `sbatch`,
    `squeue`, `scancel` on my own jobs, `mkdir`, and wait loops.
  - One `sed -i` edited a loop line in my own job script (`measure.py`), and heredocs wrote
    `job_measure.sh` and this report, all under `reports/587-final/`.
  - No python, no library import and no data read ran on n0035.
- **TMPDIR:** `sbatch --export=ALL` carried n0035's `TMPDIR` into the first three jobs. Slurm logged
  "Unable to create TMPDIR" and fell back to `/tmp` until each script set its own on its first line.
  Later submissions used `env -u TMPDIR`.
- **Jobs:** 10 submitted, at most 2 running at once, 2 cancelled by me.
- **Hashed prose not edited after the cold build.** The five wave `community_prices.py` docstrings and
  the PP comments in `nigeria.py` were checked for stale W1 / W5 wording, and none was found:
  - W1's says "price of N units";
  - W5's names the `c2q2a` division only.
- **PR body.** It carries forward the previous body's PH decoder-fix paragraph, the per-wave price basis,
  the `c2q2a` evidence (paraphrased, F6) and the label-judgement tables, marked as ruled. So it describes
  the whole base-to-head diff.
- **GitNexus:** the MCP server was unavailable. The blast-radius sweep was done with `rg`/`grep`:
  - the new symbols are referenced only in `pp_community_prices.py`, `derivations.yml` and the tests;
  - the `harmonize_food` edit is caught by the GH #1537 pins, which now assert its exact extent.

Report by Claude Opus 5.5.
