## Delta verdict: APPROVE

Candidate `2aa1e428736450e5190992019c1b5eaa0b71b5ba`, previous `d5fdf326c`, base `f138a3d61c203f892148592675a7ef72e160a322`. Every check you listed passed. I measured each one cold in private roots (jobs 41281240 and 41281241); nothing below is taken from the worker's figures.

**1. The exception.**
- **Selection is exactly my 63.** I derived the crosswalk from the raw files with my own rule. The roster condition alone ("no pre-loaded member is still a member", the same set as "every one is 2") holds on 81 households. What narrows it to 63 is the other half of the rule: PanelHH = Panel and the key exists in 2018-19 (6,878 households); the other 18 are unlinked anyway.
- **Committed JSON.** It equals my derived map, which is also the old JSON minus the 63: 6,815 links, 1,217 unlinked, 0 chains, 0 occupied targets, 0 duplicate targets, and `panel_ids.json` consistent with `updated_ids.json`.
- **Regeneration.** `make panel-ids` in a job-local clone ran the hazard checks and the `EXPECTED` count check, and left `git status` clean, so the output is byte-identical.
- **Registration.** The loader accepts it (`rows: 0`, valid bases), and `derivation_inputs` returns 428 rows covering exactly my 63 households.
- **Visibility.** `rows: 0` is documented as a deletion, so it fits a withheld link only loosely; CONTENTS says so. Under the `panel_ids` table slot it shows on no served frame: `sample().attrs['derivations']` lists only the strata key. That is the worker's question 1 to the user. One option is the `sample` slot, which would list it on the frame whose `i` it changes without attaching it to every Benin table.

**2. `panel_weight`.**
- 2021-22 `panel_weight` is non-NA on exactly the 6,815 linked households and NA on the 1,217 others.
- On the linked households it is base × 1.098872, constant to 1e-15. That equals the raw ratio mean(`poids`, all households) / mean(`poids`, linked), so it is the framework's mean-1 rule; the linked mean is 1.0.
- `weight`, `strata`, `Rural`, `Derivation` and every 2018-19 column of `sample` are unchanged.

**3. F1 is fixed.** The Makefile comment, the CONTENTS paragraph and the PR text now say failure behaviour is unchanged. AGENTS.md:807 carries the dated "stale for Benin" note, and its count is corrected from 14 to 13.

**4. F2 is fixed.** From `d5fdf326c` to this head, the only change in the two unit columns is Kilogramme → Kg (2,363 rows in `Unit_purchased`, 272 in `Unit_received_as_gift`). Every other 2018-19 `plot_inputs` column is identical, and 2021-22 is unchanged.

**5. F5–F7.**
- The three FCFA-shaped purchase quantities are recorded and served as recorded.
- The verification scripts are copied under `slurm_logs/`, and CONTENTS cites them there.
- The generator's tests now execute it: each hazard is refused by its own message, and it runs on invented and on real files.
- **In-process import (note, not a blocker).** Importing `panel_ids.py` reads no data. It does leave `Benin/_` at `sys.path[0]` and loads a top-level module named `benin` into the process, which I confirmed directly. That is still safe:
  - no module outside Benin imports `benin`, `panel_ids` or `plot_features` by bare name;
  - Tanzania's 2019-20 / 2020-21 `mapping.py` already insert into `sys.path` the same way, in-process.

  Using `append`, or loading `benin` by path, would be cleaner.

**6. Every Benin table, cold.**
- **Base against this head, 73 calls:** no row count moves, and the only differences are the two expected ones.
  - `sample`: only `panel_weight` on the 8,032 rows of 2021-22.
  - `plot_inputs` 2018-19: the purchase/gift columns and the Unknown unit.
- **Re-keyed rows:** they match the worker's per-table figures (for example 6,815 in `sample`, 174,703 in `food_acquired`).
- **Every other table, and both waves' unaffected slices, are equal after mapping base ids through the crosswalk.**
- **Same as base:**
  - warm same-process, `assume_cache_fresh` and fresh-process reads: 70 of 73 equal, with the same three known exceptions as base and 0 `IdWalkError`;
  - `Feature` equals `Country` on every table;
  - `check_panel_consistency` passes;
  - 0 Mover rows.

**7. Hash census.** `build_transforms_fingerprint` 0/42, `framework_imports_fingerprint` 0/38, `_table_cache_hash` 23/556 and `Wave._input_hash` 46/2,323, all Benin; no errors on either side.

**Tests at this head.**
- Benin and panel files with data: 726 passed, 2 skipped, 0 failed.
- `-k Benin` on the multi-country files: 42 passed.
- Data-free suite: 15,532 passed, with 1 failure, the same `test_gh932` failure seen at `d5fdf326c`; that test passes when run alone on both base and head.

**Note:** `test_gh1575`'s invented frame now depends on `E2_42009` being in the committed crosswalk, so it would break if a future regeneration dropped that link.

**Not re-run:** the make backend and `LSMS_BUILD_WORKERS=2` routes. Nothing in this delta touches the build route; those were verified at `d5fdf326c`.

**Must-states that remain:**
- Sue regenerates the coverage snapshot for the new Benin `panel_ids` cells.
- The 63 are excepted under the "Unlink the 63" ruling. The 19 households with no pre-loaded roster stay linked.
- 2021-22 `panel_weight` is a cross-sectional weight, served on the linked households only.

No red-team jobs remain. Outputs are under `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/benin-redteam/out/delta/` and `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/benin-redteam/out/delta_tests/`.
