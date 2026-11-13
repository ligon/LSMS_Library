# Prior-Art Ledger — GH #1245 `id_walk`: single-pass rename, applied exactly once

**Search tier used:** ripgrep + git (floor).  The gitnexus MCP server did not
connect this session (CONNECTION_CLOSED); `rg` call-site sweeps substituted,
per CLAUDE.md "Code intelligence".

## §1 Task, restated
@ligon's ruling on #1245 (2026-10-23): `local_tools.id_walk` applies each
wave's `updated_ids` map as ONE substitution (every household id to its own
canonical target, chains never followed), replacing the closure
(`_close_id_map`) that sent Burkina Faso 2021-22 households to a different
household's id and forced Burkina's `panel_ids.py` to drop 80 links (#1240,
PR #1249).  A single pass is correct only if applied exactly once to any given
rows, so the work is: map every application site, measure whether any rows are
walked twice today, choose the exactly-once mechanism, remove Burkina's chain
guard, and verify served `i` moves only for Burkina's 80.

## §2 Existing machinery (this task's area)

| symbol | path:line (d353b7ea7) | what it does | tested? | decision |
|--------|-----------|--------------|---------|----------|
| `id_walk` | `lsms_library/local_tools.py:3719` | per-wave rename of `i` through `updated_ids` (closure-resolved), sets `attrs['id_converted']` | `tests/test_id_walk.py` | **change**: single pass + per-wave exactly-once detection |
| `_close_id_map` | `local_tools.py:3663` | `{A:B, B:C}` -> `{A:C, B:C}`; raises on cycles | `tests/test_id_walk.py` | **remove** (no caller once `id_walk` is single-pass) |
| stage-2 walk | `country.py:5489-5494` (`load_from_waves`) | walks every wave result with `i` before augment/normalise/concat/L2 write | via every panel build | reuse |
| finalize walk | `country.py:3984-3990` (`_finalize_result`) | walks a frame without the flag, if `_updated_ids_cache` is loaded | `tests/test_gh819_vjoin_order.py` | reuse |
| `derivation_inputs` walk | `country.py:2934-2940` | re-keys raw derivation inputs | `tests/test_derivations.py` | reuse |
| country-script walks | Ethiopia `_/{8 tables}.py` (library `id_walk`); Uganda `_/{13}.py`, Tanzania `_/{11}.py` (their own `uganda.id_walk` / `tanzania.id_walk`, already single-pass) | walk in a `make` subprocess, write the parquet with the flag | per-country builds | reuse (untouched) |
| legacy `change_id` walks | Ethiopia `_/food_acquired.py:44`, GhanaLSS `_/household_roster.py:14` | private, not `updated_ids` | — | out of scope |
| `_check_id_walk_idempotent` | `lsms_library/diagnostics.py:1667` | re-walks the served spine with attrs cleared | `check_panel_consistency` | reuse; docstring updated (still meaningful under detection) |
| `RecursiveDict` / `panel_ids()` | `local_tools.py:3442`, `:3608` | CROSS-wave chain of `(wave, id)` pairs | yes | untouched (a different chain) |
| `update_id` | `local_tools.py:3551` | `_N` split suffix, keeps a map injective | `tests/test_panel_id_collisions.py` | untouched |

## §3 Definitions & conventions in force
- `updated_ids[w]`: `{raw id in wave w: canonical id}`; per CLAUDE.md "The
  crosswalk is a committed SOURCE" (Design A, committed JSON).
- `attrs` survive an op only when every input agrees: CLAUDE.md "Panel ID
  Transitive Chains and the `attrs` Flag" (pandas 3.0.2 table).
- Read path vs. build path for hashes: `_finalize_result` is in
  `_build_registry._EXCLUDED_CALLABLES`; `id_walk` is reached from the tagged
  `_aggregate_wave_data` (stage 2), so it is in every `build_transforms_fingerprint`.

## §4 Invariants & assumptions
- **Post-walk frames hold no `K - V` id** (K = moving keys, V = their targets):
  exact, from the definitions.
- **Pre-walk frames hold no `V - K` id** iff no current household that keeps its
  id holds a target -- the same condition under which the rename does not merge
  two households.  Guarded by Mali/Niger/GhanaLSS `panel_ids.py` (drop) and now
  by Burkina's (assert).
- A wave holding neither class but some `K & V` id is undecidable by content;
  only Burkina has `K & V` ids (80), and no served frame is that small.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| exactly-once | **new** (`_walk_partition` + per-wave classification in `id_walk`) | the flag is lost by construction on every table derived from a served one (measured), so a flag-only design needs a fix per derivation site, including country scripts that call the API |
| the flag | reuse as fast path | survives the parquet round trip (measured, pinned) |
| Burkina crosswalk | `make panel-ids` | committed source, Design A |

## §6 Open questions for the human
- Mali (3) and Niger 2014-15 (4) guards drop links whose occupant moves off,
  which single-pass would now link; their remaining 8 and 1 are genuine
  rename-onto-occupied collisions.  Refine those guards to "occupant keeps its
  id"?  Out of scope here (no crosswalk but Burkina's may change).
- Should `_FOOD_DERIVED` / country scripts that call the API carry the flag,
  to save the (cheap) re-classification?  Not needed for correctness.
- The stage-2 walk runs on any table with an `i` level.  Niger 2014-15
  `community_prices`' wave frame carries `i` equal to the CLUSTER id `v` on
  100% of its 13,691 rows (round 1 wrongly called it a food item): 16 of 221
  codes equal 2014-15 targets (`V - K`), 0 equal a moving raw id (`K - V`), so
  nothing is renamed or refused today.  A code equal to a moving key would be
  renamed (as before #1245); one set hitting both classes would now raise.
  Restrict the walk to household tables?
- Commit `a7b0e703b`'s message says `food_quantities` is re-walked in "5
  countries"; the completed base run says 6 (Burkina, Ethiopia, GhanaLSS,
  Malawi, Tanzania, Uganda).  The evidence README has the full list.

---
### Phase 3 — verification
Evidence: `slurm_logs/isa_backlog_2026-10-21/1245-id-walk/README.org`.

- `id_walk` (single pass + per-wave detection) — OK (anchored on §4): served
  frames byte-identical to `d353b7ea7` for 1,175 of 1,250 (country, table,
  route) reads; the other 75 are Burkina's 15 household tables, whose 2021-22
  ids move exactly as the regenerated map predicts.  0 `IdWalkError`.
- `_walk_partition` — OK (§4): the K-V / V-K argument holds on the corpus (0
  mixed frames at any of 1,250 reads; 41 re-applications detected and skipped).
- `_close_id_map` removal — OK (§2): no remaining caller (`rg`).
- Burkina `panel_ids.py` assertion — OK (§4): passes on the real cover; the
  JSON diff is +80 entries, 0 changed.
- `_check_id_walk_idempotent` — round 1 claimed OK; the review REFUTED it
  (R1): the spine is index-only, `id_walk`'s `df.empty` guard returned it
  unwalked, so the check was vacuous, and my round-1 counterfactual only bit
  because its bare mutant had no such guard.  Round 2 (`161920d64`) guards on
  zero ROWS.  Now on the real Burkina spine (21,037 rows x 0 columns): pass
  with `id_walk`, fail with a bare single pass (77 (i, t) disappear).  Warm
  Burkina `food_quantities` with the bare pass: 3,140 instead of 3,216
  2021-22 households and 735 duplicate index rows (1,470 entries) -- the
  reviewer's figures, reproduced.

### Round 2 (GH #1245 review of `89c0df368`)
- R1 above; no served frame has zero columns (2,500 reads), so the guard fix
  moves no served data.
- `state='raw'` at stage 2 and in `derivation_inputs`: only the undecidable
  case (only chained / inert ids) is renamed instead of refused; a detectably
  walked wave is still left alone, a mixed wave still raises.
- The mixed check's limit is stated and pinned (one side holding only chained
  ids escapes; 0 such frames).
- Behaviour change for the 10 chain-free countries: a wave holding both
  `K - V` and `V - K` ids raises where base renamed it (0 occurrences).
- Pre-existing, identical on both sides: NaN-`t` rows dropped by `id_walk`;
  `interview_date` `visit` `Int64` cold vs `int64` warm (5 countries); Mali
  `household_roster` `MonthsSpent` `object` cold vs `string` warm (checked on
  `d353b7ea7` in a fresh root).
- Niger `panel_ids.py:151` comment updated (guard unchanged).
- No REINVENTION: `update_id` / `panel_ids()` untouched; the detection reuses
  nothing because nothing classified ids before.
