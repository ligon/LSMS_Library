# Prior-Art Ledger — GH #894: Malawi and Nigeria panel crosswalk to design A

**Search tier used:** ripgrep+git floor (the gitnexus MCP server did not connect).

## §1 Task, restated
Malawi and Nigeria declare `panel_ids` in `data_scheme.yml` and wire it per wave
in `data_info.yml` (`panel_ids: {file, idxvars: {i}, myvars: {previous_i}}`).
On development the library reads those blocks at run time and caches the result
as `data_root/<C>/_/panel_ids.parquet` only ("design B").  The other nine panel
countries ship a git-tracked `_/panel_ids.json` + `_/updated_ids.json` built by a
`_/panel_ids.py` ("design A", the convention).  Give both countries a
`_/panel_ids.py` that builds the same `(t, i) -> previous_i` frame, converts it
with `local_tools.panel_ids(df)`, and writes the two JSON files beside itself; a
`.PHONY: panel-ids` target, never a JSON rule; remove both from
`DESIGN_B_PENDING`.  The served dicts and every served `i` must not change.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Country._compute_panel_ids` | `lsms_library/country.py:5910` | dict branch (A) / DataFrame branch -> `panel_ids(df)` (B) | `test_gh970_no_json_make.py` | reuse, unchanged |
| `load_json_cache` | `lsms_library/country.py:5483` | candidates: `data_root/_/X.json`, in-tree `_/X.json`, `data_root/_/X.parquet`, in-tree parquet; else `load_from_waves` | `test_gh970_no_json_make.py` | reuse, unchanged |
| `local_tools.panel_ids` | `lsms_library/local_tools.py:3608` | frame -> (`RecursiveDict{(t,i):(t0,i0)}`, `{t:{old:new}}`), chains through the previous wave, `update_id` mints `_N` | `test_panel_ids_dtype.py` | reuse |
| `local_tools.update_id` | `lsms_library/local_tools.py` (above `panel_ids`) | first claimant (row order) keeps the bare id; GH #504 live-id guard | `test_panel_id_collisions.py` | reuse |
| `df_data_grabber` | `lsms_library/local_tools.py:2120` | `format_id` on every `idxvars`, raw `myvars` | — | mirrored (format_id on `i`) |
| `_enforce_declared_dtypes` | `lsms_library/country.py:7170` | `previous_i: str` -> `StringDtype` at finalize (Nigeria int32 -> '10001') | — | mirrored with `format_id` (equal on every row, measured) |
| Ethiopia `_/panel_ids.py` | `countries/Ethiopia/_/panel_ids.py` | design A with per-wave YAML blocks kept | — | precedent (shape) |
| Mali / Niger `_/panel_ids.py`, Makefile `panel-ids` | `countries/{Mali,Niger}/_/` | design A, `_HERE` writes, `.PHONY: panel-ids` | `test_panel_ids_persistence.py` | precedent (Makefile block, `_HERE`) |
| `check_panel_consistency` | `lsms_library/diagnostics.py:1732` | 8 checks incl. `panel_ids_targets_exist` | — | run before/after |

## §3 Definitions & conventions in force

- Crosswalk is a committed SOURCE; never a Make rule; write beside `__file__`:
  `CLAUDE.md` "The crosswalk is a committed SOURCE, not a build product (GH #914/#894)",
  pinned by `tests/test_panel_ids_persistence.py`.
- JSON form: `{'t,i': 't0,i0'}` single-line `json.dump`, as all nine precedents.
- PP/PH: one Nigeria wave directory is two `t` values (`nigeria.wave_folder_map`);
  `.claude/skills/add-feature/pp-ph/SKILL.md`.
- Malawi IHPS inside IHS waves: `Malawi/_/CONTENTS.org` "Sampling Design" and the
  plot_features notes (cross-sectional `cs-17-` ids, panel half raw `y3_hhid`).

## §4 Invariants & assumptions

- The design-A dicts must equal what design B serves on 9a0ed5f3c.  `panel_ids`
  compares on the normalised string form: B returns a `RecursiveDict` keyed by
  tuples, A a plain dict keyed by `"t,i"` (as the nine).  No id contains a comma.
- `update_id` is ORDER-dependent (who keeps the bare id), so the frame's row
  order must equal B's.  Measured: source-file order equals B's order in every
  wave of both countries.
- Only waves present in the frame get an `updated_ids` key; adding `{}` for
  unlinked waves (as Mali/Niger do) would break equality.  Not done.
- A new `_/panel_ids.py` / `.json` is in `_BUILD_INPUT_SUFFIXES`: every table of
  both countries moves hash; no other country should.
- `LSMS_BUILD_BACKEND=make` routes JSON methods through `load_from_waves`
  (`country.py` "if not use_dvc and method_name in JSON_CACHE_METHODS"), so it
  still reads the YAML blocks.  Measured equal.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| wiring (file, i var, previous_i var) | reuse: read from each wave's `data_info.yml` block | one place the wiring is written; script refuses a block shape it does not reproduce |
| current id | `format_id` | what `df_data_grabber` applies to `idxvars` |
| previous id | `format_id` | equals B's `StringDtype` cast on every row (frame compared row for row) |
| Nigeria `t` | `nigeria.wave_folder_map` | the object `Country` reads (`country.py:2731`) |
| dict construction | `local_tools.panel_ids` | the function B called |
| `test_gh970_no_json_make.py` | extend fixture: symlink view of Malawi without the crosswalk | keeps the cold design-B route pinned now no country is B; assertions unchanged |

## §6 Open questions for the human

- Nigeria's crosswalk (reproduced, not changed): W1, W4, W5 absent; the PH round is
  linked to its own PP round; ids come from the PP cover page, so 328/14,251
  current and previous ids are not in `household_roster` (`check_panel_consistency`
  FAILS `panel_ids_targets_exist`, before and after), and 99/4,696
  `2015Q3 -> 2013Q1` entries target an hhid absent from the W2 cover.  Which of
  these should the crosswalk change?  Each would be a JSON diff.

---
### Phase 3 — verification

- `Malawi/_/panel_ids.py` — OK (anchored on §2, §4): frame equals B's finalised
  frame row for row (values and order) in 2013-14 / 2016-17 / 2019-20; dicts equal.
- `Nigeria/_/panel_ids.py` — OK (§2, §3 PP/PH, §4): frame equals B's row for row in
  2012Q3 / 2013Q1 / 2015Q3 / 2016Q1; dicts equal.
- Makefile `panel-ids` targets — OK (§3): `make -n panel_ids.json` is "Nothing to be
  done"; `make panel-ids` regenerates byte-identically.
- `DESIGN_A` / `DESIGN_B_PENDING` — OK (§3).
- `test_gh970_no_json_make.py` fixture — OK (§5): assertions unchanged; the
  route it pins is reached (the JSON-tier parquet lands under the private root).
- Tests (31 files: the four named panel files plus every `tests/` file matching
  `panel` and Malawi|Nigeria), `-n 3 --dist=loadfile`: base 1618 passed / 97
  skipped / 4 xfailed; branch 1632 / 97 / 4.  The +14 are exactly the new
  Malawi/Nigeria parametrizations (7 tests x 2), no other test changed outcome.
- Hash census: Malawi 26/26 country + 130/130 wave hashes, Nigeria 24/24 +
  240/240, 0/40 build fingerprints, no other country.
Evidence: `slurm_logs/isa_backlog_2026-10-21/894-panel-ids-design-a/`.
