# Prior-Art Ledger - #1240 Burkina Faso panel crosswalk targets

**Search tier used:** ripgrep + git floor (the gitnexus MCP server did not
connect in this session; call sites swept with `rg`). Runtime baseline
development `9a0ed5f3c`.

## §1 Task, restated

Burkina Faso ships a Design-A panel crosswalk (`_/panel_ids.json`,
`_/updated_ids.json`, committed sources per GH #914/#894) that
`Country._finalize_result` applies with `id_walk` on every read. Its 2021-22
-> 2018-19 targets must be ids that the 2018-19 household tables serve, and
each must name the source household's own 2018-19 `(grappe, menage)`. Fix the
generator `_/panel_ids.py`, regenerate with `make panel-ids`, and review the
JSON diff.

## §2 Existing machinery

| symbol | path:line | what it does | tested? | decision |
|---|---|---|---|---|
| `burkina_faso.ehcvm_i` | `Burkina_Faso/_/burkina_faso.py:18` | 2018-19 `i`: `grappe + '0' + menage.zfill(2)` | yes (#460, #1123 tests) | reuse for targets |
| `Burkina_Faso/2018-19/_/mapping.py:i` | `:58` | the formatter the 2018-19 YAML `i: [grappe, menage]` binds to; same string as `ehcvm_i` | via API | the test calls this one |
| `burkina_faso.i` | `Burkina_Faso/_/burkina_faso.py:10` | 2021-22 `i` (no wave override): `grappe + menage.zfill(3)`; equals survey `hhid` 3,227/3,227 | via API | reuse for sources |
| Mali crosswalk | `Mali/_/panel_ids.py:50,65-69,99` | imports the country `i`; drops a target absent from the 2018-19 cover | `test_panel_id_collisions.py` | precedent followed |
| Senegal crosswalk | `Senegal/_/panel_ids.py:66-84` | same baseline-cover check | same | precedent |
| `local_tools.panel_ids` | `local_tools.py:3608` | builds RecursiveDict + per-wave map | `test_panel_ids_dtype.py` | reuse unchanged |
| `_close_id_map` / `id_walk` | `local_tools.py:3663,3719` | closure-resolves a wave's map, then renames | `test_id_walk.py` | unchanged (framework, out of scope) |
| chain guard | `Burkina_Faso/_/panel_ids.py` (GH #548) | drops entries whose target is an occupied 2021-22 id | `_check_id_walk_idempotent` (`diagnostics.py:1667`) | kept |

## §3 Definitions & conventions in force

- Crosswalk is a committed SOURCE; never a Make rule; write beside `__file__`:
  `CLAUDE.md` "The crosswalk is a committed SOURCE", pinned by
  `tests/test_panel_ids_persistence.py`.
- Household id differs by EHCVM wave: `burkina_faso.py:18` docstring (GH #460)
  and `Burkina_Faso/_/CONTENTS.org` "plot_features (GH #167)".
- EHCVM-2 tracked households within the EA of origin: `CONTENTS.org`
  "THE THING TO KNOW" (quoting `bid_bfa_ehcvm2.pdf` p. 4).

## §4 Invariants & assumptions

- The survey's `hhid_EHCVM1` is in the 2021-22 format (3,227/3,227), so it is
  NOT the served 2018-19 id when the 2018-19 menage has three digits. This
  was the bait: the old docstring's premise was true of the raw file and wrong
  about the library.
- `grappe == grappe_EHCVM1` for 3,227/3,227 rows (no tracking outside the EA).
  The hermetic test relies on this.
- `menage` was renumbered in 2021-22 (477/3,227 unchanged), so the two
  namespaces overlap on different households. Under `_close_id_map` an entry
  can link only if its target is not an occupied 2021-22 id.
- Both the `.py` and the `.json` files are build inputs (`_BUILD_INPUT_SUFFIXES`),
  so every Burkina hash moves and no other country's does.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| 2018-19 target | reuse `ehcvm_i` | the formatter the 2018-19 tables use |
| 2021-22 source | reuse `burkina_faso.i` (asserted == `hhid`) | byte-identical sources, no re-implementation |
| orphan guard | reuse Mali/Senegal baseline-cover check | 0 orphans today; a future one is dropped and counted |
| chain residue | no change | needs `id_walk` semantics (framework); reported |

## §6 Open questions for the human

- 80 panel households stay unlinked because of the namespace overlap plus
  closure. 8 of them keep a 2021-22 id that equals a DIFFERENT 2018-19
  household's id: false links at the API (head age gap 1-5 years in 0 of 8,
  versus 74.6-74.8% for true links). Removing them needs either a non-closing
  rename for cross-namespace maps in `id_walk`, or a disjoint id namespace
  for one wave. Both are framework/design decisions. Is either wanted?

---
### Phase 3 — verification

- `panel_ids.py` previous_i -> `ehcvm_i` — OK (anchored on §2, §4): every one of
  3,147 targets is the source's own 2018-19 household (raw cover inverse lookup).
- `panel_ids.py` cur i -> `burkina_faso.i` — OK (§2): sources byte-identical
  (0 changed).
- baseline-cover filter — OK (§5, Mali/Senegal precedent): 0 orphans.
- chain guard — OK (§4): unchanged logic; 95 -> 80 skips.
- `tests/test_burkina_faso_panel_ids.py` — OK: fails on `9a0ed5f3c` (1,091 of
  3,132 hermetic offenders; data test fails), passes on the branch.
Evidence: `slurm_logs/isa_backlog_2026-10-21/1240-burkina-panel-ids/`.
