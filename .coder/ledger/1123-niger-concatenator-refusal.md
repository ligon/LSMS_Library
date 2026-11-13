# Prior-Art Ledger — GH #1123 extension: Niger country concatenators refuse a missing wave

> Per-task ledger. Inherits `STANDING.md`; cites `CLAUDE.md` and
> `Niger/_/CONTENTS.org` rather than re-copying them.

**Search tier used:** ripgrep + git floor (the gitnexus MCP server failed to
connect in this session).

## §1 Task, restated
Merged PR #1241 (GH #1123) closed a make-backend hazard for Niger's
`plot_inputs`: the country recipe chained the four wave scripts with `;`
(one shell line, so only the last status reached make) and the country
concatenator `_/plot_inputs.py` -- also the framework's Python fallback
(`try_script`) after a failed `make` -- skipped a missing wave parquet.  It
fixed both halves: `&&` in the recipe, and a concatenator that refuses a
missing wave.  The maintainer accepted the refusal (2026-10-23, "#1241
refusal accepted") and extended the pair to Niger's other four
`materialize: make` country concatenations: `crop_production`, `livestock`,
`plot_labor`, `people_last7days`.  `&&` alone would still lose the wave,
through the fallback.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| #1241 inline refusal | `Niger/_/plot_inputs.py` (at `57d255585`, lines 38-51) | unwired-script assert, `data_root()` lookup, `FileNotFoundError` naming the wave | yes, `tests/test_ehcvm_plot_inputs_identity.py:254-323` | **extend**: moved verbatim into `niger.concat_wave_parquets`, parametrised by table; `plot_inputs.py` now calls it |
| `&&` recipe | `Niger/_/Makefile` plot_inputs rule | wave scripts + concatenator on one line joined by `&&` | yes (python-shim test) | **reuse** the form for the four recipes |
| `run_make_target` / `try_make` / `try_script` | `lsms_library/country.py:5219-5370` | make the country target; on failure run the country script as fallback | indirectly | unchanged; it is the route the refusal guards |
| `Country._has_country_builder` | `lsms_library/country.py` (GH #1039) | `_/<table>.py` exists -> make backend goes straight to `run_make_target` | yes | unchanged; why these five tables take the country route |
| `_build_wave_script_target` | `lsms_library/country.py:202` | default backend builds each wave by its pattern rule and raises `CalledProcessError` | yes | unchanged; the default backend was already loud |
| data_root lookup precedent | `Ethiopia/_/food_coping.py:30-36` | reads wave parquets under `data_root()` | -- | precedent #1241 cited |
| prerequisite form | `Malawi/_/Makefile:71-77` | waves as make prerequisites | -- | not adopted (#1241: lets make mtime-skip a wave parquet) |
| e2e harness layout | `tests/test_gh1091_niger_community_builder.py` | real Niger Makefile + synthetic wave scripts through `Country(...)` | yes | **reuse** the layout for the end-to-end test |
| other countries' concatenators | Ethiopia, Malawi, Mali, Tanzania, Uganda | skip a missing wave | -- | out of scope (ruling names Niger) |
| `Niger/_/plot_features.py` | -- | skips 2011-12 / 2014-15, which have NO wave script | -- | out of scope, and refusal would be wrong there |

## §3 Definitions & conventions in force
- "Declared wave": a wave in the concatenator's `WAVES` that has a
  `../<wave>/_/<table>.py`.  All four Niger waves have one for all five
  tables (`ls Niger/20*/_/`), so a missing parquet can only mean a failed or
  unrun wave script.  `Niger/_/CONTENTS.org` section "plot_inputs: distinct
  native input slots ... (GH #1123)", paragraph "A wave guard must reach the
  user on BOTH backends".
- `countries_root()` is read-only at runtime; wave parquets are read under
  `data_root()` only (CLAUDE.md, GH #803).

## §4 Invariants & assumptions
- Country scripts run with cwd `Niger/_` (the recipe; `try_script` uses
  `cwd=script.parent`), so `../<wave>/_/<table>.py` resolves and `import
  niger` finds the country module as the script directory is `sys.path[0]`
  (`_script_subprocess_env` strips `PYTHONSAFEPATH`, `country.py:175-193`).
- `to_parquet(p, '../var/<table>.parquet')` stays in each script:
  `_resolve_data_path` infers the destination from the calling file.
- Make-backend country-builder route never enters `load_from_waves`, so
  `_evict_hashless_wave_caches` does not run there: the refusal catches an
  ABSENT wave parquet, not a STALE one left by an earlier successful build.
- `runpy.run_path` on a file does not put its directory on `sys.path`; the
  #1241 runpy harness needs `syspath_prepend` now that `plot_inputs.py`
  imports from `niger`.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| refusal | extend (move #1241's block into `niger.concat_wave_parquets`) | one copy for five scripts; #1241 had no helper to import |
| recipe | reuse #1241's `&&` one-liner | same hazard, same fix |
| helper location | `niger.py` | already in every Niger hash (`mod:` and `binp:`), as are the `_/Makefile` and `_/*.py`; a `local_tools` helper would reach other countries' fingerprints |
| tests | reuse #1241's shim + runpy tests, parametrised; #1091's e2e layout | precedent |

## §6 Open questions for the human
- Stale-wave shadowing under `LSMS_BUILD_BACKEND=make` (§4): a failed wave
  with a parquet left from an earlier build is served stale on this route,
  for all five tables.  #866/#479 territory; not this ruling.
- The same skip-a-missing-wave concatenator exists in other countries
  (§2).  Not extended.

---
### Phase 3 — verification
Evidence: `slurm_logs/isa_backlog_2026-10-21/1123-niger-concatenators/`.

- `niger.concat_wave_parquets` -- OK (anchored on §2, §5): #1241's block,
  moved; same assert, lookup and message.  The #1241 tests pass against it;
  the concatenators' `var/` outputs are frame-equal to base for all five
  tables, and equal the four wave parquets read directly with pyarrow.
- Four `&&` recipes -- OK (anchored on §2): #1241's one-line form; the
  python-shim test fails on base for each table and stops at the broken
  wave on the branch.
- Four concatenators -- OK (anchored on §3, §4): base served each table with
  the broken wave's rows missing (2018-19 broken: no warning at all; 2021-22
  broken: "Makefile execution failed" only); the branch raises
  `RuntimeError: Could not materialize`, stderr naming the wave.  Healthy
  cold builds frame-equal to base (23 default-backend tables, 5 make-backend).
- Tests -- OK: 24 of the 25 new tests fail on base (the e2e positive
  control passes, as it should); 225 passed on the branch in the focused
  selection.  Mutations: one recipe back to `;` -> its 2 shim tests red; the
  helper's `raise` -> `continue` -> 5 runpy refusal tests red (and on real
  data the `andandonly-*` cases serve two waves).
- Not a REINVENTION: the only new symbol is the moved block.
- Residual, not a CONTRADICTION: stale-wave shadowing (§4, §6).
