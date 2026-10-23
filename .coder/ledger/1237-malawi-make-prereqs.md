# Prior-Art Ledger — GH #1237 Malawi per-wave Make prerequisites

**Search tier used:** ripgrep+git floor. The GitNexus MCP server did not
connect in this session; call sites were swept with `rg`.

## §1 Task, restated
Malawi's `_/Makefile` builds the wave parquets of its script-path tables
(`materialize: make`) with data-root pattern rules,
`$(LSMS_DATA_ROOT)/$(COUNTRY)/%/_/<table>.parquet`. The country
concatenator's `$(VAR_DIR)/<table>.parquet` depends on those wave parquets.
Nine rules named only `malawi.py`, and `food_acquired`'s named nothing. Under
`LSMS_BUILD_BACKEND=make`, the nine tables have a country builder, so they go
straight to `run_make_target(method_name)` with no hashless-wave eviction.
There, an edit to `<wave>/_/<table>.py` left both tiers "current", and the
stale frame was served.

## §2 Existing machinery
| symbol | path:line | what it does | reuse / extend / new |
|---|---|---|---|
| GhanaAHIES `WAVE_RULE` | `countries/GhanaAHIES/_/Makefile:39` | the only other data-root wave rule: `$(LSMS_DATA_ROOT)/$(COUNTRY)/%/_/$(1).parquet: ../%/_/$(1).py ghanaahies.py` | reuse its prerequisite spelling |
| Niger / CotedIvoire wave rules | `countries/Niger/_/Makefile:47,61,75,106` | name the `.org` beside the script exactly where it is read | reuse the per-table `.org` split |
| `run_make_target.try_make` | `lsms_library/country.py:5210` | `make -s` per target; on `CalledProcessError` it warns and goes on to the next target | unchanged; the exit code is 2 for "No rule" and for a failed recipe alike |
| `build_wave` legacy gate | `lsms_library/country.py:5337-5349` | calls `run_make_target(wave=w)` only when `method_name in wave_obj.data_scheme` | unchanged; a wave with no script is never asked |
| `_evict_hashless_wave_caches` | `lsms_library/country.py:4764` | deletes hashless wave parquets before a `load_from_waves` descent | is why `food_acquired` (no country builder since #1209) was masked |

## §3 Definitions in force
- The GH #866 class: `make` is not a judge of whether its own output is
  current. See CLAUDE.md, "`make` is not a judge…", and the `LSMS_BUILD_BACKEND=make` bullets.
- Measure a Make rule by asking make (`-n`, `-W`), not with a text regex. See CLAUDE.md's #866 bullet and the grouped-target note.

## §4 Invariants
- A pattern-rule prerequisite that does not exist makes the rule
  inapplicable for that stem. So every wave named in a `*_waves` list, or in
  `rounds` for `food_acquired`, must carry `../<wave>/_/<table>.py`.
  `tests/test_gh1237_malawi_make_prereqs.py` asserts this.
- `categorical_mapping.org` is read by path inside `malawi.py`:
  `_malawi_code_map` for crop/input/species/labor-source/price tables, and
  `_food_text_vocab` plus `apply_harmonize_food` for food. It is not read for
  food_coping, months_food_inadequate, anthropometry or people_last7days.
- The country `_/Makefile` is a build input of every Malawi table, so any edit is one Malawi re-warm.

## §5 Reuse decision
| quantity | decision | reason |
|---|---|---|
| wave-script prerequisite | reuse the GhanaAHIES spelling `../%/_/<t>.py` | same data-root target form; `%` is the wave dir name, and the prerequisite resolves from `Malawi/_` |
| helper prerequisites | keep `malawi.py`; add `categorical_mapping.org` where read (Niger precedent) | do not add `mapping.py`, which no script imports |
| recipe | unchanged | only the prerequisite list was defective |

## §6 Open questions
- `food_acquired`'s rule (a tenth rule, beyond the nine named in the issue) was
  fixed in the same way. The library never reached its defect, because the
  hashless-wave eviction runs first, but a direct `make food_acquired` did.
- The 2013-14 and 2016-17 `food_acquired.py` scripts also read
  `../../2010-11/_/ihs3_conversions.csv`, a cross-wave input that a per-wave
  pattern cannot name. This was left as is and recorded.

---
### Phase 3 — verification
- Malawi `_/Makefile`, ten per-wave rules — OK (§2 GhanaAHIES/Niger, §4).
  The measurements are in `slurm_logs/isa_backlog_2026-10-21/1237-malawi-make-prereqs/`.
  - `make -n`, cold root: every requested (table, wave) runs its recipe. The
    10 no-script stems changed from "runs the recipe" to "No rule"
    (`make_n_cold_*.txt`).
  - `make -n -W <wave script>`, warm stand-in root: on development, 0 of 40
    stems rebuild; on the branch, 40 of 40 do. `-W categorical_mapping.org`
    rebuilds the 6 tables that read the file and leaves the other 4 current
    (`make_n_warm_*.txt`).
  - End to end: a root was warmed with `plot_inputs` on development
    (Default backend; 179,949 rows), then read under the make backend.
    Development's Makefile, before and after touching
    `2013-14/_/plot_inputs.py`, rewrote nothing. The branch's Makefile did
    nothing before the touch; after it, it rewrote the 2013-14 wave parquet
    and `var/` and nothing else, and the next read was a no-op again
    (`e2e_*.json.txt`).
  - A real `make` on a no-script stem exits 2 both before and after: before
    because `python` cannot open the missing file, after with "No rule".
    The library's handling is the same (`no_script_stem_real_make.txt`).
  - Default backend, cold, development vs branch: `plot_inputs` (179,949
    rows), `livestock` (39,126) and `community_prices` (82,635) pass
    `assert_frame_equal`, and their attrs are equal. The Default backend asks
    only the in-tree `../<w>/_/<t>.parquet` names, which the generic
    `%.parquet` rule serves, and it never asks for community_prices 2016-17
    (`eq_default_*.txt`).
  - Hash census: `_table_cache_hash` moved for 26 of 549 tables, all 26 of
    them Malawi's. `build_transforms_fingerprint` moved 0 of 40, and
    `Wave._input_hash` 0 of 2,109, because the country Makefile is not a
    wave-tier input (`hash_census_diff.txt`).
