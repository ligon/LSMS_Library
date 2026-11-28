# GH1701 — the `pp_ph_area_t` example raises on Nigeria

**Search tier used:** ripgrep/grep + git floor. GitNexus was unavailable (its server needs `npx`, absent here). Base `47df62d49e614c9a56e319def7324145d138c3d9`. Inherits `STANDING.md`.

## §1 Task, restated

`transformations.pp_ph_area_t`'s docstring documents one Nigeria call, `yield_kg(c.crop_production(), c.plot_features(), area_t=pp_ph_area_t(c))`. Since GH #587 (merge `1e549e96a`, PR #1690) Nigeria `plot_features` also serves the W1-W3 post-harvest rounds, so that call maps onto rounds already served and `_area_on_harvest_rounds` refuses it. Repair the example: the post-planting rows remapped (all five waves), the W1-W3 post-harvest rows joined directly, W4/W5 reachable by the first route only. No code, route, area policy or weight changes, and no default chosen (issue question C is unruled).

Blast radius: the docstring of one function. Its callers are `tests/test_gh1460_yield_area_t.py` and `tests/test_gh1520_wave_folder_map.py`; `yield_kg`'s *area_t* prose names it. No country or wave script calls it.

## §2 Existing machinery

| symbol | path:line at base | what it does | tested? | decision |
|---|---|---|---|---|
| `pp_ph_area_t` | `lsms_library/transformations.py:6242` | earlier `t` of a two-round folder -> later one | `test_gh1460_yield_area_t.py::TestThePairs`, `test_gh1520_wave_folder_map.py` | docstring only |
| `_area_on_harvest_rounds` | `:6280` | moves area `t`; refuses a served or repeated target | `test_gh1460_yield_area_t.py` | unchanged |
| `yield_kg` | `:6304` | harvest kg / area on `(t, i, land)` | same, `test_gh1460_yield_land_route.py`, `test_gh1475_*` | unchanged |
| `_waves_joining_nothing` | `:6199` | names `(country, t)` cells with rows on BOTH sides and none joined | `test_gh1460_yield_area_t.py` | unchanged; it cannot name a harvest round that has no area rows |
| `append_ph_plot_features` / `_SOURCES` | `Nigeria/_/nigeria_ph_plot_features.py` | the W1-W3 PH rows, `t` = `2011Q1` / `2013Q1` / `2016Q1` | `test_gh587_nigeria_ph_plot_features.py` | read by the new test, not edited |
| `test_nigeria_yields_appear` | `tests/test_gh1460_yield_area_t.py:122` | the two-route call on real data | `requires_s3`, so not run offline | unchanged; its two routes are the example's |

## §3 Definitions and conventions in force

- Nigeria is PP/PH: one wave directory, two rounds, two `t` (`.claude/skills/add-feature/pp-ph/SKILL.md`).
- PH plot rows are own PH A1 records "with NEW/current-access/lost-access routing, not an instruction to duplicate PP plots" (`Nigeria/_/CONTENTS.org`, "Own PH plot records contradict the PP-only premise (GH587)").
- W4 `sa1q11` has "no unit/routing/version bridge"; W5 "has no corresponding fresh-area field". "Keep both cells held separately" (same section).
- `yield_kg`'s `area_t`: "Refused where a target round is already on `plot_features` or is named twice, either of which would sum two rounds' areas" (its docstring).

## §4 Invariants

- Never the two areas in one denominator: PP and PH area for one wave are alternatives (issue #1701).
- The refusal is correct and stays. The example changes, not the guard.
- A docstring edit is outside every fingerprint (`_build_registry._norm` strips docstrings); verify, do not assume.
- No survey build here, so no yield count is claimed. `17,751` (#1548) and `17,755` (`CONTENTS.org`, #1433) both predate the PH area rows and W3's `Quantity_kg` (#1518).

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| the example | rewrite, from `test_nigeria_yields_appear` | that test already states the two routes |
| a guard against the next drift | new test, source-free | the one test of the real call needs S3, so nothing offline ran the documented example; the test executes the docstring's own code |

## §6 Open questions for the human

- Issue question C, unanswered here: the W1-W3 default, PH area or moved PP area.

---
### Phase 3 — verification

Filled at task end.
