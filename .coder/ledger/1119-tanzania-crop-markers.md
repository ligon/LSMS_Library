# Prior-Art Ledger — Tanzania #1119 crop markers

The source-stage record below is preserved as history. Section7 records the
subsequent independently reviewed actual checkpoint and public extraction.

**Search tier:** git + ripgrep + AST floor; no GitNexus tool available.
Parent: `d66da3985fc705a8c73c2edd29e02f391274f89a`, tree
`3ab1bc5e9f96588ed5cba38336b41d4361bdffb7`. Private dispatch branch only.

## §1 Task

Implement the approved crop-axis vocabulary in Tanzania's existing modern
`plot_inputs_for_wave`, preserving source eligibility and legacy reduction.
Only synthetic/source verification is authorized. Actual-source, native Make,
Country/Feature builds, retained witnesses and provider acquisition are deferred.

## §2 Existing machinery

| Machinery | Source / tests | Decision |
|---|---|---|
| `plot_inputs_for_wave` | `Tanzania/_/tanzania.py:1423`; #1118/#637 tests | extend after legacy reduction |
| native callers | `Tanzania/{2019-20,2020-21}/_/plot_inputs.py` | preserve both colmaps verbatim |
| crop maps, unit decoders, `_yn_true`, `_unit_preferred_labels` | same helper module | reuse; no policy edits |
| Uganda marker design | `Uganda/_/uganda.py`, `tests/test_uganda_crop_markers.py`, `1119-uganda-crop-marker.md` | provenance separate from legacy keys; relabel last |
| Malawi `_seed_crop_identity` | `Malawi/_/malawi.py`; merged #1323 | numeric/literal vocabulary precedent only |
| legacy callbacks / signed sums | `tests/test_gh905_tanzania_{vectorised,signed}_sums.py`; `905-tanzania-public-extraction.md` | retain callbacks and four-ULP assertions; no optimizer |
| source/country doctrine | full Tanzania `CONTENTS.org`; panel/multi-round skills | preserve all qualifications |

Searched current branches and ledgers first: dispatch-base marker-source is
the parent; immutable PROPOSAL/LEDGER/ISSUE-1119 context reconciles retained
Tanzania #637, #846 and owner branches and finds no marker implementation.
No old country helper will replace current source. Call-site sweep finds
exactly two production callers. Shared framework and schema remain unchanged.

## §3 Definitions in force

Inherit `STANDING.md` §§3–4, `CLAUDE.md` and canonical
`lsms_library/data_info.yml:250–265`. Authority: #1119 comment 6368100317,
supplied immutable `ISSUE-1119.json` and `PROPOSAL.md`.

- Structural noncrop: `(not crop-specific)`; retained seed without a recorded
  crop: `unknown_crop`; recorded unmapped code/literal: passthrough identity.
- Structural `plot_id=Farm-level` and epistemic `unknown_plot` are different
  axes. No new plot sentinel or source-observation policy here.
- Exact pre-migration `CONTENTS.org:1846` statement:
  “non-seed inputs deliberately have no crop value”. The ruling supersedes
  that representation only; do not silently call the documentation stale.
- Four logical UPD rounds share `2008-15`; UPHI identifies back-cast Round-4
  descendants, r_hhid the interviewed round household. Panel grain remains
  unresolved. Extended 2019-20 and Refresh 2020-21 are disjoint; the latter
  includes the 545-household urban booster. WAITING #114 remains live.

## §4 Invariants

Freeze the original Quantity-or-mapped-crop gate BEFORE classification or
fallback. Preserve `crop_map` then `crop_<integer>` LEGACY keys through the
same conditional six-level groupby, its sort/dropna behavior and callbacks.
Keep min_count=1, arithmetic execution order, purchased-unit refusal, flag
maxima and the signed two-fertilizer-slot sums. `u` stays a key; both-OTHER
Unknown-unit and Improved-OR residuals remain separate #637 questions.

Carry desired labels alongside legacy keys; resolve only after old reduction.
Refuse legacy groups with distinct desired labels and newly colliding output
identities; never split, reorder or reduce again to accommodate a marker.
All payload values, EA/NumPy masks and dtypes, complete noncrop axes (including
unused levels), attrs and row order must be exact. Literal reserved text is
recorded text, not structural provenance. Zero is recorded here: Uganda's
2011-12 zero authority does not extend to Tanzania. Existing fractional and
unsupported numeric conversion refusals stay unchanged.

## §5 Reuse decision

Extend this helper only. Reuse native mappings and original reducer verbatim;
add local classification/label alignment with explicit conflict refusal.
Portable tests invoke actual helper functions on invented frames. If the
#905 full-source digest reconstruction needs adapting, preserve its historical
digest with an exact frozen original function, not a repinned old oracle.
Strong comparisons add full levels/codes, EA masks, nonmissing NumPy exact
values and recursively typed attrs (True differs from 1), calibrated adversely.

## §6 Open questions / limits

None blocks this authorized source work. #114, #905 agriculture optimizers,
#1120, #637 Unknown-unit/Improved and panel-grain questions are out of scope.
Historical native CALL/RETURN NA-domain failures (407 / 2,423 rows) remain
failed; no witness is loaded or normalized here. Actual native/API/Feature/
fresh-warm acceptance and live hash census are a separate unexecuted scope.

### Phase 3 — verification

- `plot_inputs_for_wave` — OK (§§3–5): only changed production symbol.
  Source/AST projection removing exactly the label bookkeeping recovers the
  complete original function AST; legacy reducer AST is identical, including
  its condition, group keys/order and callbacks. Native scripts/colmaps,
  scheme, mappings, country compiler/Makefile, framework and schema are exact.
- Classification/conflict checks — OK (§§3–4): mapped, recorded unmapped
  numeric/literal/reserved text, missing seed and blank-stub eligibility,
  Tanzania zero, legacy-group refusal and post-relabel collision refusal are
  exercised for both literal native colmaps. No observation-grain change.
- Synthetic comparisons — OK (§§4–5): actual current/frozen functions agree
  exactly apart from explicit ruled crop-axis replacements. Native mapping
  duplicates, signed cancellation, both fertilizer slots, distinct units,
  Unknown residual, purchased-unit refusal, order, full level dictionaries/
  codes, EA masks, nonmissing NumPy values and typed attrs are covered.
  Neighbor-double/tiny-negative, True/1 attrs and unused-domain calibrations
  reject false equivalence. Fractional/unsupported conversion refusals agree.
- `#905` oracle — OK (§5): reconstructs its unchanged full historical SHA256
  using the exact frozen original plot-input function. All twelve adverse
  comparisons, range clamps, callbacks and four-ULP bound remain. Only the
  obsolete structural crop-NA expectation is replaced with the ruled marker.
  The inherited livestock nonnegative case still differs in the last bits;
  its legacy equality boolean does NOT establish exact numerical equality.
- Attempts: unchanged baseline 69 passed, 11 held-source deselected; marker-v1
  42 passed / 2 failed new attrs assertions. The frozen original reducer
  drops nonempty attrs, while its unique path keeps them. The corrected test
  preserves both behaviors; product source was unchanged. Marker-v2 at
  `633a288e96575b509355acb8e3d9ee3b62376630`: 113 passed / 11 deselected,
  18.51 seconds. Every cohort used serial n0, a 300-second TERM / 5-second
  KILL bound, private data/config/DVC-site/tmp and pinned numerical threads.
- `make api-audit` failed because Poetry is absent; source-only `--skills`
  enumeration passed (15 listed). Both logs remain. Source hashes, AST scope,
  exact commands/heads, all logs and JUnit are in dispatch-root `REPORT.md`.
- Source-stage only (§6): private data contains zero files; no live test
  Python process remains. Actual/native/API/Feature/warm validation, cache
  stamps and live hash census remain unexecuted. No public write occurred.

## §7 Reviewed actual checkpoint and public extraction

Public extraction starts at1302cec202715c7debb8e812d13e1fc2901d52fa, preserving
1364/shared labor_obs. Read current Tanzania CONTENTS including TODO/WAITING/
LOGBOOK again: modern Extended/Refresh cohorts, UPHI/r_hhid and WAITING114
remain separate. The quoted non-seed crop-NA representation is superseded only
by the settled marker vocabulary. Copy only the six reviewed helper/test files
byte-for-byte from7e2b4e448 (reviewed1a5b00e6/tested633a288e); no operators,
private runtime artifacts or private Git ancestry enter this public branch.
Shared schema, both native callers/colmaps, compiler, Make and all other
production callables remain public bytes. No additional test/data replay is
needed for unchanged source.

Dispatch b43e2115aed84ad0a1fd58f5c23ffd4d separately completed actual validation
on four authenticated captures7252726B, then independent captured-reader
copies. Original source/native unit stopped at compiler read because an
observer shifted path-resolution stack depth; original167.2085s/exit1 remains.
Two native Make producers had completed. A separately root-reviewed/released
600+5 continuation ran compiler-only Make with -o those native prerequisites,
then FIRST normal cold Country/Feature routing (including necessary native
rebuilds from captured copies), and fresh-process warm. It closed164.6133s/0
with source/operator unchanged and owned descendants empty.

Matching native/compiler/API writer CALL/RETURN/production-storage comparisons
pass. Full Country and aligned singleton Feature(currency=None), both modern
subsets and all six fresh warm repeats pass at9709/1775/7934rows with complete
axes, dtypes, EA masks, exact nonmissing NumPy payload, typed attrs and order.
Structural labels407/2423 follow independently derived source-branch labels
after unchanged reduction. All actual seeds are mapped; unknown_crop/
passthrough/stub behavior remains prior source/synthetic evidence only.
The sole qualified dependency is an authenticated1701051B finalized sample
provider, not a fresh normal sample cache or restamp. No sample source rebuild.
Seven L1/target snapshots and captured frames remain exact; observed fresh
warm raw/saved-reader/helper/script/sanctioned writer calls are0.

Exact ORIGINAL decoder-entry count remains UNMEASURED (observer recorded0);
the task is unfinished for that instrumentation requirement. Four successful
captures are not retrospectively a proven four decoder calls. Continuation
observes exact from_dta/StataReader.read identities and zero new decoder/raw
entries. No replay merely to retrofit the observer is warranted. All original
eight strict transport failures plus five continuation RETURN/bare-reader
failures remain failed. Sanctioned production-reader matching stages pass;
no actual normalization/pruning or comparator relaxation occurred. Private
DVC tmp metadata writes were explicitly root-authorized, with only zero-byte
btime after closure; tracked capability CSV scan false-positive is preserved.
No exhaustive syscall/directory/hostile-escape/memory-peak proof is claimed.

Original archive94files/31153257B manifest9248f59b989e3802f0e4e977766575a60981b5ecba8334981b5125ad282f4aff;
continuation181files/52073455B manifestd1b911403af341f0b29d96602aa866f402f64bd8f1f817dd3a5889eea4d1dea5.
Independent review9e5d6b0abc60ca8ea6179744d75b5cb3157c545a12c23d5e1af661de92f8aaaf
authenticates both archives,117source bindings/arm and all matching predicates.
Worker REPORTe12d135ae042a4bd7c04b8efa25e64c38185ac9d0cfe4b9f5c4faad0d8012b39
separates product acceptance from unmet instrumentation. This accepts only
Tanzania marker scope, not umbrella1119/1120/1287/1288, Unknown-unit/Improved
policy, observation grain or panel decisions.
