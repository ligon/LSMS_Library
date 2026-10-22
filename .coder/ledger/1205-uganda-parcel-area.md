# Prior-Art Ledger - GH1205 Uganda parcel-area diagnosis

**Author:** Sue (GPT-6), 2026-10-22.
**Search tier:** ripgrep and git; static source first, no project/data imports.
**Source:** `491daf5654b9ee45332bec97e1296ae821ff46d5` (PR1207 over1202).
**Standing baseline:** inherits `STANDING.md` sections 2-5 and the unchanged
`AGENTS.md` / `CLAUDE.md` instructions (byte-identical to release f8).

## Section 1 - Task, restated

Diagnose the incomplete Uganda parcel-area matches in `yield_kg`, then replace
the unsupported 100% coverage statement with what the source-backed diagnosis
establishes. This is a documentation/diagnosis branch, not authorization for a
new identifier rule, join grain, area imputation, eligibility screen or factor
change. The actual source includes native `production_obs` and PR1207's factor
bindings. The issue's 14,022/40,409 pre-factor counts are historical, not a target
to force onto this source. Root reviews the static diagnosis and bounded data
plan before any held-source copy, decode or project/data import.

## Section 2 - Existing machinery

| Symbol or definition | Source anchor | Existing verification | Decision |
|---|---|---|---|
| `yield_kg` | `lsms_library/transformations.py:4888` | `tests/test_plot_id_canonical.py:156,191`; declared-unit/shipped-factor tests | Reuse the exact current public route; no alternate yield estimator. |
| `_parcel_from_crop_plot` / `_parcel_from_feature_plot` | `transformations.py:4860,4877` | Synthetic parcel-mode tests above; historical held-source controls in GH1198/849 notes | Reuse exactly for measured route keys; their real coverage is the question. |
| `harvest_kg_factors` / `harvest_kg` | `transformations.py:4282,4698` | `tests/test_crop_kg_factor.py`, `test_shipped_factors.py`, `test_gh849_uganda_reported_factors.py` | Reuse current per-observation conversion and explicit reduction. |
| `local_tools.id_walk` (Default); `uganda.id_walk` (country Make compiler) | `local_tools.py:3636`; `countries/Uganda/_/uganda.py:679` | `tests/test_id_walk.py` | Reuse the wave-specific household walk; do not infer prefix substitutions. |
| `plot_features_for_wave` | `countries/Uganda/_/uganda.py:864` | Existing area/schema checks and held-area controls; no current-stage coverage proof yet | Trace source declarations, area preference and eligibility before interpreting absence. |
| `crop_production_for_wave` / `CROP_COLMAPS` | `countries/Uganda/_/uganda.py:1260,1844` | `tests/test_gh1198_uganda_production_observations.py`, `test_gh849_uganda_reported_factors.py` | Preserve release/physical-row identity, slots, conditional old-null boundary and all measures. |
| Table declarations | `countries/Uganda/_/data_scheme.yml:88,133` | Schema/plot-axis tests | Reuse declared `(t,i,plot_id)` area and native crop grain. |
| Sanctioned IO, freshness and API finalization | `STANDING.md` sections 2/4; `CLAUDE.md` | Existing library tests | Use normal readers/builders with isolated guarded roots after approval. |

## Section 3 - Definitions and conventions in force

- The complete current Uganda `_/CONTENTS.org` (3,741 lines, including all
  TODO/WAITING/LOGBOOK entries) was read before other task work. Its agriculture
  sub-sample is not the whole household survey. Its rotating refresh,
  household/cluster identifiers and visits differ by wave.
- `CONTENTS.org` section "2026-09-10 -- the plot axis is now spelled plot_id"
  explicitly says a shared axis NAME is not a shared VOCABULARY: crop
  `hhid-parcel-plot` differs from feature `parcel_suffix`. `on='parcel'` is the
  existing bridge, not permission to manufacture a new join.
- `yield_kg` selects numeric `Area > 0`, sums area and converted harvest at its
  existing `(t,i,parcel)` grain, inner-joins and divides (`transformations.py:
  4976-5004`). Areas are Uganda's hectare-equivalent measures; source authority
  and missing-area reasons remain to be traced per wave.
- The current "Reported harvest rates" country note records 14,520 matched
  parcel output keys after PR1207, versus 14,022 at its actual parent. Factor
  eligibility changes must remain separate from key/area coverage diagnosis.
- `STANDING.md` sections 3/4 and `lsms_library/data_info.yml` remain authoritative
  for canonical names, IO, pre-finalize caches, `id_converted` and API `v`.

## Section 4 - Invariants and assumptions

- No edits to factors, units, conditions, `production_obs`, seasons, quantities,
  source bindings, eligibility/screens or public yield frames during diagnosis.
- No prefix guessing, silent plot/parcel substitution, area imputation or new
  scientific grain. A missing area-key match is not by itself a linkage bug.
- Preserve the documented conditional old-null crop exclusions, native record
  coordinates and API household walk; inspect native/API/converted stages
  separately. Do not let canonical `i` conceal a native embedded household ID.
- Existing live caveats stay explicit: #829's 2015 sale-release WAITING entry,
  2018 missing harvest-unit source, A/B visit semantics, #1204 Make subset route,
  #1196 storage difference, and #788 sibling writes are separate work.
- Guard every later project/numerical import before startup: mounted interpreter,
  `unshare -rn`, socket/write guards with dir_fd resolution/logging, exact import
  identities, private credential-free config/XDG/temp/DVC/data, all threads1,
  `LSMS_MAKE_JOBS=1` and actual helper None. At most two loadfile pytest workers,
  with every-worker identity and actual-parent rootdir/importlib if tests run.
- Authenticate declared authority, genuine MD5, SHA, size, mode/mtime, nlink1,
  nonsymlink and distinct physical raw copies before decoding. No acquisition,
  bypass, planted/stamped cache, in-tree payload or cache promotion.
- Raw rows, IDs, coordinates, full warnings/traces and provider paths stay
  private; only proved aggregate counts and bounded source statements export.

## Section 5 - Reuse decisions

| Quantity or action | Decision | Reason |
|---|---|---|
| Native/public/converted crop and area operands | Reuse | Existing normal builders and transforms define this task's current baseline. |
| Public parcel keys, positive area and yield output | Reuse | Mirror exact current helpers/eligibility for diagnosis; never change them. |
| Per-wave unmatched-key partition | New diagnostic evidence only | Existing issue counts do not identify why keys fail; categories must follow actual source definitions and preserve unresolved cases. |
| Guards and physical receipts | Extend private tooling | Resolve and log original dir_fd mutation endpoints; retain denials/failures rather than weaken guards. |
| Coverage prose | Extend after root/data review | Correct the unsupported statement only after measuring source-backed cases. |

## Section 6 - Open questions and approval boundary

Which current-wave nonmatches are explained by absent area collection,
nonpositive/missing area, household identity, parcel identity or unresolved
source correspondence? These are questions, not preselected conclusions.
Next: trace static definitions per wave, send root the concrete partition and
minimum provider/read/guard plan, and await the explicitly required data-stage
review while preparing only independent metadata/guard work.

## Phase 3 - Verification

Pending source diagnosis, root-reviewed bounded data stage and independent
review. No acceptance, current data counts or repair is claimed by this ledger.

## Addendum - Source-backed wiring defect and approved boundary

The preceding plan records the initial diagnosis-only scope.
Root subsequently approved the bounded held-source diagnosis and, after
reviewing its exhaustive result, the following narrow runtime repair.
This addendum precedes production edits.

At source491 plus the ledger-first commit309215e28, the normal Default cold
and fresh-process warm runs return exactly equal complete sample, area, crop,
harvest and both yield-mode frames; warm data-file receipts do not change.
The current42,481 converted route keys partition into14,520 exact positive
matches,438 exact-but-ineligible keys,27,521 directly witnessed native-household
prefix failures and2 source parcels absent from the area rosters.
These are route keys, not additive extra yields.
The prefix-failure group has21,165 positive-area,6,348 ineligible-area and8
absent-area witnesses.
The source constructor at Uganda/_/uganda.py:1430-1435 concatenates explicit
formatted household, parcel and plot fields; the Default walk changes only i
and retains the embedded native household.
The current-i-only extractor therefore fails after valid panel walking.
All56,841 retained unique native plot tuples invert exactly from the right
to their source components, and all140,961 associated raw rows have nonempty
ASCII-decimal parcel/plot suffixes.

Reuse that checked constructor inverse only for an Uganda-marked Country
operand when the existing current-i prefix branch misses.
Require a nonempty native-household head and two ASCII-decimal suffixes;
otherwise preserve the unchanged-string fallback.
An explicit country axis/column must disable the new fallback even when a
pooled frame carries a stale scalar country attr.
`yield_kg` alone passes existing country metadata; `fertilizer_rate` remains
under GH1110.
Do not rewrite observed plot identifiers, introduce fields/grain, choose a
key from area matches, impute area, or change conversion/eligibility/aggregation.
Actual491 runtime-byte discriminator tests and an independent raw-column
saved-operand oracle must verify the change, together with full fingerprint
scope and strict unchanged operand/harvest/literal-route controls.
Independent review and publication remain root-owned.

Private reproducibility and qualification records are in the authorized
`uganda1205-diagnosis` evidence prefix: `REPAIR-BOUNDARY.md`, the full partition,
source ancestry, provider/copy receipts, actor streams and preserved diagnostic
stops.
The original ancestry actor stopped on the2013 loader's mapping variable name;
its corrected successor completed without replaying the cold/warm actors.
The reviewed comparator's standalone DatetimeIndex frequency limitation is
retained; full-frame frequency controls pass, and actual axes are non-datetime
MultiIndexes.
No all-history zero-denial or outside-write claim is made.
