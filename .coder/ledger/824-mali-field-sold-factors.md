# Prior-art ledger — Mali field-only sold factors (#824)

Search tier: git/ripgrep/AST floor. Scientific candidate base0bfca3e9; source/ordinary metadata only, no survey parse/import/data actor. Inherits STANDING.md §§2–4, CLAUDE.md and canonical data_info.yml. Read Mali CONTENTS first including dated TODO/WAITING/LOGBOOK caveats; vocabulary header is its only TODO/WAITING declaration.

## §1 Task

Carry the EACI FIELD sale's reported kg-per-sold-unit factors (2014 s3aq23c;2017 s7gq21d) as optional KgFactor_sold. Reuse exact missingness-aware agreement at both existing many-to-one sale folds. Contradiction blanks only factor cells and files structured persistent audit; existing rows, quantities, units, value/window/eligibility and grain gates stay. Trees' sometimes-total equivalents are excluded. No weighted/mean/first factor, source plausibility screening or consumer rewrite.

## §2 Existing machinery

| Symbol/source | Contract and evidence | Decision |
|---|---|---|
| `build_transforms.reduce_to_agreed` | Exact value agreement, `on_conflict='na',na_is_conflict=True`; files `cells_blanked` ledger, strict raises. Tests GH323/GH868. | Reuse factor-only frame, literal options at both folds. |
| Mali2017 sale fold | `(i,crop,Unit_sold)` sums existing amounts with min_count1, then existing one-unit/single-plot gates; tests gh824/gh1250/gh1267. | Preserve amount aggregation/gates; independently reduce factor on same keys. |
| `mali.crop_production_finalize` | `(t,i,plot_id,crop)` existing duplicate fold, mixed-sold-unit gate, sums and first/max old fields. | Preserve old reducers and add independent factor agreement only. |
| Uganda native crop writers | Explicit `to_parquet(...,grain_audit=grain_reports(country,table))`; GH868 static and real parquet replay tests. | Reuse for both Mali native writers. |
| `local_tools.read_parquet_grain_audit` / `to_parquet(grain_audit=...)` | Existing footer-only public metadata reader and sanctioned writer. | Relay native footer reports through unchanged country concat, deduplicate while preserving reports. |
| `country._replay_wave_stamp`, country cache write/read/replay | Original Wave native stamp→table ledger→country footer→warm signal. | Reuse unchanged, no new attrs/audit framework. |
| `transformations._sold_kg_factors` | Merged1355: supplied reported factor screened against sold label, no harvest-factor substitution; fallback only absent sold label. | Reuse unchanged; source carries values without new clipping. |

Carrier finding: current Mali native writers pass no grain_audit, and country concatenator does not relay native footers. Existing default Wave/country warm machinery already replays stamps, but direct compiler artifact currently loses them. Candidate makes the explicit native writer and compiler footer relay visible; no invented attrs carrier.

## §3 Definitions and rulings

- Maintainer6367367289: https://github.com/ligon/LSMS_Library/issues/824#issuecomment-6367367289 — sale uses its own Unit_sold / supplied KgFactor_sold; harvest fallback only if sold unit absent.6375016034 explicitly lists Mali factor wiring as ruled remaining work.
- CONTENTS “Not served: a KgFactor_sold” is current missing implementation. Field s3aq23c/s7gq21d are documented per-unit; tree s3bq13c/s11fq14c are sometimes total kg, not admissible field-factor substitutes.
- CONTENTS older 12-month tree SALE prose is historical: revised1272 ruling6375506358/merged1326 serves tree sales as recorded; harvest multiplier remains. Preserve the printed-total versus monthly-label contradiction and Lourd/Leger1607 yes-sale missing amounts.
- KgFactor_sold is a REPORTED kg per ONE sold unit, not a summed kg quantity, not harvest KgFactor. Existing consumer screens stay; canonical/country optional float declaration extends the already named consumer carrier.

## §4 Invariants

Two existing folds are scientifically different: household-crop-sold-unit sale in2017 and final plot-crop duplicate fold. Factor agreement uses all contributors including NA; no first/non-null completion, mean, quantity weighting or added key. Factor conflict cannot remove previously served quantity rows. Existing Non→0/no-unit, one-unit, single-plot, mixed-unit, crop-label and tree-window gates remain. No tree factor, new source, map, sample, identity or unit relabel. #1357 excludes anonymous household-keyed records; known household missing plot/v is distinct.

Native out-of-process reducers explicitly name Mali/crop_production; the early fold passes WAVE and final fold reuses the reducer's original single-t index inference. Footer relay uses actual original reader path, no atime/freshness claim. Preserve preexisting reports once, not only new factor reports. Warm strict may refuse recorded conflicts under existing grain strict policy; ordinary factor-only NA remains loud. Fold audit counts describe that fold's source contributors, not eligible final-row losses.

## §5 Reuse decisions

Reuse reducer/ledger/writer/footer/default replay and existing sold consumer verbatim. Extend two native mappings, the two existing factor folds, optional schema, native writer arguments and country compiler metadata relay. New code is thin carrier wiring; no new estimator/reducer/framework. Existing amounts/rows/grain must match old source on invented fixtures independently of factor expectations.

## §6 Open scope and verification

Tree-factor interpretation and other-country/unwired-wave #824 scope remain separate; they do not block FIELD wiring. No new human scientific ruling requested. Source candidate and serialization/replay regression design go to independent red-team before any data actor. Proposed invented tests cover exact/NA conflict at both folds, old-column invariance, current field-only source routing and native→compiler footer→fresh warm replay; no survey fixture or raw/cold corpus build. Test execution and actual source validation are not claimed here.

## Phase 3 source-only anchored check

OK (§2–4): the two calls reuse the shipped reducer with missingness conflicts and only factor payload. All ten original finalizer payload reducer expressions are AST-identical;32 other top-level Mali function definitions and both waves' native helper definitions are unchanged, including tree-window/derivation functions. Existing country concat/source declarations and quantity reducers remain, with only new factor attribution and footer transport. No source positivity screen, weighted factor or tree mapping. Native footer writer is the existing Uganda/GH868 pattern; compiler original reports are retained once in first-seen order, independent from attrs, with direct caller resolver stack_depth1.

New17 invented cases and bounded serialization/compiler/replay plan are prepared, NOT executed. No project import/test/data actor or source measurement occurred. Static Python syntax and documentation diff check passed. Source proof is not typed output/scientific validation; those gates remain pending exact source/red-team review and root release. No fresh factor coverage, missingness/duplicate count, source or cache-freshness claim.
