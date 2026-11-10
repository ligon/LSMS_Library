# Malawi1190 selected Farm acquisition components

## §1 Task
Base cf5637114ed513b774ebae929e57f8252e8f4de2. User selects acquisition-only
Farm-level rows in plot_inputs, ALL admitted H quantity components, and literal
H36 quantities regardless of H34. No seed_acquisitions API or allocation.

## §2 Existing machinery (git/ripgrep floor)
Reuse _seed_source_reference, _seed_crop_identity, _int_codes, _use_answer,
_MWI_PURCH_UNIT_LABELS, drop_repeated_records and unchanged assemble_plot_inputs.
Existing tests: gh1190 purchase binding, gh1298 bounded observations, gh637 tail.
Four drivers already own source/half/ID conventions; append after assembly.
Country normalization, finalizer and Feature canonical input_obs already exist.
seed_kg/nitrogen_kg drop missing application quantities before summation;
fertilizer_rate delegates to nitrogen_kg. Verify actual consumers before any edit.

## §3 Definitions
Inherit STANDING.md, CLAUDE.md and canonical data_info.yml. Malawi CONTENTS:
"IHS3 (2010-11) holds BOTH blocks in ONE file"; "u describes the amount planted
(Module G)". CS17/CS19 prefixes and raw panel IDs remain; framework walks once.
2016 H07a/H38a contradict unconditional quantity-label claims; 2013 labels are
truncated; 2019 H36_1a/b are payers, not value. Preserve these limitations.
Design report a8a76d409cca44ddb6dd9ac079ddb9b7 recommended A/residual; explicit
dispatch charter supersedes its recommendation and pending T/Y choice only.
Current central decision mirror read; it has not yet recorded this H selection.

## §4 Invariants
Legacy G/D rows, reducers, attached projections, all-NA eligibility, attrs and
application results remain exact. H original ordinals precede 2010 full-record
dedup without a unique field in that mask. Equal payload records/slots remain
distinct. No first/sum/drop/allocation of new components. Reject malformed or
colliding keys. Source identity uses declared sidecar only. Own unit never fills
planted u. No third/gift value, transport or payer borrowing. First/second gate
is q recorded AND own value recorded/nonzero; gift/third need q/nativecrop only.
Missing/negative/zero and flag contradictions retain current semantics.

## §5 Reuse / extension
Extend country helper with a narrow component projector and append operation;
the existing lossy household summaries cannot recover source components.
Reuse canonical axes and optional country columns; no new registry/collector.
Carry original H scalar columns, with source/field bindings explicit and unresolved
wording disclosed. Raw source answers remain literal where no semantic binding is
established; only the closed third-source name is interpreted as a source field.
Four drivers keep all legacy statements and call append after legacy assembly.
Shared consumer edits only if focused tests demonstrate false application/zero.

## §6 Verification boundary
User authorizes PURE and actual extracted driver/consumer/normalization with
invented providers only. No new native/header/capture/source/API/cache campaign.
Freeze one finite cohort; preserve failures and continue affected cases only.
Use accepted R1/R3 guard/operator with private roots, network/source fences and
normal conftests. No metadata footprint collector; integration owns one final run.
Independent source review and public publication belong to coordinator.

## Phase 3 — closed source/PURE result
OK (§§2–5): one narrow H projector/append reuses the existing six-source map,
sidecar reference, native crop, unit and flag routines. Original Malawi helper
AST is exact after deleting only the two new helpers/role constant: R1/R3 and
every legacy reducer/attachment/deletion statement are unchanged. All original
CONTENTS bytes remain a prefix. No generic carrier or new API/axis/Make target.

Initial cohort v1: 67 pass/19 fail. Genuine append t-axis str/string widening
was fixed by casting new axes to legacy dtypes; the one-row H fixture had also
inserted a row at label -1. v2 affected-only: 16 pass/3 fail. Application numbers
were exact, but nitrogen's label tally treated H acquisitions as input rows;
the explicitly authorized exact-new-role filter is the ONLY shared code edit.
Two missing fixture preconditions (2010 G specify field, invented ID cache state)
were corrected without dropping an assertion. v3: 5 pass/1 fixture-state fail;
v4: that one finalizer/ID/canonical case passes. Latest outcomes cover 89 distinct
cases, all passing, across continuations; this is not one final-source 89-case run.

OK (§4): exact legacy payload/masks/types/axes/attrs, independent all-H row/field
oracle, negative/zero/No/NA gates, raw source/decoy preservation, collisions and
all four extracted driver assemblies/writer captures pass. Finalizer eligibility,
invented single-pass ID map/idempotence and canonical input_obs survive. Seed/N
mass and fertilizer rates are exact; three baseline-function inverse controls
prove role-absent/other-role/other-country nitrogen behavior exact. Its tally now
excludes only reported_H_component. No shared seed/factor/fertilizer-rate rewrite.

Accepted R1 guard/operator reused with exact job/report/head bindings. All four
attempts use normal conftests, MAIN, n2/loadfile, thread/build1, unshare-rn, private
XDG/DVC/SQLite/tmp/data/config and absent credential gates. Every controller/worker
profile remained active, zero denials; each attempt ends with private data/config
file counts zero. v3's missing cache-state flag read existing committed panel-ID
JSON via metadata access; v4 uses only the intended invented map. No native/header,
capture, data source, producer/API/cache cohort or metadata collector ran.

make api-audit originally failed because Poetry is unavailable. The explicit
charter permits this PURE scope despite project-runtime's source-only prelude.
Unchanged mount helper initially failed FUSE; extraction from the same immutable
generation 20261019T221223Z with processors1 into unused own job storage succeeded,
then unchanged helper verified/repointed only this checkout's venv. No dependency
or shared-image/cache change. Independent review, final metadata footprint and
public publication remain coordinator-owned; real-data acceptance is unrun.
