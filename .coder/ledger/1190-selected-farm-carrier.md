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
