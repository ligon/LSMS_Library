# Prior-Art Ledger — Malawi missing-plot labor observations (GH #1120)

Search tier: Git, ripgrep and AST floor. Inherits STANDING.md, CLAUDE.md and
lsms_library/data_info.yml; country CONTENTS, including all TODO/WAITING entries,
read before extraction. Public base: 88c7b540e30db924c20859343b519babf6a09016.

## §1 Task
Extract the independently reviewed Malawi labor product83fd21ad244de670ccc66f8a8dd31b5fd5d1c763.
Preserve each eligible missing-plot source observation without changing known-plot
sum/first reducers, source eligibility, units, namespaces or sample construction.
No private verification operators, payloads or fixtures enter this branch.

## §2 Existing machinery
Reuse Malawi _plot_ids, _is_unknown_plot, _seed_source_reference and full-source
positions from input_obs/GH1298; Uganda GH1364 labor_obs supplies the already
canonical observation-axis precedent. Existing _plot_labor_block and
assemble_plot_labor define eligibility and known-plot reduction. The four wave
labor drivers, helper, registry and canonical-index test on this public base are
byte-identical to the reviewed product's parent. The 17 focused invented controls
in test_1120_malawi_labor_observations cover the changed boundary.

## §3 Definitions in force
Per CONTENTS and GH1120: missing plot is unknown_plot, not Farm-level; source
observation identity is not inferred plot/worker/household identity. 2016 XS keeps
cs-17-, 2019 XS uses bare case_id; garden_plot composition and CS/Panel halves stay
distinct. Eligibility is positive PersonDays OR hired reported Wage, including zero.
Canonical labor_obs/unresolved_observation is already in data_info.yml.

## §4 Invariants and contradiction
CONTENTS says "Plot labor keeps recorded days on unknown_plot stubs." The old helper
casts nullable plot keys and assembler pools missing rows using sum/first. Preserve
this recorded contradiction and document its repair. Known reducers remain exact;
unknown wage-only rows preserve missing PersonDays, following _sum_cols and hired
eligibility plus the maintainer GH1250/Uganda1364 precedent. Keep original positions
before filtering and sidecar-qualified source identity; do not synthesize IDs.

## §5 Reuse decision
Reuse reviewed product bytes and existing canonical schema. Extend only the native
labor index with labor_obs. Preserve unknown source payloads rather than pooled
sum/first. No scientific implementation redesign or new data execution is needed.

## §6 Human questions
None for this settled narrow design. Whole-issue closure remains separate.

### Phase 3 — verification
Pending extraction: compare all eight code/test blobs to reviewed83fd21ad; preserve
source-based sample provider and staged execution limits in the country checkpoint.
