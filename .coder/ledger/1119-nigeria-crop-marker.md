# Prior-Art Ledger — Nigeria GH #1119 crop provenance

## §1 Task and fixed sources

Replace the structural non-seed crop spelling with `(not crop-specific)` using
the producing branch's provenance. Preserve seed eligibility, legacy ordering
and deduplication, channels, units, payloads and observation grain. This public
extraction starts at development `104655c158dc2521e956bc643c812808a7cf476e`.
Product/driver/test bytes come from reviewed candidate `46223de4`; its baseline
is public `1302cec2`. The current public Nigeria tree and relevant framework
files equal that baseline before extraction; unrelated country changes stay.

## §2 Existing machinery and reuse

| Machinery | Reuse |
|---|---|
| `seed_rows_for_wave` | Unchanged resolved-crop/household/plot gate and numeric refusals |
| Long/wide fertilizer and chemical constructors | Carry a private desired label alongside the legacy crop key |
| `_pick_channel`, `assemble_plot_inputs` | Existing arithmetic, five-payload hollow rule and retained-unit handling |
| Uganda post-reducer crop labels | Keep old keys/order, then relabel; refuse ambiguous groups |
| Malawi source crop identity | Precedent only; no imported eligibility or universal zero rule |
| GH #1196/#1206 comparisons | Full axes, string policy, masks, extension arrays and exact nonmissing NumPy |
| Real injected-sample finalizer tests | Preserve the actual hollow-row and canonical Feature boundary |

## §3 Definitions in force

Inherit `STANDING.md` §3, `CLAUDE.md` and canonical `data_info.yml`; GH #1119
distinguishes structural `(not crop-specific)`, crop-specific `unknown_crop`
and a recorded unmapped identity. Another axis's sentinel is not crop evidence.
Nigeria has no universal zero-as-unrecorded authority. Missing/unmapped source
seed lookups remain excluded by the existing gate; fractional numeric refusal
is unchanged. This patch creates no new unknown-crop population.

## §4 Invariants and recorded contradictions

Inherit `STANDING.md` §4. Five folders serve ten PP/PH quarters; input output
keys use PP quarters even where the feeding source round changes after W2.
CONTENTS calls no-quantity seed rows “not inputs at all” but expressly retains
them because dropping changes grain; driver/schema reported-quantity prose
does not authorize deletion. Tree-crop/no-channel eligibility stays unchanged.

The old Country `u=NA` statement is expressly superseded only for retained-unit
normalization by #1118/#1041. Quantity/u/Purchased/Quantity_purchased/Improved
alone define the hollow boundary; private provenance and keys are not payload.
False, zero and recorded unit strings count as existing payload. Preserve old
keep-first before finalizer drop, mixed channels, #867 share and own-round
identity. Purchased=False with missing purchase amounts remains unadjudicated.
WAITING's 12-month versus PAST 6 MONTHS contradiction is separate.

## §5 Implementation and compatibility

Keep private legacy `n/a` through old sorting/dedup; apply desired labels only
after the winner settles, changing only the crop axis. Discard private metadata.
No global `n/a` replacement or missing-lookup inference. Explicit nonnull direct
assembler labels remain compatible, including recorded `n/a` with provenance.
Untagged null/legacy-marker direct calls refuse even when hollow: validation
precedes the old hollow filter. Production constructors always tag their rows.
Conflicting desired identities in a legacy group, or a post-label key collision,
refuse rather than split, merge or allocate measurements. Broader grain policy
belongs to existing #1120 work.

## §6 Verification and limits

The unchanged product/test bytes passed 76 invented cases at `283cbe44`
(35 marker + 41 retained-unit), with independent source review. First run's
75-pass/one-fixture failure assumed a missing-quantity seed was hollow; existing
Purchased=False made it nonhollow. Only that expectation changed. No scientific
decision or new test/data replay follows from this extraction.

Actual native source conservation covers 96,620 rows: 34,641 structural and
61,979 seed rows. Seed gate exclusions remain nine in 2010 and fourteen in 2023;
no new eligibility. Eleven original mandatory constructor-marker comparisons
failed because the oracle forced nullable-string pd.NA at a scalar constructor
stage using np.nan policy. A separately authorized eleven-output diagnosis
confirmed every label, exact source-faithful expectations and unchanged actual
frames/attrs. Original false receipts remain; no product fix was made to satisfy
the oracle. A successful-exit comparison bug was also diagnosed; source-only
fail-fast controls passed separately. These operators are not published here.

The completed normal API unit `141c99de9d5e434889ac5f6be90c5945` / job40659774
passed all 26 mandatory and 10 transport predicates over full/five-subset Country
and singleton Feature, plus fresh-process warm. API574.222s/warm83.642s; whole
controller/export692.284s. Normal API used 94 source getters (26 input/68 sample),
warm zero, with complete source, inherited RETURN, cache and owned-child closure.
Strong numeric, full-domain/axis and typed-attrs comparisons accompany counts.
This proves production composition, not independent population/ID/unit science.

Original 300-second baseline timeout, clone128/publisher74 and late detection of
the eleven oracle failures remain. The intermediate archive lost two parquet
files (925,631B) through node-local relative export/Gitignore; deterministic
recovery found no files. They were not reconstructed. The later actual rebuilt
NEW caches and used the authenticated surviving native RETURN. Its absolute
shared private archive has 241 verified members/386,477,750B, manifest `6ff0780a`.
No private frames, operators or archives belong in the public extraction.

Getter history is 20 original + 36 interrupted continuation + 94 final = 150,
including necessary partial-read repeats. No whole #1119/#1120/#1287 closure,
new source acquisition, missing-crop retention ruling, or cache census follows.
The shared Nigeria helper changes build fingerprints; validation used private
caches, and documentation-only changes do not claim an additional re-warm.
