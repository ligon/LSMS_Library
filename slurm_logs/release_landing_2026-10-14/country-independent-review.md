SCOPE DEVIATIONS

None in this review. Country worktrees were read only. No test, cache, fixture,
source, or baseline changes were made by the reviewer. Implementation authors
made the two requested corrections before these final approvals.

## Independent review of the final country follow-ups

Review scope was the changes after the original, independently reviewed heads
were merged with development 452302ad. For each branch I compared its final tree
with the automatic merge tree, read the country CONTENTS including available
TODO/WAITING/LOGBOOK entries, checked relevant original verdicts and author
validation evidence, and checked final ancestry and clean worktree status.
This is an independent code/evidence review, not a second full cold-build run.
No final follow-up changes tests/fixtures, blessed/golden data, lockfiles,
dependency pins or DVC sidecars. Uganda's proposed patch is an unapplied artifact.

| Branch | Exact reviewed head | Decision |
|---|---|---|
| landing/niger | bb51df7e769dbddce367698a827fef5c2584e52a | Approve |
| landing/nigeria824 | 34928ec68a1703b715dfe7f6373f120703e5b31f | Approve |
| landing/nigeriaw5 | b3d035afec96f41e1bb02e59765114bebcc0b199 | Approve |
| landing/perupanama | 9fc6aa2e0efa0360f9d20489fef87f96acd5670b | Approve |
| landing/burkina | 45783af9e28276a84a89738fd3f8a9e9dfa43108 | Approve |
| landing/uganda | 059373913186fb7c018e272d6eda07a15f979cdd | Approve evidence/report; historical nutrition baseline gate remains blocked |

### Niger

Final follow-up only adds cold-build evidence and clarifies the before-build
citation. The ten output counts agree with the named source contract and original
verdict. The final normal pytest run is 139 passed, no skips. Earlier failures
came from naming the checkout `niger`, which shadowed the country module; moving
it to `niger-followup` fixed the harness without modifying tests or the product.

Named idiosyncrasies: ECVMA2 household identity includes EXTENSION; extension 2
is a separate split-off household and is deliberately unlinked. Niger s16d
replicates a crop sale by harvest LINE, not by distinct plot. The original
follow-up's exact known-orphan assertion remains intact: only crop_production
2011-12 hid 27010 is accepted, rather than an xfail hiding other waves.

Contradictions were preserved and corrected with evidence: CONTENTS explicitly
says the former 2021-22 unit agreement of "63.0%" was a spelling artefact;
current priced-row agreements are 98.7/89.4/92.3/89.0%. The older "280 in
livestock, 367 in plot_inputs" counts referred to all raw rows, not kept rows;
the current explanation distinguishes 17/50 kept-row collision groups and the
remaining 35 destroyed seed-slot rows. The old 18-table cache count names build
tables; the review's 22/22 includes four derived tables. No assumption of staleness
was needed. The unlinked id 20012 cross-wave alias, 27010 orphan and seed-slot
collisions remain explicit residuals. Wider #824/#833 drafts preserve already
landed Malawi work rather than opening duplicate implementations.

### Nigeria sold units

The additional assertion now explicitly requires Unit_sold in the output.
Follow-up code changes are comments; the original extraction is preserved.
Corrected duplicate-sale counts match the independent verdict: 125/81 duplicate
keys touch a sale, 78/43 contain multiple positive sales, 96/66 positive rows are
lost by the existing deduplication, and 40 keys lose their only positive sale.
Author validation: 22 passed; cold crop output 62,844 x 10 with unique index.

Named idiosyncrasy: W1/W2 sa3q11b records sold units, while W3-W5 household/crop
sales have no plot linkage and correctly remain NA. The newly documented
remaining deduplication and non-first-buyer issues are not solved by extracting
this unit column. The existing CONTENTS WAITING caveat about pooled nonfood
recall periods also remains live; no broader closure is claimed.

### Nigeria wave 5

The follow-up only corrects source/version prose and a test explanation. It
preserves the original mover identity guard, exact community expectations and
#978 follow-up. Author validation: 34 passed; cold cluster output 7,070 across
ten quarters and W5 community rows 27,318.

Named idiosyncrasy: ea=0 denotes movers and needs household-specific identity;
518 real EAs plus 393 movers produce 911 clusters per round. The quoted claim
"ships NO household geovariables ... by source, not by omission" was contradicted
by catalogue v02, which adds the 4,883 x 55 geo file and other files absent from
acquired v01. The branch now distinguishes versions and leaves the missing geo
wiring as #978 rather than calling the fields optional to suppress the guard.

### Peru and Panama

Three added source-backed tests exercise the intended behavior. Peru's 1994
sample has 3,623 rows and 364 three-digit SEGMENTO clusters; all 19,285 roster
rows join the sample cluster labels. The raw weight field is all ones, an export
artifact, so the test does not invent a weight or panel weight. Exceptions in
sample tests are limited to Peru's three acquired but unwired waves and its
missing meaningful weights; no other country's exception is broadened.

Named Peru idiosyncrasy: SEGMENTO is nationwide, while HOGAR encodes a fraction,
not a household ordinal. Source evidence supports the new sample and roster
checks. Acquired 1985/1990/1991 sources remain unwired and are not portrayed as
implemented waves.

Named Panama idiosyncrasy: lowercase `value` (raw code 77/valor) is a survey
unit whose quantity is meaningless; capital `Value` is the separate library
Q=E convention. The raw-source expenditure test pins 356,079.56 after excluding
9999 and confirms 10,614 lowercase-value quantities stay absent, while 46
capital-Value rows keep Q=E. The follow-up corrects the description of two 7.7
rows (one unit 75, one missing unit). I requested rel=0 because pytest.approx's
default relative tolerance exceeded the claimed one-cent bound; both assertions
now enforce the absolute bound and the affected test passed again.

Author evidence: initial selection 26 passed, 8 skipped, 1 xfailed with all new
tests executed; skips are unwired Peru waves, and the xfail is the precisely
scoped absent-weight check. Final changed source-backed test: 1 passed, no skips.
These are explicit source limitations, not unexplained baseline acceptance.

### Burkina Faso

Final follow-up is documentation and diagnostic-docstring clarification, plus
aggregate cold evidence; original crosswalk/config repair remains intact.
The report distinguishes 43,906 manufactured passage-1 zeros from 219 surviving
clips and 405,127 money-only passage-2/3/4 quantities. Its comparison JSON shows
765,816 rows before/after, exact non-Quantity equality after four documented
label replacements, and exactly 449,033 zero-to-NA changes. Expenditure remains
342,110,515 and the quantity sum is unchanged. Author tests: 34 passed, no skips.

Named idiosyncrasies: four separate seven-day recalls share a wave and retain
visit; passages 2-4 have expenditure without a quantity grid. The literal NEL
crosswalk key must stay on one physical line. TODO entries for food/nutrition
work are not treated as completed by this change. The clarified money_only
warning cannot prove instrument coverage: other populated sources may hide a
purchased-only misbinding. Unread autoconsumption VALUE, the meaning of qachat,
and 219 clips remain scientific residuals under #864.

### Uganda

Final follow-up changes only its report and aggregate evidence, including the
unapplied patch. Current-development and candidate food-chain comparisons use
identical raw inputs and unchanged 15 x 113 FCT. All expenditure keys/values,
source coverage and previously labeled physical rows agree; raw-source joins
account for all 7,359 formerly blank-unit rows. The incremental kg difference
is -849.299850578, and 24 household-wave nutrient rows disappear when they have
no remaining convertible quantity (20 formerly nonzero, four formerly zero).
Sixty retained vectors change. This is a specific consequence of #967, not
validation of all older mass assumptions.

I found the report's claim of matrix replay difference "0" contradicted its
committed comparison.json. The final head corrects it to 2.9802322387695312e-08
and the separate food-by-food bound to 1.1641532182693481e-10, matching both roots.
Arithmetic checks pass at their stated tolerances. The final prose no longer
claims exact equality.

Named idiosyncrasy: curated conversion_to_kgs is dropped during canonicalization,
so the API depends on the runtime factor ladder. CONTENTS states, verbatim,
"Do not regenerate the baseline until the direction is signed off". That hold
predates #967 and remains controlling. Historical fixture: 23,801 x 15, hash
fc85fc2e...; current development: 23,801 x 15, hash 1131b358...; candidate:
23,777 x 15, hash 12d25f6e.... The fixture was not modified. The proposal changes
only nutrition shape/hash and remains unapplied. Author tests deliberately
retain one nutrition failure alongside 85 passes and 41 unbuilt-parquet skips.

Approval of this evidence does not approve a new historical nutrition baseline.
The owner must decide that scientific direction before the release gate can
close; the incremental replay alone cannot certify #850 and subsequent factors.
