# #846 Tanzania: exact wave and source-concept evidence

Search tier: Git/ripgrep, source inspection and authenticated document metadata.
Base: public development `5a4b2ac623d55bb31b8908260ad54efb34070cd6`.
Inherit `STANDING.md` and `846-tanzania-food-price-capability.md`; merged
PR1308 is the preparatory two-row precedent, not coverage closure.

## §1 Task

Extend country-local evidence to twelve exact wave/concept records for
`reported_purchase_price` and `own_consumption_valuation`. Record the accepted
C4 instrument finding separately from the nonclosing coverage verdict. No
shared accessor, canonical-feature absence, source/API replay or imputation.

## §2 Existing machinery

- `coverage_matrix.load_verdicts` preserves extra CSV columns and exact wave
  keys; `_absent_tier` leaves `unsure` open and `_dropped_tier` keeps that verdict
  inert. Existing tests: `test_coverage_matrix.py` and the country-local
  `test_gh846_tanzania_food_price_capability.py`. Extend these metadata controls.
- Merged `_/capabilities/food_price_absent_verdicts.csv` and
  `_/food_price_capability.md` are country-local, not default-loaded. Keep this
  location and schema core, adding explicit instrument scope and actual methods.
- `SeriesCapability` is whole-series/feature scoped, automatically reports
  C1;C4 for questionnaire validation and expands supplied waves. It cannot
  represent this partial evidence faithfully. Leave it and the vocabulary alone.
- Accepted independent review: recovery
  `Tanzania/846-independent-Ethiopia/REVIEW.md`, SHA256
  `370da7f8c8cce9bc1e2d817697a31642328caca4b2f07098ad970e4f6fbf9c64`.
  Its full report, BINDINGS and REVIEW.json were read. Frozen C4 packet manifest
  SHA256 `17886d8ae337b9c922e702b3f59de83a2323243a6ea5cf219fd50978f3bb8286`.
  The reviewer authenticated all336 listed files and reproduced30 critical-page
  normal-text hashes from the four authenticated PDFs.

## §3 Definitions

Inherit STANDING §3, canonical `data_info.yml`, and coverage guide “The four
checks”: all applicable checks must run for closure; an unavailable check leaves
`unsure`. A purchase unit value is paired spending/quantity, not a separately
elicited purchase price. Own-consumption valuation here means the reported
monetary value of own-produced food consumed inside the household. SectionF
outside-HH value, crop/community prices and #585 imputation remain distinct.

C4 supports `not-asked-within-module` for these twelve depicted scopes. C2 is
the completed instrument/channel and official dictionary comparison for those
concepts only, not an every-file shipped-source differential. C1 current shipped
variable/value-label census is outstanding. C3 applicability and harmonized
direct-versus-derived lineage remain unresolved; a null is never a negative,
and a derived/imputed positive is not proof of separately elicited answers.

## §4 Invariants and method limits

The complete baseline Tanzania CONTENTS, including LOGBOOK/TODO/WAITING, was
read in the preceding evidence phase; current public CONTENTS is byte-identical
to that public baseline. UPHI back-casts Round4 descendants while r_hhid is the
actual round household. The Extended2019–20 and Refresh2020–21 cohorts are
disjoint; WAITING114 persists. No grain, identifier or source eligibility change.
Retain dated contradictions: old “blocks =food_prices=” versus #112 LOGBOOK
quantity presence; FCT118 still repeats missing quantities; older community
link “infeasible” versus later “BAKED INTO the community admin codes”.

The older-wave provenance is reconstructed original R1–R4 panels in the UPD
instrument, explained by BID PDF5–6/19/29, not four independently acquired
contemporaneous original PDFs and not generic shared-folder extrapolation.
Y5 standardJ1 and released alternateJ1A both count: DDI5639 describes hh_sec_ja1
and j1_selection despite editorial marks. No sample assignment, item eligibility,
row-count or API-loss inference follows.

Author extraction covered305 text-bearing pages without warnings; independent
normal-text hashes reproduced30 selected pages. Secondary layout extraction
emitted14 rotated-text/incomplete-layout warnings, preserved separately. No
full raster/OCR/visual blackout inspection or infallible table alignment claim.
BID19's broad “quantity and value” summary does not imply a monetary field for
every channel; the explicit channel columns govern this exact module finding.

## §5 Reuse decisions

Extend the existing country-local CSV: keep `verdict=unsure`, add
`instrument_finding`, exact `module_scope`/PDF locators, actual completed
methods, outstanding C1/C3 and `closes=False`. The literal source-concept names
remain in legacy `feature`, outside canonical vocabulary. An explicit-path
loader exercises serialization only and cannot adjudicate scientific closure.
Default verdict loading and series registry remain unchanged. Documentation
explains the accepted C4 evidence and exact residual; the original #846 live
integration requirement remains open. No duplicate public issue comment.

Placement inherits the existing build-input boundary: CONTENTS is excluded;
Markdown and nested capabilities CSV are not direct `_/*` build inputs. No
production callable or table configuration changes, and no live hash measured.

## §6 Acceptance and residual

Root accepted the independent C4 review and authorized this narrow metadata
integration, deferring a shared accessor. Coverage remains unsure/nonclosing
for every row until C1 scope and C3 applicability/lineage are adjudicated on
existing #846. No new scientific choice about the actual module columns is
needed. Test plan: the existing country-local metadata controls, updated to
verify twelve exact keys, uncertain tier behavior, unchanged default/series
dispatch and closed-verdict evidence gate. Approved mounted Python, private
paths, core36/thread1, bounded runtime; no microdata or Country/Feature actors.
