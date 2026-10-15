# Independent completed Feature-audit review

**APPROVE the completed-with-findings evidence and its sanitized archival
package.** This is not a clean semantic audit or release approval. The five
tenure FAILs, 19 ERRORs, remaining warning triage and Uganda's nutrition
scientific hold all remain explicit.

Reviewed frozen source `7fa05179c8519f44ea329ba6387f5d6d6441a8ea`, final
REPORT/TRIAGE, raw private diagnostics and the 20-file publishable package.
`validation.json` records every reviewed artifact's size/SHA256;
`check_evidence.py` independently reconstructs counts, source history and sums.
No API calls, builds, source/Git mutations, external writes, or original/active
cache access were made. Read the Feature README, cross-country-features skill
and Aggregate contract before interpretation. Skills' suggestions to rerun
cold were subordinate to this task's explicit saved-evidence/read-only scope.

## Verified scope and findings

- **39 features, 38 countries, 549 declared Country pairs, 38 baseline Feature
  calls, 17 configured variants, zero scope skips.** Reconciled the inventory,
  39 ordered start/completion log entries, per-feature country counts, record
  counts, TSVs and unchanged scanner control flow. panel_ids has 11 Country
  property loads and no DataFrame assembly by existing harness design.
- **3,544 diagnostic records = 3,504 WARN + 19 ERROR + 5 FAIL + 16 PASS.** Phase
  totals are 1,234 / 808 / 1,502. All 3,528 non-pass records account for 266
  clusters (259 WARN, 2 ERROR, 5 FAIL). The scanner omits most passing checks;
  no test-pass total or blanket adjudication is inferred. Sequential jobs=1
  does not enforce its printed timeout, correctly disclosed.
- **The 19 hard errors exactly equal the prior corpus's 19 unique error
  pairs**, with no extras: Armenia 2, Nepal 11, Peru 6. Existing corpus review
  and country contracts supply their missing-source/unwired classification;
  they remain errors, not passes. This is unchanged failure scope, not proof
  that those countries have been repaired.
- **Five plot_features FAILs exactly equal KNOWN_UNHARMONIZED** in the existing
  test and ledger602 §7. Recomputed all three revisions' diagnostic-function,
  vocabulary-schema, known-defect and country-tree hashes. The country trees
  are identical since v0.14.0 except Cambodia's Makefile glob quoting. The
  recorded shared/private tenure-column equality is corroboration; I inspected
  its comparison script but did not reread cache frames or execute historical
  environments. Closed #602 added enforcement and expressly left mappings
  unresolved. Neither the scanner nor this review waives the five failures.
- **60 caught Feature exceptions = 52 source/unwired + 8 Region dependencies.**
  Independently matched the former to exact corpus error pairs and the latter
  to GhanaSPS's three food plus two household variants and Panama's three food
  variants. These countries are omitted from those returned variants. Existing
  independent Region review and live issue #1009 confirm the Panama scope;
  GhanaSPS remains under #140. Missing geography is distinct from available v.
- **Contract-B sums reconcile.** Retained assets Quantity equals 992,524
  exactly. Retained derived-food Expenditure differs from the Aggregate output
  by 1.90735e-6; Quantity equals it at the stored-float precision. Retained
  food_acquired totals minus Aggregate output still contain Ghana's known
  16,931.817543859648 Quantity / 20,272.567543859648 Expenditure deletion;
  residual floating subtraction is -2.73721e-6 / +0.000167017. These are
  raw-column audit checksums, not comparable mass or economic totals. Country
  selection explains between-call drift, not Ghana row loss or every other
  semantic flag. No source-meaning claim is inferred from matching totals.
- **Ghana #1008 remains material:** the previously completed independent pure
  replay establishes 13,472 null-key deletions separately from visit summation.
  Its exact non-null fsum equality and tiny all-terms rounding remainder are
  accurately reported. No historical input-count equivalence is claimed.

## Country contracts read before new country inspection

CONTENTS prose and TODO/WAITING/LOGBOOK searches were read for the five tenure
countries, plus Malawi/EthiopiaRHS before inspecting their saved checksum
contributions. Earlier GhanaLSS/GhanaSPS/Panama/Uganda/Nigeria and corpus-gap
reviews are reused. No LOGBOOK drawers were found in the new seven files.

- **Albania:** 2003/2004 are 2002-panel subsets; 2004 movers retain original PSU
  but current district. The plot helper's privatisation and title-document
  categories are distinct from canonical ownership/legal-system labels. Its
  assertion that the schema permits extensions does not make those extensions
  present in the actual declared vocabulary. CONTENTS does not resolve tenure.
- **Cambodia:** the LSMS+ is a one-quarter CSES EA subsample with different
  households. CONTENTS says both "Cambodia is not yet accessible via the
  =Country()= API" and later "Cambodia is integrated into the =Country()= API".
  The latter names plot_features; the completed corpus corroborates delivery.
  The plot YAML's "no mapping is needed" because labels are human-readable is
  also contradicted by the canonical-vocabulary failure. Readable acquisition
  categories are not automatically valid canonical mappings. Nutrition remains
  TODO/#560; no FCT conclusion follows from this audit.
- **Kosovo:** a separate Serb sampling frame and dwelling-type option grid are
  documented idiosyncrasies. The existing "first and fourth categories" quote
  conflicts with its own fourth category "Vacant", already recorded in
  CONTENTS. Plot TenureSystem instead carries rent/payment arrangements; its
  failure is separate from dwelling selection and survey eligibility.
- **Tajikistan:** plot codes restart across owned/rented-in/rented-out modules
  and require tenure tags; title evidence is asked only on owned plots. The
  documented 1999 oversample is intended for exclusion in national analysis.
  CONTENTS's "housing ... simply not yet wired" conflicts with current
  declared successful housing. Neither this mismatch nor old Area /100 evidence
  licenses guessing a TenureSystem mapping.
- **Timor-Leste:** the designed 2001–2007 panel is explicitly unreleased.
  CONTENTS calls plot_features "implementable 2007-08", while it is declared
  and delivered. Part owner remains ambiguous (5,099 of 6,308 plot values);
  normalizing that category needs source review.
- **Malawi:** raw versus boiled-vendor Groundnut has different Aggregate
  meanings by wave. IHS2 has label-derived factors, IHS3+ region-table factors;
  the older "lacks any unit-to-kg conversion factors entirely" caveat conflicts
  with the later documented 50/90kg label factors. This review does not settle
  those older notes or #116. Matching checksums does not harmonize semantics.
- **EthiopiaRHS:** 1989 food and roster household coverage differ, and 2004/2009
  have coarse household aggregates rather than item-level food. Its live
  "Status: WAITING on @ligon" wrong-wave *lvs5/*inc5 entry remains pending.
  Recorded real repeated acquisitions and judgment-call double-punch removal
  explain why additive conservation alone cannot decide all source truth.

TRIAGE's proposed tenure follow-up notes correctly preserve the missing
source-backed mapping decisions. No country documentation or schema was edited
by this reviewer.

## Preservation and publication

The copy manifest, preservation output and verifier code consistently account
for **1,519 files / 340,885,785 bytes**, initially equal digests with distinct
copy inodes, and unchanged original digests/file set/size/mtime/device/inode
after the audit. Only dvc-cache is symlinked. I inspected that proof and matched
its recorded hashes; I did not access original bytes to repeat it. The proof is
about original L2, not a claim that shared L1 could never populate new blobs.
Normal lazy authenticated access remained enabled; LSMS_SKIP_AUTH is not a
credential-free guarantee. Identity assertions and thread/process limits are
present in the launcher. No shared-cache promotion is implied.

All publishable TSVs reconstruct from the private records and contain only
public feature/country/check names, counts, exception types/categories, literal
configured kwargs and timing. REPORT/TRIAGE/history contain aggregate
measurements and vocabulary labels, with no survey keys or example rows.
MANIFEST checks all 19 payload files exactly; raw records/logs/clusters/private
summary are absent. Their hashes, not contents, are published. The sole minor
correction requested during review was renaming provenance's misleading
read_only_replay_generators key: the separate Ghana API replay may write its
private cache. It is now replay_and_verification_generators, accurately scoped.

Final provenance SHA256:
`f2d9e145c563084a9779261fe8c08757c283dc3735d9a4be129872c57da95e25`.
Any later historical-snapshot comparison is additional evidence outside this
approval's exact hashes. Uganda's held scientific baseline, credentialed CI
investigation and release decision remain open.
