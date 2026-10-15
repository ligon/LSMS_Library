SCOPE DEVIATIONS: None. Read-only Git, saved scan records and saved corpus
metadata only; no Country/Feature API calls, data rebuilds or cache writes.
All independently reviewed scan results and FAIL/ERROR statuses remain
unchanged. Review also corrected both archived generators' evidence-root
resolution; generator/provenance/manifest metadata changed, not outcomes.

No comparable complete prior Feature snapshot was found. The older records
support limited historical corroboration, not a matched release regression
comparison. After checking the unmatched warning categories against the
reviewed changes, emitter history and cold-corpus value/index attribution,
**I identified no remaining warning category with an unexplained release-specific
change.** This is a bounded negative finding, not proof that every diagnostic
is historical or that all country data are scientifically correct. The lack
of a comparable full baseline limits any stronger zero-regression claim.

## Prior evidence and comparison limit

Searches covered tracked files in the frozen worktree and v0.14.0 tree, the
main node-local checkout including ignored bench files, the local release
control artifacts, and the original Lustre mirror's ignored
bench/feature_audit/results. The 2026-06-19 handoff led to the latter.

| Saved records | Features represented | WARN | FAIL | Scope skips |
|---|---:|---:|---:|---:|
| 2026-06-18 results.jsonl | 13 | 562 | 3 | 13 |
| 2026-06-19 scan_run1.jsonl | 8 | 1,722 | 0 | 10 |
| 2026-06-19 scan_perfeature.jsonl | 13 | 16,774 | 0 | 13 |
| 2026-06-20 food results.jsonl | 1 | 696 | 0 | 1 |

The June20 log requests four features, but only food_acquired is represented
in the saved records. Its handoff explicitly says "derived features not
finished -- scan was stopped in favor of the warm-cache probe". It must not
be counted as a completed four-feature run. The April13 scan is an even
earlier, different harness with111 probes and no declared plot_features;
it is unsuitable for this comparison.

The June scans exclude known no-microdata countries, precede later country
registrations/diagnostics, and lack the current source SHA plus physical
cold-cache manifest. Their data roots and build histories are not matched to
this run. The large ResourceWarning traffic in the June19 per-feature snapshot
also makes raw warning-count subtraction misleading. No v0.14 or immediately
pre-train full39-feature/all549-pair snapshot with equivalent provenance was
found. The October review verdicts contain useful targeted comparisons, not
a full matched snapshot; some referenced raw files were on now-absent jobs.

Using the frozen cluster.py normalizer and the SAME union of country names
for old and current records, **26 of259 current warning patterns** match at
least one old snapshot. Those patterns cover487 current records. The other
**233 patterns are unmatched, not "233 new defects"**. Conversely, matching
a normalized pattern does not establish that its present quantities or
country occurrences are unchanged. Patterns/examples stay private; only
counts and artifact hashes are in comparison.json.

## Release-delta checks

The full feature.py blob, quantity_audit.py, null_read_audit.py and population.py
are identical at v0.14.0, pre-train development and frozen source. The source
segments for Country._join_v_from_sample, _finalize_result,
_enforce_canonical_spellings, _normalize_dataframe_index, _audit_index_collapse,
roster_to_characteristics and the two derived food quantity/price entry
functions also match at all three refs. Exact hashes are recorded. This proves
emitter/entrypoint continuity, not unchanged transitive behavior or inputs;
the deliberate changed callees are addressed below.

- **New diagnostics are accounted for.** Four scale records are the exact
  three country/table cases independently enumerated in the #830 review:
  GhanaLSS food_acquired/community_prices and GhanaSPS crop_production (two
  waves). The detector is a new warning-only observation; it changes no served
  data. GhanaSPS's eight-year monetary-scale interpretation remains
  unadjudicated, with different source instruments already documented. Seven
  unrenamed-label warnings are the intended #787 visibility of incomplete
  crosswalks; the numeric-target rejection and aggregate-country selection
  were independently tested. They are not evidence of newly changed quantities.
- **Grain warning replay changed deliberately under #868.** Sixty-six records
  now report cells blanked by the existing reducer. The original independent
  verdict and final runtime tests establish cold/warm replay and preservation
  of values. The twelve Uganda sold-condition conflict groups incorporate the
  already-reviewed second-condition addition; they are not silently relabeled
  as the old eleven. These reports remain substantive source/grain caveats.
- **Food price warning denominators changed deliberately under #864.** The
  money-only recall bucket and Burkina's source-backed zero-to-NA correction
  alter warning classification; the tested quantity-support floor and separate
  source evidence are required. Remaining price-loss and missing-conversion
  warnings are not dismissed. Panama's amountless-row removal, Nigeria's sold
  unit and W5 rows, Niger's rekeys/sentinels, Uganda's condition slot and Ghana's
  label-encoding repair all have separate reviewed source/value comparisons
  in the completed corpus findings.
- **Feature shape exclusions were examined explicitly.** Eight warning
  records exclude incompatible country frames from assets, community_prices,
  crop_production, livestock, people_last7days, plot_inputs or plot_labor. The
  selection implementation is unchanged. Across all declaring countries for
  these seven features,100 old-shared/cold-private country parquet pairs have
  exactly equal index-name layouts; none changed. Seven newly cached pairs
  lack an old shared country parquet (Malawi assets/people_last7days/plot_inputs/
  plot_labor; Uganda assets/livestock/plot_labor), and Nepal assets has neither
  because its source is unavailable. Those eight gaps are explicit; cache
  metadata alone cannot prove their historical served frames. The unchanged
  assembly implementation and country-source/targeted-test evidence support
  continuity, but the exclusions remain real interface coverage limitations.
- **Other warning families have no identified unexplained train-specific
  change.** They comprise quantity screens, null-column/index guards,
  population comparability, scheme/index mismatches, constant columns,
  residence/mover handling, missing wave dependencies, empty reported-price
  modes and source fallback notices. The cold-corpus comparison attributed
  all changed cached outputs rather than assuming the shared cache true.
  New-wave/table coverage and cold rebuilds can expose existing problems that
  a warm or smaller old scan never observed. comparison.json preserves every
  category's record count; categorization is not scientific adjudication.

The strongest apparent loss signals were independently investigated rather
than inferred from warning similarity: GhanaLSS13,472 null-key deletions are
an unchanged pre-existing assembly mechanism (#1008); Aggregate totals track
the explicit retained-country sets; Region dependency exclusions match
unchanged schemes/helpers (#140/#1009); the five tenure FAILs match unchanged
checker/schema/known-defect declarations and exact old/new tenure columns.
The19 errors exactly match the declared-corpus source/unwired gaps.

No warning is cleared merely because the scanner says expected=True, because
its pattern existed in June, or because its emitter is unchanged. The
remaining scientific issues, unavailable historical cache pairs, full-baseline
gap and Uganda nutrition decision remain explicit limits on release evidence.

## Reproduction and privacy

compare_prior.py performs the read-only comparison. Invoke the archived copy
with `--evidence-root /local/job39919092/release-work/control/runtime/feature-audit`
(also its default); the script resolves private inputs independently of its
own archival location. It requires the named private/current/historical inputs
to remain accessible and writes only the comparison aggregates under that
evidence root. It cannot reconstruct raw inputs from this public aggregate
package alone. comparison.json contains
prior raw-file sizes/hashes, scope/status counts, normalized-pattern overlap,
aggregate warning categories, source hashes and per-country/table index-layout
comparison dispositions. It contains no warning examples, survey keys or raw
rows. Raw historical/current scans stay private. This comparison does not
edit any current diagnostic record or counted outcome.
