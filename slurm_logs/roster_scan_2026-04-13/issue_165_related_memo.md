# Issue #165 — related-issue scan

## #165 itself (1 paragraph)

Issue #165, "Wider adoption of age_handler() for robust Age calculation across countries," proposes extending the existing `age_handler()` function in `lsms_library/local_tools.py` (line 1298) — currently used only by Niger's country script and Senegal's two wave scripts — to all other countries whose `household_roster` `Age:` mappings are direct column references.  The motivating case is Togo (#164), where Age is 94.5% missing because the wave config points at a column that is largely unpopulated; `age_handler()` could recover Age from a DOB or year-of-birth column instead.  The issue is labeled quality (not blocker) and cross-references #163, #164, #161, #162 as siblings from the same April 13 2026 audit pass.

---

## Search strategy used

- Fetched #165 body/comments + full JSON metadata; extracted all cross-references explicitly named in the body (#161, #162, #163, #164).
- Searched all open + closed issues for the label set on #165 (no labels) plus text-similarity searches on key terms: `age_handler`, `Age household_roster`, `DOB birth year Age`, `negative age validation range`, `data quality audit`, `household_roster coverage`.
- Queried which issues mention "#165" in body or comments via `gh api` (found: #173, #171, #167).
- Scanned recent-neighbor issues created within ±14 days of 2026-04-13 (issues #107–#173).
- Grepped `SkunkWorks/audits/household_roster.md`, `CLAUDE.md`, and recent commit messages for `age_handler`, `DOB`, `#165`.

---

## Related issues

| # | Title | State | Relation | Confidence | 1-sentence thrust |
|---|-------|-------|----------|------------|-------------------|
| 164 | Togo household_roster: Age 94.5% missing (1,514 / 27,480) | OPEN | prerequisite / motivating case | high | The concrete per-country symptom that #165 proposes to fix via age_handler adoption; must be resolved as part of #165 work. |
| 173 | household_roster: cross-country Age data quality (negatives, non-numeric, object dtype) | OPEN | tightly-coupled follow-up / partially addressed | high | Broader Age quality findings (504 negatives, 1,086 non-numeric rows, object dtype) that #165's age_handler adoption would directly help fix; #173 itself explicitly calls #165 as the solution vehicle. |
| 172 | household_characteristics: silent coverage gaps (Armenia, Guyana, 5 declared-but-never-built) | OPEN | follow-up / downstream consequence | medium | Age quality bugs from #165 cascade into household_characteristics derivation (age bucketing in transformations.py), but #172's primary concern is silent skipping of countries — separate fix path. |
| 171 | individual_education: cross-country harmonization + Uganda 56% nulls | OPEN | orthogonal-but-same-area | medium | Mentions #165 as a vocabulary-helper analogue but is not about Age; Uganda null rate problem parallels #164 in pattern but is about educational attainment, not age. |
| 163 | Feature('sample') oddities: int_t leak, Rural spellings, China/Azerbaijan weight quality | OPEN | orthogonal-but-same-area | high | Named sibling in same April 13 audit pass; entirely different feature (sample) and issue class. |
| 162 | Cross-country: stray `i` column in cluster_features (8 countries) | OPEN | orthogonal-but-same-area | high | Named sibling; about cluster grain and stray household IDs — no Age content. |
| 161 | Uganda cluster_features: wrong grain, stringified-float District, missing GPS wiring | OPEN | orthogonal-but-same-area | high | Named sibling; Uganda-specific cluster_features issues — no Age content. |
| 129 | household_roster: kinship decomposition not producing expected columns | CLOSED | false-positive | low | Closed; about Kroeber decomposition columns, not Age. Surfaced by keyword search. |
| 49 | Automatically look for spelling changes for all columns? | CLOSED | false-positive | low | Closed ancestor of the canonical-spellings machinery; not age-related. |

---

## Must be addressed inside the #165 plan

- **#164 (Togo Age 94.5% missing)** — This is explicitly named as the motivating case. The #165 plan must investigate Togo's waves for available DOB/YOB columns and apply age_handler. Closing #164 is effectively the first acceptance criterion for #165.
- **#173 (cross-country Age data quality: negatives, non-numeric, object dtype)** — #173 explicitly states "Aligns with #165" and calls #165 the "solution vehicle." The plan should treat the scope of #165 as covering both Togo (#164) and the broader negative/non-numeric inventory (#173). Importantly, #173 reveals that Age dtype is `object` across the Feature output — this may require a framework-level change (dtype coercion in `_enforce_declared_dtypes` or `_finalize_result`) on top of per-country age_handler adoption. Without #173 being in scope, the Age column would still be object dtype even after per-country fixes.

---

## Deferred / noted

- **#172 (household_characteristics: silent coverage gaps)** — Age quality problems from #165 cascade into the age-bucketing step in `transformations.py::roster_to_characteristics`, so fixing #165 will partially improve #172 (more non-null Ages → fewer dropped rows). However, #172's core concern is silent country skipping (Armenia, Guyana, etc.) which is a separate fix. Note the connection but do not bundle.
- **#171 (individual_education cross-country harmonization)** — Same audit batch; named #165 as a structural analogue. The pattern (per-country vocabulary helper) overlaps conceptually, but the implementation domains don't intersect. Note for the harmonization milestone.

---

## Noise

- **#163, #162, #161** — Named siblings in #165's own body but explicitly scoped as "not directly related." All three are pure cluster-features / sample issues. No Age content whatsoever. Listed here to confirm they were checked.
- **#129** (closed: kinship decomposition) — Surfaced by keyword; household_roster but about Kroeber columns, not Age.
- **#49** (closed: automatic spelling changes) — Early ancestor of current canonical-spellings machinery; age-unrelated.
- **#167** (Implement plot_features) — Came up in "#165 mentioned-by" search because its body cross-references #165 only as a sibling from the same audit batch; no Age content.

---

## Recommendations for the #165 planner

1. **Treat #164 and #173 as sub-tasks, not separate issues.** #164 is the country-level test case; #173 provides the cross-country inventory of broken rows. The plan should close both as part of #165's acceptance criteria.

2. **Scope includes a framework layer, not just per-country YAML changes.** #173 shows Age is `object` dtype across the Feature output. `age_handler` fixes the population problem (missing values) but the dtype problem (string "99", negative integers, non-numeric strings) likely requires a change to `_enforce_declared_dtypes` or an explicit coercion step in `_finalize_result` for `type: int` columns. Per-country fixes alone won't resolve the dtype-level findings.

3. **Survey which countries already use age_handler and which have DOB/YOB columns available before coding.** Current users: Niger (`_/niger.py`) and Senegal (two wave scripts). Many countries may lack DOB — for those, the fix is correcting the wrong source column name or adding a sentinel-value mapping, not adding age_handler. Don't over-engineer.

4. **Start with Togo as the proof-of-concept wave.** It has 3 waves, clear 94.5% gap, and is the named motivating case. A per-wave probe of Togo source `.dta` files will immediately reveal whether DOB columns exist.

5. **Do not conflate with #172 (household_characteristics coverage).** That issue involves countries where roster doesn't load at all (Armenia, Guyana). Age improvements from #165 won't help those countries until their roster loading is fixed independently.
