# Prior-Art Ledger - #1121 Mali plot-labor source casing

**Status:** approved implementation boundary, 2026-10-21. Dispatcher authorized this focused f8-based checkout after reading the diagnosis.
**Search tier:** ripgrep + git floor; no index refresh.
**Examined heads:** baseline `f8d236694c26fe9e709c5548f02c70f74ea72c33`; persistent Mali `aba2badfe2f701a8b3793f1243fd8ed4fce18a53`.
**Tracked path:** `.coder/ledger/1121-mali-labor-source.md`; committed separately before code.

## §1 Task, restated

Correct only Mali's `plot_labor.source` Preferred Labels from `Family/Hired/Other` to `family/hired/other`, per live #1121's Tier A scope. Preserve each reported labor row and every other key, value, unit, attribute, source eligibility, demographic contribution, passage, reducer and role. Keep GhanaSPS/GhanaLSS category-design questions under Tier C.

## §2 Existing machinery

| symbol | path:line | purpose / evidence | reuse |
|---|---|---|---|
| `harmonize_labor_source` | `Mali/_/categorical_mapping.org:629` | synthetic Code -> Preferred Label; three capitalized output labels cause the defect | change only three Preferred Label cells |
| `_labor_source_labels` | `Mali/_/mali.py:625` | string trim, exact Code mapping through existing categorical resolver; preserves missing/unmapped policy | unchanged |
| `plot_labor_finalize` | `Mali/_/mali.py:635` | clears sentinels, drops content-free splits, sums PersonDays/Wage with min_count=1 at native key | unchanged; private invariant controls pass |
| wave drivers | `Mali/2014-15/_/plot_labor.py:1`, `Mali/2017-18/_/plot_labor.py:1` | four EAC-I PP/PH sources; person-count times days-each across men/women/children | unchanged; actual cold builds and metadata checked |
| country concatenator | `Mali/_/plot_labor.py:1` | joins two contributing EAC-I waves, leaves v/id remapping to framework | unchanged |
| categorical resolution | `local_tools.py:get_categorical_mapping:2417` | first matching table, whole-table local override, no global labor table needed | reuse; `tests/test_categorical_mapping_resolution.py` |
| lowercase selections | `transformations.py:5007`, `:5061`, `:5071` | total-family/total-hired helpers select exact `family`/`hired`; current Mali raises KeyError | reuse; new targeted regression warranted |
| canonical index | `data_info.yml:108-121`, `:280-283` | preserves stage/worker/season using existing sentinels; no value harmonizer is implied by this index contract | unchanged |
| standing I/O/cache/finalizer | `STANDING.md §2-§4` | sanctioned readers/writer, current-hash gates, pre-finalize caches, id_converted attrs | reuse |

No existing Mali-specific labor tests were found. GhanaSPS vocabulary tests explicitly preserve its seven categories (`tests/test_ghanasps_plot_labor.py:136`); they are evidence against broad category replacement.

## §3 Definitions and conventions

Inherit `STANDING.md §3/§4`, `CLAUDE.md`, and `lsms_library/data_info.yml`.
Live #1121 explicitly classifies Mali casing as Tier A and cross-country vocabulary design as Tier C.
The `source` index level names a fixed questionnaire block, not a decoded raw category column (`categorical_mapping.org:620-628`).
Family includes men, women and children; hired means non-family labor carrying reported remuneration; other is the mutual-aid/free/exchange block.
Source labels are shared lower-case in sibling ISA code and exact lowercase selectors in transformations.
No global schema enum currently adjudicates GhanaSPS casual/permanent/communal/self or GhanaLSS exchange categories.

Mali's native index is `(t,i,plot_id,source)` before API-time v join; Feature uses the registered expanded index, preserving all rows.
PersonDays and Wage are reported item-level quantities, not the WB collapsed totals or imputed labor valuation.

## §4 Invariants and assumptions

- Only EAC-I 2014-15 and 2017-18 currently contribute Mali labor; the EHCVM waves are unwired for this table. Do not infer EAC-I design from the generic EHCVM overview.
- Preserve both PP/PH sources and all demographic splits, component sentinel clearing, positive-content keep rule, exact Code eligibility, nullable amounts and existing min_count=1 reductions.
- Preserve the 2017 PH child hired-days choice `s7eq13c1*s7eq13c2`; `s7eq13c3` is remuneration, verified from source metadata. Do not copy the historical .do wage-column slip.
- Preserve source-specific meaning: do not merge other with hired or family, and do not harmonize unrelated Ghana categories.
- Retain v join, panel/id attrs, native/Feature grain, population/recall attrs and currency behavior unchanged.
- Existing complete actual cold/fresh-process-warm f8 vs aba2 frames are exact: Mali 52,057; healthy Niger 53,041; Feature 105,098.
- Mapping is one-to-one: no lower-case collision or row merge is needed. Baseline source counts are Family 33,655 / Hired 9,565 / Other 8,837.
- The country org file is a whole-country build input. Virtual three-cell hashing moves all 24 Mali hashes, not merely plot_labor; 525 other pairs and 39 tagged fingerprints stay unchanged. Hash-only results make no output claims for those other Mali tables.
- #822 is independent: relevant drivers/table/concatenator and helper function bodies are unchanged from f8; actual complete labor outputs match exactly.

## §5 Reuse decision

| behavior | decision | reason |
|---|---|---|
| Code -> labor label | reuse existing curation table | exactly three Preferred Label cells settle casing |
| labels resolver / finalizer / sources | reuse unchanged | current behavior already retains the intended reported rows |
| lower-case selectors | reuse unchanged | existing interface expects exact lowercase |
| source grouping / Feature alignment | reuse unchanged | registered levels already preserve grain; value case is the defect |
| global harmonize_labor_source design | defer | Tier C needs a separate human decision; no Ghana change |

## §6 Open questions / approved review boundary

None. The dispatcher approved the three Preferred Label edits, narrow schema-comment correction, country idiosyncrasy note and focused tests on this fresh f8-based checkout. The #822 branch and its code remain independent.
No missing source or acquisition blocks this boundary.
The broader category design remains open under #1121 after the Mali case repair.

### Diagnosis controls

Three desired lowercase binding tests fail at both baseline and current head; three existing finalizer invariants pass at both.
Actual Country lower-case family/hired selection raises KeyError today; a pure index-case projection permits the existing selectors and leaves total labor days exact.
These are pre-implementation controls. Verification below will compare complete source builds to this saved baseline, with exact PersonDays/Wage values and attributes.

Sue

### Phase 3 - verification

- Curation boundary - **OK (§2/§3)**: exactly three Preferred Label cells change; exact Code eligibility remains. No helper, driver or framework edits.
- Source frames - **OK (§4)**: both native wave frames and complete public cold/fresh-process warm frames equal the saved f8 baseline after only the source-case projection. All 52,057 rows and Country attrs are exact.
- Amounts and selectors - **OK (§4)**: PersonDays 11,544,571.100000001, Wage 214,685,819 FCFA; complete total-labor output unchanged. Family/hired outputs match baseline source selections, totaling 7,867,691.5 / 1,197,091.3 person-days.
- Feature - **OK (§3/§4)**: 105,098 rows with fresh Niger 53,041-row comparator; full case-only frame and every attr exact. No row/grain/category merge.
- Regressions - **OK (§2/§4)**: 335 focused tests pass, no skips; new 10-test file is 7 fail / 3 pass at baseline. Real table/resolver/finalizer and existing selectors exercised. API audit clean.
- Scanner - **OK (§4)**: complete phases 1/2 finding multisets: eight pre-existing warnings exact after path/timing normalization, zero errors/failures.
- Hash scope - **OK (§4)**: actual census of 549 pairs moves only 24 Mali hashes; 525 other pairs and all 39 tagged fingerprints remain unchanged. No output claim is made for other unbuilt Mali features.
- Wider taxonomy - **DEFERRED (§5)**: GhanaSPS/GhanaLSS category design remains Tier C under #1121.

Detailed private verification is under `/local/job40255479/control/isa-backlog-20261021/Mali/1121`; source-safe aggregates and exact commands are in the task report.
