# Prior-Art Ledger — GH #1575 Benin1575-StrataWiring

**Search tier used:** ripgrep + git floor; GitNexus tools unavailable.
**Source pin:** 9f89151ddd783a07fa57b9277da3677f7024ce7c.

## §1 Task, restated
Wire only Benin 2018-19 sample.strata from its own cover s00q01 department
label and s00q04 residence label using the existing GH #812 construction.
The 2026-11-10 approved-four-country CHARTER supersedes the design packet's
historical NOT SELECTED language. Preserve all other extraction and identity.

## §2 Existing machinery
| symbol | path:line | purpose / coverage | decision |
|---|---|---|---|
| strata_label / derive_region_milieu_stratum | lsms_library/ehcvm.py:820 | strip and concatenate labels; NA for missing/blank; dedicated StratumError for nonstring codes; test_gh812_ehcvm_strata.py::TestLabel | reuse unchanged |
| stamp_region_milieu_stratum | lsms_library/ehcvm.py:854 | copies frame, nullable string key only on nonmissing strata; TestStamp | reuse unchanged |
| inputs_region_milieu_stratum | lsms_library/ehcvm.py:866 | derive own cover paths and household extraction from YAML; original names at (t,i) | reuse unchanged |
| Wave.column_mapping / df_data_grabber | lsms_library/country.py:1605 / local_tools.py:2613 | actual YAML extraction and formatters | reuse with invented providers only |
| derivation_records / call_inputs | lsms_library/derivations.py | registered scope, callable resolution, typed metadata | reuse |
| sample cover and weights | countries/Benin/2018-19/_/data_info.yml:4 | s00_me_ben2018.dta; ehcvm_ponderations_ben2018.dta; merge_on v; final_index i,t | preserve except strata |

## §3 Definitions & conventions in force
Inherit STANDING.md §§3–4, CLAUDE.md sections Derived Values and sample() and
Cluster Identity, and lsms_library/data_info.yml sample optional Derivation.
Benin/_/CONTENTS.org Sampling Design: "each grappe is visited in exactly one
vague" and household identifier "(grappe, menage)". Its own DDI states
"23 explicit sample strata" and calls Littoral "a purely urban region".
The dated GH #1575 note quotes the own second-edition BID section 4.1,
French PDF p13 (English p14): "La strate est la combinaison département/milieu
de résidence." This is survey design documentation, not a questionnaire item.

## §4 Invariants & assumptions
- Preserve the entire existing CONTENTS prefix, including the DDI "randomly
  divided each enumeration area into two equal groups" versus one-vague-per-
  grappe tension and the historical department-only inference advice.
- Preserve every nonstrata YAML node, existing hook body, join, row/order/key,
  cluster v, Rural, raw hhweight in both weight columns, and attrs contract.
- New thin wrappers only; shared helpers and their existing stamp semantics
  remain exact. The stamp owns Derivation on sample rows; it does not union
  an unrelated preexisting Derivation key. No new rowkey semantics.
- Missing or blank either component yields NA; no old-region fill, numeric
  coercion, majority choice, crosswalk, pruning or observed-count golden.
- Registry scope only 2018-19. Explicit unsupported input-wave calls retain
  inherited missing-spec errors; metadata scope is not a new execution fence.
- No native/runtime/data reads for acceptance here. Pure invented fixtures may
  call actual extraction helpers with provider fences. No Country/Feature
  normal data API calls, shared cache work, other waves, or source publication.

## §5 Reuse decision
| quantity | decision | reason |
|---|---|---|
| department × milieu label | reuse | existing tested GH #812 helper exactly implements approval |
| raw input retrieval and sample provenance | reuse | shared input/stamp paths with country-bound thin hooks |
| native categorical alias | none | preserve stripped native labels; no new Benin table |
| focused Benin test module | new | own actual YAML/thin hooks with invented frames and no data providers |

## §6 Open questions for the human
No scientific selection remains pending. Own2018 SEALED native label card not
found in the 1575 documentary/design packets or review-wire-benin (that card is
2021-22 only); request routine manager/deputy metadata admission if still absent.
Native missingness/cluster consistency, actual Country/Feature cold/warm,
independent review, final footprint and full gates remain separate acceptance.

---
### Phase 3 — verification
- `sample.df_cover.myvars.strata` — OK (§§1–5): only the approved label pair
  replaces the original scalar; all other declaration nodes are exact.
- `inputs_strata`, `sample`, imported `strata_label` — OK (§2/§5): thin
  delegation to unchanged shared helpers; no duplicate computation.
- Own registry key — OK (§3/§4): only 2018-19, original raw names, existing
  optional nullable carrier. Actual-row validation remains explicitly UNRUN.
- CONTENTS appendix — OK (§3/§4): complete prefix retained, original DDI/BID
  statements and cluster-split tension preserved without a crosswalk/count rule.
- Focused pure run at code/test freeze 4d672ece4: 19 own tests plus 14 existing
  shared label/stamp controls passed, -n2 --dist=loadfile, network namespace,
  absent credentials and private empty data root. Controller/gw0/gw1 package,
  config, interpreter, UID and namespace identities recorded. API source audit
  passed using the prepared interpreter (initial Make lacked Poetry).
- Preserved failures: initial system-Python metadata serialization, empty-dir
  preflight, pure01 fixture alias errors, pure02 bound-method/import errors,
  pure03 by-path callable object-identity assertion. All corrected without
  changing production code after its first freeze; logs/XML retained outside
  source. By-path resolution is checked by exact callable spec and code identity.
- No CONTRADICTION or REINVENTION against §§2–5. Native cover cards/rows,
  normal Country/Feature cold/warm validation, final footprint and independent
  integration/full gates are UNRUN here and remain acceptance residuals.
