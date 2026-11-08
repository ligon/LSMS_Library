# Prior-Art Ledger — GH1576 Mali Taoudenit spelling

**Search tier used:** ripgrep + git floor. Base c780562b299bb100cdd006037ba424c8ad3264e9.

## §1 Task, restated
Implement the specified Tier A cosmetic Region fold `Taoudenit` → `Taoudénit`, and replace the reverse-direction exceptional strata pair with the ordinary Region × {Urbain,Rural} cross-product. Remove the test's EXTRA_ROWS exception. No source acquisition, geography/design inference, weights, crop or food policy changes. Root owns the actual 14-cluster/source/warm acceptance gate.

## §2 Existing machinery
| symbol | path | behavior / test | decision |
|---|---|---|---|
| Region / strata org tables | `lsms_library/countries/Mali/_/categorical_mapping.org:5` | curated preferred spelling; `tests/test_gh812_ehcvm_strata.py::TestStrataTable` | extend rows only |
| `Country._apply_categorical_mappings` | `lsms_library/country.py:3723` | name-dispatched mapping of columns/index; preserves attrs; categorical resolution/key tests | reuse |
| `Country._finalize_result` | `lsms_library/country.py:4091` | applies mappings after native cache read | reuse; no code edit |
| `derive_region_milieu_stratum` | `lsms_library/ehcvm.py` | combines own recorded label and milieu; GH812 TestLabel | reuse |
| `Wave._input_hash` / `Country._table_cache_hash` | `lsms_library/country.py:1353,4499` | declared org inputs participate even when mapping executes post-read | reuse, metadata census only |

## §3 Definitions & conventions in force
- Inherits `STANDING.md` §§3–4 and `CLAUDE.md` automatic categorical mappings / sampling identity. Native cache values stay native; public categorical spelling follows country curation.
- Mali CONTENTS lines3129–3146: EHCVM `sample.strata` is region × milieu label; 2014 empty and2017 design codes remain distinct.
- CONTENTS lines3160–3164: “The 2018-19 cover writes =Taoudénit=” while consumption “writes =Taoudenit=” and “the better end is a =Region= row and dropping this pair”. GH1576 repeats this Tier A specification; live issue unchanged/no comments.
- Full prior3477-line Mali read authenticated SHA4a1351bcf49caa7a754725b8393b32b86fbba7d1dceba5f734281741d2ee01d7; all current changed text read, current SHAac024fba2e14eada16f25ce9f29844adc24fc85e719963f83971e487800707e6. Receipts in external report.

## §4 Invariants & assumptions
- Only exact declared spelling keys fold: no fuzzy accents/case, region merging, new geography or treatment of missing values. Already-accented names, unrelated categories, units and design codes pass unchanged.
- Region × milieu remains exact row-for-row. Remove OLD accented→unaccented pair, add unaccented→accented pair corresponding to new Region row; no exceptional test allowance remains.
- Keys/rows/order/native nonlabel amounts/nulls/attrs remain unchanged under mapping, and source files/YAML/wave modules/framework remain byte-identical.
- No raw/source/shared cache reads in author controls. Claimed14cluster effect remains a prior issue/document claim until root's own-source gate confirms.

## §5 Reuse decision
| quantity | decision | reason |
|---|---|---|
| canonical Region and strata labels | extend existing tables | exact cosmetic fold specified in1576 |
| application/parser | reuse | existing Country mapping path already performs exact replacement |
| native amount/design semantics | reuse unchanged | no estimator or scientific definition changes |
| verification | new hermetic controls plus existing GH812 | actual Country mapping and finalization with invented frames; parent negative controls |

## §6 Open questions for the human
None for authorized cosmetic implementation. Root-managed exact source14cluster/public API/warm evidence and independent review remain gates; no shared rewarm is authorized here.

---
### Phase 3 — verification
Pending implementation and bounded hermetic verification; source acceptance unrun by author.
