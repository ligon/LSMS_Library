# Prior-Art Ledger — GH1576 Mali Taoudenit spelling

**Search tier used:** ripgrep + git floor. Base c780562b299bb100cdd006037ba424c8ad3264e9.

## §1 Task, restated
Implement the specified Tier A cosmetic Region fold `Taoudenit` → `Taoudénit`, and replace the reverse-direction exceptional strata pair with the ordinary Region × {Urbain,Rural} cross-product. Remove the test's EXTRA_ROWS exception. No source acquisition, geography/design inference, weights, crop or food policy changes. The coordinator granted the bounded four-source2018-19 cold/candidate-warm stage after the product freeze; the exact scope and remaining baseline-warm/CI limits are recorded below.

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
- Hermetic controls use no survey sources. The later explicit coordinator grant permits ONLY the four held2018-19 Mali cover/consumption/GPS/weight sources via sanctioned getters and physical privateL1 copies; no acquisition/shared writes. No baseline cold cohort may be replayed after completion. Candidate/API14cluster effect is now measured; baseline warm remains unverified.

## §5 Reuse decision
| quantity | decision | reason |
|---|---|---|
| canonical Region and strata labels | extend existing tables | exact cosmetic fold specified in1576 |
| application/parser | reuse | existing Country mapping path already performs exact replacement |
| native amount/design semantics | reuse unchanged | no estimator or scientific definition changes |
| verification | new hermetic controls plus existing GH812 | actual Country mapping and finalization with invented frames; parent negative controls |

## §6 Open questions for the human
No unresolved scientific choice for this specified cosmetic fold. Baseline warm is UNVERIFIED and has not been silently promoted to passed. Full offline CI and final-head configured GitHub CI remain required coordinator gates before merge; no shared rewarm is authorized.

---
### Phase 3 — verification
- **OK (anchored on §§2–5), exact product4f5e50672b03dc9557e7200652cdef7df604f2d0, basec780562b299bb100cdd006037ba424c8ad3264e9.** Existing Region/strata tables are extended; framework/producers/source declarations/YAML/other category tables are unchanged. The strict Region×milieu equality assertion remains, without `EXTRA_ROWS`. No reinvention or scientific-policy change.
- **OK (§§3–4), hermetic170passed/1inherited optional-environment skip.** Fourteen new actual mapping/index/finalizer controls are INCLUDED in170. Untouchedparent negative proof6expectedfail/8pass. Exact package/config/private-data identity, false credential gate, unshare, BLAS/build1 andpytest2/loadfile; `-m 'not requires_s3'` excludes source-marked tests. Strict API audit0. Retained initial operator/fixture failures are disclosed externally, not counted as product defects.
- **OK (§§3–4), independent review180passed/1inherited optional-environment skip**, including13new external controls plus author14 and existing support; these overlap author counts and are not350distinct controls. Exact4f5 review verdict has no introducedP1/P2. Independent sealed-own-source arithmetic authenticates12artifacts and confirms14clusters/168all-Rural cover rows. Reviewer source arithmetic did not reopenDTA or substitute for publicAPI checks. See external `review-1576-mali-taoudenit-20261108/VERDICT.md`.
- **OK (§§2,4), independent full metadata enumeration:**38registeredcountries;450declaredcountrytable/1895wave/35build/186import fingerprints.20country+80wave changes, ALLMali;0build/0import changes;18None unchanged;0errors/denials. Public/runtime553/2313/39 surface is separate from persisted scope. Whole-country org dependency means conservative hash invalidation; it does not mean80served payloads change. Original audited1510operator reused unchanged.
- **OK (§§3–4), bounded actual source/API equality.** Basec780 cold cohort job41017859 is closed and was NEVER replayed. Candidate4f5 cold+fresh-process warm continuation41017883 COMPLETED0:0, free4CPU24G, cold/private roots/network-isolated. Four admittedDTA MD5/size/sidecar bindings and physicalprivateL1 copies authenticate inputs. Publicsample6602rows changes ONLY168strata values `Taoudenit Rural`→`Taoudénit Rural`; publiccluster_features551rows changes ONLY14Region values `Taoudenit`→`Taoudénit`. Keys/order/dtypes/NA/allotherpayload/typedattrs exact. Full-key closed analysis confirms6602cover/consumption household keys and within-key geography agreement after committed curation.168is the whole2018cover cohort; historical155is a different cross-wave common-household cohort.
- **OK (§4), candidate warm only.** Raw getter/projection/build/writer tripwires allow ordinaryprivateL2parquet reads. Both frames/attrs exactcold; all4cachebyte/contenthash/coverage manifests unchanged. PhysicalL2snapshot savedBEFOREwarm and rehashed against independent4f5census expected hashes; countrycoverage2018-19. Reader copies600 removed beforewarm/finally; allcleanup receipts pass. These actualDTAgetter calls materializeframes/fullStata parsing; no metadata-onlyphysical-read claim. SealedPKLanalysis adds0getter/Stataparse calls.
- **UNVERIFIED (§4), baseline warm.** Its earlier operator falsely refused the legitimate privateL2get_dataframe read, and the allocationcache was reaped. No product rawread/build failure is inferred, no baseline source replay was done, and this gate is not labelled passed. Allwaves/actualMake/Feature/fullCI were not part of this bounded source stage. Coordinator accepts the specified source spelling scope and retains final CI gates.
- **Operator evidence retained (§4):**41017807 pre-source namespace collision;41017813 privateDVCtmp/btime creation false rejection;41017839 absentconfigsourceprobe rejected beforeENOENT;41017859 baselinecoldPASS then warmparquet tripwire false rejection. New versioned operators preserve these failures. Eight final invented ACTUAL sanctioned-loader controls exercise MD5-privateL1DTA, absentprobe, realDVCbtime creation, privateparquetwarm and unexpectedraw/sharedwrite/warmraw/writer refusals. Earlier predicate controls overlap and are not extra8actualcaller controls. Native-label oracle initially missed existingSegou/Ségou curation; corrected ONLY closed comparison normalizes both sides with committedRegiontable. Publicbase→candidate deltaoracle remained exactnewaccent-only.
- **Final evidence:** external `reports/1576-mali-taoudenit-20261108/{report.md,SOURCE-ADDENDUM.md,SOURCE-RESULT.json}` and versioned frozen operators/receipts; no rawkeys in public reports. Final ledger is its own docs-only commit; library/config/tests must remain byte-identical4f5. Full offline and final-head configured CI remain coordinator-required before merge.
