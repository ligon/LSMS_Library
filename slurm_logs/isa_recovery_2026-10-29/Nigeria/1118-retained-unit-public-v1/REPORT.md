# Nigeria #1118: normalize retained missing units without changing eligibility

Retained missing plot-input units now use the existing U_UNKNOWN sentinel after channel reduction. The fill preserves the baseline returned-payload boundary: a completely hollow payload remains hollow for the unchanged finalizer. No source mapping, quantity, unit factor, channel reducer, dedup, household/PP-PH convention or scientific eligibility decision changes. The no-channel/tree-crop record-validity question and seven mixed-channel cases remain separate.

This extraction starts at development `0b88881e9bd7e8b8838552fb4b2d4e7cabab5268`. All five selected source/config-documentation/test/ledger files preserve final reviewed `8a73343ff554485c3f9b2166787a451f4dac2245` bytes. Their current-base predecessors match original public `10ad99d6`, so existing CONTENTS and prior edits are preserved. No private ancestors, operators, raw data, caches or witnesses are imported. No tests or data were replayed for extraction.

## Existing rule and source boundary

Nigeria CONTENTS preserves the old dispatcher exception, “Country() serves them u = NA, not Unknown,” and its later explicit contradiction with #1041. The retained-unit rule supersedes that exception, not the separate question of whether no-channel or tree-crop rows belong in plot_inputs.

Only the returned payload Quantity, u, Purchased, Quantity_purchased and Improved defines existing nonmissing payload. Its any-not-NA predicate is evaluated before unit normalization. True-null hollow records remain all NA. Observed False/zero and literal blank/string-marker units preserve their old non-NA disposition; keys and ignored extra fields do not count. There is no early deletion: original sort/keep-first dedup remains before the finalizer, including the case where a hollow first duplicate suppresses a later observed duplicate.

Missing/blank units and existing map_index string markers nan/<NA> become Unknown only within that retained boundary. Genuine nonblank units remain exact, including recorded units on missing-quantity rows. Filling stays after channel reduction: otherwise a missing channel could become a second unit and change one-unit sums into mixed-unit selection. The channel functions and other 62 Nigeria top-level symbols are unchanged.

## Actual original-worker result, attributed to its source

The original worker measured public `10ad99d6` and candidate `a345ca139` on 34 declared cached input/sample sources, 131,957,944 bytes. Sidecar size/MD5 and source/private-copy SHA256 checks are recorded. Independent review authenticated 38 checksum members, 39 publication files, 1,091 final source bindings and ten baseline bindings; it matched all 34 sidecar MD5/size receipts to Git metadata without rereading raw data.

| PP output quarter | Rows unchanged | Null units changed to Unknown | Unknown after |
|---|---:|---:|---:|
| 2010Q3 | 18,052 | 1,639 | 1,736 |
| 2012Q3 | 16,284 | 1,229 | 1,288 |
| 2015Q3 | 19,392 | 1,100 | 1,122 |
| 2018Q3 | 26,228 | 1,086 | 1,086 |
| 2023Q3 | 16,664 | 492 | 492 |
| Total | 96,620 | 5,546 | 5,724 |

These were fresh observations, not historical count-band assertions. Country changes 5,546 null units / 178 Unknown into zero null / 5,724 Unknown. Feature already served zero null / 5,724 Unknown. All other values, keys, masks, dtypes and Country attrs remain exact; 91,074 populated-unit rows and 90 recorded-unit/no-quantity rows remain intact. Feature values remain exact; its documented unit-promotion count changes 5,546 to zero.

The strict comparator checks axes, EA class/dtype, null masks, exact nonmissing NumPy values and attrs, calibrated by 14 negative controls including tiny and nextafter changes. It does not rely on pandas 3 nullable-Float64 frame equality alone. Cross-API equality explicitly aligns country/season/input_obs axes, crop names and string storage after checking unit values; raw cross-surface row-order identity is not claimed. seed_kg (4,888 rows) and nitrogen_kg (4,359 rows) remain exact before/after and across aligned APIs.

Baseline empty-private-L2 build took 167 seconds. Candidate used ordinary hash-triggered rebuilding, 182 seconds, not a second empty-root run. A separate 17-second warm check preserves saved frames and parquet mtimes. The recorded country hash and ten-quarter manifest match that original source; observed native output is the five PP keys above. This does not establish new round timing or output in every registered quarter.

Initial builds relied on allocation memory limits; explicit 8-GiB virtual-memory caps were added later and peak RSS was not separately measured. Exact runtime/package/config/data/DVC identities, one-CPU/thread inheritance and Python DNS/connect fallback refusal after package import are recorded; this is not kernel network isolation. Setup/audit failures, ordinary Make-to-Python fallback warnings and redacted-log provenance remain in the original packet. No build-time network refusal was reported.

## B1 and the independently reviewed successor

Original candidate a345ca139 was held because unconditional filling could rescue an otherwise all-NA payload at Country's generic hollow-row drop. That blocked source remains a historical failure; it was not silently adopted or relabelled as accepted.

The repaired successor is tested `adc6d132`, final ledger-only `8a73343f`. Its four-file selection passed **74 cases**, with five requires_s3 cases deselected. This includes the previous 60-case selection plus 14 new actual-finalizer regressions, not an additional 134 independent passes. The retained missing-quantity fixtures now carry observed Purchased=False; true-null hollows, False/zero, literal markers, ignored fields and both duplicate orders are exercised through the real finalizer with invented sample metadata and refusing source readers. No fixture or generic-finalizer implementation changed.

The incremental saved-operand actor opens only manifest-bound original before/after Country frames. All 96,620 recorded payloads are nonhollow; zero need the new hollow exception. The previous 5,546 unit-only changes and 90 recorded-unit/no-quantity rows remain strict-exact. Original and repaired actual assemblers agree on every payload in all five saved-wave groups. Operand bytes remain unchanged. Independent review authenticated all 18 successor artifacts, tested/final product bindings, four operators and all 74 unique passing JUnit identities; final source differs from tested source only in the ledger.

This is saved finalized-population equivalence plus source/synthetic boundary proof. It is not a successor raw/native/API/Feature/Make/corpus rebuild or new cold/warm/cache-hash acceptance. The old actual measurements retain their original source binding and limitations. No original data cohort was replayed to repair B1.

## Hash cost, evidence and remaining scope

Nigeria's shared module structurally changes all 24 measured country-table hashes. Only the original plot_inputs/sample scope was built; other affected tables are not verified or re-warmed here. The final helper can trigger another ordinary rebuild despite unchanged measured payloads. No generic freshness repair, alternate backend, assume_cache_fresh acceptance or global hash census.

Original evidence: dispatch `a46aa180f03d45c9a092d72cbe69799a` under the ISA campaign's merged81-useful-v1 archive. Successor sealed report SHA256: `2216352d374d32b5eabe02fdfab1e1785160f8c969ef57e0dd5746b0ad963c7c`; export `ab5e945e00278a03ef95265b9d9c0d3e220809ffd0d2ba7cc0832e8f3c2652d9`. Independent repaired-source review SHA256: `f5d1716a086e5c51e8378fb9811ed731b937356aadbd34a8389befc581c862c4`. Private operands and original logs remain outside Git.

Root accepted the bounded source/scientific review. This addresses Nigeria's retained-unit inconsistency only. #1118 remains open for its non-ISA residuals; no-channel/tree-crop validity, mixed-channel science and broader cache/runtime acceptance are not closed by unit normalization.
