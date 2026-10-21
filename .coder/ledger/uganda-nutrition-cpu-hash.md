# Prior-Art Ledger - Uganda nutrition CPU fingerprint

**Search tier used:** ripgrep + git floor; source anchored on 852ac2bfcd5f798533f56e7efa1f77db80c0138f.

## §1 Task, restated

Extend the existing raw Uganda parquet invariance test to accept an optional finite list of measured exact content-hash variants. Preserve the primary approved nutrition hash, every structural assertion, all other entries' exact content checks, and existing null-debt semantics. Add only the exactly reproduced Haswell nutrition variant; do not change production data or numeric calculations.

## §2 Existing machinery

| Symbol | Anchor | Existing behavior / coverage | Decision |
|---|---|---|---|
| `_fingerprint` | `tests/test_uganda_invariance.py:69` | Raw `hash_pandas_object(index=True)` SHA256, preserving row/column order; exercised by the parametrized invariance test | reuse unchanged |
| `test_parquet_matches_baseline` | `tests/test_uganda_invariance.py:175` | Coverage gate, then exact shape/columns/index/dtypes/content assertions; requires S3 only to obtain real inputs | extend final hash acceptance only |
| `_coverage_skip_reason` | `tests/test_uganda_invariance.py:144` | Skips explicit short wave manifests; missing metadata still compares | reuse unchanged |
| `_canonical_dtypes` | `tests/test_uganda_invariance.py:112` | Existing narrow string/datetime dtype equivalences | reuse unchanged |
| Manifest | `tests/fixtures/uganda_baseline.json:1364` | Nutrition 23,777 x 15, primary 12d25f6e...50e3a | append optional variant only |
| Nutrition algebra | `lsms_library/countries/Uganda/_/nutrition.py:19,53-70` | Runtime kg quantities grouped to household/wave/item, multiplied by unchanged FCT densities | reuse for diagnosis; never edit |
| Baseline generator | `tests/generate_baseline.py` | Writes new primary fingerprints, never nulls | do not invoke or change |

## §3 Definitions and conventions in force

Inherits `STANDING.md` sections 3-4 and `CLAUDE.md` data-access/cache rules.
`tests/test_uganda_invariance.py:13-23` defines null fields as explicitly unpinned debt; a null primary content hash remains unpinned even if an optional variants field exists.
Uganda `_/CONTENTS.org:2025-2051` documents coupled FCT/nutrition writes and unchanged approved nutrition values; the coupling is separate from arithmetic portability.
The October 15 maintainer decision at `CONTENTS.org:3306-3327` lifts the historical #967 direction hold for precisely 23,777 x 15 / primary 12d25; it does not authorize whole-fixture regeneration.

## §4 Invariants and assumptions

- The primary 12d25f6ec6d069af7d3cff703b5091bc4770944eaafacf2e87bc2f058f450e3a stays unchanged.
- Only nutrition receives an optional exact variant, f3d3c34d3fad5253d6b026d3d397d2895bc92ce3124dbdd16a92da1553d89fd6.
- Fixed exact-852 operands under SkylakeX -> Haswell -> SkylakeX reproduce primary -> variant -> primary. One 2019-20 row has eight changed float64 cells, at most two ULPs / 3.637978807091713e-12 absolute / 2.888772476871367e-16 relative; all structure/order/input hashes unchanged.
- CI's selected hardware kernel was not logged and no raw artifact exists. Exact local reproduction establishes the bounded alternate, not a blanket numerical-equality claim or direct observation of CI hardware.
- Future unmeasured fingerprints still fail. No broad tolerance, rounding, primary replacement or kernel forcing.
- Optional variant acceptance must not bypass structural or coverage gates, accept arbitrary hashes, or leak to unrelated entries.
- Production source, data, Windows digest expectations and build fingerprints remain unchanged.

## §5 Reuse decision

Reuse the complete existing invariance test and fingerprint; extend only its final permitted exact-hash set. The manifest's optional `content_hash_variants` list records measured alternatives, with entry-local provenance explaining the measured kernel and evidence. New auth-free controls call the actual invariance test with synthetic parquet and substituted fingerprint metadata, testing both hash acceptance and structural refusal without raw microdata or S3.

## §6 Open questions for the human

None: parent explicitly authorized this finite, evidence-backed optional variant mechanism and preservation of the primary approved hash. No broader numerical contract is proposed.

### Phase 3 - verification

- `test_parquet_matches_baseline` - OK (sections 2-5): only its final finite accepted-hash set extends; structure/coverage and null-primary behavior remain unchanged. Fifteen auth-free actual-test controls pass.
- Manifest - OK (sections 3-4): primary unchanged, only the measured nutrition alternate added; stripping that one field yields JSON identical to exact852.
- Numerical counterfactual - OK (section 4): exact primary -> alternate -> primary replay on fixed operands, unchanged structure/order/input hashes, maximum two ULPs. Both real full parquets pass; a deliberately changed value is rejected under both kernels. Exact852 code passes the primary and fails the alternate.
- Production/cache inputs - OK (section 4): no production arithmetic/config change; 549 country, 2,139 wave and 39 tagged fingerprints unchanged. Additive country note preserves approval and #788 policy.
- Reinvention search - none: existing fingerprint and complete invariance test reused; no duplicate reader, reducer, tolerance or hash generator.
- Scope limit - future unmeasured fingerprints still fail. CI kernel is inferred by exact counterfactual, not directly observed. Aggregates/scripts: `slurm_logs/framework_campaign_2026-10-20/nutrition-cpu/REPORT.org`.
