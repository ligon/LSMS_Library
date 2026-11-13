# Mali GH #822 - verification and handoff

Sue, 2026-10-21. SCOPE DEVIATIONS: none.

The implementation serves all three native 2014-15 community-price readings:
155,931 eligible rows, up from 40,218. Native Country grain is now
`(t,v,j,u,variety,visit,obs)`, with obs equal to the original form slot1/2/3.
It preserves variety codes, passage visits, per-lot Price, lot-weight Quantity
and passage-specific Description. No row is selected or aggregated.

Ledger/design commit `3728e4bb0` preceded implementation `70f6ff81d` on
`isa/Mali-20261021`, based on `f8d236694c26fe9e709c5548f02c70f74ea72c33`.
Both commits were published by the post-commit hook. This report and the final
ledger verification are a separate documentation commit.

## Source accounting

| Reading | Passage1 | Passage2 | Total eligible |
|---|---:|---:|---:|
| 1 | 28,672 | 38,027 | 66,699 |
| 2 | 20,636 | 28,378 | 49,014 |
| 3 | 16,851 | 23,367 | 40,218 |
| total | 66,159 | 89,772 | 155,931 |

Every full key is unique and non-null. All 23,646 old base cells survive;
readings1/2 expand the `(t,v,j,u)` coverage to39,245 cells. The source-defined
26,745 count reproduces as product-variety-passage rows priced in readings1/2
but not3, before the unit-validity filter. It is not the count of new base cells.

Passage2 q03/q06 lack their Stata unit value-label dictionaries. The exact
common COMMUNITY dictionary is identical on all three p1 unit fields and p2
q09; the runtime dictionary matches that held metadata exactly. Restoring only
those missing labels avoids discarding66,434positive-price slots. Unlisted
and fractional codes are not rounded, inferred or assigned known units.

Eligibility and weight rules are unchanged. All129positive-price entries with
unresolved units remain excluded:49code99Manquant,19code0,58otherintegercodes,
3fractional codes. Their weight masks are79positive nonsentinel,22zero,
15sentinel999,13missing. Price>0 does not establish validity; `RESIDUAL.md`
drafts the separate diagnosis-first issue. The exact original reading3 keep
and complete drop sets are preserved, including its19such excluded entries.

Quantity is still the recorded lot weight in KG. This definition reuses the
questionnaire evidence in Mali CONTENTS; the PDF was not newly read because
its blob is not held. Fresh Stata metadata confirms all three repeated
Unit/Poids/Prix identities. No acquisition or new sentinel interpretation was
performed. The existing999/9999weight mask is unchanged; no9999weight was
observed. Zero and otherwise missing weights remain present under the old rule.

## Verification

- Initial actual-script controls on baseline:14failed/1passed, with failures
  caused by missing native obs. The actual public Country control separately
  fails baseline on the six-level native index.
- Expanded script, native-observation, duplicate-key, exact/fractional-unit,
  price/weight and public Country/Feature controls plus existing declaration/
  assembly tests:33passed. Existing real Mali built controls:6passed.
- Actual source builds used physically separate private baseline/candidate
  L2 and relevant held L1 copies, with matching checkout/config identity and
  outbound-network guards. Baseline was immutable detached `isa-baseline` at f8d.
- Full `assert_frame_equal`, including index/column dtypes, passes baseline
  cold/warm, candidate cold/fresh-process warm and candidate obs3 versus the
  complete old public frame. The independently assembled source expectation
  matches every eligible candidate Price/Quantity/Description/key.
- Mali2014-15 regrades `sane`,155931rows. Existing `has_household_index`
  warning is expected for this native cluster table; no blessing is claimed.
- Paired Mali/Niger Feature audit:3findings before,2after. The removed finding
  is Mali's neutral-obs canonical-filler warning. The two unchanged findings
  are the cluster tables' missing-household-i warnings. No added finding,
  collapse or row-conservation failure. Community prices has no kwarg grid.
- Unchanged Niger comparator:46,043rows, fresh current hash and full wave
  coverage. Its physically copied L2 bytes remain unchanged.
- Hash scope: exactly24Mali country/table hashes move of549;525other-country
  hashes and all39framework fingerprints are unchanged.
- No outbound attempt was recorded. No shared-cache write/promotion/clear,
  source acquisition, fixture/golden/dependency edit, framework edit, GitHub
  write or branch switch occurred. No task process remains running.

Source-safe aggregate evidence: `source-census.json`, `exact-frame-proof.json`,
`hash-summary.json`, `audit-summary.json`, `regrade.json`, `comparator.json`.
Detailed source frames, full scanner records and logs remain private under
`/local/job40255479/control/isa-backlog-20261021/Mali/822/`.

## Reproduce

Use the mounted image and matching checkout/config roots. Set a private
LSMS_DATA_DIR containing relevant held L1 and fresh country caches; do not
point these commands at a shared writable cache. Pin OPENBLAS_NUM_THREADS,
OMP_NUM_THREADS, MKL_NUM_THREADS and LSMS_BUILD_WORKERS to1. Add the image bin
to PATH for the actual make/script subprocesses. Assert package/config real
paths before executing. With these environment settings:

```sh
/local/job40255479/venv_img/bin/python -m pytest \
 tests/test_mali_community_price_readings.py \
 tests/test_community_prices_variety.py::TestDeclaration \
 tests/test_community_prices_variety.py::TestAssembly \
 -q -n 2 --dist=loadfile -o cache_dir=/private/pytest-cache
/local/job40255479/venv_img/bin/python -m pytest \
 tests/test_community_prices_variety.py::TestBuilt -k 'Mali or mali' -q \
 -o cache_dir=/private/pytest-cache
/local/job40255479/venv_img/bin/python bench/feature_audit/scan.py \
 --features community_prices --countries Mali Niger --phases 1,2 --jobs 1 \
 --out /private/audit.jsonl
```

The full source census, cold/warm comparisons and original drop-mask checks
are runnable now from private `diagnose.py`, `decoded_census.py`,
`unmapped_patterns.py`, `api_check.py`, `hash_inventory.py` and their recorded
environments. Their detailed outputs are intentionally not committed.

The shared test expectation changes carry this source provenance; no Niger
region was edited. The tomato-paste control now counts distinct varieties,
since native obs multiplies rows within each variety. No assertion was skipped,
xfail-marked or weakened. Native obs is already canonical, so no framework
change is needed. All-Mali L2 invalidation is intentional and measured; no
other Mali table was rebuilt for this focused proof.

Independent review is next. No PR was created. The separate2018-19module
remains#821; the unit-error residual and adjacent labor-casing#1121remain open.

Sue
