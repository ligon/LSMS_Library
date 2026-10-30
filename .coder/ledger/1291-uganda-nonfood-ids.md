# Prior-Art Ledger — GH #1291 Uganda nonfood household IDs

Inherits [STANDING.md](STANDING.md). Search tier: ripgrep and git.
Baseline: `57d2555855f23739a4beca86297c6d5fa702fcf3`.

## §1 Task, restated

The 2009-10 and 2010-11 nonfood scripts pass numeric `GSEC15c.hh` to
`uganda.nonfood_expenditures`, whose returned household index stays numeric.
Use the existing canonical ID formatter on that result before the scripts
stamp `t`, so it joins the corresponding `sample().i`. Preserve the reported
expenditures and the helper's existing item aggregation and sparsity.

## §2 Existing machinery

| Machinery | Source | Evidence / reuse |
|---|---|---|
| `format_id` | `lsms_library/local_tools.py:3461` | Reuse; `tests/test_format_id.py` covers numeric, string and missing input. |
| `nonfood_expenditures` | `Uganda/_/uganda.py:631` | Reuse unchanged; `tests/test_nonfood_expenditures_schema.py` covers shape, sparsity and historical totals. Its first-value string-only repair at 697 does not handle numeric floats. |
| Early wave scripts | `Uganda/{2009-10,2010-11}/_/nonfood_expenditures.py` | Extend the shipped script boundary before `t` is stamped; retain the final uniqueness assertion. |
| Later household bridge | `Uganda/2015-16/_/nonfood_expenditures.py:23` | Existing `gsec1.hh` → `HHID` crosswalk stays separate from rendering normalization. |
| Sample cluster join and sanctioned writer | STANDING.md §2/§4 | Reuse; do not bake `v` into the native frame or introduce another writer. |

## §3 Definitions and conventions in force

- Canonical IDs: `local_tools.format_id` docstring and `CLAUDE.md:910`.
  Integral numeric floats become decimal strings; string leading zeros survive.
  Missing input returns `None`. Fractional numeric input follows the existing
  integer-truncating formatter contract; this task does not redefine it.
- Native nonfood grain: `Uganda/_/CONTENTS.org`, section
  `nonfood_expenditures (GSEC14B / GSEC15C)`, and
  `lsms_library/data_info.yml`'s nonfood schema. Cite STANDING.md §3/§4 for
  canonical index ownership and API-time `v` joining.
- The 2019-20 printed recall banner does not settle older CAPI `[RECALL]`
  semantics (CONTENTS.org's WAITING recall entry); no recall field changes.

## §4 Invariants and assumptions

- Rekey only `i` in these two scripts; no new household linkage or singleton.
- Preserve every helper row, reported amount, dtype, null mask, attrs, `j` and
  order. The existing post-rekey uniqueness assertion must refuse collisions.
- Genuine missing IDs remain missing, never the literal `"None"`/`"nan"`.
- Before accepting held-source results, establish finite integral `hh`, exact
  representation range, a bijection on retained keys, and correspondence to
  the independent GSEC1 household keys. No real-source measurement yet.
- Baseline 2009-10 sample contains genuinely unlocated households; correcting
  `i` does not itself guarantee all public `v` values become nonmissing.
- STANDING.md §4 governs IO, caches and import identity. Private validation
  only; no shared-cache mutation, acquisition, fixture changes or baseline edits.

## §5 Reuse decision

- ID formatting: **reuse** `local_tools.format_id`; no hand-written stripping.
- Expenditure computation: **reuse** unchanged country helper.
- Script execution tests: **new**, to exercise the actual two entry points and
  catch their missing formatter call; formatter-only tests already exist.
- The shared helper and 2015 bridge need no extension in this bounded repair.

## §6 Open questions

No implementation policy is unresolved within the approved two-script scope.
Held-source integrality, ranges, collisions and joins remain verification
gates. Source reads require the dispatcher's finite-plan approval first.

### Phase 3 — verification

- **OK (§2/§3):** both scripts call the existing `format_id`; no new formatter
  or shared-helper edit. Only the two early scripts' `i` rendering changes.
- **OK (§4):** ten executable-script cases cover canonical sample joins,
  exact payload/dtypes/attrs/order, leading-zero strings, missing IDs,
  existing fractional numeric policy, and loud collisions before writing.
  Together with `tests/test_format_id.py`: **43 passed, zero skipped**.
- **OK (§4):** executing the same new cases against the actual parent scripts
  gives **eight expected failures and two string-control passes**. The stub
  returns its own copy, matching the real helper's frame ownership; an earlier
  aliasing stub was corrected before the final parent/candidate comparison.
- **Pending (§4/§6):** no real data was opened. Source integrality/range,
  complete-frame key correspondence, Country/Feature behavior and cache scope
  remain acceptance gates. The dispatcher held this stage while prioritizing
  the existing PR queue. This synthetic stage is not final source validation.
- Test runtime: mounted `/local/job40390255/venv_img/bin/python`, checkout
  import realpath asserted, CPU affinity 14/16, two `loadfile` workers,
  BLAS/OMP/MKL threads and `LSMS_BUILD_WORKERS` pinned to one.

### Resumed public-d65 integration,2026-10-30

The preceding phases are retained historical LocalEast evidence. Root explicitly
confirmed no managed live LocalEast actor; live issue1291 has no comments, the
public branch inventory has no1291branch, and the local mirror retains exact
source4bc231c83. Latest development remainsd65. Reuse productf248d5376 exactly;
its two scripts and shared helper are byte-identical at the old baseline andd65.
No formatter/science reinvention, shared helper edit or housing1321scope overlap.
This ledger precedes current code integration.

Current user/root authority supersedes the old finite-plan permission sentence:
ordinary cached-source/API/tests are authorized,1CPU/core34/8GiB/thread1,
private package/config/data/HOME/tmp/DVC_SITE_CACHE_DIR, no provider acquisition,
shared cache mutation or new scientific policy. First verify both GSEC15c.hh
sources are finite integral/exact-range keys and compare each to its corresponding
GSEC1.HHID plus native helper complete-frame preservation. Then measure standard
Country/Feature, requested subsets and fresh-process warm with exact private
cache snapshots. A real source miss stays a source miss; do not force historical
54,027or fullv coverage. Sample's2009hybridEA/GPSbins and24unlocated households,
#829WAITING, unknown recall,2015sourcebridge and all weight/panel caveats stay.
Any accepted code is the same two calls to format_id before existing uniqueness
assertions and writer; no alternative household linkage or expenditure arithmetic.
