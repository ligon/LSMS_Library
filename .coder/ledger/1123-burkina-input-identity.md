# Prior-Art Ledger - #1123 Burkina Faso input identities

Search tier: ripgrep, git and AST. Runtime baseline
`f8d236694c26fe9e709c5548f02c70f74ea72c33`. Root approved this exact boundary
on 2026-10-21 after reviewing the complete held-source diagnosis.

## Section 1. Task

Preserve every reported Burkina Faso 2018-19 household/input line-item on the
existing `(t,i,input,crop,u)` grain. Repair the demonstrated non-injective
seed-crop map and guard the wave output before writing. Preserve extraction,
identifiers, measurements, units and shared reducers. Retain code 21's existing
input=Seed classification explicitly as unverified legacy curation.

## Section 2. Existing machinery

| Machinery | Location | Existing coverage | Decision |
|---|---|---|---|
| Native source builder | `Burkina_Faso/2018-19/_/plot_inputs.py:8,40,71,94,121` | Actual held-source script and Country diagnosis | Reuse extraction and payload code |
| Crop mapping | `Burkina_Faso/_/categorical_mapping.org:613-633` | 22,877 raw rows, 170 colliding groups | Change three target cells only |
| EHCVM household key | `Burkina_Faso/_/burkina_faso.py:18` | 4,509 source households; injective, zero sample orphans | Preserve ehcvm_i |
| Accepted named-category split | Benin/Togo commit `36170a57b04317e55716919eb5eb1054b5b43102` | `tests/test_gh323_benin_togo.py`; healthy current builds | Reuse other-cereal/tuber labels |
| Pre-write uniqueness guard | `Benin/2018-19/_/plot_inputs.py:149`; Togo `2018/_/plot_inputs.py:163` | Guard AST rejects BF baseline and two-split partial fix | Reuse logic with count-only error |
| Visible native-code fallback | `Tanzania/_/tanzania.py:1259-1262` | Unresolved crop code prefixed with crop_ | Reuse source-identity principle |
| Canonical normalization/selector | `country.py:_normalize_dataframe_index`, `most_complete_row`, `_audit_index_collapse` | Independent observed-row selection matches complete BF Country raw frame | Preserve unchanged |
| Feature alignment | `data_info.yml:122-135,207,284`; `feature.py` | Full BF/Benin/Togo frame and row checks | Preserve aliases and sentinels |

## Section 3. Definitions in force

Inherit `.coder/ledger/STANDING.md` sections 3-4, repository guidance and
`lsms_library/data_info.yml`. BF `data_scheme.yml:223-233` declares household x
input grain with no plot_id: one row per reported applied input. Purchased is an
existing API attribute, not an identity key or verified purchase interpretation.
Country retains native grain; Feature
adds the registered farm-level/season sentinels. An observed-row selector
cannot restore an identity the crop map erased.

## Section 4. Invariants

- Retain all 22,877 applied native lines, including all 183 currently lost:
  163 conflicting-payload extras plus 20 equal-payload extras from distinct
  native slots. There are 170 collision groups, zero repeated native keys and
  zero null native/declared keys.
- Preserve every Quantity, Purchased, Quantity_purchased, unit, household key
  and input value. Change exactly three existing crop targets, on 877 rows.
- Root approved `Unknown [Burkina_Faso 2018-19 s16bq01=21]` as an identity
  fallback, not a crop classification. Its 602 native records have no 2018
  Stata label; input=Seed is legacy-only and unverified. Never borrow the
  different 2021 code-21 meaning.
- Only the current 2018-19 builder consumes this map. Preserve the zero-row
  2014/2021-22 public subset behavior; new-wave coverage is separate.
- Guard the finished native index immediately before to_parquet. Fail future
  collisions even when payloads happen to be equal; emit no source keys.
- Use sanctioned source IO, held MD5-verified physical private L1/L2 and an
  active offline guard. Never acquire, restamp stale companions or hide build
  dependencies. The Org edit moves all 24 BF table hashes; 525 others and all
  39 framework fingerprints must remain exact.

## Section 5. Approved reuse decision

Exactly these Preferred Label changes:

| Original Label | Before | Approved target |
|---|---|---|
| Semences d'autres céréales | Autre crop | Autre céréale |
| Plants/boutures de tubercules | Autre crop | Autre tubercule |
| 21 | Autre crop | Unknown [Burkina_Faso 2018-19 s16bq01=21] |

The accented source/target strings are those already used by Benin/Togo.
Keep Autres semences and unused bare 20 unchanged. The two named
splits alone leave 98 duplicate extras; the third source-qualified fallback
is necessary. Correct the false comment claiming code 21 is Autres semences,
record these source/coverage facts in CONTENTS and add focused regressions.
No country-module, schema, data-scheme, shared-framework or comparator edit.

## Section 6. Residuals and evidence boundary

The exact token and three-cell boundary are approved. Source authority for
code 21 remains unresolved: no 2018 dictionary binding, four instrument
sidecars without held blobs, and no acquisition. Draft a separate curation
issue for its legacy Seed classification. Keep #1119 named-crop vocabulary
joins and other #1123 countries separate.

Private diagnosis: 22,877 -> 22,694 Country rows; 2,409.58 reported kg lost,
including 931.60 on unlabelled code 21. Benin/Togo remain 10,605/13,730 rows.
The complete proposed native frame and unchanged finalizer retain all source
payloads, accounting for existing public Kilogramme -> Kg normalization.
Implementation must prove actual fresh private cold and separate-process warm
builds, exact comparator frames/cache bytes, full Feature finding deltas,
actual 549/39 hash scope, meaningful tests and API audit before review.

## Verification

Implementation: `69ed4d2028a6f989a8ac45d117a5cffc60a831fd`.
Source-safe evidence and residual drafts:
`slurm_logs/isa_backlog_2026-10-21/Burkina_Faso/1123/`.
The ledger was committed first in `d780445e8`; the ordinary publishing hook
published every implementation/evidence commit. Independent full review passed
at `3848778d0e8676c798883c11ea3e33fc88e8a591`; the implementation remains
unmerged. The documentation-only clarification below awaits narrow review.

- **OK (sections 2-5):** repeat call-site search confirms existing extraction,
  ehcvm_i, mapping lookup, native grain, normalizer and Feature machinery are
  reused. Parsed before/after tables differ in exactly the three approved
  target cells; the entire legacy input map is exact. No parallel resolver,
  source classifier or reducer was introduced.
- **OK (sections 3-4):** two physically absent BF L2 roots, each seeded only
  with eight MD5-verified held L1 blobs, rebuild through actual Country. Source
  wave rows stay 22,877; public rows rise 22,694 -> 22,877. Every original
  selected and all 183 previously lost source lines retain complete payloads,
  units, other keys and dtypes. Twenty equal-payload extras remain distinct.
- **OK (sections 2-4):** all public/pre-final country and six raw wave sample
  and cluster frames, dtypes and full logical attrs are exact across revisions.
  All expected cache hashes/coverage are current after the builds. No stale
  companion is trusted/restamped. Separate-process warm and new-Country calls
  are exact and change no parquet byte or nanosecond mtime.
- **OK (sections 2-4):** nine new regressions and four scoped existing
  Benin/Togo tests pass with zero skips. The final new file on immutable f8
  gives eight failures and one pass. Actual-script write-spy tests reject
  the incomplete two-split mutant and future even equal-payload duplicates
  before writing. The independent real-source oracle compares every native
  line's complete measurements and unit. API audit passes.
- **OK (sections 3-4):** the current 2014/2021-22 public subset requests stay
  empty; the 2018-qualified token reaches only 2018-19. Code 21's physical
  meaning and legacy Seed classification remain explicitly unverified.
- **OK (section 4):** actual inventory moves exactly 24 BF hashes, with
  525 other-country hashes and all 39 framework fingerprints exact. Every
  comparator Country/Feature frame and full logical attrs remain exact;
  all 12 Benin/Togo cache files are byte-identical and fresh.
- **OK (sections 2-4):** Feature rows rise 47,029 -> 47,212 with no additional
  loss. Scoped phases 1/2 findings change from eight warnings to six: only
  the two BF input-collapse warnings disappear, and all other six full
  normalized records are exact. No fail/error/skip or unrun kwarg grid is
  claimed. Raw frames and warning examples remain private.

No identity-conservation CONTRADICTION or REINVENTION against this ledger was
found. The corrected code-21 comment and historical sample-count discrepancy
are quoted in the report rather than declaring country documentation stale.
The old a63/#1148 worktree, other-country #1123 work, source authority and
#1119 vocabulary alignment remain outside this implementation.

### Independent-review semantic caveat (documentation only)

The full source/runtime review remains pinned to 3848778d/runtime 69ed4d.
It found an existing source-semantic contradiction outside the identity repair:
the script calls q05/q07a purchase fields, while held metadata identifies them
as gift/donation flag and quantity. Actual purchase fields are q08/q09a, with
their own quantity unit q09b. Preserving Purchased/Quantity_purchased means
preserving the existing API bindings and NA masks, not verifying purchase
semantics. Of the 183 recovered source lines, 18 report actual purchases.
[#1188](https://github.com/ligon/LSMS_Library/issues/1188) owns separate
field/unit-routing diagnosis; no binding, physical-unit or grain correction is
included here. [#1187](https://github.com/ligon/LSMS_Library/issues/1187) owns
unlabelled 2018 code 21's source meaning and unverified legacy Seed classification.
The dispatcher narrows/files residuals during PR readiness with the Burkina
implementation explicitly reviewed but unmerged, without claiming production
resolution. No source/API/test rerun is required for this documentation-only
descendant; all 549 hashes and 39 framework fingerprints must have zero
additional movement from 3848778d, and runtime/config/test blobs must be exact.

Sue
