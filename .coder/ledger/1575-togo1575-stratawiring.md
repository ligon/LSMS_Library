# Prior-Art Ledger — GH1575 Togo 2018 strata wiring

**Search tier:** Git + ripgrep floor; GitNexus tools are unavailable. Inherits
`STANDING.md` §§3–4 and `CLAUDE.md`. Exact initial source:
`9f89151ddd783a07fa57b9277da3677f7024ce7c`.

## §1 Task, restated

Implement the user-selected documented combination for Togo/2018 sample only:
cover-page region label plus residence-milieu label using the existing GH812
helpers. The 2026-11-10 approved-four-country CHARTER supersedes the historical
design packet's NOT SELECTED/no implementation grant. Own only the 2018
`sample.df_cover.myvars.strata`, wave mapping imports/two thin hooks, one own
derivation registry entry, a CONTENTS appendix, this ledger and the new focused
test module. No data execution is part of this worker's construction/pure scope.

## §2 Existing machinery

| symbol / evidence | anchor | tested / reuse |
|---|---|---|
| `strata_label`, `derive_region_milieu_stratum` | `lsms_library/ehcvm.py:821–849` | reuse unchanged; `tests/test_gh812_ehcvm_strata.py::TestLabel` |
| `stamp_region_milieu_stratum` | `lsms_library/ehcvm.py:852` | reuse unchanged; GH812 `TestStamp`; copy frame, nullable string key only on non-NA strata |
| `inputs_region_milieu_stratum` | `lsms_library/ehcvm.py:863` | reuse unchanged; uses own cover YAML, raw names and `(t,i)` input grain |
| wave thin binding convention | `countries/Senegal/2018-19/_/mapping.py` | reuse pattern; no country-module or shared-helper edits |
| derivation loader/carriers | `lsms_library/derivations.py`; canonical `data_info.yml` sample Derivation | existing optional carrier and typed records; `tests/test_derivations.py` |
| own source/IDs/join | `countries/Togo/2018/_/data_info.yml:4–37`; `Togo/_/togo.py:i` | retain Data1 paths, grappe v, grappe/menage i, v merge and both hhweight columns |
| reviewed design/documents | burst `reports/1575-tier-c-strata-wiring-design-20261108/{WIRING-PROPOSAL.md,PROPOSED-COUNTRY-NOTES.org,VALIDATION-PLAN.md}` | construction/validation basis under approved charter |
| merged documentary/holding work | commits `585021853` (#1637), `27577bc2d` (#1640); prior 1575 ledgers | no new document reads/acquisition needed; existing appendices retained |
| GH812 review closure | burst `reports/ehcvm-strata/REPORT_final.md`, `reports/review-ehcvm-strata/VERDICT.md` | preserve dedicated refusal and duplicate-registry protection; earlier four-country results are not Togo validation |

Call-site sweep found shared helpers in the original four GH812 countries only.
Own wave has no `sample` or `inputs_strata` hook yet. New thin hooks are consumed
by YAML extraction/df_edit and the registry. Whole mapping/YAML are build inputs;
expect Togo cache invalidation beyond sample, but no measured footprint is claimed.
The deputy owns joint shared-test adaptation and a single final footprint census.

## §3 Definitions & conventions in force

- Schema/IDs/attrs/IO: `STANDING.md` §§3–4; `CLAUDE.md` sections "Derived
  Values" and "sample() and Cluster Identity"; canonical `data_info.yml`.
- Full own `CONTENTS.org` (902 lines) read, including TODO/WAITING/LOGBOOK search.
  It records: "each grappe is visited in exactly one vague" and retains `2018`
  with `Data1/`, so neither wave naming nor household/cluster identity changes.
- June 2021 BID: "except Lome Commune where all respondents are from rural
  areas"; September 2023 Table 1a names Grand Lomé, 98 urban / 0 rural clusters.
  Own CONTENTS's 2026-11-08 appendix preserves this disagreement explicitly.
- Own September 2023 BID §4.1, French PDF p12 / printed p11: "La strate est la
  combinaison région/milieu de résidence." It retrospectively describes the
  original 2018/19 design; no independent 2021/22 first-stage draw is inferred.

## §4 Invariants & assumptions

Preserve every prior CONTENTS byte as a prefix, including the earlier regional
recommendation; supersede that recommendation only for this selected 2018
construction. Preserve every source/join/extraction/weight/panel/ID/Rural and
non-strata value/order/axis/attrs contract. Missing/blank either label is NA;
nonmissing nonstring coding retains `StratumError`. No coercion, old-region
fill, cluster majority, pruning, weight inference or geographic alias/crosswalk.
No fixed observed-cell count. No country categorical table is required.

The shared stamp overwrites a pre-existing Derivation column; the own sample
currently has no derivation hook/key. Do not modify the shared helper or invent
new union behavior. Existing registry entries and attrs remain unchanged.
Own new key scopes only 2018; explicit unsupported input calls retain inherited
failure semantics (registry scope alone does not validate explicit waves).

## §5 Reuse decision

Reuse all three shared helpers unchanged and bind Togo through two thin wave
hooks. Extend only own registry/docs/YAML. New tests exercise actual own YAML,
hooks and helper on invented frames with source/provider fences, using existing
Wave/extraction and derivation contracts; no new test framework or actual API
calls. Preserve shared GH812 tests and list their obsolete Togo assertions for
the deputy. Runtime: approved immutable image, own checkout/config paths, empty
private data/runtime, one BLAS/build thread, network isolation and source identity
receipts; exact finite pure scope supersedes initial source-only runtime profile.

## §6 Open questions / residuals

No scientific selection remains pending for construction. Complete native cards
are not established by config. Named 1575 packets say UNVERIFIED; existing812
source oracles concern other countries. Requested an existing own SEALED card
pointer from manager/deputy through the absolute steering channel; supplied a
finite one-cover zero-record metadata plan if genuinely absent. No native read
will be self-started. Native coding, missingness/cluster consistency, original vs
candidate normal Country/Feature/cold-warm acceptance, independent final review,
metadata footprint, CI and Root readiness remain later gates. Togo's geographic
domain/version disagreement remains unresolved and does not block this wiring.

## Phase 3 — verification

Frozen implementation `4aa06de08a4180f6c0fb133d52dc1fbb29cfc07a` ran 35
selected pure tests: 32 passed, 3 failed because the new test imported the
package's public `derivations` function instead of its module. The original
log/XML remain in evidence. Test-only correction frozen at
`09594a4935a2b2533fe9c508e8bd37c9b9a53ee4`: **35 passed**, 0 failed/skipped,
including 21 own controls plus GH812 TestLabel/TestStamp. Production files were
unchanged between these runs. Both used the approved immutable image, exact
checkout/cwd/PYTHONPATH/countries identity, private empty data/runtime,
BLAS/build1, `-n2 --dist=loadfile`, explicit rootdir and network namespace.
Controller/gw0/gw1 identities and JUnit are retained separately per run.

OK (§§2,5): YAML and hooks use the unmodified shared helpers; no copied formula
or independent reader. OK (§§3,4): full non-strata frame/order/ID/v/weights/Rural
conservation, missing/blank refusal boundaries, all existing Rural case aliases,
nullable key, copy/empty hook and typed attrs passed on invented frames. OK
(§§2,4): registry uniqueness/scope/function/input identity and raw input grain/path
passed with synthetic providers; explicit unsupported 2021-22 retained a truthful
failure. The existing sample had no earlier Derivation key to merge; no shared
stamp behavior changed. No contradiction or reinvention found in owned changes.

`make api-audit` completed source-only through a three-line run-python adapter
for MAIN (the immutable image has no Poetry executable); no installation. Skill
index and untracked-build-input checks passed. `git diff --check` passed. No
functional replay is required for this verification-only ledger edit.

Original setup receipt failure is also retained: system Python 3.6 rejected
`subprocess.check_output(text=True)` after the image/absent-mount checks; one
authorized extraction still completed. Corrected metadata capture used
`universal_newlines=True`; neither image nor extraction was retried/changed.

Shared GH812 assertions intentionally left unchanged for deputy: Togo member
of REGION_ONLY and TestWiring's two region-only/no-key assertions (lines 161–162),
plus TestTheFrameAgrees' Togo no-key/no-milieu assertions (lines 433,435). No
normal-data test was run to demonstrate an already-known obsolete assertion.
Actual native cards/rows, normal API/cold-warm/Feature comparisons, final joint
metadata footprint, CI and independent integration acceptance remain UNRUN.
The manager/deputy card-location/metadata-only plan request remains on the
absolute shared channel; no native inspection was self-started. No release,
merge or readiness claim follows from this pure construction.
