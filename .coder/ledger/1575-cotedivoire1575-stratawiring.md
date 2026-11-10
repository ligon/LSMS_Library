# GH1575 — CotedIvoire 2018-19 strata wiring

**Search tier:** ripgrep + git floor; GitNexus tools unavailable. Inherits
`STANDING.md` §§2–4. Exact starting source: `9f89151ddd783a07fa57b9277da3677f7024ce7c`.

## §1 Task, restated

Apply the user-selected documented region-label × milieu-label construction
only to CotedIvoire/2018-19 `sample.strata`, using unchanged GH812 machinery.
The 2026-11-10 four-country CHARTER supersedes the design packet's historical
NOT SELECTED notice. No renewed scientific choice or approval pause is needed.
Own only the sample cover's strata declaration, wave thin hooks/imports, own
registry key, append-only CONTENTS, this ledger and the uniquely named tests.

## §2 Existing machinery

| Symbol/source | Anchor | Decision |
|---|---|---|
| `derive_region_milieu_stratum`, `strata_label` | `lsms_library/ehcvm.py:820` | Reuse unchanged; stripping, missing/blank → NA, nonstring → dedicated `StratumError`; tested by GH812 `TestLabel`. |
| `stamp_region_milieu_stratum` | `lsms_library/ehcvm.py:855` | Reuse unchanged nullable string key; missing strata receives no key; GH812 `TestStamp`. |
| `inputs_region_milieu_stratum` | `lsms_library/ehcvm.py:867` | Reuse actual own cover spec/household extraction and original field names at `(t,i)`. |
| `Wave.column_mapping`, `df_data_grabber`, `_merge_subframes` | `country.py:1605,1850`; `local_tools.py:2613` | Exercise actual extraction and merge with fenced invented providers, no normal data API. |
| Registry and optional carrier | `derivations.py`; canonical `data_info.yml` sample | Extend own registry once; existing carrier/schema unchanged. |
| Own `i` formatter | `countries/CotedIvoire/_/cotedivoire.py:7` | Preserve cluster + three-digit household formatting. |
| Prior documentary results | ledger `1575-ehcvm-stratification-documentation.md`; merges `585021853` (#1637), `27577bc2d` (#1640) | Reuse own BID quotation/custody; no PDF reread. |

The complete own CONTENTS (1,141 lines at base) was read first, including its
TODO vocabulary and all caveats. Idiosyncrasy, verbatim: “CotedIvoire 2018-19
uses uppercase =CIV= in filenames”; own tables say “Abidjan has 80 urban + 4
rural clusters, and Yamoussoukro also has both milieux.” Keep literal
`Menage/s00_me_CIV2018.dta` and `Menage/ehcvm_ponderations_CIV2018.dta`.

## §3 Definitions & conventions in force

Cite `STANDING.md` §§3–4, `CLAUDE.md` “sample() and Cluster Identity” and
“Derived Values”, canonical `lsms_library/data_info.yml`, and own CONTENTS
“2026-11-08 — documented EHCVM region/milieu strata”. The own second-edition
BID §4.1 French PDF p.13 explicitly states “La strate est la combinaison
région/milieu.” This retrospectively describes the original 2018/19 draw;
2021/22 cluster revisit, price/NSU samples and older CILSS are separate.

## §4 Invariants & assumptions

Preserve all other YAML bytes/meanings: sources, dfs, merge_on v, final_index
(i,t), IDs, cluster, weights/panel weights and Rural. No normalization change.
Preserve the full CONTENTS prefix, including the conflicting historical
“sample.strata is wired from the cover-page region code” advice and the
CILSS rotating-panel/frame/weight caveats. No capital urban-only rule, numeric
coercion, region-only fallback, fill, majority, pruning, alias/crosswalk, fixed
observed-count golden, 2021/22 wiring or shared-code/schema/test edits.
Keep existing attrs contracts. Do not claim preservation of an incoming
Derivation column the unchanged shared stamp replaces; the existing own
sample has no such key, and other table hooks/registrations remain exact.

## §5 Reuse decision

Reuse the three shared helpers literally through thin own-wave functions.
Add one registry record scoped to 2018-19 with documentary/static validation
only; actual rows UNRUN. No country categorical table is needed: preserve
stripped native labels. New tests cover the actual YAML and hook path with
invented frames and existing test controls; no new test framework.

## §6 Open questions / residuals

No scientific blocker to construction. Existing GH812 and GH1575 evidence
packets establish design and holdings but no complete SEALED own native card
was located there. Request manager + deputy's bounded cover-only zero-record
metadata check (exact sidecar md5/size); do not run it here. Actual native
missingness/coding/cluster checks, normal cold/warm/Feature/typed-input
acceptance and final metadata/full gates are later independent stages.
Shared GH812 region-only assertions are intentionally obsolete for this
selected country; deputy owns adaptation. No shared cache work/GitHub writes.

## Phase 3 — verification

OK (§§2–5): the YAML uses the unchanged `strata_label`; wave functions delegate
only to the existing country-bound input/stamp helpers. The single registry
key resolves the existing derivation and input callable, scopes only 2018-19,
and accurately records actual-row validation UNRUN. Full original CONTENTS
prefix is preserved. All other sample declarations and pre-existing hook
bodies remain exact; no alias/crosswalk/shared/helper/schema edit.

OK (§4): frozen pure v2 passed 32 checks (18 new own checks and 14 existing
GH812 TestLabel/TestStamp controls), `-n 2 --dist=loadfile`, private empty data
and config, immutable image generation 20261019T221223Z, BLAS/build threads 1,
and a distinct user/network namespace. Controller and both worker receipts
assert exact checkout/package/config/countries/runtime identity. Native and
parquet opens/network and nonprivate writes were denied; actual extraction
used only fenced invented providers. Complete nonstrata frame/axes/order/
dtypes and raw weights/Rural match the original declaration; nullable key,
missing/blank, nonstring refusal, attrs, default scoped inputs and explicit
unsupported-provider failure are covered. Typed attrs preservation is shown
at the hook boundary, not claimed as normal Country/Feature acceptance.

Failures preserved in external evidence: pre-freeze system-Python 3.6 receipt
generation rejected `text=True`; corrected to `universal_newlines=True` before
tests. Pure v1: 28 passed/4 failed because the test imported public
`derivations` (a function) instead of direct registry functions. Only that
test import/use was corrected and re-frozen; all production v1/v2 source
hashes match. The setup helper passed; a subsequent optional Poetry binary
probe was absent (no install attempted). The API audit ran its exact stdlib
script through MAIN, enumerated all 15 skills and exited 0.

No CONTRADICTION or REINVENTION found against §§2–5. Shared GH812's two
region-only assertions for CIV are intentionally obsolete and reported to
deputy without changing/running that cell. Shared helper commentary also
describes historical scope and stays untouched as required. Native card and
all actual/full integration/metadata acceptance remain residual gates (§6).
Source-qualified runtime authority is the explicit charter plus
APPROVED-RUNTIME-BINDINGS, superseding the initial source-only project profile
only for this finite pure scope. Detailed logs/JUnit, freezes and identities
live in the dispatch evidence directory named by the worker handoff.
