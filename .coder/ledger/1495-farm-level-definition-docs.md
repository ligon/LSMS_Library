# Prior-Art Ledger — GH #1495 Farm-level definition documentation

**Status:** bounded documentation patch complete; root independent review pending.
**Frozen documentation commit:** `4e1da9597f6360d941db27b46586713a04be315d`.
**Source:** `6863a6bf6a7ad1e4af1c9743fa4992f393a1e4ce`.
**Search tier:** git + ripgrep + source/AST inspection (floor); no index refresh.
Inherits [STANDING.md](STANDING.md), especially §§3–4, and `CLAUDE.md`.

## §1 Task, restated

Reconcile the explanatory `plot_id` comments in `lsms_library/data_info.yml`
and the null-key control's docstring in
`tests/test_gh1469_farm_level_occurrence.py` with the existing Farm-level
rows. This is the authorized documentation-only residual, not an allocation,
occurrence-rule, eligibility, weight, schema or default decision. No survey
source, Country/Feature build, acquisition or shared-cache operation is needed.

The supplied issue snapshot predates partial fixes: the pinned schema already
mentions tree harvests, and the test already says Mali's trees *were* null
until GH #1476. Preserve that fact rather than claiming a present-tense stale
docstring was found. Remaining prose still says the record "has no plot" and
"the sale is there", and omits the existing EHCVM harvest-total case.

## §2 Existing machinery

| Source | Existing behavior / receipt | Verification to reuse |
|---|---|---|
| `lsms_library/transformations.py:8114` | `_without_redundant_farm_level` tests keys and the sentinel, not sale/harvest content; keeps a Farm-level row only where no non-Farm-level row repeats its household-wave-crop (country when present) | Existing pure GH #1469 controls; no helper edit |
| `tests/test_gh1469_farm_level_occurrence.py:105` | Synthetic null plot key is not the structural sentinel | Existing assertion and fixture stay exact |
| `tests/test_gh1476_mali_tree_farm_level.py:33` | Static control checks both EAC-I producers write `FARM_LEVEL`, not null | Run this existing source-only control |
| `Mali/2014-15/_/crop_production.py:142`, `Mali/2017-18/_/crop_production.py:226` (under `lsms_library/countries/`) | EAC-I tree records use `'plot_id': FARM_LEVEL`; numeric tree columns come from `mali.tree_crop_rows` | Read source, do not execute producers |
| `lsms_library/countries/Mali/_/mali.py:623,1171` | Tree rows carry harvest and sale answers; `derive_hh_crop_harvest_total` fills or creates a Farm-level row for a multi-row crop, even without a sale | Existing implementation plus dated CONTENTS receipts; no new oracle or data replay |

## §3 Definitions & conventions in force

Canonical source: `lsms_library/data_info.yml:7–35` (per STANDING §3).
The structural household-grain sentinel and epistemic missing-plot sentinel
remain distinct. The occurrence and value-sum rules are unchanged.

Full Mali `_/CONTENTS.org` read, including the TODO/WAITING convention and dated
decisions. Concrete grounding (existing documentary receipts, not new counts):

- Lines 2652–2686, GH #1476: the tree roster is household/species grain.
  Lines 2672–2676 say scattered trees can name a parcel but "the parcel is an
  attribute of the line"; this qualifies the schema's "has no plot" wording.
- Lines 2680–2684: "Farm-level now means two things here: a tree row carries a
  harvest; a 2021-22 sale row (above) carries none." The historical observation
  that none of the 627 tree rows repeats a plotted crop is not re-measured here.
- Lines 2808–2820 and 2911–2915, GH #1437: household harvest totals may occupy
  Farm-level rows, including rows created without a sale. This is the missing
  documented example. Lines 3031–3038 keep scattered trees Farm-level by ruling.

No new country idiosyncrasy is discovered, so no CONTENTS addition is warranted.
No other country's actual module/example is used as evidence; existing sibling
citations in the schema remain unchanged and are not independently revalidated.

## §4 Invariants & assumptions

Inherits STANDING §4 and the dispatch's stricter private-runtime/no-source fence.
Only explanatory YAML comments and the named Python docstring may change.
Parsed schema must be entirely equal (no descriptive-value exception planned).
All executable Python AST, non-docstring tokens, test fixtures/assertions,
markers and tolerances must remain exact. No new tests, skips or baseline edits.

`Wave._input_hash` (`country.py:1354` onward) hashes wave/country build inputs;
`Country._table_cache_hash` (`country.py:4517` onward) folds those hashes plus
country inputs/framework closures. The edited global YAML comments are not a
wave/country YAML input, and the test docstring is outside those inputs. Record
the source-based hash implication separately from an unperformed hash census.

## §5 Reuse decision

| Item | Decision | Reason |
|---|---|---|
| Row semantics and occurrence policy | reuse | Existing code and Mali receipts already establish them; do not change helper behavior |
| Explanation | extend | State household-grain allocation and possible harvest/sale contents accurately |
| Verification | reuse | Existing pure GH #1469 and static GH #1476 controls; exact structural proof is evidence only |

## §6 Open questions for the human

None blocks this bounded documentation patch. Scientific/default/eligibility,
allocation, source/API/cache acceptance and full CI remain outside authorization.
Independent review and source/GitHub integration belong to root. The supplied
issue's older description is explicitly reconciled above; no broad reassessment.

### Phase 3 — verification

- **OK (anchored on §§3–4):** global YAML changes only comments. Full parsed
  schema equality and exact non-comment bytes pass; no descriptive-value
  exception, schema-structure change or cache-schema bump.
- **OK (anchored on §§2–4):** only the named null-key docstring changes in Python.
  Executable AST (docstrings removed), every other Python token and all 16
  assertions in that file are exact. Git's full tracked diff proves every other
  test, fixture, assertion and executable file unchanged. `transformations.py`,
  Mali's module and CONTENTS also have exact source-byte proofs.
- **OK (anchored on §5):** existing pure GH #1469 tests plus GH #1476's existing
  static wave-source control: **12 passed** with `-n 2 --dist=loadfile`, thread
  limits/build workers 1, private prepared runtime. `requires_s3` tests were
  excluded by selection, not altered or newly skipped. Two pre-existing Google
  dependency Python-version FutureWarnings remain in the file log/JUnit.
- **OK (anchored on §4):** strict static API audit passed (10 kwargs documented,
  15 hand-authored skills listed, no untracked hashed build inputs). Ran the
  Make target's script directly with `PROJECT_PYTHON` to retain the prepared
  interpreter; no Poetry/setup/dependency action. Five public helpers without
  call sites are its existing informational findings, not repaired here.
- **OK (anchored on §§4–5):** `git diff --check` passes. Package/config/data
  realpaths match the prepared binding, credentials remain absent and private
  data-root path inventory is unchanged; private nonsymlink L1 remains empty.
- **Hash implication (source-based):** edited global YAML comments are outside
  the directly hashed wave/country YAML inputs; parsed schema values are exact,
  and the test file is outside build inputs. No cache invalidation is expected.
  No Country/Wave hash census or shared-cache check/rewarm was performed.
- **Limits:** no survey bytes/PDFs, producers, actual Country/Feature/table
  builds, source/API/cold/warm campaign or full CI. Prior documentary counts
  are cited as historical receipts only. No new GhanaLSS example or module was
  used; existing citations remain unchanged. No independent review is claimed.
  Root owns review/integration and any further scientific decisions.

Reproduction and machine-readable proofs: dispatch evidence
`verify_docs_only.py`, `source-byte-proof.json`, `runtime-boundary.json`,
`private-root-closure.json`, `farm-level-controls.log`/`.xml`, `api-audit.log`,
`verification-plan.json` and `report.md`.
### Current-stack wording qualification (2026-11-09)

Root authorized a separate candidate on frozen GH1468 final
`0bd646141b1a657ba483e94a4295dd82b8f74554`, ordinarily merged with reviewed
GH1495 `2de6733ed931ce7808eae0ccfc69f4777f727555`. This is not a development
or GitHub merge. The complete 19165-leaf merge is the disjoint mode/type/blob
union; only the three1495 paths differ from the1468 parent.

The global comment now says **household-level harvest records**, rather than
an unqualified household harvest total. Mali's registered
`harvest-from-dispositions` construction and separate `harvest-to-date` window
remain unchanged. GH1437's latest comment6646862292 records Mali's half as
completed by1515/ad44c8d46; the issue's open Burkina scope does not reopen the
Mali decision. Finished/one-unit/no-recorded-quantity admission, mixed-pair
refusals and unfinished-harvest window disclosure remain as documented, along
with the seed/loss/state/unit caveats. The wording makes no new claim that a
constructed sum is a complete physical or full-season harvest.

This child changes only that explanatory comment and this ledger qualification.
Full parsed YAML, every executable Python AST/assertion and all other source
objects remain identical to the1468 parent, except the already reviewed1495
test docstring and this task ledger. No data, scientific/default/weight,
allocation, `_without_redundant_farm_level`, count or value rule changes.
Historical0/627 and Ghana37,280 counts are not remeasured. No tests/source/API/
hash census were replayed; the author12controls/API0 and independent static
review stay qualified evidence. Root owns final doc carry, current metadata/
API/CI, dependent PR publication and any development merge.
