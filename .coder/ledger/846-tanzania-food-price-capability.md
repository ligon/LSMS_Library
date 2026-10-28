# Prior-Art Ledger — #846 Tanzania food-price capability

**Search tier used:** ripgrep + git; no GitNexus tool is available. Baseline:
`8aa4d86b6594eb0b03bdd50fe3ca04f4f4ac6b26` (historical investigation).
Continuation starts at `3831b94d96967fef63934774444f2ec758f21109`, the
coordinator's four-file recovery from `845856fe58f202c659b68ad601992142af5f1fd5`
on preserved parent `02de2e8b65fbb9bf73c9441d2946b3c0263821c6`. The required
development pin `d65e8b13683409ab0005f1a5590208cd7d94ae62` was absent from
ancestry and merged normally as `a96529bf71896de3e94f07b03e182a53d97df00a`.
Inherits `STANDING.md`. No whole-snapshot restoration.

## §1 Task, restated

Record the questionnaire-backed absence of a **separately reported purchase
price** and **reported own-consumption valuation** in Tanzania NPS-SDD 2019–20
household Section J. Preserve purchased-food unit values, Section F outside-HH
market valuation, and existing tables. This is a country-local evidence record,
not a new global feature or an all-NPS-series absence claim.

## §2 Existing machinery

| symbol / evidence | location | role / test | decision |
|---|---|---|---|
| `SeriesCapability` / `verdict_for` | `lsms_library/capability.py` | questionnaire validation proposes `not-asked`; `tests/test_capability.py` | reuse validation semantics; do not edit shared registry |
| `SeriesCapability.checks_run` | same | currently hardcodes questionnaire validation to `C1;C4` | cannot use automatic row generator for C4-only retained evidence |
| `proposed_absent_verdicts` | same | expands every `lacks` member over arbitrary supplied waves | no unbounded NPS record |
| `load_verdicts`, `_absent_tier`, `_dropped_tier` | `lsms_library/coverage_matrix.py` | existing CSV schema, exact wave keys, evidence gate, tier-specific adjudication; `tests/test_coverage_matrix.py` | reuse via explicit country-local CSV path in tests |
| `feature_vocabulary` | same / `lsms_library/data_info.yml` | source concepts are not canonical features; `tests/test_feature_vocabulary.py` | preserve vocabulary |
| `Country._FOOD_DERIVED` | `lsms_library/country.py` | derived `food_prices` is broader than reported price | preserve all tables |
| retained C4 quotation | `slurm_logs/price_sources/Tanzania/REPORT.org`, §1 / “The questionnaire, quoted” | explicitly names 2019-20 household PDF, Section J qq2–6 and q4 instruction | reuse historical instrument evidence; no new PDF/API measurement |
| EPAR distinction | `slurm_logs/2026-09-09_epar_curation/issues/COMMENT_846.md` | own-consumption value constructed from purchased prices; #585 separate | no imputation authorization |

Retained branches: `git branch -a` and `git log --all --oneline` search for
846 / Tanzania capability found historical price-study/issue publication work,
no existing #846 capability implementation. Public issue/queue bodies are frozen
assets, not refreshed live history.

## §3 Definitions & conventions in force

- Per `STANDING.md` §3 and `CLAUDE.md` “Derived Tables”, food tables derive from
  `food_acquired`; no new `data_scheme.yml` entries.
- `docs/guide/coverage.md` “The four checks”: C4 is mandatory for a permanent
  close. C1 is a label sweep, C2 sibling differential, C3 positive-only WB
  harmonized cross-check. None of C1–C3 is newly run here or documented as a
  completed negative check in the retained Section J passage.
- REPORT §1: q4 spending refers to q3 purchased quantity; q5 records
  own-production quantity, not value. Section F outside-HH valuation is a
  different module/universe/grain. Price-source names follow that report.
- `CONTENTS.org:16` / `#uphi-backcasting`: descendant UPHI records are back-cast;
  `CONTENTS.org:1087` / `#issue-114` remains WAITING despite closed issue.
- `CONTENTS.org` / `#panel-design`: 2019–20 Extended and 2020–21 Refresh + urban
  booster are distinct sub-panels. Do not transfer an instrument claim by analogy.

## §4 Invariants & assumptions

- Inherit `STANDING.md` §4. Initial profile forbids raw/header/provider/L1/cache
  access; only source/sidecar text and retained aggregate evidence are inspected.
- Only **2019-20, household Section J** is covered by this retained C4 locator.
  Other five logical waves remain unadjudicated for these concepts.
- `checks_run=C4` attributes a historical questionnaire read to REPORT.org,
  not a fresh PDF read or a new API zero-row result.
- Country CSV uses existing verdict schema with **price-source concepts** in
  `feature`; it is explicitly staged, never installed in default global verdicts.
  Whole `food_prices` / `food_acquired` negatives would contradict §3.
- Shared code, global registries, snapshots, scientific baselines and ID JSONs
  are coordinator-owned; no changes here.
- `CONTENTS.org` is excluded from cache inputs. Markdown is not hashed; the
  verdict CSV lives under `_/capabilities/`, outside the direct `_/` file glob
  in `Country._table_cache_hash` / `Wave._input_hash`. A direct `_/*.csv` is
  hashed too, as the API audit caught during placement review; avoid that
  location and new `_/*.org` build inputs for this documentation.
  Thus no intended table-build fingerprint change; no live hashes measured.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| not-asked semantics and CSV loading | reuse | existing validation/tier machinery |
| evidence serialization | reuse CSV schema | explicit wave and actual checks, country-local staging |
| broader live coverage integration | proposal only | existing registry cannot express concept/wave scope and actual checks faithfully |
| purchase unit value | reuse existing API unchanged | instrument pairs numerator and denominator |
| own-consumption imputation | out of scope | separate #585 analyst-facing decision |

Continuation source finding: `CONTENTS.org` says the two-row country-local
record is at `_/capabilities/food_price_absent_verdicts.csv`, but the file is
absent at the recovery head. The unchanged five staged tests on the merged
baseline give **4 failed, 1 passed**; all four failures follow from that missing
file (`load_verdicts` deliberately returns `{}`). Add only the two documented
2019-20 Section J rows, using the existing schema and retained REPORT §1
locator. Do not restore a snapshot to recover them. Add a negative control that
the default loader and Tanzania series registry still contain no such verdicts.

## §6 Open questions for coordinator

Recorded in the absolute `QUESTIONS_FROM_AGENT.txt`; no interactive wait.
Prefer keeping source-concept evidence local. If live integration is desired,
coordinator should own explicit concept + wave scope and actual-check metadata,
with tests for out-of-scope waves, ordinary food table preservation and C4-only
provenance. Do not register source concepts as public features solely to create
matrix cells. A global registry insertion of NPS lacks food_prices is rejected.
No shared implementation patch is authorized here. Such read-only metadata
integration should not move build hashes; exclude any read-path callable from
build fingerprints only if its placement otherwise makes it reachable.

`docs/guide/coverage.md` requires all applicable checks before a live close;
the staged C4-only source-concept record does not satisfy or bypass that live
integration policy. The loader validates evidence presence and tier, not the
scientific sufficiency of C1–C4. Tests exercising `_absent_tier` with an explicit
path are serialization/dispatch checks, not authority to close a matrix cell.

### Phase 3 — verification

Baseline: unchanged staged file, 4 failed / 1 passed in 0.45 s, one CPU,
approved runtime and exact package/config/data identity asserted; no blocked
data/network/database operations. Final source/metadata check pending. Finite
selection: the staged #846 file and existing synthetic `tests/test_capability.py`;
120 s process bound, native thread pools and build/make workers fixed at one,
private temporary fixtures, constructors/data-reader leaves refused by runner.
No scientific/API acceptance claimed. Fresh questionnaire verification and all
real-source/API stages remain unexecuted and require coordinator authorization.
