# Prior-Art Ledger — GH1122 nonhashed plot-registration prose

**Source:** `11714b185fe2c90f1bc6c60164aa40d358acf0f3`.
**Search tier:** ripgrep + git + static source/AST inspection. No GitNexus index
refresh or network history lookup. This checkout has one commit (shallow);
issue context comes from the supplied `current-issue.json` and the tracked
ledgers, tests and current declarations, not a newly fetched GitHub history.

## §1 Task, restated

Reconcile human prose about `plot_labor` / `plot_inputs` registration with the
current `Index Info` declarations and the separate Country and Feature paths.
Only nonhashed `CONTENTS.org`, cross-country skill prose and incorrect human
`AGENTS.md` prose may change. `CLAUDE.md` is a symlink to `AGENTS.md`, not a
second copy. No scientific, schema, product, test, fixture, Makefile, cache or
held build-input changes. The first new commit contains this ledger alone.
The task charter authorizes proceeding after the surfaced checkpoint.

## §2 Existing machinery and history

| Machinery / record | Current anchor | Reuse and coverage |
|---|---|---|
| Canonical plot registrations | `lsms_library/data_info.yml:152,167,198` | All three tables registered; includes later `production_obs`, `labor_obs`, `input_obs`; `tests/test_feature_canonical_index.py::TestDeclarations`, `TestPlotTableDeclarations` |
| Assembly aliases / sentinel policy | `data_info.yml:296,368`; `feature.py:214,242,276,303,780` | Reuse unchanged; aliases and promote/fill are Feature-side. `TestRenameIndexLevels`, `TestAlignToCanonicalLevels` use in-memory frames |
| Country finalizer | `country.py:_finalize_result`; no Feature alignment call | Country preserves native declared index names / columns (apart from its existing read pipeline); do not claim registration canonicalizes Country frames |
| Canonical column registry | `data_info.yml:428` (`Columns`) | No `plot_labor` Columns entry today; registration does not imply one |
| Documentation exclusion | `country.py:1083,1395,4606` | `_ORG_HASH_SKIP = {"CONTENTS.org"}`; both wave/country loops skip it. Static reasoning only; no actual hash census or performance measurement |
| Build input closure | `country.py:1470,4599`; `_build_registry.framework_imports_fingerprint` | Country/wave Python and schemes remain held; GhanaLSS nutrition imports transformations, so no free-comment-edit claim for transformations |
| #569 / #775 record | `.coder/ledger/feature-canonical-crop-production.md`; `TestPlotTableDeclarations` | Earlier crop registration/alias bridge, then plot labor/input registration; retain stage/worker/season and v, structural vs epistemic sentinels |
| #1047 record | supplied issue1122; `tests/test_gh1047_nigeria_unit_codes.py:207,302` | Issue's old line descriptions have successors: current tests explicitly refer to #1118 retained-unit normalization. Do not edit tests or call true Country-path limitations stale |
| #1119 record | `data_info.yml:278`; `.coder/ledger/1119-nigeria-crop-marker.md` | Crop marker `(not crop-specific)` differs from structural `plot_id='Farm-level'`; missing crop is `unknown_crop`. GhanaLSS still writes `j='Farm-level'`: describe that as current country behavior pending migration, never silently normalize it |

## §3 Definitions and documentary contradictions

Inherit `STANDING.md` §3, canonical `lsms_library/data_info.yml` and `CLAUDE.md`
(the `AGENTS.md` symlink target). Read both Ghana CONTENTS files in full,
including TODO entries; neither contains a LOGBOOK drawer or WAITING task.

- GhanaLSS quirk: the GLSS1→GLSS2 panel half is retained but clusters are
  renumbered; the record says "none keeps its =v=". Its Section 9 paid-labor
  cells are costs, not person-days; unpaid exchange is a household total.
- GhanaSPS quirk: `stageid` in 2017–18 is the ordinal position of stages used,
  unlike 2013–14's stage code. Wages are three sex/age rates plus a unit, not
  the cash-total `Wage` in other surveys. Seasons differ between labor and
  crop modules; registration cannot make their vocabularies equivalent.
- CONTRADICTION: GhanaLSS CONTENTS:5308 says "=plot_labor= has no canonical
  =index_info= or =Columns= entry (GH #569)". Current `index_info` DOES
  register it; the `Columns` half remains true. Correct these halves separately.
- HALF-TRUE: CONTENTS:5401 says "They predate this convention; aligning them
  is a separate question" about EHCVM inputs and EthiopiaRHS crops. Feature
  already supplies EHCVM `plot_id='Farm-level'`; Country keeps native grain.
  Do not extend that claim to EthiopiaRHS or to a crop-marker migration.
- INCOMPLETE: cross-country skill:40 says "landed for =crop_production=";
  AGENTS:352/354 duplicates a paragraph naming only crop registration. Current
  declarations include labor and inputs. Latest observation coordinates must
  be cited or included if listing the complete shape.

## §4 Invariants and verification boundary

Inherit `STANDING.md` §4. Preserve every production/test/config byte and every
scientific interpretation; no canonical config edits. Never change the held
GhanaLSS wave scripts, GhanaSPS scheme, or transformations. Preserve generated
GitNexus bytes and the CLAUDE symlink. Keep genuine Country-only caveats.

Use the supplied mounted PROJECT_PYTHON, exact checkout cwd/PYTHONPATH and
LSMS_COUNTRIES_ROOT, ordinary private data/config/DVC_SITE_CACHE_DIR before
imports, absent credentials, BLAS/OMP/MKL=1 and LSMS_BUILD_WORKERS=1. No native
home/state/observer inspection, dependency install, acquisition, source/warm
build, LSMS_NO_CACHE, shared-cache writes, GitHub operations or source push.
Only invented in-memory frames/providers may exercise classes; max two pytest
workers with --dist=loadfile and file logs. No new tests for prose.

Proof will compare all tracked production bytes against source, compare Python
AST/config bytes and existing executable Org nodes, and verify documentation
scope and generated-block/symlink identity. This is STATIC input-scope proof,
not a measured `_table_cache_hash` census, rewarm, performance or full CI claim.

## §5 Reuse decision

| Work | Decision | Reason |
|---|---|---|
| Registration, canonicalization, sentinels | Reuse, unchanged | Already implemented and tested |
| Country behavior / crop-marker semantics | Reuse, unchanged | #1047/#1118 and #1119 successors are not prose repair authority for data changes |
| Prose | Extend existing records minimally | Fix false registry claim and distinguish the two API paths; no duplicate policy |
| Verification | Reuse strict API audit and existing hermetic test cases | No new behavior or prose-mirroring tests |

## §6 Open questions / held work

No question blocks the authorized documentation repair. Hashed script/scheme
comments and transformations prose stay HELD for separately authorized cost /
rewarm evidence. Shallow history limits commit-by-commit reconstruction; do not
claim a live GitHub status. Full source/warm/census/CI acceptance belongs to root.

### Phase 3 — verification

UNRUN at ledger checkpoint. Runtime identity has been asserted at source HEAD.
`make api-audit STRICT=1` encountered absent Poetry (recipe error127); the actual
recipe's prepared-interpreter script was then run and reported 15/15 skills
listed, 10/10 kwargs documented and no untracked hashed inputs. Exact exits,
final byte/node comparison, selected test counts and final SHA will be recorded
in evidence and this ledger's separate final commit.
