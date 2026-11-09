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

Documentation frozen in `3bc1868b503020b2f16a32774eb825eba56e35bb`; this
final ledger update is a separate commit. The initial ledger-only checkpoint
was `cfe790f8ddbb4e46075d9d75962f394546d3fb94`, surfaced before prose edits.

- OK (§2/§3): GhanaLSS now distinguishes the registered Feature index from its
  native Country index and the still-absent canonical Columns block. EHCVM
  input alignment is explicitly Feature-side; the coarse EthiopiaRHS caveat
  stays. #1119 markers are distinguished without relabeling GhanaLSS data.
- OK (§2/§5): skill prose names all three registrations and later observation
  axes. AGENTS has one corrected human paragraph; CLAUDE remains its symlink.
  No new callable, estimator, test, fixture or schema was introduced.
- OK (§4): prepared runtime identity at source HEAD passed. Final strict audit
  fallback `PROJECT_PYTHON scripts/api_surface_audit.py --strict` exited 0:
  15/15 skills listed, 10/10 kwargs documented, no untracked hashed inputs.
  Five no-call-site machinery advisories remain. `make api-audit STRICT=1`
  itself exited 2 (Poetry absent, recipe error127); it is not reported green.
- OK (§2/§4): existing `TestDeclarations`, `TestRenameIndexLevels`,
  `TestAlignToCanonicalLevels`, `TestPlotTableDeclarations` passed: 29 cases,
  exit0, 0 skipped, 0 failures, three warnings (two dependency FutureWarnings
  and an assembly-sentinel warning on an invented frame). Run with the mounted
  interpreter, `-n 2 --dist=loadfile`, ordinary private bindings, pinned threads
  and file/JUnit logs. No actual country/table build or source getter ran.
- OK (§4): static source comparison passes for 799 production Python files /
  572,869 AST nodes and 268 YAML files, all byte-identical to source. The other
  13,529 production paths remain unchanged. Both country records preserve all
  548 table rows and six executable source blocks; GhanaSPS is wholly unchanged.
  Held files, every test/fixture, generated GitNexus block and CLAUDE symlink
  remain equal. `git diff --check` exited0. Final evidence rechecks this at the
  final ledger commit; no further documentation edit is planned.
- Cost qualification (§4): CONTENTS exclusion is established statically from
  `_ORG_HASH_SKIP` and its consumers. No actual cache hash census, warm/cold
  source build, performance timing or full CI ran. Hashed comments stay HELD.

Execution notes: an initial patch failed its context check and changed no file;
the corrected patch applied. A speculative test filename and absent ledger glob
also returned search errors; neither is verification success. All required
artifacts, exact exits, private-publication receipt and final SHA are under the
absolute dispatch evidence directory, outside tracked source. Independent
review/current integration/CI and overall issue closure remain root-owned.


## P2 author revision — checkpoint (2026-11-08)

### §1 Exact ROOT scope and preserved history

This is a new author's narrow source-only revision of frozen candidate
`63ebd54c5bbceeae8f21b4a3df9d21b800689465`. The complete preceding ledger
prefix remains byte-exact; its original four-path candidate and 29-test result
are historical evidence, not a fresh acceptance claim. The supplied independent
review ended at `7cc28672d7a3a067df7422e7566deab6d96c929e` with verdict
`P2_DOCUMENTATION_REVISION_REQUESTED`, one P2 and no P1. Its coordinator
preflight records state `succeeded`, phase `finished`, scheduler `COMPLETED`;
this author has not inspected native state or the observer.

ROOT's exact bounded scope:

> Then edit ONLY the newly added paragraph at GhanaLSS CONTENTS5417–5419.
> ROOT's required meaning: EthiopiaRHS's coarse crop table is excluded from
> Feature crop assembly when it lacks a plot axis AND is assembled alongside
> a canonical plot-grain provider; a sole provider or providers ALL sharing
> the same reduced shape are retained; crop registration supplies no crop
> plot sentinel. Use plain accurate prose, preserve the surrounding accepted
> text and all original country caveats. Do not change registration, canonical
> requirements, shape winner, inputs, rows, mappings, test assertions or any
> other semantics.

Only `lsms_library/countries/GhanaLSS/_/CONTENTS.org` and an append to this
ledger may differ from 63eb. The first new commit is ledger-only; the prose
commit and final ledger-only commit are separate. The charter authorizes
continuation after this surfaced checkpoint without another permission gate.

### §2 Reused machinery and static anchors

Search tier: own Git + ripgrep + source text/AST (floor). Reuse all behavior:
`feature.py:434-491` (`_select_kept_shape`) keeps one provider or all agreeing
shapes before considering a canonical competitor, then prefers the canonical
shape if present. `Feature.__call__` applies that selector at `:1307` and
concatenates kept frames at `:1326`. Existing controls at
`tests/test_feature_canonical_index.py:421`, `:472`, `:477` cover the canonical
winner, all-agree and sole-provider cases; they are read only, not rerun.
`data_info.yml:238-240` imposes required axes only on two other features;
`:369-373` declares crop sentinels without `plot_id`. No actual EthiopiaRHS
metadata or other country's metadata/fixture is needed or inspected.

### §3 Definitions and contradictions personally checked

Inherit `STANDING.md` §3/§4, `CLAUDE.md` (actual symlink to `AGENTS.md`) and
canonical `lsms_library/data_info.yml`; do not redefine shape or structural
sentinel semantics. Personally read all 5,854 GhanaLSS CONTENTS lines,
including TODO text; no LOGBOOK drawer or WAITING task occurs beyond the
TODO declaration. Actual idiosyncrasy: Section 9 is household-grain;
"Do not read =Y09A:IDFARM= as a plot identifier" (`:4640`), and Part B
asks no harvest quantity (`:5112-5117`). Preserve those caveats.

CONTRADICTION (preserved, not assumed stale): the earlier record says
"=plot_labor= is deliberately NOT wired" (`:5090`), whereas `:5301-5304`
says "=plot_labor=: hired and exchange labour at household grain" and
"Wired from Section 9 Part D for both panel waves". This revision does not
adjudicate that history. The target's "remains excluded ... when it lacks a
plot axis" is overbroad against the selector's `len(frames) <= 1` and
`len(shapes) == 1` early returns (`feature.py:461-466`); qualification is
confined to the newly added paragraph.

### §4 Invariants and verification boundary

Preserve every other mode/type/blob object, original country caveat, table,
source block, production Python node, config, test and fixture. No import of
`lsms_library`, collection, runtime tests, Make invocation, actual provider,
data getter/build/cache/acquisition, dependency install, shared-cache write,
LSMS_NO_CACHE, GitHub operation or source push. Static package resolution and
the supplied setup attestation establish the intended path without executing
the package. Mounted PROJECT_PYTHON, exact cwd/PYTHONPATH/countries root,
private data/DVC-site bindings and thread/build limits were verified before
source checks. HOME/XDG/CODEX_HOME/SDK guards are unchanged.

The top-level Make recipe was read, not invoked (ROOT explicitly forbids
Make). Its actual stdlib/text/AST script was run directly through the prepared
interpreter: `scripts/api_surface_audit.py --strict`, exit 0. The five required
evidence files were initialized UNFINISHED/UNRUN before long reading. Initial
large text outputs were truncated and recovered by bounded reads; an unset
`git config --get core.hooksPath` returned exit 1 (no configured hook path),
not a failed product test. No new runtime acceptance is claimed.

### §5 Reuse / extend decision

Reuse the current selector, canonical requirements and sentinel registration
without edits. Extend only the paragraph with the canonical-competitor
condition and both retention cases. Verify by the literal two-path diff,
whole-tree mode/type/blob comparison, original-ledger-prefix comparison and
unchanged production ASTs. CONTENTS hashing exclusion is source-supported at
`country.py:1083,1396,4606`; actual hashes/performance were not measured.

### §6 Questions and held work

No blocking question. Original 29 tests and failed P2 review stay historical;
no rerun or reclassification. Named hashed script/scheme/transformations
wording remains held. Independent wording/object closure, final current
composition, CI, GitHub and merge belong to ROOT.
