# Prior-Art Ledger — GH218 retired food route

**Source:** `9514be02e06d360dd4418db67426c2555ddff5be`.
**Search tier:** git/ripgrep/AST floor; no GitNexus tool is exposed. Inherits
`STANDING.md`, fully read `AGENTS.md` / same-file `CLAUDE.md` symlink and the
prepared shared prior-art-ledger skill. First new commit was ledger-only
`22c5946a0f5d57e79238cb2a3cf8ab082917901c`, surfaced before task code/probes.

## §1 Task and effective scope

Remove only the retired `food_prices_quantities_and_expenditures` exception in
`Country._aggregate_wave_data` and its dead replacement-comment mapping.
Preserve explicit declarations, the other exceptions and modern food APIs.
There is no owned country and no scientific/default/schema change.

The original zero-impact-only implementation gate is historical. ROOT's
08:21UTC steering explicitly supersedes it: measure/disclose actual cost first,
then implement proven-dead references even with nonzero impact, preserve hash
semantics, no shared rewarm. ROOT's 08:30UTC finite grant additionally authorizes
one base/candidate run of the accepted pure metadata census, including its
`Country(preload_panel_ids=False)` metadata constructors. No serving/source/
Make-data build or actual data API is authorized. Independent integration,
CI, public publication and merge remain ROOT-owned.

## §2 Existing machinery and reuse

| Source | Existing machinery / tests | Decision |
|---|---|---|
| `country.py:5779` at source, `_aggregate_wave_data` | ALL-table `@build_transform()` orchestrator; unknown methods warn/return empty; explicit discovery plus special exceptions admitted | Remove one retired exception only |
| `country.py:3116`, `Country.data_scheme` | ordered resource-based discovery plus `_FOOD_DERIVED` / `_ROSTER_DERIVED`; dead comment follows return | Remove retired comment mapping, preserve executable node |
| `_build_registry.py:375-416`, `_source`, `_norm`, `clear_caches` | source/AST normalization strips comments/docstrings; explicit memo reset for source-edit probes | Reuse unchanged |
| `_build_registry.py:552-604`, `_closure_parts`, `build_transforms_fingerprint` | executable callable closure and per-table fingerprint | Reuse unchanged; no exclusions added |
| `tests/test_build_transform_hash.py::_probe_edit` | inspect.getsource source-edit seam plus clear_caches | Reuse technique for exact candidate/negative/positive controls |
| same file, orchestrator/self-method/determinism/scoping/tag/cache controls | existing source-free registry assertions | Seven existing controls run unchanged |
| accepted `1501-current686-release-proposal-20261108/{hash-census,run-hash,prepare-private-hash,compare-metadata,layout}.py` | guarded source-free complete hash-map/nonhash comparison | Reuse once per base/candidate; current path/head/runtime substitutions only |
| `Country._FOOD_DERIVED`, `_materialization_entry`, generated dispatch | existing transforms, public discovery/order/materialization | Reuse actual code on invented provider leaves |

Literal source inventory: no tracked retired script, no retired-name declaration
in current data_scheme/data_info, exactly three remaining missing-script Make
recipes (Afghanistan, CotedIvoire, Togo). Remaining Python references outside
country.py are historical comments/docstrings or diagnostic strings. Search
included all tracked declarations/Makefiles/callers; large historical log hits
are retained separately and are not classified as live callers.

Current GH218 is OPEN, zero comments, unassigned, updated 2026-10-25. PR1021
merged `5b64a779640db65a18ed7a05232b994e1ad0cd07`; PR1208 merged
`529161c4e270e196ffde3587f11ad1655d39bd99`; both discussions empty. Corrected
public exact-route PR search returns only #1021, no newer matching fix. A
connector query that ignored is:pr is recorded, not treated as exhaustive.

## §3 Definitions and grounding

Per `STANDING.md` §3/4, `AGENTS.md` “Derived Tables” and canonical
`lsms_library/data_info.yml`: modern food derivatives are runtime products of
food_acquired, not raw registrations. No transform, unit, factor, weight,
stratum, recall, taxonomy or default changes are part of this cleanup.

Actual framework idiosyncrasy: the admission guard lies inside an ALL-table
build-transform closure; even this small executable deletion moves shared
hashes. The normalized executable node, not raw comment text, determines cost.
Country CONTENTS/LOGBOOK/WAITING/TODO is N/A to the invented fixtures. The finite
census grant permits metadata enumeration only, without interpreting country
semantics. The food skill says “Several (Serbia, Cambodia) carry a *legacy*
`food_acquired.py` + `food_prices_quantities_and_expenditures.py`”; literal
tracked-file inventory contradicts the retired-script-availability statement.
That contradiction is reported without rewriting country records or the skill.

## §4 Invariants

Inherit `STANDING.md` §4. Prepared mounted PROJECT_PYTHON; exact cwd/PYTHONPATH/
COUNTRIES_ROOT/import identity and source heads. Ordinary private config/data,
absent credentials, private DVC site cache; HOME/XDG/CODEX_HOME/SDK guards
preserved. BLAS/OMP/MKL/build workers=1; pytest at most two loadfile workers
with durable logs/JUnit. No install, cache bypass, DVC CLI, actual data serving,
raw-source acquisition, shared-cache write/rewarm or source/GitHub push.

Census-only metadata exceptions use clean private baseline/candidate checkouts,
fresh explicit 0700 empty config/data/nonsymlink-L1/DVC-site roots before import,
network isolation and the accepted audit/profile source/build refusals. Tracked
conversion CSV metadata blob is verified against current9514, not an old map.

Production ownership is country.py only: one exception and one dead mapping.
Preserve explicitly declared retired/custom names, unknown-method warning/empty
return, other_features/updated_ids, discovery order, materialization, kwargs,
attrs and input immutability. Country configs/Makefiles, skills, AGENTS,
.gitignore, schema, registry/exclusions and every other production path stay
byte-identical. Existing tests/fixtures/assertions/skips/tolerances unchanged.

## §5 Measured reuse and cost

The actual registry controls establish 4/4 selected fingerprints changed for
allowlist-only/combined, 0/4 for comment-only/repeat, 4/4 for executable positive.
Exactly one callable closure part changes: Country._aggregate_wave_data. The
actual candidate bytes/fingerprints match the pre-edit source simulation.

The accepted full comparison measures **broad next-access invalidation at both
harmonized tiers**, across all 38 registered countries: 450/450 country-table
hashes, 1877/1895 wave hashes (18 identical None), 35/35 build fingerprints move;
0/186 framework-import fingerprints move. All 14 nonhash fields are exact,
including 553 public country-method/2313 wave-method cells, physical folders,
28 override entries and 0 unsupported entries. Both actors exit0, errors0,
denials0, clean source; private config/data roots remain empty, mode0700.
No rewarm ran. Census elapsed time is not rebuild or user-facing performance.

## §6 Remaining ROOT-owned work

Independent review/current integration/CI/publication/merge and actual rebuild
scheduling. GH218 remains open: lsms_library/countries/Togo/_/Makefile:15,
lsms_library/countries/CotedIvoire/_/Makefile:18,
lsms_library/countries/Afghanistan/_/Makefile:15 still invoke the retired missing script; food-acquired
skill:310-314 still names legacy availability; AGENTS:775 is issue-mentioned
historical prose; .gitignore:97-101 retains its unitlabels exception. Optional
missing-script recipe guard remains separate. No compatibility policy for
unsupported declarations is invented; explicit declaration admission stays.

## Phase 3 — anchored verification

Code/tests freeze: 05272dd902146e31d455c9b779a81713117f3186, tree
`bacafc62ec1fdc61c32c09a213d4ad40c7932590`, identical to the clean census
snapshot `b2f1f7542b82178c490d9f75efdb7593fd93506f`. Final ledger commit is separate.

- OK (§2/4): 12 new invented-provider cases on original9514: one expected
  refusal failure, 11 passes. Candidate plus seven unchanged pure registry
  controls: 19 passes, zero failures/skips. All original tests untouched.
- OK (§3/4): declared retired/custom methods and remaining exceptions route;
  unknown warning/empty behavior, ordered modern discovery/materialization,
  native food arguments/dispatch, attrs and immutable inputs are controlled.
- OK (§2/5): four callable controls and the separately granted complete census
  use existing hash machinery. All counts above are executed, not inferred.
- OK (§4): source proof compares 218 function AST nodes: only the aggregate
  node changes, other217 identical; data_scheme executable AST and every
  other production path unchanged. Staged code tree equals census snapshot.
- OK (§2): strict API audit direct recipe exits0 before/after. make exits2
  because poetry is missing (recipe127); no install. Two dependency
  FutureWarnings remain. The expected baseline failure is not relabeled green.

Evidence is in the dispatch's required five exact external artifacts plus
logs/JUnit, original/candidate source/AST/closure/hash controls, full census
maps/operator context diff/receipts/comparison. Full CI, actual data/API/Feature/
warm compatibility, independent acceptance and rebuild duration are unexecuted.
