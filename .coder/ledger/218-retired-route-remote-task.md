# Prior-Art Ledger — GH218 retired food route cost gate

**Source:** `9514be02e06d360dd4418db67426c2555ddff5be`.
**Search tier:** git/ripgrep/AST floor; no GitNexus tool is exposed. Inherits
`STANDING.md`, `CLAUDE.md` (symlink to the fully read `AGENTS.md`), and the
shared prior-art-ledger skill supplied in the prepared runtime assets.

## §1 Task, restated

Measure the build-fingerprint effect of removing only the retired
`food_prices_quantities_and_expenditures` exception from
`Country._aggregate_wave_data` and its exact dead replacement comment in
`Country.data_scheme`. This is a shared-framework task with **no owned
country**: no real-country constructor, table, source, or build is authorized.
If the exact executable removal moves a build fingerprint, leave all product
code/tests unchanged and deliver the external candidate diff and measured cost
for ROOT scheduling. The charter authorizes continuing after this checkpoint;
no additional routine approval is required.

## §2 Existing machinery

| Symbol / source | Existing behavior / tests | Decision |
|---|---|---|
| `country.py:5779-5797`, `Country._aggregate_wave_data` | ALL-table `@build_transform()` orchestrator; allows retired name outside `data_scheme`; warns and returns empty for other unknown names | Measure exact source edit, do not exclude dependencies |
| `country.py:3116-3150`, `Country.data_scheme` | derives discovery from resources and `_FOOD_DERIVED` / `_ROSTER_DERIVED`; replacement alias is commented after return | Preserve executable node; measure comment-only control |
| `_build_registry.py:375-416`, `_source`, `_norm`, `clear_caches` | inspect source, AST-normalize without comments/docstrings; reset memoization when simulating edits | Reuse actual functions |
| `_build_registry.py:552-604`, `_closure_parts`, `build_transforms_fingerprint` | closes over registered entry points and serializes executable ASTs | Reuse actual functions, invented table labels only |
| `tests/test_build_transform_hash.py:118`, `test_orchestrator_versioned_but_read_path_is_not` | pins orchestrator and nested concat in build closure | Run this existing pure control |
| same file, `test_fingerprint_is_deterministic`, `test_per_table_scoping`, `test_cache_mechanism_is_excluded_but_to_parquet_is_not` | source-free registry controls | Select only tests without real-country metadata/constructors |
| same file:855, `_probe_edit` | uses `inspect.getsource` monkeypatch plus `R.clear_caches()` to measure source changes | Reuse this technique; do not invoke its real-country probe harness |
| `Country._FOOD_DERIVED`, `Country.data_scheme`, derived `__getattr__` dispatch | runtime modern food routes, ordered discovery/materialization | Preserve; synthetic behavior tests only if zero-cost gate permits cleanup |

Read-only source inventory found no tracked retired script, no occurrence of
the retired name in current `data_scheme.yml` / `data_info.yml`, three dangling
Make recipes (Afghanistan, CotedIvoire, Togo), and the two country.py references.
Remaining Python matches in country modules/scripts are historical comments or
docstrings. No country semantics were interpreted or used as fixtures.

Public GH218 refresh: OPEN, zero comments, unassigned, updated
2026-10-25T14:22:19Z. Its body explicitly requires measuring shared callable
cost. PR1021 merged as `5b64a779640db65a18ed7a05232b994e1ad0cd07`; PR1208
merged as `529161c4e270e196ffde3587f11ad1655d39bd99`; both discussions are empty.
Do not repeat their shipped script/registration work. Connector searches are
supplementary discovery only (one query returned issues despite `is:pr`);
retain that limit and refresh through public primary metadata as needed.

## §3 Definitions & conventions in force

Per `STANDING.md` §3/§4 and `AGENTS.md` “Derived Tables”, modern expenditures,
prices, and quantities are runtime products of `food_acquired`, not raw
registrations. Their order, materialization, arguments, frame attributes and
input immutability are outside this cleanup's authority to change.

The relevant actual framework idiosyncrasy is `_aggregate_wave_data`'s
ALL-table closure tag: even the small admission guard's executable AST can
change the build hash. `_norm` says “only logic does” invalidate; comments and
docstrings are stripped. Neither cost is accepted on assertion alone.

Country `CONTENTS.org`, LOGBOOK/WAITING/TODO: N/A, no owned country or actual
country fixture. No contradiction with an owned-country record is claimed.
The food-acquired skill's gap prose says “Several (Serbia, Cambodia) carry a
*legacy* `food_acquired.py` + `food_prices_quantities_and_expenditures.py`”; the
literal tracked-file inventory finds no retired-named script. Record the
contradiction without editing the skill or inferring country data semantics.

## §4 Invariants & assumptions

Inherit `STANDING.md` §4. Use the prepared mounted interpreter, exact cwd,
PYTHONPATH/import identity, declared private config/data/countries roots,
private DVC site cache, absent credentials, BLAS/OMP/MKL=1 and build workers=1.
Preserve HOME/XDG/CODEX_HOME/SDK guards. Max two pytest workers, `--dist=loadfile`,
durable log/JUnit. No dependency install, source acquisition, DVC CLI, actual
Country/Feature/table/build/getter, shared-cache write, cache bypass or rewarm.

Product scope, only after measured zero hash impact: one allowlist entry and
the exact dead comment in country.py plus one focused synthetic test. Preserve
explicitly declared methods, unknown-method warning/empty answer,
`other_features`, `updated_ids`, all modern routes, non-ISA configs/Makefiles,
`.gitignore`, schema, scientific/default/factor/weight/recall behavior.
If nonzero, production and existing tests remain byte-identical to source.
Do not infer a 450-table/corpus count or runtime cost from a small pure probe.

## §5 Reuse decision

| Quantity | Decision | Reason |
|---|---|---|
| normalized callable / closure parts / build fingerprints | reuse | actual `_build_registry` machinery and existing source-edit probe technique |
| candidate text / external diff | new bounded evidence only | exact two-site proposal, not another hasher/census/harness |
| negative and positive controls | reuse/extend in external probe | comment-only and executable no-op controls using same machinery |
| refusal and modern discovery | existing behavior preserved | new meaningful invented-provider tests only if cost gate permits production cleanup |
| full hashes / runtime / warm acceptance | unmeasured | not authorized by source-only finite grant |

## §6 Open questions for ROOT

No routine permission is pending. If nonzero cost is measured, ROOT must
schedule any later cache-invalidating cleanup; this worker must not implement
it or weaken the hash closure. Issue-wide Makefile/docs/gitignore cleanup and
independent integration/CI/review/merge remain ROOT-owned.

## Phase 3 — verification

UNRUN at ledger-first checkpoint. No production changes; cost results, exact
commands, failures, footprint limits and final SHA will be recorded in the five
required external evidence artifacts and this ledger's separate final commit.
