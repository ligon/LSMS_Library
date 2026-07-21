# Workshop Charter — Country × Feature × Wave readiness matrix + HTML readout

> Phase-1 artifact of the `workshop-problem` skill. Agreed with the human BEFORE
> implementation. §2 is the oracle the engine (phases 2–6) loops against. On
> sign-off this is copied to a git-tracked `.coder/charter-coverage-matrix.md`.

## Context (why)
We track work and downstream-readiness as a **country × feature** matrix, but the
real object is a **ragged 3-tensor**: each `(country, feature)` resolves into
per-wave cells, and waves are where readiness actually varies (a feature can be
clean for 6 of Uganda's 8 waves and broken for 2). Today the truth is scattered
across `catalog.py` (declared 2-D matrix), the `bench/feature_audit` harness
(per-country build status, no wave axis), and `slurm_logs/.../COVERAGE_MATRIX*`
(hand-maintained). There is no single, refreshable, consultable readout. This
charter scopes one: a `make matrix` target that grades every enumerable
`(country, feature, wave)` cell and renders a self-contained HTML view, so a
human can answer "what's left to do" and "what's safe to analyse" at a glance.

## §0 Question, restated
Build a low-friction, refreshable view of the `(country, feature, wave)` cube
that tells me, per cell, **a status tier** spanning "no source" → "broken" →
"builds" → "sanity-clean" → "human-blessed", and renders it as a consultable
HTML readout. It must stay correct as features, countries, and waves are added,
and must reuse the existing audit/sanity/catalog machinery rather than reinvent
it.

**Premises challenged in phase 0 (and their resolution):**
- *"A dynamic object that keeps the matrix updated"* vs *"nightly CI"* — these
  conflict because the two questions have opposite cost. **Coverage** (is the
  cell declared / source present) is cheap, config-only, live. **Readiness**
  (does it build clean) is expensive, build-based. **Resolved:** split the cube
  into a live **coverage layer** (always current) and a snapshot **readiness
  layer** (from the last `make matrix`); the readout shows both, timestamped.
- *"Ready" undefined* — **resolved** to a tiered ladder (§2), not a binary.
- *Build the heavy sweep in CI* — **resolved** to a local `make` target first;
  CI/cron deferred (finding: `ci.yml` already holds S3 creds, so GH-Actions is
  feasible later — but out of scope now).

## §1 Scope & non-goals
**In scope (v1):**
- `bench/matrix.py`: enumerate every valid `(country, feature, wave)` cell from
  config; compute a **coverage** tier (no builds) and a **readiness** tier
  (warm-cache builds, graded per wave by slicing the assembled table on `t` and
  running the existing `is_this_feature_sane`).
- A documented **tier ladder** (§2) computed entirely from existing machinery.
- A `make matrix` target (top-level `Makefile`).
- A **self-contained HTML readout** (pandas `Styler.to_html`): country×feature
  grid, per-wave drill-down, colour-coded by tier; non-clean cells link to a GH
  issue / audit fingerprint where one exists.
- A small **git-tracked status snapshot** (CSV/JSONL, one row per cell) as a
  diff-able coverage journal; the HTML is a gitignored artifact.
- A **thin reader** (`ll.coverage()` or similar) returning the status DataFrame:
  live coverage layer + readiness from the snapshot.
- Tests for the new code; prior-art ledger at `.coder/ledger/coverage-matrix.md`.

**Out of scope / non-goals (stops scope creep):**
- Nightly cron / GitHub-Actions wiring, GitHub-Pages publishing. (Deferred; the
  make target + status format must be proven first.)
- *Fixing* any red cell — the dashboard **reports**, it does not repair. Existing
  GH issues remain the worklist; we link to them, not duplicate them.
- Per-wave *independent* builds (`country[wave].feature()`) as the primary path —
  v1 grades by slicing the country-level aggregate (see §5).
- Cross-country label harmonization; any change to feature semantics.
- Reinventing sanity/severity logic — must reuse `diagnostics` / `scan.py`.

## §2 Definition of done — the loop oracle (each line decidable)
The work is complete when ALL hold:

- [ ] **Tier ladder documented & machinery-backed.** A named, ordered ladder —
  `n/a` (country-level-only feature, e.g. `panel_ids`) · `absent` (source not in
  `Wave.data_scheme`) · `dropped` (declared for the wave but its `t` is missing/
  empty in the built table) · `broken` (build raises / whole feature empty) ·
  `builds` (wave slice non-empty but `SanityReport.ok is False`) · `sane`
  (`SanityReport.ok is True`) · `blessed` (`sane` ∧ listed in a git-tracked
  blessing file). *Decidable:* code-review confirms each tier is derived from
  `Wave.data_scheme`, the assembled frame's `t` levels, and
  `diagnostics.is_this_feature_sane` — **no duplicated check logic** (grep).
- [ ] **`make matrix` covers the whole cube.** Produces exactly one tier row per
  enumerable `(country, feature, wave)` cell (plus `wave=None` rows for
  country-level-only features) without crashing on ragged/sparse cells.
  *Decidable:* row count == enumeration count; `panel_ids`/`updated_ids` present
  and not erroring.
- [ ] **Coverage layer needs no data.** Runs under `LSMS_SKIP_AUTH=1` with no S3
  access and still emits every cell's coverage tier (`n/a`/`absent`/declared).
  *Decidable:* a test runs it auth-less and asserts coverage rows exist.
- [ ] **Readiness reuses the warm cache.** A second `make matrix` with no code/
  config change rebuilds **zero** cells (v0.8.0 content-hash). *Decidable:* a
  "0 rebuilt" log line / wall-clock drop on the second run.
- [ ] **Self-contained HTML.** Opens offline; country×feature grid, wave
  drill-down, tier colour-coding; non-`sane` cells deep-link to a GH issue or
  fingerprint where available. *Decidable:* reviewer opens the file; no network
  requests; spot-check a link.
- [ ] **Git-tracked status snapshot.** A small CSV/JSONL keyed
  `country|feature|wave → tier (+ detail)` is written and committed; HTML stays
  gitignored. *Decidable:* file exists, one row per cell, tracked by git.
- [ ] **No regressions.** Full `pytest` green; no public-API change; new code has
  a passing test (`test-generator`). *Decidable:* CI/`make test`.
- [ ] **Accepted by Ethan.** ← judgement gate.

## §3 Constraints & assumptions
- **Budget:** on-demand local `make matrix`; first run heavy (full cube cold),
  re-runs cache-cheap. No nightly budget commitment in v1.
- **Tools:** pandas `Styler.to_html` (already a dep — **no new deps**); reuse
  `lsms_library.catalog`, `lsms_library.diagnostics`, `bench/feature_audit`.
- **Data:** readiness layer uses the warm parquet cache (+ S3 for cold cells via
  the lock-free bypass — never `dvc pull`); coverage layer is config-only.
- **Must not break:** existing tests, public `Country`/`Feature`/`catalog` API.
- **Conventions:** `$(POETRY) run python`, `bench/` artifacts, setup-stamp
  Makefile pattern; pandas-3 rules (`pd.NA`, no `inplace`); honor the
  prior-art-ledger discipline (cite, don't duplicate).

## §4 Links
- **Prior-art ledger:** `.coder/ledger/coverage-matrix.md` (to create in phase 2;
  cites `.coder/ledger/STANDING.md`).
- **Declared matrix:** `lsms_library/catalog.py` (`countries`, `features`,
  `_country_dirs`, `_declared_tables`, `_discover_countries_for_table`).
- **Readiness machinery:** `lsms_library/diagnostics.py`
  (`is_this_feature_sane` → `SanityReport`, `load_feature`, `_PROPERTY_FEATURES`);
  `bench/feature_audit/scan.py` (`Finding`, `_build`, `_scan_one_country`,
  severity A/B/C); `bench/feature_audit/cluster.py`.
- **Wave axis:** `lsms_library/country.py` `Country.waves` (~L1430),
  `Wave.data_scheme` (~L542); derived map `lsms_library/feature.py`
  `_DERIVED_SOURCE`.
- **Render/CI:** `bench/build_feature.py` (HTML precedent), top-level `Makefile`,
  `.github/workflows/ci.yml` (`data-tests` holds S3 creds — future CI path).
- **Prior hand-maintained matrix:**
  `slurm_logs/2026-06-03_session/COVERAGE_MATRIX_other_features.md`.

## §5 Open questions & risks (resolve at the gate or in phase 2)
1. **Per-wave grading method.** *Recommend:* grade by slicing the country-level
   aggregate on `t` (cheap; faithful to what downstream actually gets — a wave
   absent from the aggregate IS unavailable downstream). The pricier
   `country[wave].feature()` per-wave build (catches build-path-only failures) is
   a deferred deeper mode. **Confirm the slice-the-aggregate default.**
2. **`blessed` tier mechanism.** *Recommend:* a git-tracked `.coder/` YAML list
   of blessed cells (may start empty); render the tier regardless. Minimal.
3. **Snapshot location.** *Recommend:* committed `.coder/coverage/latest.csv`
   (journal) + gitignored `bench/results/<date>/matrix.html` (artifact).
4. **Reader scope.** *Recommend:* ship a thin `ll.coverage()` in v1 (live
   coverage + snapshot readiness); fuller object deferred.

---

## Implementation plan (phases 3–6 preview — for the gate)
Engine runs **by hand** (no Workflow opt-in given). Steps:

1. **Ground (phase 2):** write `.coder/ledger/coverage-matrix.md` citing
   `STANDING.md`; confirm the reuse surface above is current (signatures of
   `is_this_feature_sane`, `Wave.data_scheme`, `Country.waves`).
2. **Enumerate (cheap layer):** new `bench/matrix.py` — for each country (
   `catalog._country_dirs`), `Country(c).waves`; per wave, valid cells =
   declared features ∩ `Wave(c,w).data_scheme`; emit `n/a`/`absent`/declared.
   Country-level-only (`panel_ids`,`updated_ids`) → `wave=None`.
3. **Grade (readiness layer):** per `(country, feature)`, build once via
   `diagnostics.load_feature`, slice on `t` per wave, run `is_this_feature_sane`
   → map `SanityReport` to a tier; catch build errors → `broken`; declared-but-
   missing `t` → `dropped`. Carry `detail`/fingerprint for links.
4. **Persist:** write `.coder/coverage/latest.csv` (committed journal) and a
   tidy status DataFrame.
5. **Render:** `Styler.to_html` self-contained grid (country×feature, wave
   drill-down, tier colours, issue links) → `bench/results/<date>/matrix.html`.
6. **Wire:** `matrix:` target in top-level `Makefile`; thin `ll.coverage()`
   reader.
7. **Test (phase 3):** auth-less coverage test; tier-mapping unit tests on
   fixtures; warm-cache "0 rebuilt" assertion. `test-generator` for coverage.
8. **Red-team + review (phases 4,6):** adversarially check tier mis-grading
   (e.g. `dropped` vs `absent`), ragged-cell crashes, link correctness; run
   `code-reviewer`. Loop 3↔4 until §2 green.

## Verification
- `make matrix` → inspect `.coder/coverage/latest.csv` row count == enumeration;
  open `matrix.html` offline; spot-check an `absent`, a `dropped`, a `sane`, and
  a `broken` cell against a known case (e.g. Uganda food_expenditures = `sane`;
  Iraq individual_education pre-#532 = `dropped`).
- `LSMS_SKIP_AUTH=1 make matrix` (or the coverage-only entrypoint) emits coverage
  tiers with no S3.
- Re-run `make matrix`; confirm "0 rebuilt".
- `make test` green.

## Process note
Running the `workshop-problem` skill. Phase 0 (red-team) done; this charter is
phase 1. On sign-off: copy to `.coder/charter-coverage-matrix.md`, create the
ledger, run phases 2–6 by hand (no multi-agent Workflow unless you opt in).
