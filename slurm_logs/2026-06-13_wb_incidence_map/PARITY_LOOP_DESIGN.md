# Parity-loop design notes (Stage 4) — captured 2026-06-13

Anchors the construction loop that closes GAP_RANKING.org. Not built yet
(waiting on the incidence map). Records the maintainer's design constraints.

## Work unit vs isolation unit

- **Work unit / loop = (country, feature).** Each ranked gap is one
  construction task: wire feature F for country C.
- **Isolation unit = COUNTRY, not (country, feature).** The files that
  contend are per-COUNTRY shared: `_/data_scheme.yml` (every feature appends
  an entry) and each wave's `_/data_info.yml` (every feature appends a block).
  Two (C, F1) and (C, F2) agents editing C concurrently collide on those
  files, and N per-(country,feature) worktrees that each append to the same
  `data_scheme.yml` produce both-append merge conflicts on recombine
  (the exact pain the food-security PR stack hit). So isolate at the country.

## Concurrency model

- **Across countries → parallel, one git worktree per country** (`feat/parity-{country}`).
  A single working tree can only be on one branch at a time, so country-level
  parallelism REQUIRES separate worktrees. Country branches touch disjoint
  files → clean, conflict-free merge-back (sequential PRs).
- **Within a country → serial features** on that country's worktree branch.
  Removes the data_scheme.yml / data_info.yml contention without per-feature
  worktrees, and honors worker/verifier exclusion on a shared worktree.
- Fallback if worktree overhead isn't worth it for a small push: process
  countries **sequentially** in the main checkout on a feature branch
  (no parallelism, but simplest; verification "just works").

## The .pth / COUNTRIES_ROOT gotcha forces per-worktree venvs

Confirmed on the EthiopiaRHS task: `.venv/.../*.pth` pins to the main
checkout and `COUNTRIES_ROOT = Path(__file__).parent/"countries"` is
package-relative. So a worktree's `data_info.yml` edits are INVISIBLE to a
functional build — `Country(C).F()` runs the MAIN checkout's config.
=> Each per-country worktree needs **its own venv** (`python -m venv` +
editable install) so its `.pth`/COUNTRIES_ROOT resolve to the worktree;
otherwise functional verification is meaningless. Cost ~2 min/worktree,
amortized over all of that country's feature gaps. (If we go the
sequential-main-checkout fallback, no extra venv is needed.)

## DVC discipline (the swamp-avoidance rule) — bake into EVERY agent prompt

Reads are **lock-free by construction** (`local_tools.py:_ensure_dvc_pulled`
bypasses `Repo.fetch`'s `@locked` path and pulls blobs direct from S3; it
"never takes the DVC repo lock"). There is NO queue — there is a path to use
and a CLI to forbid:

> Read ALL source data via `get_dataframe()` / `df_data_grabber()` from
> `local_tools` (lock-free direct-S3). NEVER run `dvc pull`/`fetch`/`add`/
> `push` from the CLI — `dvc pull` re-introduces the global `.dvc/tmp/lock`
> and fails ~92% at 12-concurrent. Verify builds with `LSMS_NO_CACHE=1`
> (reads stay lock-free). Any genuinely new source file that must be
> `dvc add`-ed is SERIAL and human-gated (the write path takes the lock;
> only `_run_dvc_with_lock_retry` may touch it, one at a time).

## Two cautions inherited from the scorecard brief

1. **Aggregation in code, not data.** Their `agg`/index/ratio/valuation
   variables → item-level feature + a `transformations.py` function, never an
   aggregated parquet.
2. **Tasteful decomposition.** A theirs-only wide block → a SET of dataframes
   at natural grains, not a wide mirror.

## ARCHITECTURE USED (2026-06-14 run) — supersedes the worktree plan below

The per-country-worktree design (next sections) was NOT used. The shared
squashfs venv + `.pth`-pinned, package-relative `COUNTRIES_ROOT` make worktree
functional verification impossible without a per-worktree venv (~2 min × 7 ×
every gap — fragile for an unattended run). Instead, organize **by GAP, not by
country**:

- ONE branch per gap (`feat/parity-<gap>`); all 7 country agents run in the
  SHARED main checkout, each editing ONLY its own `countries/{C}/_/`.
- Disjoint per-country files → no contention; agents run NO git (coordinator
  commits after the parallel barrier); functional verification happens in the
  main checkout where the live venv/package sees the edits.
- Gaps run SEQUENTIALLY (merge gap N before gap N+1) so `data_scheme.yml`
  edits never conflict across gaps.

Why this beats worktrees HERE: the work is disjoint-by-country (nothing to
isolate), verification needs the live package anyway, and one venv serves all
agents. Worktrees are only needed when MULTIPLE features of the SAME country
run in parallel (contending on that country's `data_scheme.yml`) — which the
by-gap organization avoids. DVC pushes use `.venv/bin/dvc` (has dvc-s3), NOT
the PATH `dvc`.

Result: 7 gaps merged cleanly, each adversary-verified + cold-build + test-gated.

## Unit #0 (FIRST parity-loop unit) — shared label foundation

Approved by maintainer: extend the harmonized label tables BEFORE any
crop/price feature build, as one coordinated per-country pass — so the
later feature agents don't each extend the same tables (per-country YAML
contention). Two tables, because both `j` (food/crop) and `u` (unit) must be
shared across `food_acquired` ↔ `crop_production` (GAP 1) ↔ `community_prices`
(GAP C) for them to join.

Both tables are Uganda-style org tables: `| Code | Preferred Label | <per-wave
code columns> |`, referenced from `data_info.yml` via
`mappings: ['<table>', '<wave>', 'Preferred Label']`.

**A. `harmonize_food` (the `j` axis).** Per-country sub-case (verified 2026-06-14):
- *Extend* (already in `categorical_mapping.org`): **Mali, Malawi, Nigeria**.
- *Migrate then extend* (standalone `food_items.org` → consolidate into
  `categorical_mapping.org` as `harmonize_food`; this IS the maintainer's
  earlier food_items.org→categorical steer): **Ethiopia, Tanzania, Uganda**.
- *Create* (no food-label table; Niger has a `u` table but no food one): **Niger**.
Target: one `harmonize_food` whose `Preferred Label` spans the UNION of
{consumed-food items (food_acquired), grown crops (crop module), community-price
items (price questionnaire)} — so crops, foods, and priced goods share labels.

**B. `u` table (the unit axis).** The canonical unit-label table is named **`u`**
(NOT `harmonize_unit`) — same `Code | Preferred Label | <per-wave>` shape.
Present for 5 of 7:
- *Extend*: **Mali, Malawi, Niger, Nigeria, Uganda**.
- *Create*: **Ethiopia, Tanzania** (both lack a `u` table; food_items.org only).
(EthiopiaRHS uses a non-canonical `harmonize_unit` — outside the 7; optional
rename to `u` for consistency.) Extend each `u` table's `Preferred Label` to span
{food_acquired units, crop/harvest units, price units}. kg-conversion factors
stay in `transformations` — the `u` table is the *label* axis only.

**Verification bar (Unit #0, per country):**
- `food_acquired` (+ derived `food_prices`/`food_quantities`/`food_expenditures`)
  still builds cold-cache, with NO regression in row counts or label-resolution
  % vs a pre-pass baseline (migrating food_items.org / renaming a unit table
  must be label-preserving for existing foods).
- Report the NEW label coverage added (crop items, price items, their units).
- `is_this_feature_sane(food_acquired).ok` holds.

**Isolation:** this is the FIRST serial step inside each country's worktree
(own venv), before that country's `crop_production` and `community_prices`
builds. No cross-country file overlap (each country edits only its own
`categorical_mapping.org`), so the per-country worktrees merge back cleanly.

## Categorical-harmonization discipline (general, applies to every feature build)

Maintainer principle: **any time a feature surfaces a group of labels whose
cross-wave variation is unimportant, add a `categorical_mapping` table** (a
`Code | Preferred Label | <per-wave>` org table). This is already the de-facto
practice — ~30 such tables exist today (soil, tenure, irrigation, roof, floor,
water, region, district, u, harmonize_food, harmonize_acquire, relationship,
sex, …). Two scoping facts:

- **Multi-level.** Tables live at the COUNTRY level
  (`{C}/_/categorical_mapping.org`, default — for cross-wave harmonization) OR
  the WAVE level (`{C}/{wave}/_/categorical_mapping.org`, when one wave needs
  its own mapping; 7 wave-level files exist today, e.g. GhanaLSS). Default to
  country level; drop to wave level only when a wave genuinely diverges.
- **Shared join-key tables vs feature-local tables.**
  - *Shared* (multiple features JOIN on them → must pre-exist): `harmonize_food`
    (`j`) and `u` (unit). These ARE Unit #0 above.
  - *Feature-local* (a feature's own categoricals → harmonize WITHIN that
    feature's build): reuse where a table exists, add where it doesn't.

New tables the parity features will likely need (surface → add):
- **Crop inputs** (plot_inputs): fertilizer types (urea/DAP/NPK/organic/manure/
  compost), pesticide/herbicide/fungicide types, seed/improved → e.g.
  `harmonize_input` (+ maybe `harmonize_fertilizer`).
- **Livestock species** (livestock): → `harmonize_species`/`harmonize_livestock`.
- **Assets/durables** (assets — maintainer thinks a table may exist; inventory
  shows none today): durable-good item names vary across waves → candidate
  `harmonize_asset`.
- **Plot** (plot_features): reuse `harmonize_soil`/`harmonize_tenure`/
  `harmonize_irrigation`; possibly add erosion-protection.

Rule of thumb for a feature agent: after extraction, list the categorical label
columns; for each with unimportant cross-wave variation, reuse the existing
table or add one (country level by default), referenced via
`mappings: ['<table>', '<wave>', 'Preferred Label']`.

## Per-(country,feature) loop skeleton (when we build it)

For each country C (parallel; own worktree+venv):
  for each ranked feature F in C's gaps (serial):
    1. read GAP_RANKING.org entry for (C,F): target grain, .do construct to consult
    2. consult their .do for construction LOGIC (not their aggregation)
    3. write data_scheme.yml entry + per-wave data_info.yml (or _/{F}.py if script-path)
    4. cold-cache verify: LSMS_NO_CACHE=1 Country(C).F() + is_this_feature_sane
    5. commit to feat/parity-{C}
  -> adversary red-teams the country branch -> Sue reviews -> merge
