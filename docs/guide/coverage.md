# Coverage Matrix

The library tracks data coverage and downstream-readiness as a ragged
**country × feature × wave** tensor. Each cell gets a *status tier* — from "no
source" through "builds with warnings" to "sanity-clean" — so you can answer two
questions at a glance:

- **Where is work still needed?** (the red/amber cells)
- **Which cells are safe to feed into downstream analysis?** (`sane` / `blessed`)

The grid below is rendered from the committed snapshot
(`.coder/coverage/latest.csv`); it reflects the last time the matrix was built.

## The tier ladder

Each tier is derived from existing machinery — `Wave.data_scheme` (coverage),
the built table's `t` index (drops), and
[`diagnostics.is_this_feature_sane`](../api/coverage.md) (readiness). Nothing
re-implements the sanity checks.

| Tier | Meaning |
|------|---------|
| `n/a` | No per-wave readiness applies (a country-level-only feature like `panel_ids`, or a table with no `t` axis). |
| `absent` | The feature applies to the country, but its source is **not declared for this wave**. An expected gap, not a defect. |
| `declared` | Source present for the wave; readiness not assessed (a coverage-only run). |
| `dropped` | Source **declared** for the wave but the wave is **missing/empty** in the built table — a silent-drop hazard. |
| `broken` | The country-level build raised, or the whole feature is empty. |
| `builds` | The wave slice is non-empty but fails a sanity check (`SanityReport.ok is False`). |
| `sane` | The wave slice is non-empty and passes every sanity check (warnings allowed). |
| `blessed` | `sane` **and** listed in the git-tracked blessing file `.coder/coverage/blessed.csv`. |

## Blessing a cell

`sane` and `blessed` answer different questions:

- **`sane`** — *the automated checks passed.* No human has necessarily looked at
  a single number.
- **`blessed`** — *a human read the actual numbers for this cell and believes
  them.*

For anything feeding published analysis, `sane` is not enough. A feature can
build cleanly, pass every sanity check, and still be quietly wrong — wired to
the wrong source column, or carrying a unit conversion nobody verified.

**The rule: if you used a cell in real analysis and looked at its numbers, bless
it in the same PR.**

```csv
country,feature,wave,blessed_by,date,note
Uganda,food_expenditures,2019-20,ligon,2026-07-12,used in demand estimation; totals reconcile
```

`wave` is blank for country-level features. Only `country`, `feature`, and
`wave` are read by the matrix — `blessed_by` / `date` / `note` are provenance
for humans, and they are what make a blessing auditable rather than a bare
assertion.

Blessings accrete; they are never bulk-seeded. An empty blessing file is honest.
A file full of blessings nobody actually gave is worse than no file at all,
because it would make `blessed` a synonym for `sane` and destroy the only
distinction the tier exists to draw.

!!! warning "Do not put `#` comments in `blessed.csv`"
    `load_blessed()` reads it with `pd.read_csv(..., keep_default_na=False)` and
    no `comment=` argument, so a comment line is parsed as a **row** — a phantom
    blessed cell. Header and data only.

The grid cell shows a worst-first glyph tally over a country's waves
(e.g. `⚠2 ✓5 –1` = 2 `builds`, 5 `sane`, 1 `absent`) and is coloured by the most
actionable tier present.

## Generating / refreshing the matrix

```bash
make matrix              # full readiness cube (heavy first run; cache-cheap after)
make matrix C="Uganda Malawi"   # scope to countries (C=) / features (F=)
make matrix-coverage     # coverage layer only — no builds, no data access
```

`make matrix` writes the committed journal `.coder/coverage/latest.csv` and a
self-contained HTML readout under `bench/results/<date>/matrix.html`.

!!! note "Authoritative runs use a clean cache"
    Readiness is graded by building each feature. A **warm cache** reflects
    whatever is cached, which can surface stale-cache hazards (e.g. a stale
    script-path wave parquet reporting a feature `broken`). For a canonical
    "ready from source" snapshot, build each country from a cleared cache
    (`lsms-library cache clear --country <C>` first). The committed snapshot is
    produced this way.

### Publishing a refreshed snapshot

The page on this site is rendered from the committed `latest.csv` at docs-build
time, and **the docs site only deploys on a push to `master`**. So the full loop
to refresh what readers see is:

1. **Rebuild authoritatively** — clear caches, then build. For the whole cube,
   the per-country clean-cache builds parallelise well (one country per job);
   for a spot refresh, scope with `C=`:
   ```bash
   lsms-library cache clear --country Uganda   # repeat per country, or clear all
   make matrix C="Uganda"                       # omit C= for the full cube
   ```
2. **Commit the journal** (it is tracked despite the global `*.csv` ignore):
   ```bash
   git add .coder/coverage/latest.csv
   git commit -m "data(coverage): refresh matrix snapshot"
   ```
3. **Land it on `master`** via the normal `development → master` flow. That push
   triggers `mkdocs build` + deploy (`ci.yml` `docs` job, gated to `master`), and
   the page re-renders from the new CSV — no data access needed in CI.

### Automated refresh (opt-in, Savio)

The loop above can run unattended via two self-resubmitting Slurm jobs in
`bin/` — used because Savio disables both `scrontab` and user `crontab`, so the
Slurm scheduler itself becomes the cron (each run `sbatch`s its own successor).
They are **off until seeded**:

```bash
# weekly warm/incremental refresh + monthly authoritative (clean-cache) reseed,
# each set to retire on COV_EXPIRES unless renewed:
sbatch --export=ALL,COV_EXPIRES=2026-12-25 bin/coverage_refresh.sbatch
sbatch --export=ALL,COV_EXPIRES=2026-12-25 bin/coverage_reseed.sbatch
```

Properties (see `bin/coverage_lib.sh`): builds in a **dedicated clone** (never
your checkout); **change-gated** (skips the build unless `countries/**` or the
matrix code changed); **connectivity-gated** (most-but-not-all nodes reach
GitHub — a bad node is skipped, never breaks the chain); pushes to `development`.

It **tells you before it retires**: ~21 days before `COV_EXPIRES` it opens a
GitHub issue (which emails you), and this page shows a freshness/expiry banner
that escalates as the date nears. Stop it anytime with
`touch ~/.lsms_coverage_refresh.STOP`; renew by re-seeding with a later
`COV_EXPIRES`. Check the schedule with `squeue -u $USER` (a pending future job).

## Reading it from Python

```python
import lsms_library as ll

ll.coverage()                       # the committed snapshot (last `make matrix`)
ll.coverage(refresh="coverage")     # live coverage layer — config only, no builds
ll.coverage(refresh="readiness")    # recompute the full cube in-process (heavy)
```

See the [Coverage API reference](../api/coverage.md) for details.

## Status snapshot

<!-- COVERAGE_MATRIX -->
