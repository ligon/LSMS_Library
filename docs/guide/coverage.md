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
| `not-asked` | Adjudicated: the survey instrument genuinely never asked. Closed. |
| `asked-not-distributed` | Adjudicated: the instrument **did** ask, but the shipped extract does not carry the variables. An *acquisition* problem. |

## Adjudicating an `absent` cell

`absent` alone says only *"the feature is not declared for this wave."* That
conflates states that could not be more different, and until they are separated
the number can never reach zero:

| verdict | meaning | closes the cell? | routes to |
|---|---|---|---|
| `todo` | the data **is** there; nobody wrote the config | no — stays `absent` | `add-feature` |
| `asked-not-distributed` | asked, but the extract lacks it | **yes** | acquisition / `add-wave` |
| `not-asked` | genuinely never asked | **yes** | closed forever |
| `unsure` | a required check could not be run | no — stays `absent` | human / OCR |

Verdicts live in the git-tracked `.coder/coverage/absent_verdicts.csv`:

```csv
country,feature,wave,verdict,checks_run,evidence,adjudicated_by,date
Albania,shocks,2005,todo,C1;C2,Data/migrationE_cl.dta m6e_q00 = 'Type of Shock Code' (10 types),sue,2026-07-12
```

### Evidence is not optional

A **closing** verdict (`not-asked` / `asked-not-distributed`) is a *permanent,
unsupervised write*: it removes a cell from the work queue with nobody
reviewing it. So `load_verdicts()` **refuses** a closing verdict whose
`evidence` field is empty, and warns.

This is not defensive pedantry. It already went wrong:
`Albania/_/data_scheme.yml` asserted *"earlier waves have no shocks module"* —
and Albania 2005's `migrationE_cl.dta` carries `m6e_q00 = 'Type of Shock Code'`
with ten shock types. The claim was false, nobody could catch it (nothing
recorded *how* it was reached), and it silently suppressed work on ~5 cells.

**An unevidenced negative is unfalsifiable, and therefore permanent whether or
not it is true.**

### The four checks

A cell may be closed only when **all applicable** checks were run and all came
back negative. Record which ran in `checks_run`. A check you *cannot* run makes
the verdict `unsure` — silence is never evidence.

1. **C1 — variable/value-label sweep.** Metadata-only reads across every source
   file for the wave. Be extension-agnostic (LSMS ships Stata as `.tab`, SPSS as
   `.sav`) and **search in the instrument's own language** (French, Spanish,
   Portuguese). Distinguish *"no labels found"* from *"the files carry no labels
   at all"* — the latter means the check did not run.
2. **C2 — sibling-wave differential.** Necessary whenever the country has the
   feature for another wave, but **never sufficient**: module vocabularies change
   completely between waves (Iraq's 2007 `"Problem: theft"` shares no words with
   2012's phrasing, yet both waves have the module).
3. **C3 — Harmonized LSMS-ISA Ag cross-check.** `countries/Harmonized_LSMS-ISA_Ag/lsms_ag.parquet`
   (7 ISA countries). **Positive-only**: a non-null harmonized column proves the
   concept was asked; a *null* one proves nothing — it means the WB harmonizers
   didn't code it.
4. **C4 — the questionnaire. Mandatory before any permanent close.**
   *Absence in the shipped `.dta` is not absence in the instrument.* Every
   data-side check can be defeated by a data-*distribution* gap, so no amount of
   data-side evidence may close a cell as `not-asked` — only the questionnaire
   can separate `not-asked` from `asked-not-distributed`, and those route to
   completely different queues.

## Capability: adjudicate at acquisition, not by archaeology

Probing an `absent` cell is expensive, and it is usually rediscovering something
that was knowable the day the survey was acquired. South Africa's General
Household Survey has no consumption module; once it lands,
`South Africa / food_acquired / 2015` will grade `absent`, someone will file it
as a gap, and a probe will eventually establish a fact the WB catalog told us
up front.

So `lsms_library/capability.py` records what a **series** measures at
acquisition time:

```python
SeriesCapability(
    country="South Africa", series="GHS",
    provides=("household_roster", "housing", "individual_education", ...),
    lacks=("food_acquired",),
    validation=CATALOG_ONLY,
    evidence="WB catalog id 2773: topics are 'employment', 'unemployment', "
             "'LABOUR AND EMPLOYMENT', 'DEMOGRAPHY AND POPULATION' -- no "
             "consumption/expenditure topic ...",
)
```

`proposed_absent_verdicts(country, series, waves)` turns each `lacks` entry into
`absent_verdicts.csv` rows — one per `(feature, wave)` — in the schema
`load_verdicts()` already reads. The probe sweep then only has to adjudicate the
cells we inherited *without* a capability record: a shrinking set, not a
permanent tax.

### A capability record may NOT close a cell on catalog metadata

This is the whole discipline, and it is easy to get wrong. A capability asserted
from a **cataloguer's topic list** is not C4. It is good evidence for *where to
look*; it is not evidence that a question was never asked. Closing a cell on it
would be the Albania mistake with better paperwork.

So each record carries a **validation level**, and the verdict is *derived from
it* — never chosen by the caller:

| `validation` | established by | proposes | closes? |
|---|---|---|---|
| `catalog-only` | WB catalog topics / abstract | `unsure` | **no** |
| `data-validated` | C1 label sweep over the shipped extract | `unsure` | **no** |
| `questionnaire-validated` | **C4 — the questionnaire was read** | `not-asked` | **yes** |

`data-validated` deliberately does not close either: a negative label sweep is
exactly as consistent with `asked-not-distributed` as with `not-asked`. That is
precisely why C4 exists.

An `unsure` row is still worth writing — it turns "nobody has looked at this"
into "we have a catalog-level expectation, unconfirmed", and it carries the
evidence forward so the probe does not start cold. It simply cannot close
anything.

**Upgrading** a series from `catalog-only` to `questionnaire-validated` — one RA,
one PDF — is what converts its `unsure` rows into permanent `not-asked`. That is
a bounded human step **per series**, not per cell, and it is the only step that
may close a cell.

`capability.audit()` enforces the invariant; `tests/test_capability.py` proves a
`catalog-only` record cannot close a cell through the real `load_verdicts()`.

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

    !!! note "A scoped run upserts; it does not replace"
        `save_snapshot()` **merges** on `(country, feature, wave)`, so
        `make matrix C="Uganda"` updates Uganda's cells and leaves every other
        country's alone. Before 2026-07-12 it replaced the file wholesale --
        which meant that following this very procedure with `C=` committed a
        67-cell snapshot over the authoritative 1849-cell one. A partial
        measurement must never be able to erase a complete one.
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
