# EthiopiaRHS canonicalization — design brief

Shared artifact for the `ethiopiarhs-canonicalize` workflow. Agents
implement against THIS spec; do not re-derive the decisions below.
Author: Sue (scrum master). Date: 2026-06-13. Base: `development` @ 5c02cdbc.

## Goal

Push **EthiopiaRHS** (`lsms_library/countries/EthiopiaRHS/`) into the
canonical LSMS country shape — directory structure AND content — even
though the IFPRI Dataverse ships the raw data as a single flat archive
of ~300 cryptically-named `.tab` files (`_dataverse_archive/`, DVC-tracked).

EthiopiaRHS is the Ethiopian Rural Household Survey (ERHS), a **distinct**
survey from the World Bank ESS/LSMS-ISA `Ethiopia` module. Purposive
15-village longitudinal panel, 8 rounds 1989–2009. Source: IFPRI Dataverse
doi:10.7910/DVN/T8G8IV.

## Locked decisions (from the maintainer — do NOT relitigate)

1. **Promote 2004 (R6) and 2009 (R7) to full waves.** They are currently
   `Documentation/`-only stubs, excluded from `Country.waves`. The IFPRI
   archive has NO person roster and NO item-level food for R5–R7 — only
   pre-aggregated household consumption scalars (+ prices, area-output,
   livestock aggregates, hhsize). So "full wave" here means: add them to
   the waves list AND wire the **bespoke `(t,i)` aggregate tables** the
   data actually supports (the same documented HH-level value-table
   pattern already used for `assets`/`livestock`/`income`; see
   `data_scheme.yml` comments and the `_no_v_join` exemption in CLAUDE.md).
   They must NOT fake a roster/food_acquired the data can't support.
2. **Full re-wire to current canon for the already-wired rounds
   (1989–1999).** Don't merely preserve what builds — audit every feature
   config against current conventions: `sample`-`v` join vs `_no_v_join`,
   dtypes (Int64/pd.NA), kinship decomposition, canonical spellings,
   `convert_categoricals` correctness, `format_id` on `myvars`.
3. **Food-item harmonization lives in `categorical_mapping.org`, not a
   standalone `food_items.org`.** EthiopiaRHS already complies
   (`harmonize_food` is in `categorical_mapping.org`; no `food_items.org`
   exists). Encode this as a canonical rule, assert it stays true, and
   migrate any wave-level `food_items.org` if one is discovered.

## Canonical target shape (from add-feature / add-wave skills + Malawi/Ethiopia exemplars)

Per-country `{Country}/`:
- `_/data_scheme.yml` — feature → index/columns (single source of truth).
- `_/{country}.py` — country module incl. df_edit hooks, the explicit
  `waves` list, shared `i` formatter.
- `_/CONTENTS.org` — overview, design decisions, status. **Must be
  internally consistent** (see contradiction note below).
- `_/categorical_mapping.org` — ALL cross-wave label harmonization
  (`harmonize_food`, `harmonize_unit`, region/woreda, relation/sex tables).
- per-feature country scripts only where script-path is required
  (`panel_ids.py` here).

Per-wave `{Country}/{wave}/`:
- `_/data_info.yml` — raw→canonical variable mapping for that wave.
- `Data/` — the source files for that wave, DVC-tracked (`*.tab.dvc` /
  `*.dta.dvc`), sourced from `_dataverse_archive/`.
- `Documentation/SOURCE.org` (+ questionnaires, codebooks) — provenance;
  wave discovery and human reference.

`_dataverse_archive/` stays as the **immutable raw provenance drop**
(do not delete it). Wave `Data/` dirs are populated FROM it. Current
state is inconsistent: some files converted to `.dta`, some left `.tab`,
some waves under-populated.

## Current state (from scrum-master recon 2026-06-13 — don't rediscover, verify)

- `waves = ['1989','1994a','1994b','1995','1997','1999']` (explicit list
  in `_/ethiopiarhs.py`). 2004/2009 excluded.
- Wired features (declared in `data_scheme.yml`): `household_roster`,
  `food_acquired` (→ derived `household_characteristics`,
  `food_expenditures/quantities`), `sample`, `cluster_features`,
  `plot_features` (1994a only), `assets` (1989 R1 bespoke `(t,i)`),
  `livestock`+`income` (1999 R5 bespoke `(t,i)`), `panel_ids` (`!make`).
- **Doc contradiction to FIX**: `CONTENTS.org` header (lines ~6–7) claims
  "auto-discovered … no Waves dict" — STALE; contradicted by the explicit
  `waves` list and the 2026-06-05 decision later in the same file
  (lines ~103–116). Reconcile the narrative to match the code.
- `1994a/Data/` mixes `demo123.dta`, `food1.dta` (converted) with
  `land1all.tab` (raw). `1989/Data/` is raw `.tab`. Normalize.
- Untracked OCR scratch at country root: `EthiopiaRHS/{1989,1999,2004,2009}.txt`
  — relocate into the relevant `{wave}/Documentation/` (or gitignore);
  do NOT leave at country root, do NOT commit raw OCR noise blindly.

## Guardrails (BUILD phase — non-negotiable)

- **Branch**: work on `feature/ethiopiarhs-canonical` cut from
  `development` @ 5c02cdbc, in the MAIN checkout (NOT a worktree — the
  `.pth` pin + package-relative `COUNTRIES_ROOT` make worktree config
  edits invisible to functional builds). **Never commit to / merge into
  `development` or `master`.** Commit every change to the feature branch.
- **DVC writes are SERIAL.** `dvc add` takes the global
  `lsms_library/countries/.dvc/tmp/lock`; never run `dvc add`/`push` in
  parallel. Batch where possible. Reads go through `get_dataframe()`
  (lock-free S3); never `dvc pull`/`fetch` from the CLI.
- **No data loss.** Every `.tab` in `_dataverse_archive/` that is wired
  into a wave must be accounted for; nothing silently dropped. Row counts
  for existing features must not regress vs the baseline captured in the
  Map phase.
- **Stop-list (STOP and report, do not proceed on own judgment):**
  - any file under `tests/` or named `baseline`/`golden`/`expected`/`snapshot`
  - `pyproject.toml`, `poetry.lock`, any dependency pin
  - any country OTHER than EthiopiaRHS
  - framework code under `lsms_library/*.py` (only country config/data changes)
  - deleting `_dataverse_archive/` or any raw provenance
  - weakening/skipping/xfailing any test assertion
- **Report format**: lead with `SCOPE DEVIATIONS` (files touched outside
  `lsms_library/countries/EthiopiaRHS/`), then commit SHA, verification
  output, surprises.

## Verification bar (must pass before I merge)

1. Cold-cache build of EVERY EthiopiaRHS feature for EVERY wave (incl. the
   promoted 2004/2009): `lsms-library cache clear --country EthiopiaRHS`
   then `LSMS_NO_CACHE=1` build via `diagnostics.load_feature`.
2. `is_this_feature_sane(df,'EthiopiaRHS',feat).ok is True` for each
   (only the framework-joined-`v` `index_levels_match_scheme` warn allowed).
3. `pytest tests/test_schema_consistency.py tests/test_covers_all_waves*.py
   -k EthiopiaRHS` (or the wave-coverage test) green — `test_covers_all_waves`
   is the one that previously forced the 2004/2009 exclusion; promoting them
   means `sample()` MUST cover them.
4. No regression in existing feature row counts vs the Map-phase baseline.
5. `CONTENTS.org` internally consistent; `data_scheme.yml` documents every
   new bespoke 2004/2009 table with its index + rationale.

## Workflow role split

- **Map** (parallel, read-only): archive→wave→feature file map; current
  wiring + contradiction audit; canonical-target checklist; 2004/2009/1999
  data-availability deep-dive + baseline row counts.
- **Spec** (1 agent): consolidate Map into a concrete per-wave/per-feature
  implementation plan + file-move manifest.
- **PlanReview** (adversary): red-team the PLAN — data-loss risk,
  `test_covers_all_waves` breakage, canon violations, items contradicting
  documented rationale. Returns go/no-go + required fixes.
- **Build** (1 agent, feature branch, serial DVC, NO merge): implement.
- **RedTeam** (parallel adversaries): attack the BUILD per the
  Verification bar; independent cold-cache builds; data-loss + DVC-integrity
  + scope-violation audit. Returns verdict + residual-risk register.
- **Final review**: Sue (human-in-the-loop) reviews diff + verdicts, decides merge.
