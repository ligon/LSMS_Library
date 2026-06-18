# Feature() cross-country audit harness

Systematically evaluate `ll.Feature('foo')` and `ll.Feature('foo')(**kwargs)`
across every declaring country, surface candidate issues, red-team them, and
file the survivors.

This depends on **GH #508** (`Feature.__call__` forwards per-feature kwargs —
`market`/`labels`/`units`/`age_cuts` — by signature introspection). Merged into
`development` on 2026-06-18.

## Two layers

1. **Deterministic wide-net** (`scan.py`) — no agents. Builds every feature and
   a per-feature kwarg grid, runs `diagnostics.is_this_feature_sane` per country
   plus cross-country *assembly invariants*, writes one JSON record per check to
   `results.jsonl`. Cheap; this is the wide net.
2. **Agentic triage + red-team + file** (`audit.workflow.js`, *planned*) — runs
   only on the warn/fail/error subset of `results.jsonl`. Each candidate is
   investigated, then independently red-teamed (mandate: *refute*), and only the
   survivors are filed. See "Downstream" below.

## Issue severity classes (the `severity` field)

| Class | Meaning | Caught by |
|------|---------|-----------|
| **A** | Loud — exception, empty frame, sanity `fail`. | deterministic |
| **B** | Assembly defect — index collapse / unnamed level (GH#325/#326), missing canonical level, row loss vs sum-of-parts. | deterministic |
| **C** | Silent semantic — units mode ~100% NaN, reaggregation total drift, kwarg cardinality change, unknown runtime warning. *Doesn't throw; needs judgement.* | deterministic flag → **agentic triage** |

## scan.py

```bash
# smoke (warm cache, fast)
python bench/feature_audit/scan.py --features food_prices --countries Uganda \
  --out /tmp/fa_smoke.jsonl

# full deterministic sweep (heavy). Cold = authoritative; run from a neutral CWD
# (the .pth pins lsms_library to the main checkout — see CLAUDE.md / REALBUILD).
LSMS_NO_CACHE=1 python /abs/path/bench/feature_audit/scan.py --features canonical \
  --out bench/feature_audit/results/$(date +%F)/results.jsonl
```

Flags: `--features canonical|all|<names…>`, `--countries <names…>`,
`--phases 1,2`, `--limit-countries N` (smoke), `--out`.

**What it checks**

- *Phase 1a* — per country: `Country(c).<feature>()` + `is_this_feature_sane`
  (16 checks: index levels, null index, dup index, float-stringified IDs,
  non-canonical `format_id`, value constraints, …). Build error → Class-A finding.
- *Phase 1b* — `Feature(f)()` assembly: unnamed-level collapse, missing canonical
  core level (market legitimately drops `v`), `market` adds `m`, row conservation
  vs the sum of per-country builds.
- *Phase 2* — `Feature(f)(**kwargs)` over the per-feature grid in `KWARG_GRID`
  (units modes; `labels='Aggregate'`; `market='Region'`; an `age_cuts`):
  units-mode NaN-rate blow-up, `labels='Aggregate'` additive-total conservation,
  kwarg cardinality change vs baseline, ignored-kwarg warnings.

**Output** — `results.jsonl`, one record per check (pass included). Each carries a
stable `fingerprint` = `feature|country|kwargs|check` for downstream dedup. The
warn/fail/error subset is the candidate-finding list.

### Deliberate caveats (no silent caps)

- Couples to a few private helpers (`feature._canonical_index_levels`,
  `diagnostics._PROPERTY_FEATURES`). Stable within the repo; an internal tool.
- **Flag, don't judge.** Row deltas / NaN blow-ups / total drift are emitted as
  `warn` *with the numbers*. Whether each is a bug or by-design (units' documented
  "no silent fallback", the GH#501 additive collapse, no-microdata countries
  Nepal/Armenia/Timor-Leste-2001/Guatemala that *expectedly* warn `Failed to
  load`) is the red-team's call.
- Index-name equality is intentionally **not** asserted: `Feature` legitimately
  widens the index (`currency` level for monetary tables under default
  `currency='index'`; `m` under `market`) and `market` drops `v`. We check only
  unnamed levels and missing canonical core levels.
- Warm vs cold cache: warm sweep is fast but a stale L2 parquet could mask/fake a
  finding. Red-team reproduces survivors **cold** (`LSMS_NO_CACHE=1`) from `/tmp`.

## Downstream (planned `audit.workflow.js`)

Consumes the warn/fail/error records:

- **Phase 3 — triage** (`pipeline`, one agent per finding): read source config +
  transform + contract (CLAUDE.md, `.claude/skills/cross-country-features`,
  CONTENTS.org); classify real-bug / by-design / harness-false-positive; return a
  minimal repro and *a number for every claim*.
- **Phase 4 — red-team** (`parallel`, 2–3 skeptics per survivor, distinct lenses):
  (a) stale-cache — re-run cold from `/tmp`, vanishes ⇒ stale; (b) by-design
  contract; (c) source-truth (read the Stata variable *label*, cross-validate
  values). Default-refute-if-uncertain, majority kills. *Caveat from the skill:* a
  collapse-dismissal that didn't **prove** losslessness is flagged for human eyes,
  not auto-killed (interview_date's visit-drop was wrongly ruled benign once).
- **Phase 5 — file the survivors** ("passes"): per-feature markdown memo +
  `confirmed_issues.jsonl`, then **GitHub issues** —
  - title `[feature-audit] {feature} / {country}: {symptom}`
  - body: classification, exact repro (the `Feature(...)` call + `/tmp` +
    `LSMS_NO_CACHE=1`), every number, red-team verdict + which lenses, memo link,
    and `audit-key: {fingerprint}` for idempotency.
  - labels `feature-audit` + class (`assembly-defect`/`silent-corruption`) +
    `auto-filed`.
  - **dedup**: `gh issue list --search "{fingerprint}"` skips any match *open or
    closed* (closed ⇒ a human already triaged it; don't refile).
  - **dry-run by default**: writes `would_file.jsonl`; actual `gh issue create`
    only under an explicit `--file` flag.

`ISSUES.md` stays human-maintained and is never auto-modified (CLAUDE.md rule).
