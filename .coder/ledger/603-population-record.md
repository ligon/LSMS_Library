# Prior-Art Ledger — GH #603 / #601, the population record

> Per-task ledger. Inherits `.coder/ledger/STANDING.md` (§0 baseline); cites it,
> `CLAUDE.md`, and `lsms_library/data_info.yml` rather than re-copying.

**Search tier used:** ripgrep + git (floor). gitnexus not consulted.
**Line anchors as of:** `328c7d8b` (origin/development).

## §1 Task, restated

Promote the per-`(country, wave)` **universe tag** — the editorial reading
assigned to all 111 wave directories in
`slurm_logs/POPULATION_STATEMENTS_2026-07-21.org` — from prose into *config*,
attach it to returned frames via `df.attrs`, and **warn** (never fence) when a
`Feature()` call pools materially different universes.  The record is a property
of the `(country, wave)` cell, not of the country and not of a table.  Three
fields must travel together: `universe_tag` (editorial), `source_type`
(`local-documentation` | `wb-catalog` | `not-found`) and `confidence`
(`high` | `medium` | `low`).  Nothing is dropped from a `Feature()` result;
@ligon declined #603's `specialized` fence, and declined #603's
`kind: general | general-with-restriction | specialized` vocabulary in favour of
the ten-value universe tag.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Country._finalize_result` | `lsms_library/country.py:2626` | the single post-read pipeline; already sets `df.attrs['country']` | integration surface | **extend** (one call) |
| `Feature.__call__` | `lsms_library/feature.py:424` | cross-country assembly; already re-attaches `attrs` after a drop (`labels_unavailable`) | `tests/test_feature*.py` | **extend** |
| `lsms_library/capability.py` | whole module | the closest precedent: a typed, validated, per-series config record with a `validation` ladder that gates what it may conclude | `tests/test_capability.py` | **pattern reuse** |
| `lsms_library/provenance.py` | whole module | per-wave provenance written into `Documentation/SOURCE.org`, with `PROVENANCE_VALIDATION: content-validated \| catalog-only` | yes | **pattern reuse** |
| `scripts/backfill_wave_provenance.py` | whole file | precedent for a committed, re-runnable doc→config promotion script | — | **pattern reuse** |
| `Country.waves` / `wave_folder_map` | `lsms_library/country.py:1700` | the API wave labels, and the label→directory map for Nigeria PP/PH and Tanzania `2008-15/` | yes | **reuse** |
| `paths.countries_root` | `lsms_library/paths.py:37` | `LSMS_COUNTRIES_ROOT`-honouring config root, `lru_cache`d with `cache_clear` | yes | **reuse** |
| `yaml_utils.load_yaml` | `lsms_library/yaml_utils.py` | the sanctioned YAML reader | yes | **reuse** |
| `_build_registry._EXCLUDED_CALLABLES` | `lsms_library/_build_registry.py:115` | read-path callables kept out of `build_transforms_fingerprint`; **`_finalize_result` is already in it** | `tests/test_null_read_guard.py` | **rely on** |

`lsms_library.population` is **new**: nothing in the library records what a
sample represents (`rg universe --type py lsms_library/` returns three unrelated
hits).  This is the gap #603 names.

## §3 Definitions & conventions in force

- **The universe tag is an editorial reading.**
  `slurm_logs/POPULATION_STATEMENTS_2026-07-21.org` §2, verbatim: *"THE TAG IS AN
  EDITORIAL READING.  IT IS NOT A QUOTE, AND NO DOCUMENT USES IT."*  Controlled
  set of ten values with counts 37/28/18/11/7/5/2/1/1/1 (ibid., §2 "Tag counts").
- **`confidence: low`** means, ibid. §1 "Field schema": *"this is sample-design
  text and should not be laundered into a universe"*.  `medium` means *"this is a
  coverage or representativeness claim, not a universe declaration"*.
- **A coverage claim is not a universe statement** (ibid. §1) — which is why
  `national-all-households` (the document names the population) and
  `national-claimed` (only "National"/"nationally representative" exists) are
  two tags and not one.
- **Exclusions are the point** (ibid. §1 "Field schema"): *"they are what makes
  two 'nationally representative' surveys represent different populations."*
- **`attrs` is dropped by `merge()` / `set_index()` in pandas 2.x/3.x** — see
  `STANDING.md` §4 and `CLAUDE.md` §"Panel ID Transitive Chains and the `attrs`
  Flag" (the BF 2021-22 `id_converted` bug, commit `4db41a27`).
- **Cache-hash inputs**: `Wave._input_hash` (`country.py:612`) and
  `Country._table_cache_hash` (`country.py:2520`) hash `data_info.yml` /
  `data_scheme.yml` **by name**, every `_/*.org` except `CONTENTS.org`, and every
  `_/` file whose suffix is in `_BUILD_INPUT_SUFFIXES = {.py,.csv,.json,.txt,.tab,.tsv}`
  (`country.py:449`).  **`.yml` files other than the two named ones are not
  hashed.**

## §4 Invariants & assumptions (the landmines)

- **DO NOT FENCE.**  `Feature()` must return every country it returns today.
  This is the whole adjudication; a default that drops data is the behaviour the
  exercise exists to fight (#603 "Against:", @ligon's decision).
- **Never record the tag alone.**  `universe_tag` without `source_type` +
  `confidence` launders an editorial reading into a fact.  The loader rejects a
  record missing any of the three (mirrors `capability.audit()`'s invariant).
- **A `population.yml` block must NOT go into `data_scheme.yml`** — that file is
  hashed by name into every table's `_table_cache_hash`, so the promotion would
  cold-rebuild the entire corpus.  §3 above is the measurement that decides it.
- **`_finalize_result` is in `_EXCLUDED_CALLABLES`.**  Attaching there costs no
  cache invalidation — but only as long as the attach point stays inside it.
  Touching `_aggregate_wave_data`, `Wave.grab_data`, or the generated method
  (`country.py:3717`) *would* move `build_transforms_fingerprint` for every
  table (the trap the brief names).
- **`_finalize_result` is contended** — a concurrent agent is editing it for
  weight normalisation.  Keep the diff there to a single call.
- **Wave labels are not directory names.**  Nigeria's `Country.waves` are PP/PH
  round labels (`2010Q3`, `2011Q1`, …) mapping 2→1 onto wave directories;
  Tanzania's four NPS rounds live in one `2008-15/` directory.  `wave_folder_map`
  is the map.  The doc keys by *directory*; config keys by *API wave label*,
  which is what appears in `t`.
- **`t` is not always the wave label.**  Resolution must degrade to "all the
  country's records" rather than to silence, and must say which it did.
- **pandas 3.0** (`STANDING.md` §4): no `inplace=`, `pd.NA` for string missing.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| config location | **new file** `{C}/_/population.yml` | `data_scheme.yml` is cache-hashed by name (§3/§4); a sibling `.yml` is not |
| config reader | reuse `load_yaml` + `countries_root()` | §2; honours `LSMS_COUNTRIES_ROOT` |
| record type + validation ladder | **new**, patterned on `capability.SeriesCapability` | §2 — no existing type carries a universe |
| attach point (country) | extend `_finalize_result` | §4 — the only read-path hook excluded from the build fingerprint |
| attach point (cross-country) | extend `Feature.__call__` | §2 — already re-attaches `attrs` post-concat |
| doc→config promotion | **new** `scripts/promote_population_records.py` | patterned on `scripts/backfill_wave_provenance.py` (§2) |
| warn-time comparability collapse | **new**, warn-time only | @ligon: `national-all-households` + `national-claimed` are the same *target*; the difference is provenance, which `source_type`/`confidence` already carry |

## §6 Open questions for the human

- **Answered by @ligon mid-task**: collapse `national-all-households` +
  `national-claimed` into one `national` class **for the warning only** — never
  in config, `attrs`, or reports.
- **Delegated to me, decided in §7 below**: whether `not-stated` and
  `region-excluded` are "materially different" from `national`.
- **Left open, flagged in the PR**: a `confidence: low` `national-claimed` wave
  (Albania 2012, Niger 2021-22) can license a "homogeneous national" verdict.
  The record is visible in `attrs` and named when the warning fires, but low
  confidence alone does not warn.

## §7 The two delegated judgement calls

**`region-excluded` warns against `national`.**  The doc's own legend says the
exclusions "are what makes two 'nationally representative' surveys represent
different populations"; Ethiopia 2021-22's weighting sentence puts Tigray
outside the *represented* population, and Malawi's Likoma inclusion from IHS4 is
"a real break in the universe … that a naive panel would silently absorb" (§5.3).
That break is exactly #603's motivating case.  The marginal firing cost is near
zero on broad calls (which already fire on `subnational-area` / `specialized`)
and concentrated precisely on the narrow panels where the fact matters — see the
firing measurement in the PR.  The alternative, folding "national minus Tigray"
into `national`, is the laundering §2 of the doc forbids.

**`not-stated` warns, as *unknown* rather than as *different*.**  It is an
absence of information, so the message must not claim the universes differ.  But
treating an undocumented universe as equivalent to a documented national one is
"silence masquerading as knowledge", which the provenance module already refuses
(`local_status='unknown'` with `local=False`) and which the coverage matrix
already refuses (`unsure` keeps a cell in the queue).  Seven waves; bounded cost.

---
### Phase 3 — verification

- `lsms_library/population.py` — OK (new, §5): no existing symbol records a
  universe; validation ladder patterned on `capability.py` per §2.
- `Country._finalize_result` (one added call) — OK (anchored on §4): stays inside
  an `_EXCLUDED_CALLABLES` member; cache hashes measured unchanged (PR body).
- `Feature.__call__` (attrs merge + pool check) — OK (anchored on §2/§4): no
  frame is dropped; the check reads the *kept* frames so it describes what is
  actually returned.
- `scripts/promote_population_records.py` — OK (anchored on §2): mirrors the
  `backfill_wave_provenance.py` precedent; asserts its own output against the
  doc's published tag counts.
