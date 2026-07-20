# Ledger — GH #602: declared spellings are never enforced

**Search tier used:** ripgrep + git (floor), plus a live-API cross-country sweep
(34 countries x every spellings-constrained `(table, column)`). gitnexus not used.
**Inherits:** `.coder/ledger/STANDING.md` §2/§3/§4 — cited, not restated.

---

## §1 Task

`lsms_library/data_info.yml` declares accepted values via `spellings` blocks, but
nothing **enforces** them. `_enforce_canonical_spellings()` maps *known variants*
and never *rejects unknown ones*. Uganda's `sample.Rural` was the literal string
`'0'` for 2,263 households (72% of the 2005-06 wave), so `df[df.Rural=='Rural']`
silently returned **zero rows** — and `is_this_feature_sane(...).ok` was `True`.

Deliverable was the **cross-country sweep**; the check is how you do it.

## §2 Existing machinery reused (did NOT rebuild)

| symbol | path | why I reused it |
|---|---|---|
| `_enforce_canonical_spellings` | `country.py:3931` (STANDING §2) | the variant→canonical map. My check *normalizes through it* before testing membership, so it is correct on pre-finalize parquets **and** post-finalize API frames. |
| `_load_api_derived()` | `diagnostics.py:40` | exact prior-art pattern for reading `data_info.yml` `Columns` in diagnostics (module-level constant, same `(OSError, YAMLError)` handling). `_load_declared_vocabularies()` mirrors it. |
| `Check` / `SanityReport` / `is_this_feature_sane` | `diagnostics.py` | the check is one more `Check` in the existing battery — no new reporting machinery. |
| `spellings` block in `data_info.yml` | STANDING §3 | the *sanctioned* place to declare accepted variants. Mali's `Urbain` / Guinea-Bissau's `Urbano` were fixed by **extending the existing block**, not by writing per-country mapping code. |
| `categorical_mapping.org` auto-discovery | CLAUDE.md | GhanaLSS's `Semi-urban` was a *mislabelled row in an existing mapping table*, fixed there — not by adding a new transform. |

**Did NOT reinvent:** a value-validation layer. `_check_value_constraints`
(`diagnostics.py:368`) already existed — but it reads the *country's*
`data_scheme.yml`, only acts on **list** declarations, and returns `warn`. Every
country declares `Rural: str` (a scalar), so it is skipped entirely. I did not
extend it (its `warn` status is exactly the status quo that let this rot); I added
a sibling that reads the **canonical** `data_info.yml` and returns `fail`.

## §3 Definitions in force (cited)

- Canonical schema = `lsms_library/data_info.yml` (STANDING §3). Tests read it;
  **never hardcode schema rules** — so the check reads `Columns` at runtime.
- The vocabulary for a column is `spellings.keys()`, per the file's own header
  comment ("The canonical values are simply spellings.keys()").
- `Rural` semantics: cluster/HH urban-rural indicator, canonical `Rural`/`Urban`.

## §4 Invariants I had to respect (the landmines)

1. **Key on `(table, column)`, never column name alone.** `housing.Tenure` exists
   in ~12 countries with a legitimately different vocabulary (dwelling tenure) and
   `housing` declares none. A name-keyed check emits ~12 false failures. Pinned by
   `test_housing_tenure_is_not_flagged`.
2. **Do not reuse `country._load_canonical_spellings()`.** Its final `if
   variant_map:` guard **drops** every column whose variant lists are all empty —
   `Affinity`, `Tenure`, `TenureSystem`. Reusing it would silently leave those
   three unchecked. Hence a separate loader. Pinned by
   `test_empty_variant_vocabularies_are_still_checked`.
3. **Normalize before membership.** Cached parquets are pre-`_finalize_result`
   (STANDING §4) and legitimately hold *known variants* (Malawi's `rural`/`RURAL`).
   `tests/test_table_structure.py` reads parquets directly, so a check that tests
   membership without first applying the variant map fails Malawi/sample and every
   other country whose parquet holds a variant. Pinned by
   `test_passes_on_declared_variants`.
4. **`Rural` under `sample:` must NOT be `required: true`.** `required` is read by
   `feature.py:316`; many countries' `sample` has no urban/rural column at all.
5. **Shared cache + concurrent agents.** `~/.local/share/lsms_library` is shared
   (CLAUDE.md scrum-master addendum 1). Other worktrees were concurrently
   rebuilding `sample`/`cluster_features` with *their* config and overwriting my
   parquets (fresh mtime, stale content). All verification was redone under an
   isolated `LSMS_DATA_DIR`. **A parquet-level result from the shared cache is not
   trustworthy while other agents run.**

## §5 What the issue got wrong (verify the diagnosis before fixing it)

- **The Uganda proximate cause is a YAML key TYPE mismatch, not a missing mapping.**
  Raw `urban` is object-dtype but *mixed*: python `int 0` (2263) + `str 'urban'`
  (860). The declared key was the **string** `'0'`, which never matches, so the
  value passed through and was stringified to `'0'`. Commit `9959b9f3`
  (2026-04-14, "close GH #163 item 3") introduced exactly that string key and was
  **silently dead for three months**. The fix is one character: `'0'` → `0`.
- **A second, independent gap the issue does not mention:** `sample` was absent
  from `Columns:` entirely, so `_enforce_canonical_spellings` was a *total no-op*
  on `sample()` — it did not merely fail to reject unknowns, it never mapped the
  *known* variants. That is the Tajikistan bug (9,020 lowercase rows).
- **Uganda is the least severe find, not the only one.** Malawi
  `cluster_features.Rural` was **sign-flipped between waves**; India `sample.Rural`
  was **100% fabricated**.

## §6 Instrument validation (a negative result from an unvalidated check is a
result about the check)

`scratchpad/plant.py`: the check FIRES on planted violations in **columns and
index levels** (`'0'`, `'0.0'`, `'Urbain'`, trailing-space `'Urban '`, bad `Sex`),
is SILENT on clean frames, on known variants, on all-NaN columns, and on the
`housing.Tenure` trap. It also *sees* `Tenure`/`TenureSystem`/`Affinity`, which
the pre-existing loader drops. All 12 cases pass.

## §7 Residual / deliberately not fixed

Five `plot_features` `Tenure`/`TenureSystem` pairs (Albania, Cambodia, Kosovo,
Timor-Leste, Tajikistan) ship raw unharmonized survey labels. The check correctly
**fails** them; they are enumerated in `tests/test_declared_spellings.KNOWN_UNHARMONIZED`
and xfailed, **not** downgraded to `warn`. Mapping them needs the questionnaire
codebooks (Cambodia's "GIVEN BY THE GOVERNMENT OR LOCAL AUTHORITY"; Timor-Leste's
"Part owner", 81% of plots), and guessing would replace a *visibly* wrong value
with an *invisibly* wrong one — the exact class-1 failure this issue is about.

Same reasoning for **India**: `stratum` provably carries no urban/rural information
(perfectly collinear with `state`: UP vs Bihar), so `Rural` is **deleted**, not
invented. Silently-missing (class 2) beats silently-wrong (class 1).

**Pre-existing caveat surfaced, not introduced:** Malawi 2004-05 `ea` is not a
clean cluster key — 19 of 110 EAs contain both rural and urban households, so the
cluster-level aggregate picks one. My change only relabels the code as a word; it
does not alter which household is picked.
