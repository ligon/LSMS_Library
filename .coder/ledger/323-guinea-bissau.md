# GH #323 — Guinea-Bissau (the CONTROL) + the class-level fix

Task: `fix/323-guinea-bissau`.  Branch off `development`.
Cell assigned: `2018-19 / cluster_features`, declared index `(t, v)`, 5,410 rows -> 450.

## §1 What was asked

Close the Guinea-Bissau instance of #323 (`_normalize_dataframe_index` silently
collapsing a non-unique declared index with `groupby().first()`), **without**
leaving the class alive — the failure mode that got #323 closed the first time.

## §2 Instrument validation (done FIRST, per the brief)

A scan of the L2-**country** parquets (`{C}/var/`) is worthless: those are written
POST-collapse, so the evidence is already destroyed.  All scanning ran against the
L2-**wave** parquets.

My first scanner returned a false negative on 16 countries — including Mali —
because `yaml.safe_load` raises `ConstructorError` on the `!make` tag and a bare
`except: continue` swallowed it.  Fixed with a multi-constructor loader; then
calibrated against the two known positives before trusting any zero:

| calibration cell                    | required | measured |
|-------------------------------------|----------|----------|
| Mali/2014-15/household_roster        | 32,026   | **32,026** |
| Guyana/1992/housing                  | 311      | **311**    |

`Country('Mali').household_roster()` legacy per-wave count for 2014-15: **5,149**
— reproducing the brief's own API-level number exactly.

## §3 Diagnosis — Guinea-Bissau is BENIGN (reproduced from the raw .dta)

The 4,960 duplicate rows decompose exactly into two terms, both **byte-identical
repeats**, neither a distinct entity:

- **TERM 1 (4,901)** `cluster_features` builds `df_main` from `s00_me_gnb2018.dta`
  — the household COVER PAGE: 5,351 households, 0 duplicate households, over 450
  grappes.  But it declares only `idxvars: v: grappe`, so it projects a
  household-grained file onto a cluster-grained index and each household emits a
  row carrying ITS CLUSTER's attributes (`Region` s00q01, `Rural` s00q04).  Both
  are **perfectly constant within grappe at source** (verified: 0 grappes with >1
  distinct value on either).  5,351 − 450 = 4,901.
- **TERM 2 (59)** `grappe_gps_gnb2018.dta` has 450 rows but only 445 unique
  grappes: 5 records are byte-identical duplicates (same grappe, vague, Lat, Lon,
  Accuracy, Altitude **and Timestamp** — an export artifact in the WB-released
  file, not two readings).  `merge_on: v` fans those 5 grappes out ×2; they
  contain exactly 59 households.

5,351 + 59 = **5,410** (observed, to the row).  5,410 − 450 = **4,960** (the
reported duplicate count, to the row).

**The collapse is provably lossless.**  `drop_duplicates(FULL ROW)` → 450;
`drop_duplicates(KEY t,v)` → 450.  Equal.  0 clusters carry >1 distinct
`Region` / `Rural` / `Latitude` / `Longitude`.  `groupby().first()` is here the
IDENTITY function.  **rows_recovered = 0 — 450 is already the correct answer.**

Guinea-Bissau's value to #323 is as a **CONTROL**: it is the case that proves the
fix must *dedup-then-verify*, not *dedup-then-guess*.

RULED OUT: not INDEX_INCOMPLETE — adding `i` would turn `cluster_features` (the
table that OWNS `v` per CLAUDE.md) into a household table and break
`_add_market_index` + the v-join for the whole country.  Not PHANTOM_NAN_ROWS
(0 NaN `v`; all 450 clusters present).  Not INTENDED_AGGREGATION (nothing is
aggregated — the values are constant, so there is no reducer to choose).

## §4 The class, measured

Across every cached L2-wave parquet (119 collapsing cells):

| | rows |
|---|---|
| destroyed today by `groupby().first()` | **7,244,929** |
| …byte-identical repeats (collapse LOSSLESS) | 6,783,753 (93.6%) |
| …**CONFLICTING payloads (REAL silent loss)** | **461,176** (6.4%) |

So 93.6% of the collapse is harmless and the existing #323 warning cries wolf over
it — which is part of why the real 6.4% stayed invisible.

## §5 The fix

`country.py::_resolve_duplicate_index` (new), called from
`_normalize_dataframe_index`.  Three paths:

1. **ADDITIVE** (`food_acquired`) — unchanged declared SUM reducer.
   **Deliberately NOT de-duplicated.**  *The trap*: two byte-identical
   `food_acquired` rows are two REAL transactions; dropping one would silently
   HALVE the household's quantity/expenditure.  Benin has 145 such rows.  A naive
   "dedup first" would have turned this fix into a new class-1 (silently wrong)
   bug.  Verified: 14/14 additive cells byte-identical.
2. **DEDUP** (lossless) — drop rows byte-identical on key AND payload.  Provably
   output-identical to `.first()`, which ignores row multiplicity (verified
   empirically).  Guinea-Bissau is entirely this case: 5,410 → 450, silently.
3. **VERIFY** (loud) — anything still duplicated carries CONFLICTING payloads, so
   any collapse destroys information → **raise `DuplicateIndexError`**, naming the
   table, wave, key, disagreeing columns, row count and example keys.
   `LSMS_INDEX_COLLAPSE=warn` restores the legacy lossy behaviour for triage.

Also preserved: `groupby(dropna=True)`'s NaN-key drop (step 1b).  Without it the
dedup path *resurrected* a NaN-keyed row in 2 cells (GhanaLSS 2016-17
`food_security` +1, CotedIvoire 1988-89 `individual_education` +1), changing two
countries I do not own and injecting NaN into the index.  Caught by the regression
harness; see §7.

### Why RAISE, and why that is not overreach
`SkunkWorks/grain_aggregation_policy.org` — the repo's own contract — states:
> "The composition/access path — `country.py` (`_finalize_result` /
> `_normalize_dataframe_index`) … **NEVER reduces grain**."
and names this exact site: *"Both core `.first()` collapses are the GH #323 / #325
silent-data-loss footguns."*  A silent grain reduction here is a contract
violation, and "loudly BROKEN beats silently WRONG."

### Cache poisoning — why the fix actually reaches users
The #323 warning only ever fired on a COLD build; the collapse was then baked into
the L2-country parquet, so the bug hid behind the cache it had poisoned.
`_normalize_dataframe_index` is decorated `@build_transform()`, and
`_build_registry._closure_parts` folds the **normalised AST of the function *and
every global it calls*** into every table's cache fingerprint.  So the new helper
is automatically in the hash: changing the logic invalidates every poisoned
parquet.  **No `LSMS_CACHE_SCHEMA` bump needed** — verified empirically (Mali
raised straight through a warm, poisoned cache).

## §6 `aggregation:` is inert prose (reported, not used)
`aggregation:` is declared in 9+ countries' `data_scheme.yml` and read by
**nothing** (`grep` for consumers: zero).  It is parsed only as a skip-key.  Per
the design doc it belongs to an explicit `transformations.collapse()` (step 4,
unimplemented) — NOT to normalize-time reduction.  So a sibling agent told to
"declare it in `aggregation:`" would ship a no-op.  Deliberately NOT wired in here.

## §7 Regression evidence
Behaviour changes ONLY inside the `not df.index.is_unique` branch, so the blast
radius is exactly the 119 collapsing cells.  Legacy vs new, on the real wave
parquets, all 119:

| | cells |
|---|---|
| IDENTICAL (14 additive + 34 lossless-only) | **48** |
| RAISE (the genuine #323 instances) | 85, across 23 countries |
| **REGRESSIONS (output changed)** | **0** |

## §7b Bonus defect found IN the assigned cell: the `Urbano` leak

Caught by my own invariant test (`test_rural_is_categorical_not_a_raw_code`),
which failed on the first run — a test that only passes is a test that taught
you nothing.

`cluster_features.Rural` was emitting **`Urbano`** for 169 of the 450 clusters.
Guinea-Bissau is LUSOPHONE — raw `s00q04` ∈ {`Rural`, `Urbano`} — but the
`cluster_features` mapping block carried only the FRENCH keys
(`Urbain` / `urbain` / `URBAIN`), copied from a francophone EHCVM sibling.  They
never matched, so the value passed through unmapped, violating the canonical
domain `{Rural, Urban}` declared in `lsms_library/data_info.yml`
(`cluster_features.Rural.spellings`).  The `sample` block in the SAME wave file
already mapped `Urbano` correctly — only this block was missed.

| `cluster_features.Rural` | before | after |
|---|---|---|
| `Rural`            | 281 | 281 |
| `Urbano` (OFF-SCHEMA) | **169** | 0 |
| `Urban` (canonical)   | 0 | **169** |

450 rows before and after.  Fixed in the wave `data_info.yml`; pinned by
`test_rural_is_categorical_not_a_raw_code`.

Minor, NOT fixed (reported): `cluster_features.Region` has inconsistent casing
(`BOLAMA_BIJAGOS` vs `bafata`, `biombo`, …).  `Region` declares no canonical
spellings, so this is not a schema violation — but it will not group cleanly.

## §8 Scoped OUT (with reasons)
- **`feature.py::_collapse_duplicate_index`** (feature.py:106) — the *second*
  `.first()` collapse, named in the same design-doc row (GH #325).  It fires after
  `_harmonize_country_frame` DELIBERATELY drops a level, so a collapse there is
  intended; the doc's prescribed remedy is the union+sentinel redesign, not a
  raise.  Changing it blind would be the "validate where it's free, not where it's
  needed" error.  **Left alone and reported.**
- **Phantom NaN-key rows** — a distinct silent-loss class (GhanaLSS 2016-17
  `food_security`: 110 rows with a NaN household id).  Pre-existing; not made
  worse; reported.
- **Uganda `cluster_features` NaN GPS / 11 GNB clusters with NaN Lat-Lon** —
  separate geo-coverage tickets.
