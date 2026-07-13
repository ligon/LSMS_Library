# Prior-Art Ledger — GH #323 (Ethiopia)

**Search tier used:** ripgrep + git floor (gitnexus MCP tools not available in this
worktree session; the affected surface — `country.py` dfs/normalize + the Ethiopia
`_/` config tree — was enumerated exhaustively by grep, so the floor is complete).

## §1 Task, restated
`_normalize_dataframe_index` (`country.py`) collapses a non-unique DECLARED index
with `groupby().first()`, silently discarding the dropped rows. For Ethiopia this
fires on 14 (wave, table) cells / 142,876 duplicate rows. The task is to remove the
duplicates AT SOURCE where they are an extraction artifact, and to DECLARE the
reduction where it is real — never to leave a silent `first()`. Ethiopia is
explicitly *not* the Guyana pattern: in 4 of the 5 affected tables the declared
index is correct for the intended grain and the EXTRACTION manufactures the
non-uniqueness.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `country.py:4215` | reorders/drops index levels, collapses dups | partially | **extend** (honour `aggregation:`) |
| `_ADDITIVE_MEASURE_COLUMNS` | `feature.py:101` | per-table additive cols SUMmed on collapse | yes | reuse as precedent; not extended (food_acquired only) |
| `aggregation` key | `country.py:2387` (`_skip` set) | **reserved but a silent NO-OP** — parsed, never honoured | no | **extend** — wire it up |
| `Wave.grab_data` `dfs:` merge | `country.py:1032` | `pd.merge(..., how='outer')` on `merge_on` | no | **extend** (cartesian guard, `merge_how:`) |
| GH #515 optional-sub-df fallback | `country.py:1000-1022` | swallows KeyError, drops sub-df with a warning | no | **extend** — hard error when the sub-df owns a REQUIRED column |
| `Country._assert_built_required_columns` | `country.py:2381` | required-vs-optional scheme-column parsing | yes | **reuse** — same `_skip` / `optional:` semantics for the new guard |
| `crop_production_for_wave` | `Ethiopia/_/ethiopia.py:950` | builds §9 harvest for all 5 waves | no | **extend** (dedup + declared collapse) |
| `Ethiopia/_/panel_ids.py` | country-level, `materialize: make` | correct household-grain panel_ids | — | reuse; delete the vestigial wave-level YAML block |

## §3 Definitions & conventions in force
- `cluster_features` owns `v`; no other feature may put `v` in its index — `CLAUDE.md`
  "`sample()` and Cluster Identity".
- Required vs optional declared columns: a scheme-entry key is REQUIRED unless
  `optional: true` — `country.py:2387` (`_skip = {index, materialize, backend, aggregation}`).
- `format_id` is auto-applied to `idxvars`, NOT to `myvars` — `CLAUDE.md` "Gotchas".
- Wave-level frames carry the raw `myvars` spelling (`int_t`); the canonical /
  cached country frame carries the declared name (`Int_t`).
- class-2 (silently MISSING) is strictly safer than class-1 (silently WRONG).

## §4 Invariants & assumptions
- **EA-constancy (measured, not assumed):** Region/District/Rural have 0 EAs with >1
  distinct value in ALL five waves (333/433/432/535/435 EAs) ⇒ `first` is provably
  lossless for them. Latitude/Longitude are NOT EA-constant (38/433 W2, 72/432 W3,
  72/435 W5) ⇒ they need a real reducer (`median`).
- **Cartesian ceiling (proof, not heuristic):** for an outer merge where at least one
  side has unique keys, `len(result) <= len(left) + len(right)`. Exceeding that ceiling
  therefore *proves* a many-to-many explosion — so the guard can raise with no false
  positives. `country.py:_merge_subframes`.
- **Sales are broadcast, harvests are not:** §11 sells at (holder, crop) grain, so one
  sale record is attached to EVERY §9 harvest row sharing that (holder, crop_code).
  Summing `Value_sold`/`Quantity_sold` across such rows DOUBLE-COUNTS. Hence the
  two-stage collapse in `crop_production_for_wave` (stage 1 pre-sales, stage 2 post).
- **W5 has no household-level date:** `sect_cover_hh_w5.dta`'s only timestamp column
  (`saq19__Timestamp`) is `**CONFIDENTIAL**` in all 4,959 rows.
- **Warm/cold parity:** the #323 class hid behind the cache it poisoned. Any fix must be
  verified on BOTH the cached and the `LSMS_NO_CACHE=1` path — they disagreed once
  during this task (see Phase 3) and that disagreement deleted a whole wave.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| EA coordinate | **new** (`aggregation: median`) | `_ADDITIVE_MEASURE_COLUMNS` only does `sum`; a location is not additive. Wiring the already-reserved `aggregation:` key is the declared-policy mechanism the schema anticipated. |
| EA Region/District/Rural | reuse (`first`) — but **declared** | measured EA-constant, so lossless; declaring it removes the silence. |
| W5 interview date | **new** (`aggregation: min`) | holder-grain source; `min` = "first contacted". Explicitly not `first` (arbitrary). |
| crop harvest Quantity | **extend** the wave script (SUM at the declared grain) | keeps the collapse a stated policy of the build, not a side effect of index normalization; also the only place that can dedup the raw §9 exact duplicates BEFORE summing. |
| education ids | reuse the roster's ids (`household_id2`/`individual_id2`) | pure extraction fix; `id_walk` + `panel_ids` already map them back to the W1 baseline. |
| panel_ids W2 | reuse country-level `_/panel_ids.py` | the wave-level YAML block was a wrong-source duplicate. |

## §6 Open questions for the human
- **crop_production `u` for W4/W5 multi-entry rows.** Summing assumes the several §9
  entries for one (field, crop) share a unit; where they do not, they remain separate
  `(plot, j, u)` rows (correct, no cross-unit addition). Confirm the survey intent is
  "several harvests of one crop from one field" and not, e.g., separate seasons that
  should stay distinct.
- **W5 `interview_date` coverage is 31.9%** (1,581 of 4,959 households) and cannot be
  improved from the data — the household cover's timestamp is redacted. Confirm that
  a partial table is preferred to no table.

---
### Phase 3 — verification

- `_merge_subframes` (`country.py`) — **OK (anchored on §4 cartesian ceiling)**: raises only
  above a ceiling that is unreachable by any non-cartesian merge; swept all 47 `dfs:`
  wave-files, nothing else trips it.
- `aggregation:` wiring (`country.py`) — **OK (anchored on §2)**: completes a key the parser
  already reserved (`_skip`) and never honoured. No other country declares one, so the
  blast radius is Ethiopia-only.
- Required-column sub-df guard (`country.py`) — **OK (anchored on §3)**: reuses the exact
  required/`optional:` semantics of `_assert_built_required_columns`, so it cannot
  disagree with `test_declared_columns_present`.
- `_collapse_crop_rows` / `_unique_or_na` (`Ethiopia/_/ethiopia.py`) — **OK (anchored on §4
  sales-broadcast)**: the two-stage split exists specifically to avoid double-counting the
  broadcast §11 sale. Verified: total reported §9 harvest is conserved at ratio 1.000 in
  every wave.
- **CONTRADICTION found and fixed during the task (§3/§4):** the first cut of the
  `aggregation:` spec keyed on the wave spelling `int_t` only. The cached country frame
  carries `Int_t`, so the strict "every column needs a reducer" check raised on the WARM
  path only — and the whole 2021-22 wave silently vanished from `interview_date` while the
  COLD path looked perfect. Reducers are now resolved case-insensitively and warm/cold
  parity is asserted. This is the §4 warm/cold invariant catching a live regression.
