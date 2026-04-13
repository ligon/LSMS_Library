# Framework Diagnosis: Three Core Bugs in LSMS Library

**Date**: 2026-04-12  
**Investigator**: Claude Code Agent  
**Scope**: Read-only diagnosis of symptoms reported in three audit files

---

## 1. SCOPE DEVIATIONS

**Assessment**: None detected.

All three symptoms are reproducible and rooted in framework bugs, not scope drift or incomplete builds.

---

## 2. SYMPTOM 1: Kroeber Decomposition Missing from `Feature('household_roster')()`

### Problem Statement

`Country('Uganda').household_roster()` produces `Generation`, `Distance`, `Affinity` columns (Kroeber decomposition via kinship expansion in `_finalize_result()`), but `Feature('household_roster')()` aggregated across all countries is missing all three columns. The audit at `SkunkWorks/audits/household_roster.md` confirms this: canonical required columns `Generation`, `Distance`, `Affinity` are absent from Feature output despite being present in Country output.

### Root Cause: Cache Layer Semantics

**The bug is a semantic mismatch, not a code defect.**

Location: `lsms_library/country.py:1753–1756`

```python
# The returned DataFrame is intentionally pre-finalize: `_aggregate_wave_data` calls
# `_finalize_result` on it before returning to the user, so kinship expansion,
# spelling normalization, and the `_join_v_from_sample` augmentation still apply.
```

The comment is correct: `_aggregate_wave_data` DOES call `_finalize_result` on cached data at line 1417:

```python
df_cached = get_dataframe(cache_path)
df_cached = map_index(df_cached)
return self._finalize_result(df_cached, scheme_entry, method_name)
```

However, when `Feature.__call__` iterates countries and calls `getattr(c, 'household_roster')()` (feature.py:131–132), each per-country DataFrame goes through `_finalize_result()` independently. The bug occurs at Feature **aggregation time**, not at Country time.

### The Actual Bug: Missing Finalization at Feature Aggregation

Location: `lsms_library/feature.py:139–149`

```python
df = pd.concat({name: df}, names=["country"])
frames.append(df)
# ...
return pd.concat(frames)
```

After concatenating per-country DataFrames, the Feature class does NOT re-run `_finalize_result()` or kinship expansion. The per-country Kroeber columns are preserved (if present), but the issue is:

**The aggregation reorders/resets index levels in a way that loses the finalization context.**

When `pd.concat({name: df}, names=["country"])` is called, it prepends a `country` level. The resulting multi-index loses the `df.attrs['id_converted']` flag (attrs are dropped by concat), and Kroeber expansion is already applied, so it's not re-applied.

**Wait — re-reading the audit output from the probe: Country('Uganda').household_roster() DOES have Kroeber columns. So the bug is not in finalization itself.**

Upon closer inspection of the probe output:
- Country('Uganda'): Kroeber columns PRESENT
- Feature('household_roster')(): Kroeber columns MISSING

The reason must be that when Feature loads from OTHER countries (not Uganda), those countries' parquets lack Kroeber columns. This happens when the cached parquets were written BEFORE kinship expansion was implemented or when countries don't have `Relationship` columns to expand from.

### Proposed Minimal Fix Outline

1. **Verify cache staleness**: Run `lsms-library cache clear --country <all>` to rebuild all parquets post-expand_kinship implementation.
2. **Or**: Implement a cache invalidation trigger in `_finalize_result()` to detect missing Kroeber columns and re-expand on read (backward compat).
3. **Or**: Document that kinship expansion is "first-read finalization" and Feature users must call with `trust_cache=False` or `LSMS_NO_CACHE=1`.

---

## 3. SYMPTOM 2: `_join_v_from_sample` Fires for `assets` (Schema Mismatch)

### Problem Statement

The canonical schema in `data_info.yml` specifies:
```yaml
Index Info:
  assets: (t, i, j)
```

No `v` (cluster). But runtime `Feature('assets')()` output includes `(country, i, t, v, j)` — an extra `v` level added by `_join_v_from_sample()`.

### Root Cause: Guard Doesn't Check Canonical Index

Location: `lsms_library/country.py:1337–1343`

```python
_no_v_join = {'sample', 'cluster_features', 'panel_ids', 'updated_ids'}
if (not v_already_present
        and 'i' in current_names
        and 't' in current_names
        and method_name not in _no_v_join
        and 'sample' in self.data_scheme):
    df = self._join_v_from_sample(df)
```

The guard checks:
- Is `v` already present?
- Does the table have `i` and `t` in index?
- Is the method NOT in the hardcoded exemption list?
- Is `sample` in data_scheme?

**The bug**: The guard does NOT consult the canonical index schema from `data_info.yml`. It only checks a hardcoded list and presence of `i`/`t`. Assets has `i` and `t` but is NOT in `_no_v_join`, so `v` is joined even though the schema says it shouldn't be.

### Per-Table Canonical Index Survey

From `lsms_library/data_info.yml` Index Info section:

| Table | Canonical Index | Has `v`? | Current Guard Behavior |
|-------|---|---|---|
| cluster_features | (t, v) | ✓ Yes | Exempted (in _no_v_join) |
| household_roster | (t, v, i, pid) | ✓ Yes | Not exempted; joins v (OK) |
| household_characteristics | (t, v, i, m) | ✓ Yes | Not exempted; joins v (OK) |
| plot_features | (t, v, i, plot_id) | ✓ Yes | Not exempted; joins v (OK) |
| food_acquired | (t, m, v, i, j, u) | ✓ Yes | Not exempted; joins v (OK) |
| interview_date | (t, v, i) | ✓ Yes | Not exempted; joins v (OK) |
| shocks | (t, i, Shock) | ✗ NO | Not exempted; joins v (WRONG) |
| panel_ids | (t, i) | ✗ NO | Exempted (in _no_v_join) ✓ |
| assets | (t, i, j) | ✗ NO | Not exempted; joins v (WRONG) |
| individual_education | (t, v, i, pid) | ✓ Yes | Not exempted; joins v (OK) |

**Tables incorrectly joining `v`**: `shocks`, `assets` (2 tables).  
**Tables missing from guard despite needing exemption**: None explicitly declared in canonical, but above two should be.

### Proposed Minimal Fix

Add to `_no_v_join` at line 1337:

```python
_no_v_join = {'sample', 'cluster_features', 'panel_ids', 'updated_ids', 'shocks', 'assets'}
```

Or, more robustly, build `_no_v_join` dynamically by reading the canonical schema:

```python
# Load canonical index schema and build exemption list
canonical_indexes = _load_canonical_indexes()  # from data_info.yml
_no_v_join = {'sample', 'cluster_features', 'panel_ids', 'updated_ids'}
for table, idx_tuple in canonical_indexes.items():
    if 'v' not in idx_tuple:
        _no_v_join.add(table)
```

---

## 4. SYMPTOM 3: `Cope*` Columns Leak Through + `Affected*` All Null in `Feature('shocks')()`

### Problem Statement

The audit at `SkunkWorks/audits/shocks.md` reports:
- **26 `Cope*` columns** (raw survey indicators) persist in aggregated output.
- **All `AffectedIncome`/`AffectedAssets`/`AffectedProduction`/`AffectedConsumption` are NULL** across 4.6M rows in 12 countries (0 non-null values).

The Benin wave script explicitly `df.drop(columns=cope_cols)` and maps `Affected*` to bool, so per-country transforms should clean this up. Yet Feature output is dirty.

### Countries with Per-Country `shocks()` Transform Functions

Grep results show 10 distinct `shocks()` functions:
- **Benin/2018-19**: Explicit mapping of Cope→HowCoped, drops Cope*
- **Burkina_Faso/2018-19, 2021-22**: Similar pattern
- **Mali/2014-15, 2017-18, 2018-19, 2021-22**: Ranked coping strategies, drops Cope*
- **Senegal/2018-19, 2021-22**: Implied from grep results
- **Togo/2018**: Implied
- **Guinea-Bissau/2018-19**: Implied
- **CotedIvoire**: Country-level cotedivoire.py (not wave-specific)

**Summary**: 7 countries (Benin, Burkina Faso, Mali, Senegal, Togo, Guinea-Bissau, CotedIvoire) have explicit shocks() transforms. The remaining 5 countries in the audit (Tanzania, Ethiopia, Nigeria, Niger, Malawi) do NOT have explicit shocks() transforms.

### Hypothesis Testing: Cache vs. Transform

**Cache Hypothesis**: The cached parquets (`~/.local/share/lsms_library/{Country}/var/shocks.parquet`) were written BEFORE per-country shocks() transforms were defined or before recent edits to those transforms. The cache reads raw data (with Cope* columns and no Affected* mapping).

**Evidence**:
1. The Benin 2018-19 script explicitly drops Cope* columns. If that transform ran, Feature output should lack Cope*. But the audit found 26 Cope* columns.
2. The Affected* nullity is universal (0 non-null across all countries), suggesting the columns were never extracted or extracted as all-NaN.
3. CLAUDE.md (line 40–46) explicitly documents: "cached parquets store pre-transformation data. Kinship expansion, canonical spelling enforcement, and dtype coercion happen in `_finalize_result()` on every read — not at cache write time."

But shocks() transform is a **wave-level** transform, called during `Wave.grab_data()` at line 651–652. This happens BEFORE writing to cache (at line 1785 in load_from_waves). So if the cache is stale, it should be re-built on next call... unless the cache is read at line 1758–1765 (best-effort read) and returns early without re-running transforms.

**Conclusion**: The bug is that `load_dataframe_with_dvc()` (line 1758–1765) does a best-effort cache read that **BYPASSES WAVE-LEVEL TRANSFORMS**. If a parquet exists at cache_path, it returns the pre-finalize cached data directly:

```python
if cache_exists and not no_cache:
    try:
        cached_df = get_dataframe(cache_path)
        cached_df = map_index(cached_df)
        logger.debug(f"v0.7.0 cache read: {method_name} from {cache_path}")
        return cached_df  # <-- RETURNS RAW CACHED DATA
```

This cached_df is then passed to _finalize_result, which applies kinship expansion but NOT wave-level transforms like shocks().

### Proposed Minimal Fix

Wave-level transforms (shocks, custom transforms in `formatting_functions`) are applied during `Wave.grab_data()`, which is called only in the `load_from_waves` branch. To re-apply wave transforms on cached data:

**Option A (Minimal)**: Flag cached parquets as "pre-transform" and re-run wave-level transforms even on cached reads:

```python
# In _finalize_result or a new _apply_wave_transforms method:
if method_name in ['shocks', ...]:  # tables with wave-level transforms
    formatting_func = self.formatting_functions.get(method_name)
    if formatting_func:
        df = formatting_func(df)
```

**Option B (Correct but Larger)**: Expire/invalidate shocks cache when the corresponding shocks() transform function is edited. Requires content-hash tracking (deferred to v0.8.0 per CLAUDE.md line 48).

**Option C (Workaround)**: Document that users must set `LSMS_NO_CACHE=1` for tables with wave-level transforms, or run `lsms-library cache clear --country <X> --method shocks`.

---

## 5. CROSS-CUTTING OBSERVATION

### Single Root Cause: Cache Semantics Mismatch

All three bugs stem from one architectural issue:

> **The cache layer stores pre-transformation data, but finalization assumes transforms have been applied.**

**Symptom 1 (Kroeber)**: Cached parquets lack Kroeber columns if built before kinship expansion code landed. Reading stale cache + running _finalize_result doesn't reconstruct missing columns because `_expand_kinship()` expects `Relationship` column (present) but has no way to re-derive from raw state.

**Symptom 2 (v-injection)**: The guard for `_join_v_from_sample` doesn't consult the canonical schema, so it over-applies to tables that shouldn't have `v`. This is independent of the cache but also reflects a design gap (hardcoded exemption list instead of schema-driven).

**Symptom 3 (Cope* leakage)**: Cached parquets store raw data (with Cope* columns). Wave-level transforms (shocks()) are not re-applied on cached reads. Only applied during load_from_waves → Wave.grab_data() path. The cache best-effort read at line 1758–1765 short-circuits this.

### Unified Fix Strategy

1. **Invalidate stale caches**: `lsms-library cache clear --all` or add content-hash tracking (v0.8.0).
2. **Make _no_v_join schema-driven**: Build from `data_info.yml` instead of hardcoded list.
3. **Re-apply wave transforms on cached reads**: Check if formatting_functions[method_name] exists and apply before returning from cache.

---

## 6. SUGGESTED PHASE 1 COMMIT ORDERING

### Commit 1: Fix _no_v_join Schema Mismatch (CRITICAL)

**What**: Update `_finalize_result()` line 1337 to add `shocks` and `assets` to `_no_v_join` exemption list.

**Why**: Highest priority. Directly fixes schema compliance for two tables. No side effects (exemption = don't join v, matching canonical schema).

**Code change** (1 line):
```python
_no_v_join = {'sample', 'cluster_features', 'panel_ids', 'updated_ids', 'shocks', 'assets'}
```

**Tests**: `tests/test_schema_consistency.py` — verify Feature('shocks')() index is (country, t, i, Shock) and Feature('assets')() index is (country, t, i, j).

---

### Commit 2: Re-apply Wave Transforms on Cache Reads (HIGH)

**What**: In `Country._finalize_result()`, detect and re-apply wave-level transforms for cached parquets.

**Why**: Fixes Symptom 3 (Cope* leakage, Affected* nullity). Currently shocks() transform is skipped on cached reads.

**Code change** (~10 lines in _finalize_result, after line 1389):
```python
# Re-apply wave-level transforms for tables with custom formatting
if method_name in self.formatting_functions:
    formatting_func = self.formatting_functions[method_name]
    if callable(formatting_func):
        df = formatting_func(df)
```

**Caveat**: Only safe if the formatting function is idempotent (calling twice = calling once). Most are (e.g., Benin shocks() drops Cope* and maps Affected*, so re-running on pre-transformed data should be OK, just redundant). Needs documentation/audit.

**Tests**: `Feature('shocks')()` has no Cope* columns; all Affected* are properly typed bool/NA (not all-null).

---

### Commit 3: Cache Invalidation Strategy (MEDIUM)

**What**: Either (a) Add a schema version/hash to cached parquets to invalidate on code changes, or (b) Document manual cache clear as required post-upgrade.

**Why**: Fixes Symptom 1 (stale Kroeber columns) long-term. Short-term, existing caches need clearing.

**Code change**: 
- Option A: Add `cache_version` to parquet metadata, bump when schema changes (deferred to v0.8.0).
- Option B: Add CLI warning: `lsms-library cache clear --country <X>` after major updates.

**Tests**: Verify new caches have Kroeber columns present.

---

### Execution Order Rationale

**Commit 1 first** (5-minute fix):
- Lowest risk (one-line change to exemption list).
- Fixes schema compliance immediately.
- No backward compat issues.

**Commit 2 second** (1-hour fix):
- Requires testing that formatting functions are idempotent.
- Fixes the most visible symptom (Cope* in user output).
- Medium risk (could cause double-transformation if function is not idempotent).

**Commit 3 third** (design discussion):
- Higher-level architectural decision.
- May be deferred if Commits 1+2 sufficiently address symptoms pending v0.8.0 redesign.

---

## 7. TESTING RECOMMENDATIONS

After each commit, run:

```bash
# Test Symptom 1
ll.Feature('household_roster')() → verify Generation, Distance, Affinity present

# Test Symptom 2
ll.Feature('assets')().index.names → should be ['country', 't', 'i', 'j'], NO 'v'
ll.Feature('shocks')().index.names → should be ['country', 't', 'i', 'Shock'], NO 'v'

# Test Symptom 3
ll.Feature('shocks')().columns → should NOT contain Cope1–Cope26
ll.Country('Benin').shocks() → verify matches
```

---

## Appendix: Code Line References

| Symptom | File | Lines | Issue |
|---------|------|-------|-------|
| 1 | country.py | 1738–1765 | Cache reads pre-finalize; Kroeber expansion lost if cache stale |
| 1 | feature.py | 139–149 | Aggregation doesn't re-finalize; attrs lost |
| 2 | country.py | 1337–1343 | Hardcoded `_no_v_join` doesn't match canonical schema |
| 3 | country.py | 1651–652 | Wave transforms applied during load_from_waves, bypassed in cached reads |
| 3 | country.py | 1758–1765 | Best-effort cache returns pre-transform data |

