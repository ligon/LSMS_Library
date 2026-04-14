# Audit: `interview_date` Feature

## 1. SCOPE DEVIATIONS

**Status**: MAJOR DEVIATIONS DETECTED.

The canonical schema (`data_info.yml`) specifies:
- **Index**: `(t, v, i)`
- **Column**: `Int_t` (title-case, type: `datetime`)

**Findings**:
- 10 of 11 countries have index `(t, i)` — **missing `v` (cluster) entirely**.
- Mali has index `(t, visit, i)` — non-standard `visit` level instead of `v`.
- **10 of 11 countries use lowercase `int_t`; only Mali uses canonical `Int_t`**.

---

## 2. SHAPE & COVERAGE

**Total rows**: 219,014 rows across all 11 countries.

### Rows per Country
| Country | Rows | Wave(s) |
|---------|------|---------|
| Benin | 8,012 | 1 |
| Burkina Faso | 10,237 | 2 |
| Côte d'Ivoire | 12,992 | 1 |
| Ethiopia | 8,351 | 2 |
| Guinea-Bissau | 5,351 | 1 |
| Mali | 37,125 | 4 |
| Niger | 12,646 | 2 |
| Nigeria | 39,228 | 8 |
| Senegal | 14,276 | 2 |
| Tanzania | 64,625 | 3 |
| Togo | 6,171 | 1 |

**Silently absent countries**: GhanaLSS, Malawi, Nepal — declared in 13 countries' `data_scheme.yml`, but only 11 have cached parquets.

---

## 3. COLUMNS PRESENT VS CANONICAL

**Expected (canonical)**: Single column `Int_t` (title-case), type `datetime`.

**Actual columns found**:
- **`int_t`** (lowercase, string): 10 countries (Benin, Burkina Faso, Côte d'Ivoire, Ethiopia, Guinea-Bissau, Niger, Nigeria, Senegal, Tanzania, Togo)
- **`Int_t`** (title-case, datetime64): Mali only
- **`date`** (rogue column, datetime64[us]): Tanzania only (5.9% non-null; older survey rounds)

**Case sensitivity issue**:
- **Lowercase `int_t` is the spelling leak** mentioned in issue #163 (`sample` feature context).
- This suggests column names are not being normalized/canonicalized before serialization.
- Mali is the only compliant country (using title-case `Int_t`).

---

## 4. DATA TYPES

### `int_t` (lowercase, 10 countries)
- **Storage dtype**: `StringDtype` (pandas nullable string type, not plain `str`)
- **Content**: ISO 8601 datetime strings with time component (e.g., `'2019-06-12T17:17:07'`)
- **Actual datetime**: Parseable to `datetime64` with 100% success rate (except Nigeria: 66% non-null; 91% parse success when present)
- **Issue**: Should be stored as `datetime64[ns]` or `datetime64[ms]`, not string.

### `Int_t` (title-case, Mali only)
- **Storage dtype**: `datetime64[ms]` ✓ (correct)
- **Content**: Proper datetime objects
- **Status**: COMPLIANT with canonical schema

### `date` (rogue, Tanzania only)
- **Storage dtype**: `datetime64[us]`
- **Non-null fraction**: 90.9% (58,716 / 64,625)
- **Date range**: 2008-09-07 to 2016-02-03 (older survey rounds)
- **Issue**: Overlaps with `int_t` column; represents older survey methodology

---

## 5. INDEX INTEGRITY

### Combined Feature DataFrame
- **Total duplicated rows**: 122,789 / 219,014 (56%)
- **Index uniqueness**: NOT unique. Severe duplication.

### Per-Country Index Analysis
| Country | Index Names | Index Levels | Duplicates | Issue |
|---------|-------------|--------------|-----------|-------|
| Benin | (t, i) | 2 | 0 | Missing `v` |
| Burkina Faso | (t, i) | 2 | 0 | Missing `v` |
| Côte d'Ivoire | (t, i) | 2 | 0 | Missing `v` |
| Ethiopia | (t, i) | 2 | 0 | Missing `v` |
| Guinea-Bissau | (t, i) | 2 | 0 | Missing `v` |
| Mali | (t, visit, i) | 3 | 0 | Non-canonical `visit` instead of `v` |
| Niger | (t, i) | 2 | 0 | Missing `v` |
| Nigeria | (t, i) | 2 | 0 | Missing `v` |
| Senegal | (t, i) | 2 | 0 | Missing `v` |
| Tanzania | (t, i) | 2 | 0 | Missing `v` |
| Togo | (t, i) | 2 | 0 | Missing `v` |

**Root cause of duplication**: When `Feature('interview_date')` concatenates across countries and prepends `country` as an index level, the resulting index becomes `(country, t, i)` — a 3-level index. However, the canonical schema specifies `(t, v, i)`. Since `v` is missing from individual country parquets, there is no way to enforce uniqueness. When countries have overlapping household IDs across waves (common in panel surveys), duplication is inevitable.

---

## 6. FEATURE-SPECIFIC: `interview_date`

### Date Ranges per Country (parsed from `int_t` or `Int_t`)

| Country | Min Date | Max Date | Days Span | Issues |
|---------|----------|----------|-----------|--------|
| Benin | 2018-10-05 | 2019-07-28 | 297 | None |
| Burkina Faso | 2018-08-31 | 2022-07-25 | 1,429 | None |
| Côte d'Ivoire | 2018-09-26 | 2019-07-29 | 307 | None |
| Ethiopia | 2014-01-01 | 2022-07-01 | 3,097 | None |
| Guinea-Bissau | 2018-09-20 | 2019-06-28 | 282 | None |
| Mali | 2014-07-02 | 2022-07-24 | 2,983 | None |
| Niger | 2018-10-05 | 2022-08-22 | 1,417 | None |
| Nigeria | 2010-08-01 | 2013-08-29 | 1,095 | 25% missing values; limited date range |
| Senegal | 2018-09-21 | 2022-09-12 | 1,457 | None |
| Tanzania | 2008-09-07 to 2016-02-03 (date col) OR 2019-01-31 to 2022-01-15 (int_t) | Split sources | Two distinct column sources for different waves |
| Togo | 2018-09-25 | 2019-06-22 | 271 | None |

**Out-of-bounds flags**:
- All dates fall within 1985–2026 (library bounds) ✓
- **Nigeria 2010Q3–2013Q1** falls before recent survey waves but is correct for the 2010–2013 LSMS-ISA rounds.
- No anomalies detected (e.g., pre-1985 or post-2026 dates).

### Missing / NaT Fraction
- **Benin through Togo** (except Nigeria): 0% missing (100% complete)
- **Nigeria**: 24.3% missing (9,494 / 39,228 rows have null `int_t`)
- **Tanzania `date` column**: 9.1% missing (5,909 / 64,625 rows); `int_t` column 90.9% missing (58,732 rows)

### Time Zone Handling
- All datetime columns are **naive** (no timezone info) ✓
- Canonical specification does not require timezone awareness.
- Appropriate for survey interview dates without time-zone-specific semantics.

### Case Sensitivity Deep Dive
- **Column name case mismatch**: `int_t` (10 countries) vs. `Int_t` (Mali)
- **This is the leak** referenced in issue #163 (sample feature audit context).
- Likely source: Individual country `interview_date.py` scripts apply different conventions when extracting and naming columns from source `.dta` files.
- The canonicalization step in the library (expected to rename `int_t` → `Int_t`) is either missing or not being applied during Feature aggregation.
- **Both column names are present simultaneously in the combined DataFrame**, proving no normalization is happening.

---

## 7. WAVE COVERAGE

### Declared vs. Actual
| Country | Declared Waves | Unique `t` Values | Match |
|---------|---|---|---|
| Benin | ['2018-19'] | ['2018-19'] | ✓ |
| Burkina Faso | ['2014', '2018-19', '2021-22'] | ['2018-19', '2021-22'] | ⚠️ Missing '2014' |
| Côte d'Ivoire | ['1985-86', ..., '2018-19'] | ['2018-19'] | ⚠️ 4 older waves missing |
| Ethiopia | ['2011-12', '2013-14', '2015-16', '2018-19', '2021-22'] | ['2011-12', '2013-14'] or ['2015-16', '2018-19', '2021-22'] | ⚠️ Partial coverage |
| Guinea-Bissau | ['2018-19'] | ['2018-19'] | ✓ |
| Mali | ['2014-15', '2017-18', '2018-19', '2021-22'] | ['2014', '2017', '2018', '2021'] (with visit repeat) | ⚠️ Label format differs; 'visit' level present |
| Niger | ['2011-12', '2014-15', '2018-19', '2021-22'] | ['2018-19', '2021-22'] | ⚠️ Missing 2011-12, 2014-15 |
| Nigeria | 10 waves (2010Q3–2024Q1) | 8 waves (2010Q3–2013Q1 only) | ⚠️ Missing 4 recent waves |
| Senegal | ['2018-19', '2021-22'] | ['2018-19', '2021-22'] | ✓ |
| Tanzania | ['2008-09', '2010-11', '2012-13', '2014-15', '2019-20', '2020-21'] | 3 values (mixing old `date` col + new `int_t` col) | ⚠️ Mixed sources; incomplete |
| Togo | ['2018'] | ['2018'] | ✓ |

**Note**: Many countries have fewer cached `interview_date` parquets than declared waves. This indicates either:
1. Interview date extraction has not been completed for all waves, or
2. DVC cache is stale / incomplete.

---

## 8. SURPRISES & SUMMARY

### Critical Issues
1. **Column name inconsistency**: `int_t` (lowercase) in 10 of 11 countries, `Int_t` (title-case) in Mali.
   - This directly matches the leak reported in issue #163 (sample feature).
   - Canonical schema is not enforced during Feature aggregation.

2. **Missing `v` (cluster) index level**:
   - All countries except Mali lack the `v` level.
   - Canonical schema specifies `(t, v, i)`, actual is `(t, i)` or `(t, visit, i)`.
   - Results in non-unique combined index when country is prepended (56% duplicates in combined 219K-row DataFrame).

3. **Storage type mismatch**:
   - 10 countries store dates as strings; should be datetime64.
   - Mali correctly uses datetime64[ms].
   - String storage forces parsing on read; inefficient and error-prone.

4. **Multi-source Tanzania**:
   - Two columns (`date` for 2008–2016 rounds, `int_t` for 2019–2022 rounds) suggest methodological shift.
   - Requires careful handling to avoid silent data loss.

5. **Nigeria missing-value spike**:
   - 25% of Nigerian records have null interview_date.
   - Reason unclear; likely a data-collection or extraction issue.

### Index Structural Non-Compliance
- Canonical schema promises `(country, t, v, i)` from Feature().
- Actual result: `(country, t, i)` with no `v` level.
- This violates the contract and prevents proper household-within-cluster-within-wave deduplication.

### Recommendation
- **Immediate**: Normalize all column names to canonical `Int_t` before Feature concatenation.
- **Medium-term**: Extract and add `v` (cluster) to all interview_date parquets where available in source data.
- **Medium-term**: Convert string dates to datetime64[ns] or datetime64[ms] for efficiency and type safety.
- **Long-term**: Audit all countries' interview_date.py scripts to ensure canonical compliance from the start.

---

## Status 2026-04-13

**`int_t` → `Int_t` canonicalisation — RESOLVED.** A two-part fix lands:

1. Commit `ec600dbe` adds `int_t` to `data_info.yml` Rejected Spellings, so `_enforce_canonical_spellings()` renames the column at API time for all countries, no wave-script edits required.
2. Commit `702ef37a` renames `int_t` → `Int_t` in the `myvars` declarations across 6 countries (Benin, Burkina Faso, CotedIvoire, Ethiopia, Guinea-Bissau, Niger) to emit the correct name from the start.
3. Commit `08939f4e` applies the same rename in Togo and Senegal `data_info.yml` files.
4. Commit `2dca104b` rewrites Tanzania `interview_date` to use `round_match` and canonical `i` index.

The `int_t`-vs-`Int_t` case-sensitivity finding from §3 and §8 is resolved. Mali was already canonical; the other 10 countries are now normalized at source and/or via Rejected Spellings enforcement.

**Missing `v` index level** — still open; no framework change to inject `v` into interview_date parquets in this session. Index duplication issue from §5 is unchanged.

**String-typed dates** (10 countries stored as StringDtype instead of datetime64) — still open; dtype coercion not addressed in this session.

**Tanzania dual-source** (`date` + `int_t` columns) — partially improved by commit `2dca104b`; full unification deferred.
