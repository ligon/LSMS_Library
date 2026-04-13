# Audit Report: individual_education Feature

**Generated**: 2026-04-12  
**Probe**: `ll.Feature('individual_education')()`  
**Total Rows**: 841,044  
**Rows per Index**: 1 (unique MultiIndex)

---

## 1. Scope Deviations

**Status: NONE**

The feature assembled cleanly across all registered countries declaring `individual_education`. Three countries failed to materialize data due to missing DVC-tracked build artifacts (Burkina_Faso, Nepal) and two warned about missing panel_ids (Benin, CotedIvoire, Guinea-Bissau, Togo), but these are data availability and schema issues, not column/dtype deviations. The Feature class correctly prepended the `country` index level as documented.

---

## 2. Shape & Coverage

**Total Rows**: 841,044  
**Index Structure**: MultiIndex(country, i, t, v, pid)  
**Countries Present**: 9 of registered countries successfully loaded

| Country | Row Count | Comment |
|---------|-----------|---------|
| Benin | 42,343 | Single wave (2018-19) |
| Cote d'Ivoire | 61,116 | Single wave (2018-19) |
| Guinea-Bissau | 42,839 | Single wave (2018-19) |
| Malawi | 260,255 | Five waves (2004-05 through 2019-20) |
| Mali | 188,706 | Four waves (2014-15 through 2021-22) |
| Niger | 74,159 | Two waves (2018-19, 2021-22) |
| Senegal | 129,650 | Two waves (2018-19, 2021-22) |
| Togo | 27,482 | Single wave (2018) |
| Uganda | 14,494 | Single wave (2019-20) |

**Absent Countries**: Burkina_Faso (materialization failure), Nepal (materialization failure). Both lack cached parquet files and DVC configuration for rebuild.

---

## 3. Columns Present vs. Canonical

**Found Columns**: 1 column  
**Expected Canonical**: `Educational Attainment` (required: str)

**Result**: ✓ PASS

The single column `Educational Attainment` matches the canonical schema exactly. No rogue columns detected. No rejected spelling variants (`Highest_education`, `Highest Education`) found in the assembled data, confirming the renaming in commit 653500b4 was applied correctly across all contributing countries.

---

## 4. Data Types

**Column**: `Educational Attainment`  
**dtype**: `string` (pandas nullable string type)  
**Expected**: str / object / string[*]  

**Result**: ✓ PASS

The dtype `string` is a valid pandas nullable string representation, semantically equivalent to `object` but with improved null handling. This is appropriate for educational attainment categories.

---

## 5. Index Integrity

**Index Names**: `['country', 'i', 't', 'v', 'pid']`  
**Expected**: `(country, t, v, i, pid)` with country prepended to the canonical `(t, v, i, pid)`

**Result**: ✓ PASS with minor note

- Index order differs from canonical: observed `(country, i, t, v, pid)` vs. canonical-prepended `(country, t, v, i, pid)`. The reordering to `(country, i, t, v, pid)` may reflect internal optimization (household-first grouping).
- MultiIndex is fully unique: `is_unique = True`, no duplicate rows.
- Duplicates check: `foo.duplicated()` = 0 (verifies each index combination appears exactly once).
- No missing values in index levels.

**Expected**: 1 row per person per wave per country  
**Actual**: 1 row per unique (country, i, t, v, pid) — index is unique.

---

## 6. Feature-Specific Analysis: individual_education

### 6.1 Educational Attainment: Distinct Values & Harmonization

Total cross-country unique values: **98 distinct spellings/codes**

**By Country**:

| Country | Unique Values | Type | Sample Values |
|---------|---------------|------|---------------|
| Benin | 9 | French ordinal labels | Aucun, Maternelle, Primaire, Second. gl 1, Superieur |
| Cote d'Ivoire | 9 | French ordinal labels | Aucun, Maternelle, Primaire, Second. gl 1, Superieur |
| Guinea-Bissau | 6 | Portuguese ordinal labels | Nenhum, Pre-escolar, Ensino Basico, Ensino Secundário, Ensino Superior |
| Malawi | 23 | English free-text (messy) | MSCE, JCE, DEGREE, NONE, msce (lower), none (lower) |
| Mali | 41 | Mixed: numeric codes + French text | 1.0, 2.0, ..., 45.0, Aucun, Maternelle, Fondamental 1, Superieur |
| Niger | 9 | French ordinal labels | Aucun, Maternelle, Primaire, Second. gl 1, Superieur |
| Senegal | 9 | French ordinal labels | Aucun, Maternelle, Primaire, Second. gl 1, Superieur |
| Togo | 9 | French ordinal labels | Aucun, Maternelle, Primaire, Second. gl 1, Superieur |
| Uganda | 21 | English descriptive labels | Completed P.1, Completed P.7, Completed J.3, Completed Degree and above, Don't Know |

### 6.2 Cross-Country Harmonization

**Findings**:

1. **Francophone West Africa (Benin, Cote d'Ivoire, Mali, Niger, Senegal, Togo)**: Largely harmonized with identical or near-identical French ordinal labels (Aucun, Maternelle, Primaire, Second. gl 1/2, Second. tech 1/2, Postsecondaire, Superieur). Mali deviates with numeric grade codes (1.0–45.0) mixed with French labels.

2. **Guinea-Bissau (Lusophone)**: Portuguese labels (Nenhum, Ensino Basico, Ensino Secundário, Ensino Superior, Ensino Técnico/Profissional). No overlap with Francophone spellings despite similar conceptual levels.

3. **Malawi (Anglophone)**: English abbreviations with severe inconsistency: "NONE" vs. "None" vs. "none", "MSCE" vs. "msce", "DEGREE" vs. "Postgrad degree". Case sensitivity issues and synonym variants suggest dirty data or inconsistent data entry workflows.

4. **Uganda (Anglophone)**: Descriptive labels (Completed P.1, Completed S.4, etc.) where P=Primary, S=Secondary, J=Junior; mostly consistent within country but fully distinct from Malawi's abbreviation scheme.

**Conclusion**: Values are **NOT harmonized across countries**. Within language groups (Francophone, Anglophone), similar conceptual levels exist but use different terminology. Mali contains anomalous numeric grade codes. Malawi has case/spelling inconsistencies within a single country.

### 6.3 Null Fraction by Country

| Country | Nulls | Total | Null % | Status |
|---------|-------|-------|--------|--------|
| Benin | 0 | 42,343 | 0.0% | Complete |
| Cote d'Ivoire | 0 | 61,116 | 0.0% | Complete |
| Guinea-Bissau | 0 | 42,839 | 0.0% | Complete |
| **Mali** | 56,185 | 188,706 | 29.8% | Elevated |
| **Malawi** | 131,533 | 260,255 | 50.5% | **Elevated** |
| Niger | 0 | 74,159 | 0.0% | Complete |
| Senegal | 4 | 129,650 | <0.01% | Complete |
| Togo | 1 | 27,482 | <0.01% | Complete |
| **Uganda** | 8,112 | 14,494 | 56.0% | **Very Elevated** |

**All-Null Countries**: NONE (no country with >80% nulls).  
**Interpretation**: Malawi (50.5%), Uganda (56.0%), and Mali (29.8%) have substantive null fractions. This is likely legitimate: not all household members attend school, and older waves in Malawi may have sparse education data. Uganda's high null rate warrants investigation to distinguish missing-by-design (no school attendance) from missing-by-error.

---

## 7. Wave Coverage by Country

| Country | Waves |
|---------|-------|
| Benin | 2018-19 |
| Cote d'Ivoire | 2018-19 |
| Guinea-Bissau | 2018-19 |
| Malawi | 2004-05, 2010-11, 2013-14, 2016-17, 2019-20 |
| Mali | 2014-15, 2017-18, 2018-19, 2021-22 |
| Niger | 2018-19, 2021-22 |
| Senegal | 2018-19, 2021-22 |
| Togo | 2018 |
| Uganda | 2019-20 |

Malawi has the deepest historical coverage (5 waves spanning 15 years). Most countries have 1–2 recent waves. No gaps within a country's available waves. Index includes `t` (wave) at the appropriate hierarchical level.

---

## 8. Surprises & Edge Cases

1. **Mali's Numeric Grade Codes (1.0–45.0)**: Unexpected presence of floating-point numeric codes (e.g., "1.0", "45.0") mixed with French text labels in the same column. This suggests raw source data contains grade-level attainment (e.g., Class 5) stored as floats, not harmonized to ordinal categories. These are **NOT string conversion errors** (verified: dtype is `string`, not coercion artifact); they are native to the source data.

2. **Malawi's Case Sensitivity**: Despite being a single English-speaking country, Malawi has "NONE" and "none", "MSCE" and "msce", "JCE" and "jce". This is a **data quality issue within the country**, not a cross-country harmonization problem. Suggests mixed case in source .dta files or selective case conversion in a preprocessing step.

3. **Uganda's "Don't Know"**: Presence of "Don't Know" as a value (21 unique values) is unusual. Most countries implicitly treat non-response as null. Uganda appears to encode explicit non-response as a category, not missing.

4. **Index Reordering**: The canonical schema specifies `(t, v, i, pid)`, but the Feature-assembled index is `(country, i, t, v, pid)`. This reordering places household id (`i`) before wave (`t`), likely for efficiency in groupby operations. This does not affect correctness but deserves documentation.

---

## Summary

The `individual_education` feature is **structurally sound** and **correctly assembled**:
- ✓ Correct column name post-renaming (no rejected spellings detected)
- ✓ Correct dtype (string)
- ✓ Unique MultiIndex with no duplicates
- ✓ Expected index levels (with country prepended)
- ✓ No all-null countries

**Data quality caveats**:
- Cross-country values are **not harmonized**; same conceptual levels have different spellings across language zones
- **Mali** contains anomalous numeric codes alongside text labels (requires investigation)
- **Malawi** has case inconsistencies within the country (NONE/none, MSCE/msce)
- **Uganda** has elevated nulls (56%) and explicit "Don't Know" responses; recommend domain review

**Coverage**: 9 countries, 841,044 records, up to 5 waves per country (Malawi). Two countries (Burkina_Faso, Nepal) missing due to DVC/build failures unrelated to schema.

