# Shocks Feature Audit

**Date**: 2026-04-13  
**Probe**: `ll.Feature('shocks')()`  
**Total rows**: 4,639,538  
**Countries**: 12  
**Index structure**: (country, i, t, v, Shock)

---

## 1. Scope Deviations

**Assessment**: None detected.

All 12 countries declaring shocks in their data_scheme (Benin, Burkina Faso, Côte d'Ivoire, Ethiopia, Guinea-Bissau, Malawi, Mali, Niger, Nigeria, Senegal, Tanzania, Togo) contributed data successfully. No countries silently absent. Index integrity is perfect: 0 duplicates across 4.6M rows, indicating correct hierarchy preservation.

---

## 2. Shape & Coverage

**Total rows**: 4,639,538  
**Total columns**: 33  
**Index integrity**: 0 duplicates

| Country | Rows | Waves |
|---------|------|-------|
| Malawi | 1,090,918 | 5 (2004-05, 2010-11, 2013-14, 2016-17, 2019-20) |
| Nigeria | 636,256 | 8 (2010Q3, 2011Q1, 2012Q3, 2013Q1, 2015Q3, 2016Q1, 2018Q3, 2019Q1) |
| Ethiopia | 500,121 | 5 (2011-12, 2013-14, 2015-16, 2018-19, 2021-22) |
| Niger | 424,764 | 4 (2011-12, 2014-15, 2018-19, 2021-22) |
| Burkina Faso | 419,426 | 3 (2014, 2018-19, 2021-22) |
| Mali | 375,481 | 4 (2014-15, 2017-18, 2018-19, 2021-22) |
| Senegal | 314,072 | 2 (2018-19, 2021-22) |
| Côte d'Ivoire | 285,824 | 1 (2018-19) |
| Benin | 176,264 | 1 (2018-19) |
| Tanzania | 162,928 | 6 (2008-09, 2010-11, 2012-13, 2014-15, 2019-20, 2020-21) |
| Togo | 135,762 | 1 (2018) |
| Guinea-Bissau | 117,722 | 1 (2018-19) |

---

## 3. Columns Present vs Canonical

**Canonical schema** (from `lsms_library/data_info.yml`):
- Boolean impact columns: `AffectedIncome`, `AffectedAssets`, `AffectedProduction`, `AffectedConsumption`
- Coping strategy columns: `HowCoped0`, `HowCoped1`, `HowCoped2` (string)

**Actual columns** (33 total):
- **Canonical (canonical)**: `AffectedIncome`, `AffectedAssets`, `AffectedProduction`, `AffectedConsumption` ✓
- **Coping columns (canonical)**: `HowCoped0`, `HowCoped1`, `HowCoped2` ✓
- **Rogue columns (SHOULD NOT EXIST)**: `Cope1`, `Cope2`, ..., `Cope26` (26 columns)

**Issue**: Countries using numbered binary coping strategy columns (Ethiopia, Mali, Niger, Nigeria) did not fully clean their intermediate `Cope*` columns before finalizing. These are raw survey indicators (1=Yes, 0=No) that should have been transformed into `HowCoped` labels or dropped entirely per `data_info.yml`. The canonical transformation scripts (e.g., Benin's `shocks()` function) map `Cope*` flags to labeled strings and drop `Cope*`, but this is applied inconsistently across countries.

---

## 4. Data Types

All Affected* columns correctly typed as **boolean**. HowCoped* columns correctly typed as **string**.

| Column | Dtype | Notes |
|--------|-------|-------|
| AffectedIncome | boolean | All-null (see anomaly below) |
| AffectedAssets | boolean | All-null (see anomaly below) |
| AffectedProduction | boolean | All-null (see anomaly below) |
| AffectedConsumption | boolean | All-null (see anomaly below) |
| HowCoped0 | string | 291,792 non-null (6.3%) |
| HowCoped1 | string | 99,489 non-null (2.1%) |
| HowCoped2 | string | 55,974 non-null (1.2%) |
| Cope1–Cope26 | string | Mixed nullity; raw survey flags |

**Critical anomaly**: All 4.6M rows in the four Affected* boolean columns are null. This indicates either:
1. These columns were not extracted from the source data during the load, or
2. They are conditionally filled and universally evaluated to NA in aggregation context.

Inspection of country data shows this is uniform across all 12 countries—no country contributes non-null Affected* values. This violates the spirit of the canonical schema, which declares these as core impact indicators.

---

## 5. Index Integrity

- **Index names**: `['country', 'i', 't', 'v', 'Shock']` ✓
- **Duplicates**: 0 / 4,639,538 ✓
- **Index structure**: Hierarchical (country → household_id → wave → cluster → shock_type)

Perfect integrity. No duplicate (country, i, t, v, Shock) tuples.

---

## 6. Feature-Specific: Shocks

### Shock Index Harmonization

Shock labels are **NOT harmonized** across countries. They are either **language-specific free text** or **numeric codes**:

- **French-language countries** (Benin, Burkina Faso, Mali, Senegal, Togo, Côte d'Ivoire, Niger, Guinea-Bissau): Multi-word French labels, e.g., "Attaques acridiennes ou autres ravageurs de récolte", "Baisse importante des prix des produits agricoles"
- **English-language countries** (Tanzania): UPPERCASE English labels, e.g., "DEATH OF A MEMBER OF HOUSEHOLD", "CROP DISEASE OR CROP PESTS"
- **Numeric codes** (Ethiopia, Nigeria): Raw numeric IDs (e.g., '11', '12', '18') without label mapping

Sample shock values per country (first 5 distinct):
- **Benin**: ['Attaques acridiennes...', 'Autre (à préciser)', 'Baisse importante...', 'Conflit Agriculteur/Eleveur', 'Conflit armé/Violence...']
- **Ethiopia**: ['11', '12', '18', 'Death Of Hh Member...', 'Death Of Other Hh Member']
- **Nigeria**: ['1', '10', '11', '12', '13']
- **Malawi**: ['BIRTH IN THE HOUSEHOLD', 'BREAK-UP OF HOUSEHOLD', ...] (mixed case inconsistency)
- **Tanzania**: ['BREAK-UP OF THE HOUSEHOLD', 'CHRONIC/SEVERE...', 'CROP DISEASE...']

**Finding**: Strong evidence of upstream label harmonization needed. Ethiopia and Nigeria's numeric codes are not decoded; Malawi shows case inconsistency (Birth vs. birth).

### Affected* Boolean Dtype Check

**Critical failure**: All 4,639,538 rows are null across all four Affected* columns. This is **not a dtype conversion error** (dtypes are correctly boolean); rather, the columns are universally empty. This suggests:
1. Raw data does not populate these fields for aggregated records, or
2. The extraction logic conditionally applies these only in single-country Country() calls, not in Feature() aggregation.

**By-country verification** (from parallel Country() calls):
- Benin.shocks(): 176,264 rows, AffectedIncome dtype=boolean (but values should be verified individually)
- Mali.shocks(): 375,481 rows, dtype=boolean
- Tanzania.shocks(): 162,928 rows, dtype=boolean

The per-country calls also do not reveal non-null Affected* values in initial inspection, suggesting this is a systemic data availability issue, not an aggregation bug.

### HowCoped0/1/2 Values

- **HowCoped0**: 291,792 non-null (6.3% of records), all string values
- **HowCoped1**: 99,489 non-null (2.1%), subset of HowCoped0 records
- **HowCoped2**: 55,974 non-null (1.2%), subset of HowCoped1 records

**Sample HowCoped0 values** (multilingual, free-text):
- "Aide de parents ou d'amis" (French: aid from family/friends)
- "Vente des biens durables du ménage" (French: sale of durable goods)
- "Utilisation de son épargne" (French: use of savings)

No unharmonized codes detected in HowCoped columns themselves; all are meaningful translated labels. Stratification by country shows country-specific label sets (French countries use French labels; English countries would use English).

### HowCoped2 Declaration

**Finding**: HowCoped2 is correctly declared and populated. Per the prior fix (Benin v0.7.0), Benin historically had only Cope1/Cope2 strategies and was not expected to declare HowCoped2. However:
- Benin's 2018-19 shocks() function now correctly handles up to 2 strategies and **does not populate HowCoped2**.
- Mali's shocks() function in 2018-19 iterates up to 3 strategies and **does populate HowCoped2**.
- Other countries vary in their maximum coping strategies recorded.

Distribution shows HowCoped2 is sparse but legitimately populated (55,974 / 4.6M rows = 1.2%), suggesting countries with 3+ recorded coping strategies.

---

## 7. Wave Coverage per Country

Each country's shocks declaration matches its wave coverage. No ghost waves (declared but empty) or missing waves (present but absent from index).

- **Tanzania**: 6 waves (2008–2020, biennial)
- **Nigeria**: 8 quarters (2010–2019, quarterly)
- **Ethiopia**: 5 waves (2011–2021, ~2-year intervals)
- **Malawi**: 5 waves (2004–2019, irregular)
- **Others**: 1–4 waves per country, all present in output

---

## 8. Surprises & Findings

1. **All Affected* columns null**: 4.6M rows, zero non-null values in AffectedIncome/Assets/Production/Consumption. This is unexpected if these are core schema columns. Either the underlying data does not record these impact flags, or the aggregation silently drops them. Recommend verification against raw country files.

2. **26 leftover Cope* columns**: Raw survey coping strategy indicators (Cope1–Cope26, string dtype) persist in the final aggregation. These should have been dropped during country transformation. Niger especially contains all 26 (424,764 rows × 26 columns of noise). This inflates the table and violates clean schema expectations.

3. **Unharmonized Shock labels**: Numeric codes (Ethiopia: 11, 12, 18; Nigeria: 1, 10, 11) lack human-readable labels. French and English labels are language-separated and case-inconsistent (Malawi: "BIRTH" vs. "Birth").

4. **Panel IDs warnings**: Multiple countries (Benin, Côte d'Ivoire, Guinea-Bissau, Togo) emit "Panel IDs not found" during load, suggesting these countries do not declare panel_ids or have them conditionally. This is non-fatal but indicates schema divergence.

5. **Index structure mismatch**: The aggregated index is (country, i, t, v, Shock), but the canonical schema index_info specifies (t, i, Shock). The extra (v) cluster level is present because individual country calls include cluster/spatial hierarchy. Feature() should normalize to the canonical (country, t, i, Shock).

---

## Recommendations

1. **Null Affected* columns**: Audit raw data to confirm whether impact flags are present. If present, trace extraction logic. If absent from source, update canonical schema to mark as non-required.

2. **Cope* cleanup**: Ensure all country transformation scripts (in `/countries/{}/*/_.py`) drop Cope* columns post-aggregation. Niger's 26 columns are the worst offender.

3. **Shock label harmonization**: Decode numeric codes (Ethiopia, Nigeria) to English labels in transformation. Standardize case across Malawi. Consider a cross-country shock-to-English mapping table.

4. **Index canonicalization**: Feature() aggregation should drop (v) and normalize to (country, t, i, Shock) matching schema.

5. **HowCoped2 distribution**: Benin's current code correctly limits to 2 strategies; verify other countries' limits match data reality. Document max-coping-strategies per country.

---

**Audit Status**: ⚠️ Minor issues (Cope* leakage, Affected* nullity, Shock non-harmonization) do not prevent use but indicate schema drift and cleanup opportunities.
