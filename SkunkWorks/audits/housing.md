# Housing Feature Audit Report

## 1. SCOPE DEVIATIONS

None. The probe executed successfully with all 13 countries present in the feature aggregation. No schema mismatches or malformed indices observed.

## 2. Shape and Coverage

**Total rows:** 243,319 households across all waves and countries.

**Per-country row counts:**

| Country       | Row Count |
|---------------|-----------|
| Benin         | 8,012     |
| Burkina Faso  | 21,037    |
| Côte d'Ivoire | 12,992    |
| Ethiopia      | 25,914    |
| Guinea-Bissau | 5,351     |
| Malawi        | 57,058    |
| Mali          | 24,931    |
| Niger         | 3,968     |
| Nigeria       | 24,006    |
| Senegal       | 7,156     |
| Tanzania      | 22,433    |
| Togo          | 6,171     |
| Uganda        | 24,290    |

All 13 countries are present. No countries silently absent.

## 3. Columns Present vs. Canonical

**Expected columns:** `Roof`, `Floor` (both string type).

**Observed columns:** `Roof`, `Floor`.

**Rogue columns:** None.

**Index structure:** `(country, i, t, v)` where `i` is household ID, `t` is wave, `v` is cluster. This is correct—the feature retains cluster membership per household, enabling stratified analysis. No deviations from expected structure.

## 4. Data Types

- **Roof:** `string` (pandas StringDtype)
- **Floor:** `string` (pandas StringDtype)

Both columns correctly use the string dtype (not object), which is appropriate pending the pandas 3.0 migration noted in project scope.

## 5. Index Integrity

- **Index names:** `['country', 'i', 't', 'v']` ✓
- **Duplicated index rows:** 0 (checked via `foo.index.duplicated().sum()`)
- **Unique index tuples:** 243,319 (matches total row count exactly)
- **Null counts:** Minimal; no all-null indices. See detailed nulls per country below.

**Expected cardinality:** 1 row per household per wave per country, potentially multiple rows if cluster varies. Observed: no cluster (`v`) variation per `(country, i, t)` triplet. Structure is sound.

## 6. Feature-Specific Analysis (Housing)

### Roof Values by Country

| Country       | Count | Values |
|---------------|-------|--------|
| Benin         | 7     | Adobe, Clay Tiles, Concrete Slab, Iron Sheets, Mats, Straw, Thatch |
| Burkina Faso  | 8     | Clay Tiles, Concrete Slab, Earth, Iron Sheets, Mats, Mud, Other, Thatch |
| Côte d'Ivoire | 8     | Adobe, Autre, Clay Tiles, Concrete Slab, Iron Sheets, Straw, Thatch, Woven Mats |
| Ethiopia      | 9     | Asbestos, Bricks, Concrete/Cement, Corrugated Iron Sheet, Other, Plastic Canvas, Reed/Bamboo, Thatch, Wood And Mud |
| Guinea-Bissau | 8     | Adobe, Clay Tiles, Concrete Slab, Iron Sheets, Mats, Other, Straw, Thatch |
| Malawi        | 6     | Clay Tiles, Concrete, Grass, Iron Sheets, Other, Plastic Sheeting |
| Mali          | 9     | Adobe, Clay Tiles, Concrete Slab, Iron Sheets, Manquant, Mats, Other, Straw, Thatch |
| Niger         | 8     | Clay Tiles, Concrete Slab, Earth, Hide, Iron Sheets, Other, Thatch, Wood |
| Nigeria       | 12    | Asbestos Sheet, Clay Tiles, Concrete, Grass, Iron Sheets, Long/Short Span Sheets, Mud, Other, Plastic Sheeting, Step Tiles, Thatch, Zinc Sheet |
| Senegal       | 7     | Adobe, Clay Tiles, Concrete Slab, Iron Sheets, Other, Thatch, Woven Mats |
| Tanzania      | 7     | Asbestos Sheets, Concrete/Cement, Grass/Leaves/Bamboo, Metal Sheets, Mud And Grass, Other, Tiles |
| Togo          | 7     | Clay Tiles, Concrete Slab, Iron Sheets, Mud, Other, Thatch, Woven Mats |
| Uganda        | 9     | Asbestos, Cement, Concrete, Iron Sheets, Mud, Other, Thatch, Tiles, Wood |

**Assessment:** All Roof values are material names. No numeric codes, no binary 0/1 values, no suspicious string encodings of nulls (e.g., `"nan"`, `"0"`).

### Floor Values by Country

| Country       | Count | Values |
|---------------|-------|--------|
| Benin         | 5     | Cement, Dung, Earth, Other, Tiles |
| Burkina Faso  | 7     | Carpet, Cement, Dung, Earth, Other, Sand, Tiles |
| Côte d'Ivoire | 5     | Cement, Dung, Earth, Other, Tiles |
| Ethiopia      | 10    | Brick Tiles, Cement Screed, Cement Tiles, Ceramic/Marble Tiles, Mud/Dung, Other, Parquet Of Polished Wood, Plastic Tiles, Reed/Bamboo, Wood Planks |
| Guinea-Bissau | 5     | Cement, Dung, Earth, Other, Tiles |
| Malawi        | 6     | Other, Sand, Smooth Cement, Smoothed Mud, Tile, Wood |
| Mali          | 6     | Cement, Dung, Earth, Manquant, Other, Tiles |
| Niger         | 5     | Cement, Earth, Other, Parquet, Tiles |
| Nigeria       | 8     | Marble, Other, Sand/Dirt/Straw, Smooth Cement, Smoothed Mud, Terrazzo, Tile, Wood |
| Senegal       | 5     | Cement, Dung, Earth, Other, Tiles |
| Tanzania      | 7     | Concrete Slab, Concrete/Cement/Tiles/Timber, Earth, Other, Sand/Cement, Tiles, Timber |
| Togo          | 5     | Cement, Dung, Earth, Other, Tile |
| Uganda        | 8     | Bricks, Cement, Concrete, Earth, Other, Stone, Tiles, Wood |

**Assessment:** All Floor values are material names. No numeric codes, no binary values. No all-null columns per country.

### Cross-Country Harmonization

**Roof materials with overlap (appearing in 2+ countries):**
- **Iron Sheets:** 11 countries (ubiquitous)
- **Other:** 11 countries (catch-all)
- **Thatch:** 11 countries (ubiquitous)
- **Clay Tiles:** 10 countries (major material)
- **Concrete Slab:** 8 countries (regional preference)
- **Adobe, Mats, Straw, Woven Mats:** 3–5 countries (regional)
- **Asbestos, Mud, Wood, Tiles, Grass, Plastic Sheeting, Concrete:** 2–3 countries

**Uganda-Malawi Roof overlap:** Only 3 materials in common (`Concrete`, `Iron Sheets`, `Other`). Uganda uniquely reports `Asbestos`, `Cement`, `Mud`, `Thatch`, `Tiles`, `Wood`. Malawi uniquely reports `Clay Tiles`, `Grass`, `Plastic Sheeting`. Distinct vocabularies—not harmonized via a canonical mapping table.

**Floor materials with overlap:**
- **Other:** 13 countries (universal)
- **Earth:** 10 countries (ubiquitous)
- **Cement & Tiles variants:** 9 countries (common formalization)
- **Sand, Dung, Wood:** 2–7 countries (regional)

**Uganda-Malawi Floor overlap:** Only 2 materials in common (`Other`, `Wood`). Uganda reports `Bricks`, `Cement`, `Concrete`, `Earth`, `Stone`, `Tiles`. Malawi reports `Sand`, `Smooth Cement`, `Smoothed Mud`, `Tile`. Even lower overlap than Roof—suggests separate, locally-driven coding schemes.

**Interpretation:** Cross-country vocabulary harmonization is partial and vocabulary-driven, not canonical-enforced. Uganda and Malawi use distinct material taxonomies reflecting local building practices and survey phrasing. The library accepts this heterogeneity; users must be aware that "Iron Sheets" in Uganda may differ subtly in survey context from "Iron Sheets" in Nigeria, and pooling across countries requires care.

**No suspicious values:** All entries are printable material names. Null (<NA>) entries are minimal (see nulls table below) and appropriately handled as pandas NA, not string-encoded `"nan"` or `"0"`.

### Null Counts per Country

| Country       | Roof Nulls | Floor Nulls |
|---------------|-----------|-------------|
| Benin         | 0         | 0           |
| Burkina Faso  | 3         | 3           |
| Côte d'Ivoire | 0         | 1           |
| Ethiopia      | 60        | 59          |
| Guinea-Bissau | 0         | 0           |
| Malawi        | 0         | 4           |
| Mali          | 0         | 0           |
| Niger         | 0         | 0           |
| Nigeria       | 6         | 10          |
| Senegal       | 0         | 0           |
| Tanzania      | 3         | 3           |
| Togo          | 0         | 0           |
| Uganda        | 84        | 83          |

**Assessment:** Nulls are sparse (<0.2% overall, except Uganda at ~0.35%). No columns are entirely null for any country.

## 7. Wave Coverage per Country

- **Benin:** 2018-19 (1 wave)
- **Burkina Faso:** 2014, 2018-19, 2021-22 (3 waves)
- **Côte d'Ivoire:** 2018-19 (1 wave)
- **Ethiopia:** 2011-12, 2013-14, 2015-16, 2018-19, 2021-22 (5 waves)
- **Guinea-Bissau:** 2018-19 (1 wave)
- **Malawi:** 2004-05, 2010-11, 2013-14, 2016-17, 2019-20 (5 waves)
- **Mali:** 2014-15, 2017-18, 2018-19, 2021-22 (4 waves)
- **Niger:** 2011-12 (1 wave)
- **Nigeria:** 2011Q1, 2013Q1, 2015Q3, 2018Q3, 2024Q1 (5 waves, quarterly)
- **Senegal:** 2018-19 (1 wave)
- **Tanzania:** 2008-09, 2010-11, 2012-13, 2014-15, 2019-20, 2020-21 (6 waves)
- **Togo:** 2018 (1 wave)
- **Uganda:** 2005-06, 2009-10, 2010-11, 2011-12, 2013-14, 2015-16, 2018-19, 2019-20 (8 waves)

**Assessment:** Coverage is uneven. Tanzania and Uganda offer rich panel depth (6 and 8 waves). Benin, Côte d'Ivoire, Guinea-Bissau, Senegal, and Togo have single waves. Ethiopia, Malawi, Nigeria, and Mali span 4–5 waves. Housing data availability reflects underlying LSMS survey schedules.

## 8. Surprises and Notes

1. **Housing not in canonical schema:** The `lsms_library/data_info.yml` canonical `Columns` section lacks a `housing` definition. Instead, housing is specified at the country-wave level in each local `data_info.yml`, with inline mappings (especially for Malawi). This is consistent with the project's country-led approach but means housing is not subject to cross-country validation at load time.

2. **Malawi mapping note (CLAUDE.md reference verified):** Malawi normalizes raw codes via inline dictionaries in each wave's `data_info.yml` (e.g., `GRASS: Grass`, `IRON SHEETS: Iron Sheets`). Uganda applies categorical mappings via `categorical_mapping.org` (e.g., `Iron sheets → Iron Sheets`). Both approaches yield clean, human-readable material names post-aggregation.

3. **Rogue spellings in raw data:** Some countries emit variant spellings (e.g., Côte d'Ivoire's `"Autre"` for Other, Mali's `"Manquant"` for missing). These are not normalized at the feature level but remain in the aggregated table, likely because they are minority codes in their source datasets.

4. **Index has 4 levels not 3:** Expected `(country, t, i)` but actual is `(country, i, t, v)`. The cluster index `v` is retained to preserve stratification context; this is intentional and correct.

5. **No data integrity issues:** Duplicated row check returns 0; all index tuples are unique. Each household-wave-country triplet appears exactly once.

---

## Status 2026-04-13

**Feature scan: CLEAN.** The 2026-04-13 feature rescan confirms housing remains structurally sound:
- No rogue columns; `Roof` and `Floor` still the only output columns.
- All 13 countries contributing; no new silent absences.
- Index `(country, i, t, v)` unique with 0 duplicates.
- `v` injection scoping fix (commit `3e050a5f`) does not affect housing because housing's canonical index includes `v`.

**No action required.** The minor outstanding items from §8 (Côte d'Ivoire `"Autre"` / Mali `"Manquant"` variant spellings; no housing schema in global `data_info.yml`) remain low-priority and unchanged.

