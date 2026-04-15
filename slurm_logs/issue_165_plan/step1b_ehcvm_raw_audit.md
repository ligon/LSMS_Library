# GH #165 Step 1b — EHCVM raw section-1 file audit

**Date**: 2026-04-13
**Branch**: master
**Agent**: step1b read-only audit

---

## Summary

- GO (Togo-shape): 3 (Benin 2018-19, CotedIvoire 2018-19, Burkina_Faso 2021-22)
- GO (Senegal-shape): 6 (Burkina_Faso 2018-19, Guinea-Bissau 2018-19, Mali 2018-19, Mali 2021-22, Niger 2018-19, Niger 2021-22)
- BLOCK (no raw): 0
- BLOCK (raw lacks DOB): 0
- UNCLEAR: 0

**Top line**: All 9 entries previously marked BLOCK have raw `s01_me_*.dta` files in DVC with DOB columns `s01q03a` (day), `s01q03b` (month), `s01q03c` (year). The prior BLOCK verdict was based solely on the harmonised `ehcvm_individu_*.dta` files, which strip DOB. Every entry is now reclassified GO; the difference is whether month is integer-only (Togo-shape, no sentinel) or integer-with-9999-sentinel (Senegal-shape). No French text-month strings appear anywhere — `month_map` is not needed.

---

## Per-entry findings

### Benin 2018-19
- **Raw file**: `lsms_library/countries/Benin/2018-19/Data/s01_me_ben2018.dta` (DVC pull OK)
- **DOB columns**: year=`s01q03c` ("Date de naissance (année)"), month=`s01q03b` ("Date de naissance (mois)"), day=`s01q03a` ("Date de naissance (jour)")
- **File total rows**: 42,343
- **Year non-null**: 40,263; dtype=`object`; range [1922, 2019]; no -1, 9999, 99 sentinels; NaN used for missing
- **Month format**: integer (dtype `object`), values 1–12, NaN for missing; no 9999 sentinel; month unique count = 12
- **Sentinels**: none — clean NaN for all DOB columns
- **Age (`s01q04a`) null**: 40,263 (95.1%)
- **Recoverable gap**: 40,263 rows where age null but year non-null (100% of age-null rows)
- **Existing config reads**: `ehcvm_individu_ben2018.dta` (harmonised)
- **Verdict**: GO (Togo-shape) — integer month, no sentinel, NaN for missing

---

### Burkina_Faso 2018-19
- **Raw file**: `lsms_library/countries/Burkina_Faso/2018-19/Data/s01_me_bfa2018.dta` (DVC pull OK)
- **DOB columns**: year=`s01q03c`, month=`s01q03b`, day=`s01q03a` — same labels as Benin
- **File total rows**: 45,612
- **Year non-null**: 45,612 (no NaN); dtype=`float64`; range [1909, 9999]; 9999 sentinel count = 1,117
- **Month format**: integer (dtype `float64`), values 1–12 plus 9999 sentinel; 9999 count = 11,996; unique count = 13
- **Sentinels**: year 9999: 1,117; month 9999: 11,996 — Senegal pattern (9999 = unknown)
- **Age (`s01q04a`) null**: 44,495 (97.5%)
- **Recoverable gap**: 44,495 rows where age null but year non-null; 1,117 rows have year=9999 so effectively 43,378 rows with real DOB year recoverable via birth year
- **Existing config reads**: `ehcvm_individu_bfa2018.dta` (harmonised)
- **Verdict**: GO (Senegal-shape) — integer month with 9999 sentinel; treat 9999 as NA

---

### Burkina_Faso 2021-22
- **Raw file**: `lsms_library/countries/Burkina_Faso/2021-22/Data/s01_me_bfa2021.dta` (DVC pull OK)
- **DOB columns**: year=`s01q03c` ("1.03c. Quelle est l'année de naissance de %rostertitle% ?"), month=`s01q03b` ("1.03b. Mois de naissance"), day=`s01q03a`
- **File total rows**: 26,523
- **Year non-null**: 21,992; dtype=`object`; range [1922, 2022]; no -1, 9999, 99 — NaN used for missing (4,531 NaN)
- **Month format**: integer (dtype `object`), values 1–12 plus 9999 sentinel; 9999 count = 7,618; NaN count = 0; unique count = 13
- **Sentinels**: year has no 9999 (NaN only); month has 9999 = unknown (7,618 rows)
- **Age (`s01q04a`) null**: 26,153 (98.6%)
- **Recoverable gap**: 21,992 rows where age null and year non-null (year-NaN rows cannot be recovered: 4,531)
- **Existing config reads**: `ehcvm_individu_bfa2021.dta` (harmonised)
- **Verdict**: GO (Togo-shape for year / Senegal-shape for month) — year uses NaN (clean), month uses 9999 sentinel; treat month 9999 as NA. Closest to Togo-shape overall.

---

### CotedIvoire 2018-19
- **Raw file**: `lsms_library/countries/CotedIvoire/2018-19/Data/Menage/s01_me_CIV2018.dta` (already on disk, no DVC pull needed)
- **DOB columns**: year=`s01q03c`, month=`s01q03b`, day=`s01q03a` — same labels as Benin
- **File total rows**: 61,116
- **Year non-null**: 57,518; dtype=`object`; range [1900, 2019]; no 9999 sentinel; NaN used for missing (3,598 NaN)
- **Month format**: integer (dtype `object`), values 1–12; no 9999 sentinel; NaN for missing (3,549); unique count = 12
- **Sentinels**: none — clean NaN for all DOB columns
- **Age (`s01q04a`) null**: 57,517 (94.1%)
- **Recoverable gap**: 57,517 rows where age null and year non-null (1 row has year but not age-null; 57,517 recoverable)
- **Existing config reads**: `Menage/ehcvm_individu_CIV2018.dta` (harmonised)
- **Verdict**: GO (Togo-shape) — integer month, no sentinel, NaN for missing

---

### Guinea-Bissau 2018-19
- **Raw file**: `lsms_library/countries/Guinea-Bissau/2018-19/Data/s01_me_gnb2018.dta` (DVC pull OK)
- **DOB columns**: year=`s01q03c` ("Date de naissance (année)"), month=`s01q03b` ("Date de naissance (mois)"), day=`s01q03a`
- **File total rows**: 42,839
- **Year non-null**: 42,839 (no NaN); dtype=`int64`; range [1922, 9999]; 9999 sentinel count = 4,412
- **Month format**: integer (dtype `int64`), values 1–12 plus 9999; 9999 count = 9,243; unique count = 13
- **Sentinels**: year 9999: 4,412; month 9999: 9,243 — Senegal pattern
- **Age (`s01q04a`) null**: 38,427 (89.7%)
- **Recoverable gap**: 38,427 rows where age null and year non-null; 4,412 have year=9999; effectively 34,015 with real DOB year
- **Existing config reads**: `ehcvm_individu_gnb2018.dta` (harmonised)
- **Verdict**: GO (Senegal-shape) — integer month with 9999 sentinel; treat 9999 as NA

---

### Mali 2018-19
- **Raw file**: `lsms_library/countries/Mali/2018-19/Data/s01_me_mli2018.dta` (DVC pull OK)
- **DOB columns**: year=`s01q03c` ("Date de naissance (année)"), month=`s01q03b` ("Date de naissance (mois)"), day=`s01q03a`
- **File total rows**: 46,014
- **Year non-null**: 46,014 (no NaN); dtype=`int64`; range [1916, 9999]; 9999 sentinel count = 3,248
- **Month format**: integer (dtype `int64`), values 1–12 plus 9999; 9999 count = 17,148; unique count = 13
- **Sentinels**: year 9999: 3,248; month 9999: 17,148 — Senegal pattern
- **Age (`s01q04a`) null**: 42,766 (92.9%)
- **Recoverable gap**: 42,766 rows where age null and year non-null; 3,248 have year=9999; effectively 39,518 with real DOB year
- **Existing config reads**: `ehcvm_individu_mli2018.dta` (harmonised)
- **Verdict**: GO (Senegal-shape) — integer month with 9999 sentinel; treat 9999 as NA

---

### Mali 2021-22
- **Raw file**: `lsms_library/countries/Mali/2021-22/Data/s01_me_mli2021.dta` (DVC pull OK)
- **DOB columns**: year=`s01q03c` ("1.03c. Année de naissance"), month=`s01q03b` ("1.03b. Mois de naissance"), day=`s01q03a`
- **File total rows**: 43,472
- **Year non-null**: 43,472 (no NaN); dtype=`int64`; range [1915, 9999]; 9999 sentinel count = 1,569
- **Month format**: integer (dtype `int64`), values 1–12 plus 9999; 9999 count = 16,043; unique count = 13
- **Sentinels**: year 9999: 1,569; month 9999: 16,043 — Senegal pattern
- **Age (`s01q04a`) null**: 41,903 (96.4%)
- **Recoverable gap**: 41,903 rows where age null and year non-null; 1,569 have year=9999; effectively 40,334 with real DOB year
- **Existing config reads**: `ehcvm_individu_mli2021.dta` (harmonised)
- **Verdict**: GO (Senegal-shape) — integer month with 9999 sentinel; treat 9999 as NA

---

### Niger 2018-19
- **Raw file**: `lsms_library/countries/Niger/2018-19/Data/s01_me_ner2018.dta` (DVC pull OK)
- **DOB columns**: year=`s01q03c` ("Date de naissance (année)"), month=`s01q03b` ("Date de naissance (mois)"), day=`s01q03a`
- **File total rows**: 35,406
- **Year non-null**: 35,406 (no NaN); dtype=`int64`; range [1922, 9999]; 9999 sentinel count = 9,685
- **Month format**: integer (dtype `int64`), values 1–12 plus 9999; 9999 count = 25,195; unique count = 13
- **Sentinels**: year 9999: 9,685; month 9999: 25,195 — Senegal pattern; month sentinel rate very high (71.2%)
- **Age (`s01q04a`) null**: 25,721 (72.6%)
- **Recoverable gap**: 25,721 rows where age null and year non-null; 9,685 have year=9999; effectively 16,036 with real DOB year recoverable
- **Existing config reads**: `ehcvm_individu_ner2018.dta` (harmonised)
- **Note**: Niger has a local `age_handler` in `_/niger.py` that is not called for this wave; the raw s01_me path provides DOB directly
- **Verdict**: GO (Senegal-shape) — integer month with 9999 sentinel; treat 9999 as NA

---

### Niger 2021-22
- **Raw file**: `lsms_library/countries/Niger/2021-22/Data/s01_me_ner2021.dta` (DVC pull OK)
- **DOB columns**: year=`s01q03c` ("1.03c. Année de naissance"), month=`s01q03b` ("1.03b. Mois de naissance"), day=`s01q03a`
- **File total rows**: 44,080
- **Year non-null**: 44,080 (no pandas NaN); dtype=`object` (string); Stata "." missing count = 10,919; valid year count = 33,161; valid year range [1922, 2022]; no numeric 9999 sentinel
- **Month format**: integer (dtype `object`), values 1–12; NaN count = 10,889; no 9999 sentinel; unique count = 12
- **Sentinels**: year uses Stata string "." for missing (not numeric 9999); month uses NaN (not 9999)
- **Age (`s01q04a`) null**: 38,491 (87.3%)
- **Recoverable gap**: 33,161 rows where age null and year is non-null non-dot (10,919 rows have year="." and are unrecoverable)
- **Existing config reads**: `ehcvm_individu_ner2021.dta` (harmonised)
- **Note**: Year stored as Stata string type; "." sentinel requires string comparison, not integer 9999 check. Implementation must cast year via `pd.to_numeric(s01q03c, errors='coerce')` rather than the Senegal `{9999: null}` mapping.
- **Verdict**: GO (Senegal-shape, but year sentinel is Stata "." string, not integer 9999) — treat year "." as NA via `pd.to_numeric(..., errors='coerce')`

---

## Dispatch recommendation for Prompt D fan-out

| # | Country | Wave | Raw file | DOB shape | Month sentinel | Recoverable rows | Verdict |
|---|---------|------|----------|-----------|---------------|-----------------|---------|
| 1 | Benin | 2018-19 | `s01_me_ben2018.dta` | Togo | NaN only | 40,263 | GO (Togo-shape) |
| 2 | Burkina_Faso | 2018-19 | `s01_me_bfa2018.dta` | Senegal | 9999 int | ~43,378 | GO (Senegal-shape) |
| 3 | Burkina_Faso | 2021-22 | `s01_me_bfa2021.dta` | Togo/mixed | 9999 int (month only) | 21,992 | GO (Togo-shape, month has 9999) |
| 4 | CotedIvoire | 2018-19 | `s01_me_CIV2018.dta` | Togo | NaN only | 57,517 | GO (Togo-shape) |
| 5 | Guinea-Bissau | 2018-19 | `s01_me_gnb2018.dta` | Senegal | 9999 int | ~34,015 | GO (Senegal-shape) |
| 6 | Mali | 2018-19 | `s01_me_mli2018.dta` | Senegal | 9999 int | ~39,518 | GO (Senegal-shape) |
| 7 | Mali | 2021-22 | `s01_me_mli2021.dta` | Senegal | 9999 int | ~40,334 | GO (Senegal-shape) |
| 8 | Niger | 2018-19 | `s01_me_ner2018.dta` | Senegal | 9999 int | ~16,036 | GO (Senegal-shape) |
| 9 | Niger | 2021-22 | `s01_me_ner2021.dta` | Senegal variant | NaN only | 33,161 | GO (Senegal-shape, year "." via pd.to_numeric) |

**All 9 entries: GO.** Prompt D should configure each wave's `household_roster` to read `s01_me_<iso><year>.dta` (not `ehcvm_individu_*.dta`) and apply `age_handler(year_col='s01q03c', month_col='s01q03b', day_col='s01q03a', age_col='s01q04a')`.

**Implementation notes for Prompt D**:

1. **Column names are uniform**: all 9 files use `s01q03a` (day), `s01q03b` (month), `s01q03c` (year), `s01q04a` (age-in-years) — identical to Togo 2018 and Senegal 2018-19.

2. **Three sentinel patterns** — choose cleanup strategy per group:
   - **Togo-shape** (Benin, CotedIvoire): year dtype `object`, NaN for missing, no numeric sentinel. Convert with `pd.to_numeric(..., errors='coerce')`.
   - **Senegal-shape with int 9999** (BFA 2018-19, GNB, MLI ×2, NER 2018-19): all DOB columns are int64 or float64; 9999 = unknown, use `mapping: {9999: null}` in YAML or `replace(9999, pd.NA)` in script.
   - **BFA 2021-22**: year is `object` with NaN; month is `object` with integer 9999 — hybrid. Treat year via `pd.to_numeric`; treat month 9999 as NA.
   - **NER 2021-22**: year is string `object`; "." = Stata missing (not 9999). Use `pd.to_numeric(df['s01q03c'], errors='coerce')` to convert; month uses NaN cleanly.

3. **Merging with existing roster columns**: the `s01_me_*.dta` files contain the same household/individual ID columns as `ehcvm_individu_*.dta` (`grappe`, `menage`, `s01q00a` or `pid`), so the switch is a file-source replacement plus DOB column addition, not a structural join.

4. **CotedIvoire path**: `Menage/s01_me_CIV2018.dta` (subdirectory). The `ehcvm_individu_CIV2018.dta` is also in `Menage/`. Both paths are already present in `data_info.yml`.

5. **Niger 2018-19 month sentinel rate is very high (71.2% = 9999)**: this means month is largely unknown but year is more populated (only 27.4% = 9999). `age_handler` falls back to year-only when month is 9999 after coercion.

---

## DVC pull log

All 9 DVC pulls completed successfully in under 5 seconds each (no DVC pull exceeded 60s). No failures. Files were pulled from the S3 cache.

- `Benin/2018-19/Data/s01_me_ben2018.dta` — 1 file added
- `Burkina_Faso/2018-19/Data/s01_me_bfa2018.dta` — 1 file added
- `Burkina_Faso/2021-22/Data/s01_me_bfa2021.dta` — 1 file added
- `CotedIvoire/2018-19/Data/Menage/s01_me_CIV2018.dta` — 1 file added
- `Guinea-Bissau/2018-19/Data/s01_me_gnb2018.dta` — 1 file added
- `Mali/2018-19/Data/s01_me_mli2018.dta` — 1 file added
- `Mali/2021-22/Data/s01_me_mli2021.dta` — 1 file added
- `Niger/2018-19/Data/s01_me_ner2018.dta` — 1 file added
- `Niger/2021-22/Data/s01_me_ner2021.dta` — 1 file added

---

## Key surprises / design implications

1. **Step 1's BLOCK was entirely wrong**: Step 1 audited only the `ehcvm_individu_*` harmonised files. All 9 countries have raw `s01_me_*` questionnaire files with full DOB columns. The harmonised files strip DOB; the raw files preserve it. Switching file source unblocks all 9 entries.

2. **No French text-month strings in any file**: Senegal 2018-19 is the only EHCVM file with French month names (`Janvier`, `Février`, …). All 9 blocked countries use integer months (1–12) with 9999 or NaN for "unknown". The `month_map` in Senegal's `age_handler` is not needed for any of these 9.

3. **9999 is the universal "unknown DOB" sentinel** (except BFA 2021-22 and NER 2021-22 round 2, where object-type columns use NaN or Stata "." for missing). The `age_handler()` implementation must handle both numeric 9999 and `pd.NA`/NaN.

4. **CotedIvoire note**: file uses uppercase ISO code `CIV` unlike the lowercase codes for all other countries. The `s01_me_CIV2018.dta` path must be written as `Menage/s01_me_CIV2018.dta` in `data_info.yml`.

5. **NER 2021-22 year column is string, not integer**: `s01q03c` is `object` dtype with "." (Stata string-missing) rather than the integer 9999 sentinel. This is the only case requiring `pd.to_numeric(..., errors='coerce')` rather than a simple sentinel mask.
