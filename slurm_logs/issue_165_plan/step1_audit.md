# GH #165 Step 1 Audit — DOB column availability

**Date**: 2026-04-13
**Branch**: master
**Agent**: step1 read-only audit

---

## Summary

Eleven source files audited across 10 (country, wave) pairs. **Togo 2018** is the only entry that uses the non-harmonised `s01_me_tgo2018.dta` individual roster file and has full date-of-birth columns (`s01q03a` day, `s01q03b` month, `s01q03c` year), matching the Senegal 2018-19 template exactly — same column names, same labels. Its Age column (`s01q04a`) is 94.5% null (25,966 / 27,480 rows) while birth year is non-null in exactly the same 25,966 rows, confirming perfect recoverability with `age_handler()`. All eight EHCVM-harmonised `ehcvm_individu_*.dta` files (Benin, Burkina_Faso ×2, CotedIvoire, Guinea-Bissau, Mali ×2, Niger ×2) contain **only** a pre-computed `age` column — no DOB components at all. There is nothing for `age_handler()` to work with in those files. The Senegal reference file is confirmed to use the same column names as Togo. Niger has a local country-level `age_handler` function (not the canonical `tools.age_handler()`) but the 2018-19 and 2021-22 waves also use `ehcvm_individu` files with only `age`.

---

## Per-country findings

### Togo 2018
- **File**: `lsms_library/countries/Togo/2018/Data1/s01_me_tgo2018.dta`
  (Note: Togo uses `Data1/` not `Data/`; task spec said `Data/` but the config and `.dvc` files are under `Data1/`)
- **DVC pull**: OK — pulled from S3 cache, 1 file added
- **DOB columns**:
  - day = `s01q03a` ("Date de naissance (jour)")
  - month = `s01q03b` ("Date de naissance (mois)")
  - year = `s01q03c` ("Date de naissance (année)")
- **Age column**: `s01q04a` ("1.04a. Quel âge avait [NOM] à son dernier anniversaire? ANS")
- **Coverage** (full file, 27,480 rows):
  - `s01q04a` null: 25,966 (94.5%)
  - `s01q03c` non-null: 25,966 (94.5%)
  - Recoverable via birth year: 25,966 rows (100% of Age-null rows)
  - Both Age and birth year null: 0 rows
- **Sentinels observed** (full file scan):
  - `s01q03a` (day): NaN=1,494 — range [1, 31] — no -1, -2, 99, 9999
  - `s01q03b` (month): NaN=1,488 — range [1, 12] — no -1, -2, 99, 9999
  - `s01q03c` (year): NaN=1,514 — range [1909, 2019] — no -1, -2, 99, 9999
  - `s01q04a` (age): NaN=25,966 — range [0, 105]; sentinel 0: 7 rows, sentinel 99: 2 rows
- **Data format note**: columns `s01q03a/b/c` are dtype `object` (Stata categorical strings) in Togo — different from Senegal where they are `int64` with 9999 sentinel. Togo DOB nulls are true NaN, not 9999. The Senegal `Age()` formatting function in `2018-19.py` does a `month_map` string-to-int conversion for `s01q03b` — **this step is needed for Togo too** since month values appear as plain integers (1–12) in the object column, not French month names.
- **Verdict**: RECOVERABLE (full DOB: year + month + day)
- **Dispatch for Step 3 (Togo)**: **GO**

---

### Senegal 2018-19 (reference / template)
- **File**: `lsms_library/countries/Senegal/2018-19/Data/s01_me_sen2018.dta`
- **DVC pull**: OK
- **DOB columns**: same as Togo — `s01q03a` (day), `s01q03b` (month), `s01q03c` (year)
- **Age column**: `s01q04a`
- **Sentinel in DOB (100-row sample)**: 9999 sentinel present — s01q03a: 92/200, s01q03b: 87/200, s01q03c: 19/200. Columns are `int64`.
- **Existing `age_handler` usage**: Yes — `2018-19.py` `household_roster(df)` function calls `tools.age_handler()` with `-1` null sentinel for age and Senegal `Age()` function handles string months via `month_map`.
- **Verdict**: Already implemented (template). Note: Senegal uses string French month names (Janvier, Février…) via `month_map`; Togo months appear to be numeric integers directly.
- **Dispatch**: N/A (reference only)

---

### Benin 2018-19
- **File**: `lsms_library/countries/Benin/2018-19/Data/ehcvm_individu_ben2018.dta`
  (Also exists at `lsms_library/countries/Benin/2018-2019/Data/ehcvm_individu_ben2018.dta` — duplicate wave folder)
- **DVC pull**: OK
- **All columns (53)**: country, year, vague, hhid, grappe, menage, numind, zae, region, sousregion, milieu, hhweight, resid, sexe, **age**, lien, mstat, religion, nation, agemar, ea, em, ej, mal30j, aff30j, arrmal, durarr, con30j, hos12m, couvmal, moustiq, handit, handig, alfab, scol, educ_scol, educ_hi, diplome, telpor, internet, activ7j, activ12m, branch, sectins, csp, volhor, salaire, emploi_sec, sectins_sec, csp_sec, volhor_sec, salaire_sec, bank
- **DOB columns**: NONE — no annee_nais, mois_nais, or equivalent birth-year columns
- **Age column**: `age` ("Age en annees") — pre-computed integer age
- **Sentinel (100-row sample)**: range [0, 82], NaN=0. No -1, -2, 99, 9999 observed.
- **Interview date columns present**: `ea` (year, e.g. "2019"), `em` (month), `ej` (day) — but these are interview date, not DOB.
- **Verdict**: NONE — no DOB components available for `age_handler()` recovery
- **Dispatch for Step 4 (Benin)**: **BLOCK** (no DOB columns in harmonised file)

---

### Burkina_Faso 2018-19
- **File**: `lsms_library/countries/Burkina_Faso/2018-19/Data/ehcvm_individu_bfa2018.dta`
- **DVC pull**: OK
- **DOB columns**: NONE
- **Age column**: `age` ("Age en annees")
- **Sentinels (100-row sample)**: range [0, 71], NaN=0. No sentinels observed.
- **Verdict**: NONE
- **Dispatch for Step 4 (Burkina_Faso 2018-19)**: **BLOCK** (no DOB columns)

---

### Burkina_Faso 2021-22
- **File**: `lsms_library/countries/Burkina_Faso/2021-22/Data/ehcvm_individu_bfa2021.dta`
- **DVC pull**: OK
- **All columns (58)**: country, year, vague, hhweight2021, hhweight_panel, hhid, grappe, menage, pid, zae, zaemil, region, province, commune, milieu, resid, sexe, **age**, lien, mstat, religion, ethnie, nation, agemar, mal30j, aff30j, arrmal, durarr, con30j, hos12m, couvmal, moustiq, handit, handig, alfa, alfa2, scol, educ_scol, educ_hi, diplome, telpor, internet, activ7j, activ12m, branch, sectins, csp, volhor, salaire, emploi_sec, sectins_sec, csp_sec, volhor_sec, salaire_sec, bank, serviceconsult, persconsult, enquete
- **DOB columns**: NONE
- **Age column**: `age` ("Age en annees")
- **Sentinels (100-row sample)**: range [0, 79], NaN=0. No sentinels observed.
- **Verdict**: NONE
- **Dispatch for Step 4 (Burkina_Faso 2021-22)**: **BLOCK** (no DOB columns)

---

### CotedIvoire 2018-19
- **File**: `lsms_library/countries/CotedIvoire/2018-19/Data/Menage/ehcvm_individu_CIV2018.dta`
  (Note: filename is `CIV` (uppercase), unlike other countries that use lowercase country codes; file was already local, no DVC pull needed)
- **DVC pull**: N/A — file already present on disk (not managed by `.dvc` file for this wave)
- **All columns (53)**: country, year, hhid, numind, grappe, menage, hhweight, vague, zae, region, sousregion, milieu, ej, em, ea, resid, sexe, **age**, lien, mstat, religion, nation, agemar, mal30j, aff30j, arrmal, durarr, con30j, hos12m, couvmal, moustiq, handit, handig, alfab, scol, educ_scol, educ_hi, diplome, telpor, internet, activ7j, activ12m, branch, sectins, csp, volhor, salaire, emploi_sec, sectins_sec, csp_sec, volhor_sec, salaire_sec, bank
- **DOB columns**: NONE (ej/em/ea present but are interview day/month/year, not birth)
- **Age column**: `age` ("Age en annees")
- **Sentinels (100-row sample)**: range [0, 62], NaN=0. No sentinels observed.
- **Verdict**: NONE
- **Dispatch for Step 4 (CotedIvoire 2018-19)**: **BLOCK** (no DOB columns)

---

### Guinea-Bissau 2018-19
- **File**: `lsms_library/countries/Guinea-Bissau/2018-19/Data/ehcvm_individu_gnb2018.dta`
- **DVC pull**: OK
- **All columns (50)**: country, year, hhid, vague, grappe, menage, numind, zae, region, sousregion, milieu, hhweight, resid, sexe, **age**, lien, mstat, religion, nation, agemar, mal30j, aff30j, arrmal, durarr, con30j, hos12m, couvmal, moustiq, handit, handig, alfab, scol, educ_scol, educ_hi, diplome, telpor, internet, activ7j, activ12m, branch, sectins, csp, volhor, salaire, emploi_sec, sectins_sec, csp_sec, volhor_sec, salaire_sec, bank
- **DOB columns**: NONE
- **Age column**: `age` ("Age en annees")
- **Sentinels (100-row sample)**: range [0, 90], NaN=0. No sentinels observed.
- **Verdict**: NONE
- **Dispatch for Step 4 (Guinea-Bissau 2018-19)**: **BLOCK** (no DOB columns)

---

### Mali 2018-19
- **File**: `lsms_library/countries/Mali/2018-19/Data/ehcvm_individu_mli2018.dta`
- **DVC pull**: OK
- **All columns (52)**: country, year, hhid, grappe, menage, numind, vague, zae, region, sousregion, milieu, hhweight, resid, sexe, **age**, lien, mstat, religion, nation, agemar, mal30j, aff30j, arrmal, durarr, con30j, hos12m, couvmal, moustiq, handit, handig, alfab, scol, educ_scol, educ_hi, diplome, telpor, internet, activ7j, activ12m, branch, sectins, csp, volhor, salaire, emploi_sec, sectins_sec, csp_sec, volhor_sec, salaire_sec, bank, ea, em
- **DOB columns**: NONE (ea=interview year, em=interview month)
- **Age column**: `age` ("Age en annees")
- **Sentinels (100-row sample)**: range [0, 63], NaN=0. No sentinels observed.
- **Verdict**: NONE
- **Dispatch for Step 4 (Mali 2018-19)**: **BLOCK** (no DOB columns)

---

### Mali 2021-22
- **File**: `lsms_library/countries/Mali/2021-22/Data/ehcvm_individu_mli2021.dta`
- **DVC pull**: OK
- **All columns (59)**: country, year, vague, hhid, grappe, menage, numind, zae, zaemil, region, prefecture, commune, milieu, hhweight, resid, sexe, **age**, lien, mstat, religion, ethnie, nation, agemar, mal30j, aff30j, arrmal, durarr, con30j, hos12m, couvmal, moustiq, handit, handig, alfa, alfa2, scol, educ_scol, educ_hi, diplome, telpor, internet, activ7j, activ12m, branch, sectins, csp, volhor, salaire, emploi_sec, sectins_sec, csp_sec, volhor_sec, salaire_sec, bank, s02q14, s02q29, s02q31, serviceconsult, persconsult
- **DOB columns**: NONE
- **Age column**: `age` ("Age en annees")
- **Sentinels (100-row sample)**: range [0, 79], NaN=0. No sentinels observed.
- **Verdict**: NONE
- **Dispatch for Step 4 (Mali 2021-22)**: **BLOCK** (no DOB columns)

---

### Niger 2018-19
- **File**: `lsms_library/countries/Niger/2018-19/Data/ehcvm_individu_ner2018.dta`
- **DVC pull**: OK
- **All columns (51)**: country, year, hhid, grappe, menage, numind, zae, domaine, region, sousregion, milieu, hhweight, resid, sexe, **age**, lien, mstat, religion, nation, agemar, mal30j, aff30j, arrmal, durarr, con30j, hos12m, couvmal, moustiq, handit, handig, alfab, scol, educ_scol, educ_hi, diplome, telpor, internet, activ7j, activ12m, branch, sectins, csp, volhor, salaire, emploi_sec, sectins_sec, csp_sec, volhor_sec, salaire_sec, bank, vague
- **DOB columns**: NONE
- **Age column**: `age` ("Age en annees")
- **Sentinels (100-row sample)**: range [0, 80], NaN=0. No sentinels observed.
- **Note**: Niger has a local `age_handler()` function in `_/niger.py` (a different, older implementation). The 2018-19 wave uses `ehcvm_individu_ner2018.dta` with only a pre-computed `age` — the local `age_handler` is not called for this wave.
- **Verdict**: NONE
- **Dispatch for Step 4 (Niger 2018-19)**: **BLOCK** (no DOB columns)

---

### Niger 2021-22
- **File**: `lsms_library/countries/Niger/2021-22/Data/ehcvm_individu_ner2021.dta`
- **DVC pull**: OK
- **All columns (56)**: country, year, vague, hhid, grappe, menage, numind, zae, zaemil, region, departement, commune, milieu, hhweight, resid, sexe, **age**, lien, mstat, religion, ethnie, nation, agemar, mal30j, aff30j, arrmal, durarr, con30j, hos12m, couvmal, moustiq, handit, handig, alfa, alfa2, scol, educ_scol, educ_hi, diplome, telpor, internet, activ7j, activ12m, branch, sectins, csp, volhor, salaire, emploi_sec, sectins_sec, csp_sec, volhor_sec, salaire_sec, bank, serviceconsult, persconsult
- **DOB columns**: NONE
- **Age column**: `age` ("Age en annees")
- **Sentinels (100-row sample)**: range [1, 77], NaN=0. No sentinels observed.
- **Verdict**: NONE
- **Dispatch for Step 4 (Niger 2021-22)**: **BLOCK** (no DOB columns)

---

## Dispatch decisions

| Step | Target | Decision | Reason |
|------|--------|----------|--------|
| Step 3 | Togo 2018 | **GO** | Full DOB in s01_me file; 100% of Age-null rows recoverable via birth year |
| Step 4 | Benin 2018-19 | **BLOCK** | EHCVM harmonised file has pre-computed `age` only, no DOB columns |
| Step 4 | Burkina_Faso 2018-19 | **BLOCK** | Same — `age` only |
| Step 4 | Burkina_Faso 2021-22 | **BLOCK** | Same — `age` only |
| Step 4 | CotedIvoire 2018-19 | **BLOCK** | Same — `age` only |
| Step 4 | Guinea-Bissau 2018-19 | **BLOCK** | Same — `age` only |
| Step 4 | Mali 2018-19 | **BLOCK** | Same — `age` only |
| Step 4 | Mali 2021-22 | **BLOCK** | Same — `age` only |
| Step 4 | Niger 2018-19 | **BLOCK** | Same — `age` only |
| Step 4 | Niger 2021-22 | **BLOCK** | Same — `age` only |

**Summary**: 1 GO (Togo), 9 BLOCK.

---

## Key surprises / design implications

1. **PLAN risk #2 confirmed**: The EHCVM harmonised `ehcvm_individu_*.dta` files have dropped DOB columns entirely. The Plan mentioned `annee_nais`/`mois_nais` as possible column names — these do not exist in any of the 9 harmonised files audited. The `age_handler()` EHCVM fan-out (Step 4) cannot proceed as designed for any country except Togo.

2. **Togo is divergent from other EHCVM countries**: Togo 2018 uses the raw questionnaire file `s01_me_tgo2018.dta` for `household_roster` (same instrument as Senegal), not the harmonised `ehcvm_individu_tgo2018.dta`. This is why Togo 2018 has DOB columns while all other EHCVM-batch waves do not.

3. **Togo DOB format differs from Senegal**: Senegal `s01q03b` (month) contains French month names (Janvier, Février…) and uses a `month_map` in the `Age()` formatting function. Togo `s01q03b` contains numeric integers (1–12) already. The Togo wave script must NOT use Senegal's `month_map` — pass the integer directly. Also, Togo DOB columns are dtype `object` with true NaN (not integer with 9999 sentinel like Senegal). The `mapping: {-1: null, '-1': null}` sentinel in the YAML is not needed for Togo DOB columns (they use NaN), but may still be needed for the `Age` column (`s01q04a` has sentinel 0 (7 rows) and 99 (2 rows)).

4. **EHCVM alternative approach**: Since the harmonised files only have `age`, the EHCVM batch issue is about dtype consistency (`age` as `Int64`) not DOB recovery. This is addressed by the framework dtype fix in Step 2, not per-country `age_handler()` adoption. Age coverage for the EHCVM batch looks high (0 NaN in 100-row samples), so there may be no material coverage gap for those countries.

5. **Benin has a duplicate wave folder**: `Benin/2018-19/` and `Benin/2018-2019/` both contain `ehcvm_individu_ben2018.dta.dvc`. Both have the same file. Worth investigating whether the duplicate folder creates issues, but out of scope for this audit.

6. **CotedIvoire file naming**: `ehcvm_individu_CIV2018.dta` uses uppercase country code, unlike all other EHCVM files (`ben`, `bfa`, `gnb`, `mli`, `ner`, `sen`). The file is also present on disk directly (not via `.dvc` file), unlike other EHCVM countries.

7. **Niger local `age_handler` duplicate**: `lsms_library/countries/Niger/_/niger.py` contains a local `age_handler()` function that is a different implementation from `tools.age_handler()`. It is not currently called for 2018-19 or 2021-22 waves. This should be cleaned up as part of Step 2 or a separate housekeeping task.

---

## Raw data

No DVC pull failures. All 11 files were successfully retrieved:
- Stale DVC lock at `.dvc/tmp/lock` was removed before pulling (leftover from a previous process).
- `dvc-s3` was already installed in the venv; the `dvc` CLI at system PATH did not see it, but `.venv/bin/dvc` worked correctly.
- CotedIvoire file (`ehcvm_individu_CIV2018.dta`) was already present locally (not under DVC tracking for this wave).
