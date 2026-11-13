# Household food-module evidence — GH #846

Independent C4 review supports two findings within the **depicted household
food modules** in each of Tanzania's six logical waves:

| Source concept (price-study spelling) | Instrument finding within that module |
|---|---|
| `reported_purchase_price` | Purchased quantity and spending are asked together; no separately elicited purchase unit price. |
| `own_consumption_valuation` | Own-produced food consumed inside the household has a quantity/unit pair, without a monetary value response. |

These are module findings, **not whole-survey absence or coverage closure**.
All twelve country-local records have `verdict=unsure`, `closes=False`, and a
separate `instrument_finding=not-asked-within-module`. C1 and C3 remain
outstanding. The original #846 live integration requirement remains open.

## Exact wave and instrument scope

All page locators below are **one-based PDF pages**. The UPQ U panel at 87–88
is the comprehensive union, not independent proof of each original round.

| Logical wave | Depicted scope | Instrument pages | Original-round provenance |
|---|---|---|---|
| 2008–09 | Original R1 Section K1 in UPQ module J1 | UPQ 87–88,95–96 | BID 5–6,19,29; R1 panel 95–96 |
| 2010–11 | Original R2 Section K1 in UPQ module J1 | UPQ 87–88,93–94 | BID 5–6,19,29; R2 panel 93–94 |
| 2012–13 | Original R3 Section J1 | UPQ 87–88,91–92 | BID 5–6,19,29; R3 panel 91–92 |
| 2014–15 | Original R4 Section J1 | UPQ 87–90 | BID 5–6,19,29; R4 panel 89–90 |
| 2019–20 | NPS-SDD standard Section J1 | SDD 53–57 | Current standard household instrument |
| 2020–21 | NPSY5 standard J1 **and released alternate J1A** | Y5 34–42 | Standard 34–38 (printed 29–33); alternate 39–42 (printed 29–32) |

BID 5–6 explains that original R1–R4 variables/codes and round-specific
blackouts are depicted beside the union model. BID 19 names J1 in all four
rounds and the original files; BID 29 maps R1/R2 Section K to R3/R4 Section J.
This is authenticated UPD **reconstruction of original panels**, not four
independently obtained contemporaneous original PDFs. Each corresponding
round panel was read; sharing the `2008-15/` folder alone proves nothing.

The R1 purchase amount/measure pair is `skq3_amount/skq3_meas`, spending is
`skq4`, and own-production quantity/unit is `skq5_amount/skq5_meas`. R2 uses
`hh_k03_2/hh_k03_1`, `hh_k04`, `hh_k05_2/hh_k05_1`; R3/R4 use
`hh_j03_2/hh_j03_1`, `hh_j04`, `hh_j05_2/hh_j05_1`. The uniform model has
`hj_03_*` purchase quantity, `hj_04` spending and `hj_06_*` own quantity;
`hj_05` is purchase location. Field order and meaning were not inferred from
one shared variable name. BID 19's broad summary of quantity/value from several
sources does not establish a monetary value column for **every** source;
the detailed channel columns govern this scoped finding.

**Y5 alternate is released evidence.** The alternate pages carry editorial
marks, but official DDI 5639 lists `hh_sec_ja1` and its quantity, spending,
size and weighing fields, plus `j1_selection` in main `hh_sec_j1`. Both
variants were reviewed. Alternate q2a/b and q7–9 add pictures, size, metric
quantity and weighing questions, not either monetary concept above. The marks
do not prove nonuse. No actual assignment, item eligibility, sample count or
lost API row is inferred from this document/dictionary review.

## Completed methods and limits

`checks_run=C2;C4` records the actual completed methods **for these concepts
and depicted modules**: C2 instrument/channel and official-dictionary
comparison across the six waves; C4 authenticated instrument reads with an
independent review. It is not a shipped-source label census or an assertion
that every source-file difference was examined. Official dictionaries 3814,
3885 and 5639 describe 42/75/67 files and corroborate the food-channel fields.

Four public PDF downloads match both the held DVC sidecar MD5/size and the
SHA256 values below. The author extracted 305 text-bearing pages with no parser
warnings; the independent reviewer authenticated all 336 packet members and
reproduced 30 critical-page normal-text hashes. Its secondary layout extracts
emitted **14 rotated-text/incomplete-layout warnings**. These are preserved
separately from the original zero-warning normal-text extraction. Layout is
not treated as complete. No raster rendering, OCR, complete visual-blackout
inspection or infallible table alignment is claimed.

| Key | Repository document / official download | PDF SHA256 |
|---|---|---|
| UPQ | `2008-15/Documentation/nps_upq_questionnaire.pdf` ([WB3814](https://microdata.worldbank.org/catalog/3814/download/48978)) | `0c4df86698f500dbe3030704993f513d0544ff3e2957a9c77ad39d45858588c6` |
| BID | `2008-15/Documentation/NPS_UPD_Basic_Information_Document.pdf` ([WB3814](https://microdata.worldbank.org/catalog/3814/download/48979)) | `13832fd7d25894e3ad306db3fe7aef3530fbe0431a39e4b175f3168c58bf71c8` |
| SDD | `2019-20/Documentation/nps_sdd_household_qx_final_english_.pdf` ([WB3885](https://microdata.worldbank.org/catalog/3885/download/54804)) | `654eaabaf6884b03c678ad57ee07287be41fad48315f58e5b6a449b2c11792ea` |
| Y5 | `2020-21/Documentation/nps_household_qx_y5_final_.pdf` ([WB5639](https://microdata.worldbank.org/catalog/5639/download/60752)) | `97980dfdb34a222c5f3b8f4f5e4cfc291a1770a07e4f1ad1d51d3aaae6302fef` |

Official dictionary metadata: [DDI 3814](https://microdata.worldbank.org/index.php/metadata/export/3814/ddi),
[DDI 3885](https://microdata.worldbank.org/index.php/metadata/export/3885/ddi),
[DDI 5639](https://microdata.worldbank.org/index.php/metadata/export/5639/ddi).
Frozen packet `Tanzania846-c4-v1/ARTIFACTS.json` SHA256:
`17886d8ae337b9c922e702b3f59de83a2323243a6ea5cf219fd50978f3bb8286`.
Accepted review `slurm_logs/isa_recovery_2026-10-29/Tanzania/846-independent-Ethiopia/REVIEW.md`
SHA256: `370da7f8c8cce9bc1e2d817697a31642328caca4b2f07098ad970e4f6fbf9c64`.
The private document-only reader zip-imported official-hash pypdf 6.1.1;
no package installation, project dependency or shared runtime change occurred.

## Open coverage acceptance

- **C1:** current shipped-source variable/value-label census and its exact
  scope remain outstanding. Catalog dictionaries and sidecar matching are
  corroboration, not a current header census. No microdata was read here.
- **C3:** applicability and direct-versus-derived harmonized source lineage
  remain unresolved. A harmonized null is never absence evidence; an imputed
  or derived positive does not by itself prove this separately elicited
  concept. No harmonized payload was read.

The [country-local CSV](capabilities/food_price_absent_verdicts.csv) extends
merged PR1308's existing `load_verdicts(path)` schema. Its legacy `feature`
column holds **source-concept names**, not canonical API features. Added fields
record `instrument_finding`, `module_scope`, exact instrument/PDF locators,
completed methods, outstanding checks and nonclosure. The existing loader
preserves these extra fields; it does not enforce their scientific sufficiency.
Even with an explicit path, `unsure` stays open. Default live matrix loading
does not read this file. There is no new public accessor or table registration.

`SeriesCapability` still describes whole features/series, automatically
reports C1;C4 for questionnaire validation, and expands supplied waves. Do not
use that generator for these partial findings, copy these source concepts into
the canonical vocabulary, or add `lacks=('food_prices',)` to NPS.

## Preserved distinctions

- **Purchase unit values:** spending q4 explicitly refers to purchased quantity
  q3. Their ratio remains a supported purchase unit value in the native unit;
  no `food_prices` mode or `food_acquired` table is removed.
- **Outside-HH valuation:** SDD Section F PDF 27 and Y5 Section F PDF 26
  (printed 23) ask market value for food consumed outside the household. That
  different module/grain refutes any whole-survey “no food valuation” claim.
- **Imputation:** #585 and EPAR-style purchase-price valuation are separate
  constructions, not reported answers or independent wave evidence. Crop-sale
  and community prices are also different concepts. No default imputation.

The retained `slurm_logs/price_sources/Tanzania/REPORT.org` §1 remains the
historical 2019–20 source; its old API observations are not a current result.
See `.coder/ledger/846-tanzania-wave-concepts.md` for the accepted review and
unchanged integration boundary. Scientific closure and live integration remain
pending on existing #846.
