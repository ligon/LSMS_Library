# Section J food-price capability — GH #846

The retained C4 evidence supports `not-asked` for two **source concepts** in
**2019–20 NPS-SDD household Section J**:

| Source concept (price-study spelling) | Instrument finding |
|---|---|
| `reported_purchase_price` | No separately elicited purchase unit price in Section J qq.2–6. |
| `own_consumption_valuation` | Q5 asks own-production consumption quantity, without a monetary value. |

The machine-readable record is [food_price_absent_verdicts.csv](capabilities/food_price_absent_verdicts.csv),
using the existing `coverage_matrix.load_verdicts(path)` schema. Its `feature`
field contains the price study's source-concept names, **not canonical API
features**. This is country-local evidence staged for coordinator review;
the live coverage matrix does not load it, and this change closes no live
matrix cell. It is not a `data_scheme.yml` table registration.

The questionnaire read is retained in
`slurm_logs/price_sources/Tanzania/REPORT.org`, §1, **“The questionnaire,
quoted”** (2026-09-08). Its explicit household instrument locator is
`Tanzania/2019-20/Documentation/nps_sdd_household_qx_final_english_.pdf`,
**Section J: Consumption of Food over Past One Week, questions 2–6**, including
the instruction under q4 that its spending refers to q3's purchased quantity.
There is no page number in that retained citation; none is inferred here.

**Evidence scope:** `validation=questionnaire-validated`, `checks_run=C4`.
This records the earlier analyst's questionnaire evidence. This worker read
the retained report, not the PDF or microdata, and ran no new label sweep,
sibling-wave differential, harmonized-data cross-check or API measurement.
C1–C3 are not claimed. The report's historical zero-row API observations are
not a current API result. No questionnaire absence verdict is recorded here
for 2008–09, 2010–11, 2012–13, 2014–15 or 2020–21: those instruments were not
searched for this task. The shared `2008-15/` folder is not an additional wave.

Three distinctions are essential:

- **Purchased-food unit value is supported.** Q4 spending and q3 quantity refer
  to the same purchases. Their ratio is a genuine purchase unit value, with
  q3's native unit. This finding does not declare `food_prices` or
  `food_acquired` absent and does not remove any table or price mode.
- **Section F is different.** The retained report describes market valuation
  of unpurchased food consumed **outside the household**, at a different grain.
  It does not supply Section J own-production consumption value. The negative
  claim is confined to Section J, not every Tanzania market-value question.
- **Imputation is a separate construction.** The retained
  `slurm_logs/2026-09-09_epar_curation/issues/COMMENT_846.md` describes EPAR's
  imputed own-consumption value using purchase prices. That corroborates the
  distinction between a recorded answer and an estimate; it is not independent
  questionnaire coverage for another wave. No default imputation or #585
  valuation rule is authorized by this record.

## Shared integration boundary

`SeriesCapability.verdict_for()` already supplies the questionnaire-validated
`not-asked` semantics. Its registry currently describes whole features over a
whole series; its automatic `checks_run` is `C1;C4`, and its row generator
expands to every supplied wave. Using it unchanged for all NPS waves would
overstate both the evidence and the object being adjudicated. The source
concepts above are not in the canonical feature vocabulary; `food_prices` is
a broader derived feature with supported unit-value content.

Keep this record local unless the coordinator separately designs a concept-
and wave-scoped integration with explicit actual checks. Do not copy these
rows into the global verdict file as if the matrix already had those cells,
or add `lacks=('food_prices',)` to an NPS registry entry. See
`.coder/ledger/846-tanzania-food-price-capability.md` §§2–6 and the coordinator
question channel. No shared framework or build inputs are changed.

Recorded by Sue / Codex, using retained source evidence; scientific and
integration acceptance remain with the coordinator.
