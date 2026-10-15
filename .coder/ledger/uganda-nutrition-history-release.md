# Uganda nutrition historical baseline investigation (release 2026-10-14)

SCOPE: evidence only. No fixture, scientific default, or package implementation change.
Read country CONTENTS including TODO/WAITING/LOGBOOK before investigation;
relevant idiosyncrasy is that canonical food drops the curated Kgs column and
all API kg factors are computed at runtime. The explicit instruction remains:
“Do not regenerate the baseline until the direction is signed off.”

Prior art: `.coder/ledger/850-food-kg-item-axis{,-impl}.md`,
`slurm_logs/gh850_design/DESIGN.org`, `measure_impl.py`, `measure.py`,
`slurm_logs/venv_provenance/nutrition_delta.sbatch`, and #967's archived
`uganda967.org` plus `validation/{validate,attribute}_food_units.py`.
Standing definitions: `.coder/ledger/STANDING.md`, `CLAUDE.md`,
`lsms_library/data_info.yml`. Use rg/git; no GitNexus tool available.

Reuse actual historical transform source, nutrition.py's matrix calculation,
and the unchanged invariance fingerprint. Reuse the country lane's independent
cold base/candidate food and FCT frames read-only. Prove historical/current
endpoint hashes before attributing intermediate commit effects. Any
counterfactual is labelled and never substituted for shipped code. Compare
matrix results with independent food-level sums. Keep household frames private;
archive aggregate evidence and a runnable driver only. No country build or
shared writable L2, dependency change, DVC CLI, or fixture generation.

## Completed evidence

Historical and current hashes reproduce exactly with identical input; direct
parents prove all four #850 increments. Whole historical package also reproduces
old hash under current dependencies after dropping only new API attrs from a
private copy (old package lacks the recall metadata class). 12 actual stages,
four parents, one old-package endpoint, and one explicitly unshipped denominator
counterfactual are archived. All 15 nutrient changes independently sum from
native food rows to matrix deltas within 1.44e-6 absolute; current matrix equals
country lane cold nutrition exactly. Raw food/FCT hashes match historical fixture.

Country documentation contradictions are quoted in report and CONTENTS addendum;
new FCT4003 source-note-versus-numeric-A contradiction is recorded without a
scientific choice. Hold retained; fixture remains untouched. Evidence drivers
and aggregate-only JSON/CSV are in slurm_logs/release_landing_2026-10-14/
uganda-nutrition-history/; household frames remain private release scratch.
No L2 build or cache invalidation: only CONTENTS and evidence/ledger files change.
