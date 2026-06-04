# (Country × Feature) Implementable-Gap Coverage Matrix — 2026-06-03
# Read-only analysis (agent aa40b0b). Universe: 34 countries with data_scheme.yml.
# Confidence: audit | spot-check (DVC-pulled, columns seen) | filename | inferred.

## Structural finding
Current declarers of all 4 marginal features are almost entirely the LSMS-ISA /
EHCVM African set + a few outliers (Nepal, GhanaLSS, Timor-Leste). The whole gap
is the non-ISA ECA / Asia / Latin-America survey set — most of which DO carry
these modules; they were simply never wired.

## Ranked implementable gaps (the "fill-in" target list)

| Feature              | Declarers now | High-conf implementable gap | +likely | Effort |
|----------------------|---------------|------------------------------|---------|--------|
| interview_date       | 16            | ~9                           | +4-5    | LOW (cover-page day/mo/yr; near-universal) |
| assets               | 14            | ~7                           | +3-4    | MED (item-name j survey-specific) |
| individual_education | 11            | ~11                          | +5      | HIGH (98+ label spellings to harmonize) |
| shocks               | 13            | ~2-3                         | few     | n/a (module is ISA-specific; absent in gap countries) |

### interview_date implementable gap (high-confidence, spot-checked)
Iraq (*00_cover_page), India (SECT00 intdate/month/year), Pakistan (F00A dayi1/moi1/yri1),
Tajikistan (interview.dta), Cambodia (hh_sec_1 intvw_*), Guyana (COVERN DDE/MDE/YDE),
Kosovo (ID.dta s0i_dat*), China (S00.DTA). PLUS 3 declared-but-uncached free wins:
GhanaLSS, Malawi, Nepal(*) — (*) Nepal has 0 DVC pointers, not implementable.
Framework caveats (not per-country): v cluster level missing from current
interview_date parquets; dates stored as strings not datetime64.

### assets implementable gap (high-confidence)
Iraq (*durables), Albania (durables_cl), Kosovo (DURGOODS), Guyana (DRBLS item01..50),
Serbia (m2_durables), GhanaSPS (03aiii/05aii), Liberia (sect15 item-level). +likely:
GhanaLSS, Tajikistan, Guatemala.

### individual_education implementable gap (high-confidence)
Ethiopia (sect2_hh), GhanaLSS (sec2a), Iraq (*education), Albania (educationA/B/C),
Kosovo (EDUC s4), Guyana (EDUCN), South Africa (M5_EDUC), Tajikistan (SSEC),
Kazakhstan (KZ96EDU), India (SECT03AB), Liberia (sect5). +likely China/Pakistan/
Guatemala/Cambodia(embedded)/Azerbaijan. NOTE: education audit already flags ~98
unharmonized attainment spellings — biggest harmonization burden of the four.

### shocks implementable gap
Iraq (*shocks/*risks) strongest; GhanaSPS (risk-preference proxy); Tajikistan maybe.
Shocks-and-coping is an ISA/EHCVM-specific section — most ECA/LAC/Asia LSMS omit it.

## RECOMMENDATION (next cross-country target after plot_features)
1. interview_date — largest near-certain gap for least effort; every spot-checked
   country has an explicit day/month/year triple; 3 free uncached wins. Budget for
   2 framework cleanups (add v level; string->datetime64).
2. assets — second-broadest (~7 confirmed durables modules), clean (t,i,j) target.
   Avoid shocks as a broad target (only ~2-3 implementable cells).

Nepal: genuinely lacks data on disk (0 DVC pointers) — not implementable for any feature.
