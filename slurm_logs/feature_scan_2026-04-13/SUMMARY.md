# Feature Scan — Overall Summary

**Date**: 2026-04-13  
**Scope**: All canonical features except `household_roster` (scanned separately) and `panel_ids` (property, not DataFrame).

**Total (country × feature) probes**: 111  
**OK**: 100 | **Error**: 11

## Features Ranked by Severity

| Feature | OK | Error | Violations | Missing Required | Extra Cols | High-Null | Severity Score |
|---------|---:|------:|-----------:|-----------------:|-----------:|----------:|---------------:|
| `shocks` | 12 | 1 | 0 | 0 | 26 | 76 | **264** |
| `food_acquired` | 13 | 2 | 0 | 0 | 83 | 0 | **103** |
| `cluster_features` | 29 | 1 | 0 | 5 | 11 | 2 | **67** |
| `assets` | 13 | 1 | 0 | 0 | 46 | 0 | **56** |
| `interview_date` | 11 | 4 | 0 | 0 | 11 | 0 | **51** |
| `housing` | 13 | 0 | 0 | 0 | 26 | 0 | **26** |
| `individual_education` | 9 | 2 | 0 | 0 | 0 | 2 | **26** |
| `plot_features` | 0 | 0 | 0 | 0 | 0 | 0 | **0** |

## One-Sentence Summary Per Feature

- **`shocks`**: 11/13 countries OK (Uganda timed out, Malawi food chain error unrelated); confirmed Cope* rogue columns persist (Niger: 26 extra cols); AffectedIncome/Assets/Production/Consumption remain fully null in all countries.
- **`food_acquired`**: 12/15 countries OK (Malawi Makefile error, Nepal/GhanaLSS missing cache); 54+ non-canonical columns persist across countries, confirming 2026-04-12 audit finding.
- **`cluster_features`**: 28/30 countries OK; Nepal fails all 5 features (no cached data); Uganda takes ~95s (Makefile path); `District` column has float-stringified values in several countries.
- **`assets`**: 13/14 countries OK (Nepal hangs 465s with PathMissingError on Stata file); no canonical column violations; `Quantity/Age/Value/Purchase Price` present; Nepal is the sole blocker.
- **`interview_date`**: 10/15 countries OK (Uganda timed out, Malawi/GhanaLSS/Nepal missing cache); `int_t` (lowercase) vs canonical `Int_t` (title-case) mismatch confirmed in 10 countries.
- **`housing`**: All 13 countries OK with no canonical violations; Roof/Floor string columns present everywhere; minor: Uganda warns on categorical mapping h1bq1/h1bq3.
- **`individual_education`**: 8/11 countries OK (Burkina_Faso and Nepal missing cached parquet, Uganda returned ok with 14k rows); `Educational Attainment` column canonical in all successful countries.
- **`plot_features`**: Zero countries declare this feature; table remains entirely unimplemented as confirmed by 2026-04-12 audit.

## Known Hangs / Slow Probes (>60s)

| Country | Feature | Elapsed | Notes |
|---------|---------|--------:|-------|
| Nepal | assets | 465s | PathMissingError on Stata file in 1995-96 wave; no DVC fallback; killed by pool timeout |
| Uganda | cluster_features | 95s | Makefile path; completed OK |
| Uganda | housing | 101s | Makefile path; completed OK |
| Uganda | individual_education | 96s | Makefile path; completed OK |
| Uganda | shocks | 120s | Makefile !make path hangs; killed at 120s timeout |
| Uganda | interview_date | 120s | Makefile !make path hangs; killed at 120s timeout |

## Per-Feature Memo Paths

- `slurm_logs/feature_scan_2026-04-13/cluster_features_rescan.md`
- `slurm_logs/feature_scan_2026-04-13/shocks_rescan.md`
- `slurm_logs/feature_scan_2026-04-13/food_acquired_rescan.md`
- `slurm_logs/feature_scan_2026-04-13/interview_date_rescan.md`
- `slurm_logs/feature_scan_2026-04-13/assets_rescan.md`
- `slurm_logs/feature_scan_2026-04-13/housing_rescan.md`
- `slurm_logs/feature_scan_2026-04-13/individual_education_rescan.md`
- `slurm_logs/feature_scan_2026-04-13/plot_features_rescan.md`
