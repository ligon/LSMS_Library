# KNOWN ISSUES — plot_features rollout (GH #167), 2026-06-03
# Surfaced by scrum-master review of the per-country PRs. None merged.

## ISSUE 1 (systematic, all 7 EHCVM countries): Area has no plausibility clamp
The Uganda pilot clamps implausible parcel areas (>2500 acres -> NaN). The EHCVM
helper (Mali reference, copied to all siblings) does NOT clamp, so raw-data
outliers survive: Benin max ~711,000 ha, Guinea-Bissau max ~281,000,000 ha
(medians are sane ~1 ha). These poison area-weighted aggregates.

AFFECTED PRs: Mali #284, Niger, Senegal #285, Burkina_Faso, Benin #287,
Togo, Guinea-Bissau #286 (any EHCVM country).

RECOMMENDED FIX (one line in each `{country}.py` plot_features helper, right
after `area_ha` is finalized; EHCVM Area is in hectares):
    # plausibility clamp: smallholder parcels; drop data-entry errors
    area_ha = area_ha.where((area_ha <= 1000) | area_ha.isna(), pd.NA)
    area_unit = area_unit.where(area_ha.notna(), pd.NA)
(1000 ha is generous for EHCVM smallholders; tune if desired. Mirrors the
Uganda clamp, which is in acres.) Best applied as a single follow-up commit
once the EHCVM PRs land on the integration branch, OR per-PR before merge.

## ISSUE 2 (Guinea-Bissau, cosmetic): duplicate helper filename
Pre-existing `Guinea-Bissau/_/guinea-bissau.py` (hyphen, not importable as a
module) coexists with the new `guinea_bissau.py` (underscore, importable). Not a
bug, but consider consolidating to avoid confusion.

## ISSUE 3 (Uganda pilot, PR #280): RESOLVED 2026-06-04 — was a false alarm
Earlier I claimed 2019-20 "wasn't rebuildable on this node". That was MY
wrong-path probe: 2019-20 stores its files at the nested path
`Data/Agric/agsec2a.dta` (lowercase), not the flat `../Data/AGSEC2A.dta` the
other waves use; the wave script itself uses the correct path. Verified:
ran the 2019-20 wave script through the fixed helper (5,365 rows, wave-keyed
Tenure correct) AND the full `Country('Uganda').plot_features()` end-to-end:
42,593 rows, all 8 waves, index unique, is_this_feature_sane = ok. The Tenure
fix is verified on ALL 8 waves; no data-node rebuild needed.

## ISSUE 4 (framework, from interview_date audit — informational): for the NEXT
feature (interview_date), budget two framework cleanups: current interview_date
parquets lack the `v` cluster level, and dates are stored as strings not
datetime64. These are framework-level, not per-country.
