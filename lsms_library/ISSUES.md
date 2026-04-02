# Open Issues

Items that need attention. Resolved items are removed; use `git log` for history.
Actionable items should also be filed on GitHub (`gh issue list --repo ligon/LSMS_Library`).

## 2026-03-18 – categorical_mapping lookup broken for index variables

- `mapping:` references in `idxvars` (e.g., `['harmonize_food', 'Original Label', 'Preferred Label']`) may not work reliably due to `column_mapping()` calling `self.categorical_mapping` as a function instead of a property, plus wave-vs-country level lookup mismatch.
- Mali uses `mappings:` (plural) which was silently ignored; fix merged 2026-03-19.
- **Scope:** `country.py` lines 316, 376-384. Partially fixed but may still have edge cases.

## 2026-03-19 – Tanzania panel_ids design problem

- Household splits are handled by retroactively assigning new canonical IDs back to wave 1, inflating the baseline count (9,785 vs ~3,200 actual).
- The WB harmonised panel code uses a head-tracking approach that's more intuitive.
- 2019-20 and 2020-21 are two separate panel branches sharing 2014-15 as ancestor.
- **Proposed fix:** Rewrite Tanzania's `map_08_15()` to follow WB approach. See GitHub #114.

## 2026-03-19 – Housing schema inconsistency across countries

- Malawi/Uganda use binary indicators (`Thatched roof: float`); other countries use categoricals (`Roof: str`).
- The categorical approach is richer and consistent with "pass the detail" principle.
- **Fix needed:** Rewrite Malawi/Uganda housing to use categorical material types.

## 2026-03-30 – Vestigial wave-level dvc.yaml files in Uganda and Malawi

- Wave-level `dvc.yaml` files predate the `data_root()` migration and no longer work correctly.
- The 2005-06 stage poisons the country-level cache with single-wave results.
- **Recommended fix:** Remove wave-level `dvc.yaml` files and `var/` directories. See GitHub #120.
## 2026-04-01 22:19:00Z GhanaLSS – cluster_features

- Waves: 1987-88, 1988-89, 1991-92, 1998-99, 2005-06, 2012-13, 2016-17
- Error: `StageFileDoesNotExistError: '/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/.claude/worktrees/agent-af33830a/lsms_library/countries/CotedIvoire/1986-87/Data/F09D1C.DAT.dvc' does not exist`
