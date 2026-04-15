# Issue #168 POC Report — harmonize_assets Phase 1+2

## SCOPE DEVIATIONS: none

---

## 1. Worktree + Branch

- **Worktree**: `/global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library/.claude/worktrees/issue_168_poc/`
- **Branch**: `issue_168_poc`
- **Parent commit verification**: `d2552614 fix(country.py): close GH #176 — wrap yaml.safe_load with context manager`

## 2. Commits

```
88681571  feat(Uganda/assets): wire harmonize_assets mapping across 8 waves (GH #168 Phase 2)
6848f13a  feat(categorical_mapping): add global harmonize_assets canonical vocabulary (GH #168 Phase 1)
```

## 3. Uganda j value_counts (post-wire rebuild)

All 65 raw labels map to 30 canonical labels. No raw label leaks.

```
j
Furniture               22149
House                   17601
Phone (Mobile)          13974
Radio                   11767
Bicycle                  8428
Land                     7772
Appliances               5915
OtherBuilding            4420
Jewelry                  3613
Other                    3548
TV                       2901
Electronics              2596
Hoe                      2463
Solar Panel              2211
Motorcycle               1853
Panga/Machete            1809
Bednet                    991
Car                       594
Computer                  453
Enterprise Equipment      379
Generator                 226
Agricultural Tools        194
DVD/CD Player             192
Plough                    177
Refrigerator              156
Wheelbarrow               153
Other Transport           126
Boat                      111
Cooker                     30
Phone (Landline)            6
```

## 4. Verification

- Rebuild: `rm -f ~/.local/share/lsms_library/Uganda/var/assets.parquet` + `LSMS_NO_CACHE=1`
- 116808 rows, 30 unique j values (vs 65 raw)
- `tests/test_uganda_tables.py`: 1 passed, 8 skipped
- `tests/test_uganda_invariance.py`: 1 FAILED (`var/assets.parquet` baseline) — pre-existing, also fails on `development` without worktree
- `tests/test_table_structure.py::test_no_duplicate_rows[Ethiopia/cluster_features]`: FAILED — pre-existing, unrelated

## 5. Surprises

Uganda's 8 asset waves all use `!make` scripts — no `data_info.yml` stanzas exist. YAML `mappings:` pattern cannot apply because ownership filtering (`h14q3.isin(...)`) cannot be expressed in YAML. Wiring implemented as framework hook in `_finalize_result` instead of 8 × `data_info.yml` edits.

3 stringified-float codes (`113.0`, `114.0`, `115.0`) from 2005-06 wave — mapped to `Other`.

## 6. Ready for Phase 3

Mapping file has 136 rows. Global loading in `Country.categorical_mapping` means YAML-path countries can add `mappings: [harmonize_assets, 'Original Label', 'Preferred Label']` in `j:` stanzas immediately. `!make`-script countries get the hook automatically. 12 remaining countries' raw labels still need adding to `harmonize_assets.org`.
