# Malawi #1724: retain the released IHS5 cross-sectional crop code

## 1. Task
Stop dropping the IHS5 cross-sectional Module G observations whose
native crop code is 49. Keep the literal crop code used by seed inputs.
Do not identify a crop species or reroute any sale.

## 2. Existing machinery
- `Malawi/_/CONTENTS.org` #1228 notes and NSO v06 metadata F43/V1745:
  code 49 has 66 instances in the seasonal release; G does not give
  49 a crop name. D20 labels an Other slot, not its species.
- IHS5 `2019-20/_/crop_production.py` mixes distinct cross-section and
  panel sources. Only the cross-section G source supplies this evidence.
- `_harvest_block`, `_consolidate_harvest`, `assemble_crop_production`,
  `_attach_sales` and `_farm_level_sales` retain native data grain.
  The Farm-level fallback already keeps sold amounts for unmapped 49.
  Publishing the crop label before those decisions would change the price
  cell and risk serving a sale twice.
- `_seed_crop_identity` already carries literal '49'. Reuse its display
  convention, not its initially observed seed population or mapper.
- Search tier: rg, git, exact source and DDI.

## 3. Definitions
Use `data_info.yml` crop/code, Farm-level, Quantity and observation
definitions, and `STANDING.md` and `CLAUDE.md` input/axis contracts.
An Other slot is not a crop species or a proposed local-name mapping.

## 4. Invariants
No new sale amount or unit, and no early change to category-driven price
or sale decisions. Preserve each supported raw native coordinate. No
Panel, perennial, IHS6, code-48 or unmapped arbitrary code change. Witness
is private. Do not change the matrix of 2024-25 and two-text rulings.

## 5. Reuse
Extend the existing harvest driver and assembly boundary only. Annotate
the exact source rows privately. Publish their native code late and keep
all previously served Farm-level sales.

## 6. Open questions
The actual crop identity of 49, use of its text, and scientific treatment
for crop-diversity analysis still require separate evidence or policy.
