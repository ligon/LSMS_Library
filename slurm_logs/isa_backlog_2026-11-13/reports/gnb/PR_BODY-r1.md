
**Guinea-Bissau: 2018-19 label bundle; 2021-22 sample, roster, cluster_features, shocks and assets (food held)**

## Summary

**2018-19 label bundle (one GNB re-warm):**
- **#1107:** a `harmonize_input` table maps ureia → Urea, fosfatos → Phosphate and NPK/Fórmula única → NPK. The DAP catch-all stays unmatched until #1696 is ruled. A country `u_kg` table gives Quilograma 1 kg and Grama 0.001 kg. `nitrogen_kg` goes from 0 to 293 matched rows.
- **#1119:** a `harmonize_seed_crop` table gives each seed record (codes 11-20) the crop that `crop_production` serves; an unmapped seed label passes through.
- **#1539:** Djagatu → Jaxatu and Beringela → Aubergine. `crop_production` goes from 12,173 to 12,188 rows, and the `GrainCollapseWarning` (11 rows destroyed) is gone. The Djagatu relabel also moves 2,990 `food_acquired` rows and 1,356 `community_prices` rows.
- **#1226:** community-price product 94 takes household code 95's label (696 rows).
- **#1118:** a missing input unit is now `Unknown` (0 rows affected).

**2021-22 (#1697), EHCVM-2 WB 6274, on the Togo/Benin/CIV template:**
- `E2_`-prefixed ids; no panel link is served.
- `weight` = `panel_weight` = `hhweight`.
- Strata are region × milieu by the #1575 rule. The file's own `strate` equals that construction one-to-one.
- The 341 leavers are dropped, as `ehcvm_individu` does (#1565 is open).
- Shocks follow #1556; assets come from the owned lines of the grid.

**`food_acquired` 2021-22 is not wired.** Every released `s07bq03a` and `s07bq07a` is an even integer, and 3,945 consumed rows are zeroed. It is undocumented and cannot be inverted; the evidence is in `Guinea-Bissau/_/CONTENTS.org`, and the question is with the maintainer.

## Verification

- Cold, private roots, base `f138a3d61`. Every 2018-19 table is frame-equal to base apart from the Phase A relabels.
- Hash census: Guinea-Bissau only (22 of 555 tables, 44 of 2,321 waves); `build_transforms_fingerprint` 0 of 42.
- Tests: 36 targeted files pass, 3,307 passed. `tests/ -m "not requires_s3 and not slow"` passes, 15,515 passed.

## Must-state

- Strata extension: the ruling names Togo, Benin and CIV, and was applied here per the brief.
- The 2021-22 cover GPS is not 2018-19's fix.
- 2018-19's `Purchased` reads the gift question; recorded, not changed.
- `seed_kg` still reads nothing for GNB.
- Shared `kinship.yml` gains `Sobrinho(A)` (moves no hash).
- `PARTIALLY_WIRED_SAMPLE` still lists GNB 2021-22, and the coverage snapshot needs regrading at integration.

## Tests re-pinned

The GNB #1575 and gh812 strata scopes; the 2018-19 `cluster_features` fixture scope; the `ehcvm_condition_originals.json` byte hash (comment-only edit).

Addresses #1697, #1107, #1539, #1119, #1226, #1118.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

