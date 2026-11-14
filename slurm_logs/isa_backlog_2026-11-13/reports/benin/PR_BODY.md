
**Benin: serve the 2018-19 → 2021-22 household link (#1705); 2018-19 plot_inputs reads the purchase and serves `Unknown` (#1118)**

Closes #1705. Addresses #1118 (Benin site only).

### What

**1. Benin is a panel country** (rulings @ligon: 2026-11-12 "Make Benin a panel country"; 2026-11-13 "Unlink the 63").
- **The crosswalk.** `_/panel_ids.py` builds the committed `_/panel_ids.json` and `_/updated_ids.json` (design A).
- **The rule.** A household is linked to the 2018-19 household with the same `(grappe, menage)` when `PanelHH == 'Panel'` and that key is a 2018-19 household. The 2021-22 files record no 2018-19 id.
- **The exception**, registered as `Benin::panel_ids::all-preloaded-left-not-linked`: a household whose whole pre-loaded 2018-19 roster is recorded as gone is not linked. That covers 63 households. Their members match the 2018-19 roster at control level, so they read as replacement households.
- **Result:** **6,815 linked.** 1,217 keep their own `E2_` id: 1,027 new, 116 recurring Non-panel, 63 excepted, 11 Panel with no 2018-19 key.
- **Hazards.** Every key carries the prefix and no target does, so there are no chains and no rename onto an occupied id. The generator asserts both and the release's counts, and the tests execute those checks.
- **Regenerate** with `make -C lsms_library/countries/Benin/_ panel-ids`.

**2. 2021-22 `panel_weight`** (ruling "NA for unlinked").
- It is `poids` on the 6,815 linked households and NA on the other 1,217.
- It is still a **cross-sectional** weight: no longitudinal weight is shipped.
- After renormalisation, every linked value is ×1.09887. `weight` is unchanged.

**3. Benin's first `_/Makefile`.**
- It carries the `panel-ids` target and one rule per wave script.
- No served frame changes, and failure behaviour is unchanged (base already raised `CalledProcessError`).
- What does change:
  - the "Makefile not found" warning is gone;
  - errors name the make target;
  - wave scripts run in the stage-1 builder, which runs in parallel when `LSMS_BUILD_WORKERS` > 1;
  - `_probe_country_builder` runs `make -n`.

**4. 2018-19 `plot_inputs`** (ruling "Fix all three now", and #1118).
- **Purchase.** `Purchased` / `Quantity_purchased` / `Unit_purchased` read s16bq08 / s16bq09a / s16bq09b. The gift goes to the `*_received_as_gift` columns.
- **Unit spelling.** The unit columns are spelled as `u` is and as 2021-22 does (`Kg`).
- **Blank unit.** The one blank unit of use is `Unknown`.
- **Implausible quantities.** Three FCFA-shaped `Quantity_purchased` values are served as recorded. Site Q does not screen this column.

### Verification
All cold, in private roots; Benin base `f138a3d61` against this branch.

- **Link quality** against another household of the same grappe:
  - head sex 95.1% against 68.6%;
  - head age +1..+5 years 60.8% against 10.7%;
  - member birth year within 1 year 63.9% against 10.9%;
  - on the 63 excepted, head sex 54.0% against 55.6%, which is control level.
- **The 19 linked households with no pre-loaded roster stay linked.** Their members match well: 24 of 64 against 8 of 64.
- **Every Benin table:**
  - no row count moves;
  - base mapped through the crosswalk equals the branch except `plot_inputs` 2018-19 and 2021-22 `panel_weight`;
  - re-keyed rows are 2021-22 linked rows only, for example `food_acquired` 174,703;
  - 0 Mover rows and no `IdWalkError`;
  - `check_panel_consistency` passes 8 of 8;
  - `Country` equals `Feature`, and the make backend equals the default backend.
- **plot_inputs 2018-19:** purchases 383 → 4,719; `Quantity_purchased` 21,714 → 354,908; `Manquant` 1 → 0.
- **Hash census:** Benin only. 23 of 556 table hashes and 46 of 2,323 wave hashes move; `build_transforms_fingerprint` 0 of 42. One Benin re-warm.
- **Tests:**
  - 590 targeted tests pass, cold;
  - data-free suite: 15,532 passed, 1 failed (`test_gh932`, which passes alone and under xdist on both head and base).

### Still to do
- **Coverage snapshot:** Sue regenerates it for the new Benin `panel_ids` cells.
- **Bisect:** commit `efea3af54` is not bisect-safe; its JSON lags its generator by one commit.
- **Residuals:**
  - the 116, the 11 and the 96, which PanelHH leaves unlinked;
  - Site Q screening of `Quantity_purchased`;
  - Burkina 2018-19 unit columns, which have the same shape as F2 (not measured).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
