
**Benin: serve the 2018-19 → 2021-22 household link (#1705); 2018-19 plot_inputs reads the purchase and serves `Unknown` (#1118)**

Closes #1705. Addresses #1118, the Benin site only.

### What

**1. Benin is a panel country.** Ruling @ligon 2026-11-12, "Make Benin a panel country"; it authorizes a panel-ID change for Benin only.
- **The crosswalk.** `_/panel_ids.py` writes the committed `_/panel_ids.json` and `_/updated_ids.json` (design A). A 2021-22 household is linked to the 2018-19 household with the same `(grappe, menage)` when `PanelHH == 'Panel'` and that key is a 2018-19 household. No 2021-22 file records a 2018-19 id.
- **Linked:** 6,878 households are renamed `E2_<key>` → `<key>`.
- **Not linked:** 1,154 keep their `E2_` id: 1,027 new keys, 116 recurring keys flagged Non panel, and 11 flagged Panel with no 2018-19 key. There are 0 duplicate targets.
- **The single-pass hazards.** Every key carries the prefix and no target does. So there is no chain and no rename-onto-occupied. `panel_ids.py` asserts both, on Burkina Faso's pattern, and the tests pin them via `_walk_partition`.
- **Regeneration.** `make -C lsms_library/countries/Benin/_ panel-ids`.

**2. Benin's first `_/Makefile`, a structural change.**
- **Why it exists:** the `panel-ids` target needs one.
- **Why it is not inert by default:** once it exists, `Wave.grab_data` builds every script-path wave table through make. So it carries one `../%/_/<table>.parquet` rule per wave script, with the recipe the framework used to run directly.
- **No rule targets the JSON**, and `clean` leaves it alone.
- **What changes for users:** the "Makefile not found" warning is gone, and a failing wave script now fails the build.

**3. 2018-19 `plot_inputs`.**
- **The purchase** (ruling 2026-11-13, "Fix all three now"). `Purchased` / `Quantity_purchased` / `Unit_purchased` read s16bq08 / s16bq09a / s16bq09b. The gift question s16bq05 / s16bq07a / s16bq07b goes to the `*_received_as_gift` columns, as 2021-22 and Burkina Faso 2018-19 already do.
- **The blank unit.** The one blank unit of use is `u = 'Unknown'` instead of the minted `Manquant` (#1118). There is no raw `Manquant` label.

**4. Docs and records.**
- `sampling.yml` 2021-22 note: `rotating-panel` / `stable`, with the measured link.
- `CONTENTS.org`: two new sections and dated notes at the superseded passages.
- `CLAUDE.md` (AGENTS.md) and the panel-ids skill now say twelve panel countries, and say what adding a Makefile does.
- `tests/test_panel_ids_persistence.py`: Benin is in `DESIGN_A`.

### Verification (cold, private roots, base `f138a3d61` vs this branch)

**Link quality**, linked against another 2018-19 household of the same grappe:

| measure | linked | control |
|---|---|---|
| head sex | 94.8% | 67.6% |
| head age +1..+5 years | 60.4% | 10.9% |
| household size within 2 | 86.2% | 53.6% |
| pre-loaded members found by number | 100% | 70.0% |
| same sex among those | 98.5% | 61.6% |

Region and milieu agree on 6,878 of 6,878.

**Every Benin table:**
- Row counts are unchanged in all 37 calls.
- Base mapped through the crosswalk equals the branch on every column of every table except 2018-19 `plot_inputs`.
- Only 2021-22 linked rows change `i`: for example `sample` 6,878, `household_roster` 37,387, `food_acquired` 176,180.
- 0 `Mover` rows.
- `nitrogen_kg` / `seed_kg` are unchanged.
- Warm, fresh-process and `assume_cache_fresh` reads all equal cold, with no `IdWalkError`.
- `check_panel_consistency`: 8 of 8 pass.
- `Country` equals `Feature`.
- The make backend equals the default backend.

**2018-19 plot_inputs:**
- Purchases go from 383 to 4,719, and `Quantity_purchased` from a sum of 21,714.25 to 354,908.23.
- The gift columns now hold the 383 gifts.
- `Manquant` 1 → 0, `Unknown` 0 → 1.

**Hash census** (base → head `d5fdf326c`): Benin only. `_table_cache_hash` moves for 23 of 556 (the 22 existing plus `panel_ids`), and `Wave._input_hash` for 46 of 2,323. `build_transforms_fingerprint` 0 of 42; `framework_imports_fingerprint` 0 of 38. One Benin re-warm.

**Tests:**
- Data-free suite: 15,518 passed, 0 failed (base 15,503).
- Benin and panel files at the head: 660 data-free passed, 8 `requires_s3` passed.

### For maintainer review
- **The 116 recurring keys flagged Non panel** have pre-loaded 2018-19 rosters: 95.5% member-sex agreement against a 57.8% control. Per the ruling ("from PanelHH") they are not linked. Link them?
- **`panel_weight`** is still `poids` for the 1,154 unlinked 2021-22 households. Keep it, or make it NA?
- **Accept the new Benin `_/Makefile`** and its build-route consequence?

🤖 Generated with [Claude Code](https://claude.com/claude-code)
