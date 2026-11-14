# Stream: benin -- the #1705 panel crosswalk, and the 2018-19 plot_inputs fixes (own PR)

Read `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/COMMON-RULES.md` first.

- Worktree: `/local/job41201840/worktrees/benin`, branch `feature/benin-panel-1705-20261113`, base `f138a3d61`.
- Report dir: `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/benin/`.
- Read before editing:
  - `lsms_library/countries/Benin/_/CONTENTS.org` (all of it; the PanelHH section
    is near line 700-790);
  - CLAUDE.md's "The crosswalk is a committed SOURCE" and "Panel ID Transitive
    Chains" sections;
  - `.claude/skills/add-feature/panel-ids/SKILL.md`;
  - Burkina's `panel_ids.py` as the model.
- Issues: #1705, #1118 (Benin site).

## 1. #1705: make Benin a panel country (this AUTHORIZES a panel-ID change for Benin only)
Ruling, "Benin 2018-19 -> 2021-22 household link": "Make Benin a panel country.
Build and commit a Benin panel crosswalk (panel_ids.json / updated_ids.json,
Design A) from PanelHH in a FOLLOW-UP PR with its own verification (head-sex
agreement 94.8% on 6,878 flagged households was the offered evidence)."

- **Design A.**
  - Add `_/panel_ids.py`, writing relative to `Path(__file__).parent`.
  - Commit `panel_ids.json` and `updated_ids.json`.
  - The JSON may be a Make PREREQUISITE but never a rule's target, and
    `make clean` must not delete it.  Make regeneration go through a `panel-ids`
    target, as the other countries do.
- **Which links to serve.**
  - Define the rule from PanelHH and the recorded 2018 identifiers
    (s00 grappe/menage of 2018), and report every count:
    - Panel-flagged households with a matching 2018-19 household;
    - those without one;
    - recurring keys flagged Non panel (116);
    - duplicate targets.
  - If the 2021-22 panel households carry their own 2018 ids, say which
    variables.
- **Single-pass `id_walk` hazards** (CLAUDE.md):
  - chains, where X maps to T and the household whose raw id is T maps to U;
  - rename-onto-occupied.
  Assert in `panel_ids.py` that neither can occur, on the Burkina pattern, and
  test it.
- **Verification.**
  - Head-sex agreement, plus head age (2018 age + 3, within a tolerance),
    household size and cluster identity, on the linked households, against
    unlinked controls.
  - Add a `sampling.yml` panel block for the 2021-22 boundary
    (`household_continuity`, `cluster_id_continuity`) under the frozen
    vocabulary, with evidence.
  - Measure what changes in every Benin table:
    - `i` now walks for 2021-22;
    - row counts must not change;
    - `household_characteristics` / Mover flags.
- **Tests.**
  - Update `tests/test_panel_ids_persistence.py`: Benin joins the Design-A list.
  - Update CLAUDE.md's "All eleven panel countries" sentence and list to
    twelve, plus any test or docs that enumerate panel countries
    (`rg -n 'eleven panel|panel countries'`).

## 2. #1118 (Benin site)
Count the `u == 'Manquant'` rows first.  Then make the 2018-19 `plot_inputs.py`
fill `'Unknown'`.

## 3. Gift/purchase mix-up
Ruling 2026-11-13: "Fix all three now".  In Benin 2018-19 `plot_inputs`,
Purchased / Quantity_purchased must read s16bq08 / s16bq09a (the purchase),
not s16bq05 / s16bq07a (the gift question; check the variable labels).  Keep
the gift answers in their own columns if the schema has them.  CONTENTS
already records the defect; update it.  Measure the change base vs candidate.

## Out of scope
Shocks: the coping stream owns them.

## Verify
- A cold base-vs-candidate build of every Benin table, in private roots.
- Tests: Benin's, `tests/test_panel_ids_persistence.py`, `tests/test_id_walk.py`,
  `tests/test_sampling.py`, `tests/test_derivations.py`,
  `tests/test_schema_consistency.py`, `tests/test_build_transform_hash.py`.
- A hash census.  Expected: Benin only.

## Deliver
Return `REPORT.md` and `PR_BODY.md` in your final message.  Commit as you go.
