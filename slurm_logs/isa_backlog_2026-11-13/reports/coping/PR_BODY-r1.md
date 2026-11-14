
## GH #1562: coping strategies, reported positions and the unordered set

Implements the 2026-11-13 ruling "Coping-strategy implementation, GH1562" and the 2026-11-12 contract.

**Contract** (`data_info.yml`, `shocks`):
- `HowCoped0..` are the household's reported order, NA where the instrument records no order.
- `HowCopedSet` is the distinct labels, sorted and joined with `+`.
- New optional columns: `HowCoped3`, `Derivation`, `ForestProduct0..2`.

**Read time (0 hashes)**, in `lsms_library/coping.py`, called from `_finalize_result`:
- fillers and missing codes are served NA in their positions and never enter the set;
- "did nothing" is a set member only when it is the only answer;
- the set is derived from the positions wherever the build wrote none.
- Labels are per country in `_/coping.yml`, each with its evidence.

**Build time:**
- **EHCVM 2018-19 (8 countries)**: the set comes from the `s14q05__N` grid, where a positive cell means chosen; positions are present and NA. CIV's hook moved to its wave `mapping.py`. GNB codes 18-25 follow the Portuguese questionnaire (23-24 are inferred).
- **EHCVM 2021-22**: option 25's inferred position is registered (Benin 13 rows, Niger 17).
- **Nigeria**: shocks are served once, under the post-harvest quarter (−11,275 duplicate rows); assets are served once, under the post-planting quarter (−432,659 duplicate rows); W1-W3 gain `HowCoped3`; W4 is set-only.
- **Liberia**: forest products moved to `ForestProduct0..2`.
- **Guatemala**: CONTENTS line fixed.

**Measured** (cold, base `f138a3d61` against candidate, private roots):
- Every non-coping column is unchanged in all 17 shocks countries.
- Warm reads equal cold reads.
- `Feature('shocks')` has a unique index, with the set present for all 15 coping countries.
- Derivation inputs join the keyed rows one to one.

**Hash census to head `3336ffc99`:** btf 0/42, fwimp 0/38, tables 218/555, waves 538/2321, only in Benin, Burkina, CIV, GNB, Liberia, Mali, Niger, Nigeria, Senegal and Togo.

**Tests:** full static suite 15,587 passed, 0 failed (base 15,503). Data-backed shocks tests: 984 + 94 passed.

**Partial: Nigeria assets.** A cold `assets(waves=['2011Q1'])` now raises the GH #1292 empty-answer check, because W1 wires no Age or Value. It needs a decision.

**For integration:**
- `test_gh1500`'s Senegal 2018-19 AST pin was re-pinned.
- GNB 2021-22 needs the option-25 stamp (the helpers are in `ehcvm.py`).
- 8 Nigeria cells become `dropped`; draft verdicts are in the report.

The questions and residuals are in REPORT.md.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
