
## GH #1562: coping strategies, reported positions and the unordered set

Implements the rulings "Coping-strategy implementation, GH1562" and "Coping follow-ups" (2026-11-13), and the 2026-11-12 contract.

**Contract** (`data_info.yml`, `shocks`):
- `HowCoped0..` are the reported order, NA where the instrument records none.
- `HowCopedSet` is distinct labels, sorted, `+`-joined; test membership by split.
- New optional columns: `HowCoped3`, `Derivation`, `ForestProduct0..2`.

**Read time (0 hashes)**, `lsms_library/coping.py` in `_finalize_result`:
- fillers and missing codes are NA in positions and never enter the set;
- "did nothing" is a set member only when alone;
- a slot-aware class serves Uganda's lone first-slot "Nothing else" as "Did nothing";
- the set is derived from positions wherever the build wrote none.
- Labels with evidence are in `_/coping.yml`.

**Build time:**
- **EHCVM 2018-19:** the set comes from the `s14q05__N` grid (positive = chosen); positions present and NA. CIV's hook moved to its wave module.
- **GNB:** codes 18-25 follow the Portuguese form; **2,199 rows relabelled, codes 23-24 inferred**.
- **EHCVM 2021-22:** option 25's inferred slot is registered (Benin 13, Niger 17).
- **Nigeria:**
  - shocks served once, under the post-harvest quarter (−11,275 duplicates);
  - assets served once, under the post-planting quarter (−432,659 duplicates, values unchanged; ruled "Pin + W1 NA columns");
  - W1 assets Age and Value are NA, via `Nigeria/2010-11/_/2010-11.py`; one-quarter requests answer the same cold and warm;
  - W1-W3 gain `HowCoped3`; W4 is set-only.
- **Liberia:** forest products moved to `ForestProduct0..2`.
- **Uganda:** 2013-14 / 2015-16 code 40 first and alone (55 / 41 rows) is served as "Did nothing" in `HowCoped0` and the set; elsewhere it is NA.

**Must-state:**
- **Strict mode (F7).** Under `LSMS_READ_STRICT=1`, `shocks` now raises `NullReadError` for Benin, CIV, Mali, Senegal, Togo and Nigeria, where base returned rows: the 2018-19 and W4 positions are honestly empty. Burkina and Niger already raised on base; Uganda raises on both. 19 new per-wave warnings in all.
- **Coverage (F8).** 8 new `dropped` Nigeria cells: shocks in 2010Q3/2012Q3/2015Q3/2018Q3, assets in 2011Q1/2013Q1/2016Q1/2019Q1. Draft `correctly-empty` verdicts are in REPORT.md. EHCVM 2018-19 cells stay `sane`.
- **F11.**
  - The wave-level API (`Country(C)[w].shocks()`) serves the build's set before the read-time rules. For example, Mali 2018-19 keeps "Aucune stratégie" beside a strategy on 444 rows.
  - In Burkina 2014, Mali 2014-15 and 2017-18, and Niger 2011-12 and 2014-15, positions still carry "Aucune"/"Nothing" used as a filler, as ruled. Their `HowCoped1`/`HowCoped2` fill rates are not strategy counts; use `HowCopedSet`.
- **W1 assets.** The NA columns mean "not wired": `sect5b_plantingw1` holds W1 Age and Value (`s5q3`, `s5q4`), an open question.
- **Cost.** A cold re-warm of 10 countries (Benin, Burkina, CIV, GNB, Liberia, Mali, Niger, Nigeria, Senegal, Togo); CIV's 126 wave-tables are a one-time cost.

**Measured** (cold, base `f138a3d61` against candidate, private roots):
- every non-coping column is unchanged in all 17 shocks countries;
- warm reads equal cold;
- `Feature('shocks')` index is unique, with the set for all 15 coping countries;
- derivation inputs join one to one.

**Hash census to head `2ee44de32`:** btf 0/42, fwimp 0/38, tables 218/555, waves 538/2,321, confined to those 10 countries.

**Tests at `2ee44de32`:**
- static suite: 15,598 passed, plus 1 parallel-run flake (`test_gh932`, which passes 3/3 serially);
- data-backed shocks/coping tests: 1,001 + 94 passed.

**For integration (Sue):**
- **F5:** GNB 2021-22 needs the option-25 stamp on 3 rows (helpers in `ehcvm.py`); GNB's `optional:` positions then become a one-wave silencing.
- **F6:** re-measure the Windows Uganda pin.
- `test_gh1500`'s Senegal AST pin is re-pinned and will conflict with any other edit to that `mapping.py`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
