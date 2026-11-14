**Delta verdict: APPROVE** for `df2e3b15d716aa6ae6bb4c79c5daca16dceba1e5` and `dc3db3dc795d99fa82fbbcfb697323e000b4be1f`, against previous candidate `3d34550d5` and base `f138a3d61`. Every claim checked out on cold builds of both heads, diffed row by row against the previous head. There is one small residual and three notes; none blocks.

**1. D1, the liquid-cell list `W2_GRAMS_FLAG_LIQUID_CELLS`.**
- **The 13 + 6 cells are exactly the liquids my container-volume screen found.** In 2012Q3 these are tinned milk, fresh milk, bottled water, palm wine, pito, honey and butter/margarine; 2013Q1 has six of the same.
- **Exclusions are correct.**
  - Beverage powders stay on the grams reading.
  - Pepper (76) and Dry Pepper (77) are solids: as grams they come to 0.16 kg per litre of container, plausible for dried chilli; as centilitres, 1.6 kg/l, which is impossible.
- **Movement is exactly as claimed.** 106 `food_acquired` rows (64 in 2012Q3, 42 in 2013Q1) change `Quantity_kg` and nothing else. `food_quantities` moves on the same 106 rows and `food_prices` on 101. The served kg factor moves on those 106 rows only.
- **Note:** sachet water / piece (2013Q1, 1 row) is still served at 0.05 kg per sachet, though a water sachet is 50 cl. My screen cannot see it, because the oil rows give no volume for a "piece".
- **Note:** sachet water / medium derica (3 rows) fits neither reading.
- **Note:** butter and honey are not liquids, but reading them at 1 kg per litre lands within about 10% and 30% respectively.

**2. D2, `unit_override`.**
- **Only the 71 W3 harvest rows change.** `crop_production` changes only `u`, from `l` to `Jerry Can`, all in 2016Q1. `harvest_kg` is frame-equal, and every other table is frame-equal.
- **The audits clear.** Only the harvest audit's diagnostic columns move on those rows.
- **Residual (should-fix, can be filed):**
  - 20 W3 sale rows still decode `Unit_sold = 'l'`; the worker reports 15 of them on 20 or 25 l cans. 16 of the 20 are palm oil, at a median 7 units and 4,250 Naira per unit, which is a can price: harvest lines measured in true litres sell at about 233 per litre.
  - Anything converting `Unit_sold` to kilograms will read these as one litre each, putting W3 palm-oil sale prices about 20–25x high per kg.
  - The fix is to apply the same override to sale codes 190–192 in `hh_crop_sale_records`. If it is filed instead, the PR body must state it.

**3. D3 and D6.**
- **D3.** The W5 verdict now names `s1q28` / `s1q33` (parents' education), `s1q39b` / `s1q39c` (leavers' education) and `s1q4 = NO`. It is correct.
- **D6.** `_carries_key` splits on `+`. The derivation inputs frames are unchanged, apart from the 106 re-read rows' `kg_per_unit`.

**4. D4 (`dc3db3dc7`), the W2 unlabelled education codes.**
- **Exactly 3 rows change.** They are 2012Q3 `individual_education`, `"4.0"`, `"19.0"` and `"20.0"`, now `Unknown`. No numeric-like value remains in any round, and the other 36 saved frames are frame-equal.
- **#1292's test pins both sides.** A labelled-but-unmapped answer passes through, bare codes become `Unknown` in W2 post-planting, and post-harvest passes everything through. It passed in my run.
- **Note:** every callable in the new `2012-13/_/mapping.py` applies to both W2 rounds. Today it holds only the hook, which checks `t` itself; anything added there later will act on both rounds.

**5. Hash census: Nigeria only, 0 `btf` at every step.**

| step | `btf` | `fwimp` | tables | waves |
|---|---|---|---|---|
| base → `dc3db3dc7` | 0/42 | 1/38 (Nigeria) | 24/555 (Nigeria) | 240/2,321 (Nigeria) |
| `3d34550d5` → `df2e3b15d` | 0 | 0 | Nigeria only | Nigeria only |
| `df2e3b15d` → `dc3db3dc7` | 0 | 0 | 24 (Nigeria) | 48 (Nigeria; both W2 rounds, from the new `mapping.py`) |

**Tests.** At `dc3db3dc7`: 2,267 passed, 15 skipped, 0 failed, in a warm private root. I did not run the suite separately at `df2e3b15d`.

**What I ran.**
- Jobs 41280168 (cold build of `df2e3b15d`, plus census), 41280975 (diff against the previous head), 41281053 (cold build of `dc3db3dc7`), 41281054 (diff and census), 41281055 (tests) and 41281526 (the three education values).
- Outputs are in `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/nigeria-redteam/out/`:
  - `diff3.json`
  - `diff4.json`
  - `hashdiff3.log`
  - `hashdiff4.log`
  - `pytest-cand4.txt`
  - `edu4.txt`

**For Sue.** Two more kept data roots to clean on the user's go: `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/private/nigeria-redteam/job-41280168` and `job-41281053`, plus `frames-cand3` and `frames-cand4` in the same directory.
