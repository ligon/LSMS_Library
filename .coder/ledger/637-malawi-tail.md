# Prior-Art Ledger — GH #637 tail, Malawi: seed-purchase fan-out, blank harvests, 2010-11 repeat records

**Search tier used:** ripgrep + git floor (the gitnexus MCP server did not
connect this session; the call-site sweep below is `rg`).

## §1 Task, restated

The read-only diagnosis of #637's remaining tail (issue comment of 2026-10-23,
development `9a0ed5f3c`) found three demonstrated defects in Malawi's
script-path builders (`materialize: make`), all in the wave-level assembly of
`crop_production` and `plot_inputs`:

1. **B** — `assemble_plot_inputs` merges a household-crop purchase sub-frame
   that is not reduced to its merge key `(i, crop)`, fanning out seed rows.
2. **Blank → 0.0** — `_consolidate_harvest`'s bare `'sum'` serves a blank
   `ag_g13a` / `ag_p09a` as a harvest of nothing.
3. **2010-11 repeat records** — IHS3 ships records identical in every column,
   which the builders sum.

Fix those three and the documentation contradictions at those sites; leave
defect A (the crop-code merge at the end of `assemble_crop_production`) for
@ligon's grain ruling.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `assemble_plot_inputs` | `Malawi/_/malawi.py` (seed-purchase merge) | (i, crop) single-plot attach of Module H purchases | `test_1109`, `test_1111`, `test_867` | **extend** — reduce `sp` to `(i, crop)` before the merge |
| `_seed_purchase_block` | `malawi.py` | per-row source sum (`min_count=1`), then `(i, _crop_code)` sum | indirectly | reuse unchanged |
| `_consolidate_harvest` | `malawi.py` | `(i, plot, crop, u, condition, _crop_code)` consolidation | `test_malawi_sale_basis` | **extend** — `_sum_reported` (`min_count=1`) |
| defensive collapse | `assemble_crop_production`, `_grain` | defect A; sums distinct codes under one label | `test_malawi_sale_basis` pins | reducer of `Quantity` only → `_sum_reported`; grain untouched |
| `_crop_pieces` | `malawi.py` | the `derivation_inputs` / `sale_basis_decisions` replay of the wave scripts | `test_malawi_sale_basis` | **extend** — drops repeats as the 2010-11 script now does |
| PR #1263 `_sum1` | `Tanzania/_/tanzania.py` (open PR) | Tanzania's `min_count=1` for the same blank→0 defect | its own tests | precedent, pattern copied |
| GH #840 / #651 | `Niger/_/niger.py:_community_price_triples` | `~codes.duplicated(keep='first')` on the raw wide record | `test_gh840*` | precedent: a repeat record is a duplicate entry |
| #323 site-4 rule | `CLAUDE.md`, `dfs:` merge bullet | reduce a sub-frame to the merge-key grain | — | precedent for B |
| #1174 | open issue | bare `'sum'` minting zeros on 437 nonorganic plot_inputs records | — | **not touched** (needs use-gate review) |

## §3 Definitions & conventions in force

- A repeated record is a duplicate entry — #840 / #651; exactness on the FULL
  source record (brief), so the drop must sit on the raw frame in the wave
  script, before any block projects columns.
- Grain collapse selects or sums; it never mints a value the survey did not
  record — `CLAUDE.md` "Grain Collapse"; PR #1263 for `min_count=1`.
- Malawi idiosyncrasies that bear on these sites (`Malawi/_/CONTENTS.org`):
  the IHPS panel sub-sample is drawn inside IHS3 (2010-11 config reads only
  `Full_Sample/`); 2016-17 and 2019-20 each hold two catalog entries (IHS4 +
  IHPS 2016, IHS5 + IHPS 2019) read as two halves with different id
  namespaces (`cs-17-` prefix in 2016-17 XS only); #1111 / #1193 bound the
  2010-11 and 2019-20 fertilizer amounts to `Kilogramme` before the additive
  grouping; #966 (the sale-basis ladder) attaches sales after the
  consolidation this change touches; #653 guarded three `.first()` sites
  (`plot_features`, `food_coping`, `months_food_inadequate`) not touched here.

## §4 Invariants & assumptions

- `harmonize_crop` is many-to-one (Maize 1-4, groundnut / rice varieties):
  any merge on `crop` must see a frame reduced to `crop`.
- Module G / P ask no "did you harvest at all?" question in any wave (labels of
  all 12 files: `results/labels.txt`), so there is no recorded zero to rescue
  from a blank; a recorded zero (`== 0`) stays 0.
- Exact repeats exist only in 2010-11 (G 9, P 1, I 61, Q 15, H 45, D 0); every
  module of the later waves has 0 — so only the 2010-11 scripts call the drop.
- `_crop_pieces` must replay the scripts exactly (`test_malawi_sale_basis`'s
  inputs-join tests depend on it).
- Hash scope: `malawi.py` and wave `.py` are in every Malawi table's input hash
  → one Malawi re-warm, nothing else (measured).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| repeat-record drop | new helper `drop_repeated_records` (one line) | no Malawi helper existed; the Niger one is inline in a community-price builder |
| `min_count=1` sum | new `_sum_reported(grouped, keys, agg)` in `malawi.py` | the `grouped['Quantity'].sum(min_count=1)` vectorised form (#905 Panama, #871's additive overlay), NOT #1263's per-group `_sum1` callable: round 1 used a callable and cost ~2 s per wave (cold `crop_production` 28 s -> 42 s); round 2 is exact-equal to it and back to 29 s |
| purchase reduction | extend in place | the earlier `(i, _crop_code)` groupby folded into the `(i, crop)` one |

## §6 Open questions for the human

- **Defect A** (unchanged): the declared-grain collapse merges distinct crop
  codes; `production_obs` vs `crop_variety` as a column.  Its documentation is
  deliberately left: `malawi.py` `_harvest_block` ("putting it in the grain
  would split no key", ~line 1240), `data_scheme.yml`'s `crop_production`
  comment ("so it splits no key", ~line 256), and `CONTENTS.org`'s "Why the
  join keys on the VARIETY" ("splits no key") — the diagnosis measured 286
  groups split.  To be corrected with the ruling.  *RULED and done*
  (@ligon 2026-10-23: `production_obs` = native crop code) -- see
  `.coder/ledger/637a-1120-1119-malawi-plot.md`; the three "splits no key"
  passages are corrected there.
- `ag_g09a` ("harvested so far") carries a quantity on 186 of 265 blank 2013-14
  Module G rows; serving it is new wiring.
- Same-type fertilizer slot pairs (89 / 187): double entry or two applications.
  *RULED* (@ligon 2026-10-23): keep summing, two applications; identical pairs
  recorded as possible double entry (Malawi `CONTENTS.org`, #1111 entry).
- `Quantity_purchased` still has no served unit.

---
### Phase 3 — verification

- `drop_repeated_records` — OK (§3, §4): full-record `duplicated`; called on
  every `get_dataframe` frame of the two 2010-11 scripts (AST-checked in
  `tests/test_gh637_malawi_tail.py`) and in `_crop_pieces`.
- `_sum_reported` — OK (§2, §4, §5): vectorised, both crop_production reducers; served 0→NA
  32 / 283 / 200 / 276, recorded zeros kept; independent raw oracle equal row
  for row.
- `assemble_plot_inputs` purchase reduction — OK (§2, §3): seed Quantity
  −8 / −9 / −145 / −123 (plus −548 from the 2010-11 G repeats);
  `Quantity_purchased` now the `(i, crop)` total; oracle equal row for row.
- No REINVENTION found; no CONTRADICTION.  Defect A's collapse grain and every
  other reducer unchanged.
