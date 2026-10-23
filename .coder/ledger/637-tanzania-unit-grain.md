# Prior-Art Ledger -- GH #637 Tanzania tail: `plot_inputs` / `crop_production` reducers

**Search tier used:** ripgrep + git floor (the GitNexus MCP server did not
connect this session; call sites swept with `rg`, see §2).

## §1 Task, restated

GH #637's remaining tail names two Tanzania country-helper reducers in
`Tanzania/_/tanzania.py`: `plot_inputs_for_wave` (keeps the unit alongside
quantity sums) and `crop_production_for_wave` (keeps unit and sale fields
alongside quantity sums).  Both are script-path tables (`materialize: make`),
built for 2019-20 (NPS-SDD) and 2020-21 (NPS Y5) only.  Diagnose every group
each collapse aggregates, per wave, against the held sources; repair only a
demonstrated key/grain defect, at the identifier (GH #323 D1); record the
disposition of everything else.

## §2 Existing machinery

| symbol | path | what it does | tested? | decision |
|--------|------|--------------|---------|----------|
| `plot_inputs_for_wave` | `Tanzania/_/tanzania.py` | stacks seed (4A) + five 3A input channels, collapses duplicate keys | `tests/test_1118_tanzania_plot_input_units.py` | **extend**: `u` joins the key |
| `crop_production_for_wave` / `_block` | same | stacks 4A / 6A / 6B harvest, attaches single-plot sales, collapses duplicate `(t,i,plot_id,j)` | `tests/test_quantity_screen.py` (warm parquet) | **extend**: perennial `harvested` wiring + `min_count=1` |
| wave colmaps | `Tanzania/{2019-20,2020-21}/_/{crop_production,plot_inputs}.py` | per-wave column names | none before | crop colmaps gain `harvested='ag6a_06a'/'ag6b_06a'` |
| `U_UNKNOWN` | `lsms_library/transformations.py` | canonical missing-unit sentinel | `tests/test_962_unit_sentinel.py` | reuse |
| canonical `plot_inputs` index | `lsms_library/data_info.yml` Index Info | `(t, v, i, plot_id, input, j, u, season)` | `tests/test_feature_canonical_index.py` | reuse (precedent) |
| `malawi.assemble_plot_inputs` | `Malawi/_/malawi.py` | keys `(t,i,plot_id,input,crop,u)`; "Rows in different units `u` stay distinct" | Malawi tests | precedent |
| EHCVM `plot_inputs` (7 countries) | `*/_/data_scheme.yml` | `(t, i, input, crop, u)` | #1123 / #1189 tests | precedent |
| Uganda `plot_inputs_for_wave` | `Uganda/_/uganda.py` | `season` added (#646); residual groups have no unit conflict, so no `u` level needed | `tests/test_gh637_uganda_plot_inputs_season.py` | consistent |
| `#840` `variety`, `#1198` `production_obs` | `data_info.yml`, Tanzania/Uganda | finer-than-`j` levels for lumped native records | yes | cited for the open crop_production sale question; NOT applied |

Blast radius (rg): `PLOT_INPUTS_COLUMNS` / `plot_inputs_for_wave` /
`crop_production_for_wave` are referenced only by the two wave scripts each, the
#1118 test, and evidence scripts.  `transformations.seed_kg` / `nitrogen_kg`
accept `u` as a level or a column.

## §3 Definitions & conventions in force

- `u` missing-unit sentinel `Unknown`; consumer rule "`Unknown` is NOT a unit":
  `lsms_library/data_info.yml` (Single Index `u` note), GH #1041.
- A NaN on a declared index level is a deferred silent deletion: CLAUDE.md
  "Grain Collapse" §3b and Site I.
- D1 -- fix the identifier, never a reducer: CLAUDE.md / GH #323.
- GH #637's three traps: "exact is not reassurance", "invariance by
  missingness", "zero duplicates is not soundness".
- Tanzania survey design (UPHI back-casting, Extended/Refresh, 2008-15 has no
  agriculture source): `Tanzania/_/CONTENTS.org`.

## §4 Invariants & assumptions

- 2008-15 is not built for either table (no agriculture source); the
  multi-round folder is untouched.
- Source grain: `AG_SEC_4A/6A/6B` unique on `(hhid, plot, cropid)`;
  `AG_SEC_3A` unique on `(hhid, plot)`; each sale module unique on
  `(hhid, cropid)` (4 rows in 2020-21 share one across 5A/7A/7B, which the
  helper sums); no null ids (2019-20 `sdd_hhid`/`plotnum`, 2020-21
  `y5_hhid`/`plot_id`).  Every duplicate group is two or more DISTINCT source
  records -- lumped crop codes, or two fertilizer slots -- never replication.
- `u` must be `Unknown` before it becomes a key (normalization precedes
  grouping; this reverses #1118's order, which existed only to stop `'first'`
  letting an early NA displace a recorded unit).
- Every NA quantity in 6A/6B carries `ag6?_06a == 2` (not harvested in the
  past 12 months); every non-NA carries `== 1`.  The old 0.0 for those rows was
  correct by accident.
- Row-level data stays in `/local/job40390255/data-private/637-tz/`; committed
  evidence is aggregates only.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| `plot_inputs` grain | extend: add `u` to the declared index | canonical index + Malawi/EHCVM precedent; 11 incommensurable groups (6 two-recorded-unit, 5 OTHER+recorded) |
| same-unit `plot_inputs` duplicates | reuse the existing sum(`min_count=1`) / OR | commensurable |
| `crop_production` grain | unchanged | `u` constant `kg`; no unit conflict |
| `crop_production` Quantity reducer | extend: `min_count=1` + perennial `harvested` wiring | stops 19 fabricated zeros; makes 2,376 accidental zeros explicit |
| `crop_production` sale / month / intercrop reducers | unchanged, flagged | needs a finer-than-`j` level (#840 / #1198) -- a grain ruling |

## §6 Open questions for the human

1. `crop_production` lumped crop codes: add a source-code level (as #840
   `variety`) or use `production_obs` (#1198) so two lumped codes' sales are
   not reduced by `'first'` (14 / 52 groups; 1,813 kg + 2,136,000 TSh and
   10,062 kg + 10,762,200 TSh dropped)?
2. Site Q reference: exclude non-positive quantities?  Reported "did not
   harvest" zeros now in kg cells move Tanzania's screen 1 -> 4 / 13 -> 26.
3. `plot_inputs.Quantity_purchased` has no served unit; its unit differs from
   `u` on 1 / 11 seed records.  Add `Unit_purchased` (Burkina precedent)?
4. Same fertilizer named as type 1 and type 2 on one plot (1 / 6 pairs; 5 of 7
   with identical quantities): sum (today) or treat as double entry?
5. `crop_production` same crop code in 4A and a perennial module on one plot
   (2 groups, one 860 + 860 kg): different recall windows summed.

---
### Phase 3 -- verification

- `plot_inputs_for_wave` -- OK (§2, §5): helper == independent source oracle
  row-for-row both waves; quantity conserved per `(key, u)`; +1 / +10 rows,
  every other row identical including `u`; `seed_kg` -50 / -579 kg on exactly
  the 10 split plots; `nitrogen_kg` frame-identical.
- `crop_production_for_wave` / wave colmaps -- OK (§4, §5): rows / index
  unchanged; Quantity 0.0 -> NA on 14 / 5 rows, `u` NA -> `kg` on 513 / 1,863
  perennial reported zeros, nothing else; `harvest_kg` / `yield_kg`
  frame-identical.
- Tests: `tests/test_gh637_tanzania_unit_grain.py` 21 tests (15 fail on
  development; the 6 that pass there are preserve-controls); #1118 tests
  adapted -- 2 pins reversed (and renamed) with comments, 1 renamed with the
  same assertions, 3 read `u` from the index; `test_quantity_screen.py` pin
  13 -> 26 with comment.
- Evidence: `slurm_logs/isa_backlog_2026-10-21/637-tanzania-unit-grain/`.
