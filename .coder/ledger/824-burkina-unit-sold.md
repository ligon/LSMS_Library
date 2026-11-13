# Prior-Art Ledger — GH #824 (Burkina Faso 2018-19): carry the SOLD unit on `crop_production`

**Search tier used:** ripgrep + git (floor).  The `gitnexus` MCP server failed to
connect this session (`CONNECTION_CLOSED`); per CLAUDE.md "Code intelligence:
GitNexus is OPTIONAL" the declared substitute (`rg` over call sites) was used.
Blast radius found: `Unit_sold` is read by no library code (`rg Unit_sold
lsms_library/*.py` is empty); the edited `_finish_crop_production` /
`_unit_labels` are module-local to the Burkina 2018-19 wave script, and the only
other reader of that file is `tests/test_burkina_faso_seed_crop_labels.py`,
which exec's `_crop_labels` (untouched).
**Inherits:** `.coder/ledger/STANDING.md` §2/§3/§4, cited not restated.
**Base commit:** `9a0ed5f3c` (development), branch `fix/824-burkina-unit-sold-20261023`.

## §1 Task, restated

Burkina Faso `crop_production` is a script-path table (`materialize: make`)
built for 2018-19 only by `Burkina_Faso/2018-19/_/crop_production.py` from
EHCVM `s16c_me_bfa2018.dta`, at grain `(t, i, plot_id, crop, u)` where `u` is
the HARVEST unit `s16cq12b`.  The same line records the sold quantity
`s16cq16a` in its own unit `s16cq16b` (same seven labels).  The builder, a
pre-#824 clone of Niger 2018-19, never read `s16cq16b`.  Task: carry it as the
per-row column `Unit_sold`, canonicalised at build time through the same `u`
table, declared `optional: true` in `Burkina_Faso/_/data_scheme.yml`, changing
no row and no existing column.

## §2 Existing machinery

| symbol | path:line | what it does | tested? | reuse / extend / new |
|---|---|---|---|---|
| Niger 2018-19 `Unit_sold` read | `Niger/2018-19/_/crop_production.py:50` | `_unit_labels(src['s16cq16b'], unit_map)` | `tests/test_niger_sold_unit.py` | **mirror** (Burkina line 117) |
| `niger._finish_crop_production` | `Niger/_/niger.py:535` (Unit_sold block `:553-563`, keep `:578`) | string dtype, `Unknown` -> NA fold, column after `Value_sold` | same | **mirror** in Burkina's self-contained copy (`crop_production.py:56`, block `:70-85`) |
| Niger `Unit_sold` declaration | `Niger/_/data_scheme.yml:200` | `{type: str, optional: true}` | same | **mirror** (`Burkina_Faso/_/data_scheme.yml:221`) |
| canonical `Unit_sold` | `lsms_library/data_info.yml:931` | contract + consumer rule (disagreement is a FLAG) | Uganda/Niger/Nigeria tests | **reuse as is** — already declared, not edited |
| Burkina `_unit_labels` / `_crop_maps` | `Burkina_Faso/2018-19/_/crop_production.py:32,49` | build-time map through the country `u` table | via the build | **reuse** — same map for both units |
| `median_price_valuation` | `lsms_library/transformations.py:7274` | the only consumer dividing value by quantity sold | yes | **not touched** (decision for @ligon) |

## §3 Definitions & conventions in force

- `Unit_sold` contract: `lsms_library/data_info.yml:931` (column not index; NA-bearing, never sentinel-filled; disagreement with `u` is a flag).
- Build-time canonicalisation: read-time categorical mapping applies only to a column whose name matches a table (`u`), so a second unit column must be mapped at build time (GH #1188; CLAUDE.md "Automatic categorical mappings").
- "Priced" = `Quantity_sold > 0` and `Value_sold > 0` (Niger CONTENTS, GH #824 audit).
- Household id: `burkina_faso.ehcvm_i` (grappe + '0' + 2-digit menage), `Burkina_Faso/_/CONTENTS.org` GH #1123 entry.

## §4 Invariants & assumptions

- The sale sits on the harvest row in 2018-19, so no join, no re-key, no row change.  Measured: the raw key `(i, plot_id, crop, u)` is unique (0 multi-row groups), and the six all-payload-NA lines dropped by the public finaliser carry no sold unit, so the new column resurrects none.
- Both units take the same seven raw labels and go through ONE table; the global `u.org` only re-keys kilogram spellings, so the read-time pass on `u` cannot move `u` away from `Unit_sold`.
- Scope: 2014 (EMC, different instrument) and 2021-22 (crop-level `s16d`) have no builder; not wired.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| `Unit_sold` | reuse Niger 2018-19 shape exactly | identical EHCVM s16c layout; same grain |
| `Unknown` fold | kept as a no-op guard | Burkina's `u` table maps nothing to `Unknown`; keeps the contract from drifting |
| sale keying (one-unit gate) | not applicable | Niger keys only its crop-level waves (2014-15, 2021-22); 2018-19 has no keying |

## §6 Open questions for the human

- `median_price_valuation`'s `Unit_sold` rule (audit: 30 mis-scaled pooled rows, 31 kg sales excluded, +0.18% valued harvest on this wave).
- Shelled/unshelled state (`s16cq12c` / `s16cq16c`, differ on 313 priced rows) as a `condition` level.
- Wiring 2014 EMC and 2021-22 `crop_production`.

---
### Phase 3 — verification

- `crop_production.py` `Unit_sold` read — OK (anchored on §2 row 1): same call shape as Niger `:50`.
- `_finish_crop_production` Unit_sold block — OK (§2 row 2, §5 row 2): string dtype, NA kept, column order after `Value_sold`; pinned by `TestFinishTail`.
- `data_scheme.yml` `Unit_sold` — OK (§2 row 3, §3 row 1).
- No REINVENTION: no new helper, no new mapping table, no library code.
- No CONTRADICTION: rows 15,587 = 15,587; every pre-existing column exact-equal; raw oracle 15,593/15,593; hash census Burkina-only (24 country, 24 2018-19 wave, 0 btf).
