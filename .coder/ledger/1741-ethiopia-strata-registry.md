# Prior-Art Ledger — #1741 Ethiopia constructed strata

**Source:** `47df62d49e614c9a56e319def7324145d138c3d9`. **Search tier used:** ripgrep + git floor
(GitNexus did not connect). Inherits [STANDING.md](STANDING.md).

## §1 Task, restated

Register Ethiopia's constructed `sample.strata` (five waves) in `_/derivations.yml`, stamp
`Derivation`, tag the waves `served_strata.holds: constructed` in `_/sampling.yml`. Keep every
served `strata` value. No stratum is inferred. Ethiopia only, not EthiopiaRHS or GhanaAHIES.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | decision |
|---|---|---|---|---|
| five constructions | `Ethiopia/{wave}/_/sample.py` (script path, no `sample` block in `data_info.yml`) | `saq01` label + `Rural` | no test of the strata expression | reuse, moved to one function |
| `with_agriculture_cover_households` | `Ethiopia/_/ethiopia.py` | a second copy of the expression, for PP-only households (GH #1120 D1) | `tests/test_1120_ethiopia_locating_covers.py` | extend (calls the function) |
| `_agriculture_only_households`, `_decode_codes`, `CLUSTER_AGRICULTURE_SOURCES` | `Ethiopia/_/ethiopia.py` | PP-only rows, label by code, per-wave cover names | same | reuse in `inputs` |
| `served_strata` loader and two-way test | `lsms_library/sampling.py`, `tests/test_sampling.py` | vocabulary, registry <-> tag | yes | reuse, unchanged |
| `ehcvm.derive_region_milieu_stratum`, `stamp_region_milieu_stratum` | `lsms_library/ehcvm.py:824`, `:855` | EHCVM region x milieu | yes | **not reused**, see §5 |
| `sample.Derivation` | `lsms_library/data_info.yml`, `Columns: sample` | optional `str` | yes | reuse; no scheme edit |
| 2026-09-11 census row E35 | `slurm_logs/2026-09-11_derived_values/ethiopia_nigeria.org:80` | already listed this construction | n/a | cite |

## §3 Definitions & conventions in force

- Derived values: `lsms_library/derivations.py` docstring; CLAUDE.md "Derived Values".
- `served_strata`: `sampling.py`, `STRATA_HOLDS`; rulings on #1577 (2026-11-12 "Registry +
  sampling.yml tag", 2026-11-13 "Keep constructed").
- Country idiosyncrasy (`Ethiopia/_/CONTENTS.org`, read in full): no explicit strata variable;
  `Rural` changes meaning between waves (GH #683); W2/W3 key on `household_id2`; PP-only households
  (D1); Panel II is a new draw; W5 has no Tigray.

## §4 Invariants & assumptions

- `strata` values must not move: the function is the old expression with the same operands, the
  `fillna('')` region-alone fallback included.
- Stamp after the D1 append (`extra[list(sample.columns)]` raises on an extra column) and before
  `set_index`.
- One key per wave in `sampling.yml`: a second one silently drops `panel` (red team F3).
- The cross is not documented in the repository, so its basis is `modelling-choice`.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| `derive_region_rural_stratum` | new name for existing code | the EHCVM function returns NA where a part is missing and takes the cover's milieu label; Ethiopia's serves the region alone and takes the mapped `Rural`. Sharing it would change the rule, and importing `ehcvm` would fold its closure into every Ethiopia hash |
| `stamp_region_rural_stratum` | new, four lines | the EHCVM twin hard-codes the EHCVM key name |
| `inputs_region_rural_stratum` | new | must include the PP-only rows to join the served rows |

## §6 Open questions for the human

- May the sampling sections of the five public ESS BIDs be read? They would settle the basis of
  the region x Rural cross (blocks: nothing; it upgrades or contradicts one assumption).
- Is `ethiopia.py` inside "Ethiopia strata registry and sample/strata config"? A registry entry
  cannot load without its callables, and all of Ethiopia's live there. Taken as yes, disclosed.

---
### Phase 3 — verification

No prepared runtime (python3.6 + PyYAML, bare python3.12). Nothing below imported `lsms_library`.

- `derive_region_rural_stratum` — OK (§4): AST-identical to the base expression at all six sites.
- `with_agriculture_cover_households` — OK (§4): the `strata` assignment is its only AST change.
- `stamp_region_rural_stratum` — OK by reading (§4): order build < append < stamp < set_index in 5/5.
- `inputs_region_rural_stratum` — unexecuted; by reading, the D1 helpers' own calls (§5).
- registry, tags — OK (§3): PyYAML checks, panel blocks and the five older entries equal to base.
- UNRUN: pytest (all), a cold build, the data-gated tag test, hash measurement.
