# Prior-Art Ledger — Uganda country assets concatenation

**Search tier used:** ripgrep + git floor. Inherits `STANDING.md`, `CLAUDE.md`,
and `lsms_library/data_info.yml`; diagnosis at integration `17308b3d4` used only
physical private copies of completed corpus wave parquets.

## §1 Task, restated

Make `Uganda/_/assets.py`, invoked by the country's Makefile, preserve the same
real wave observations and canonical raw `Value` / `(t, i, j)` frame that the
existing `Country('Uganda').assets()` wave-concatenation route builds. The
corrected release CI (`38697245468`, source `9d5bf200f`) found 673,841 duplicate
keys among 790,688 rows in the country script's output, versus 116,808 actual
wave rows. Keep both build routes and leave the broader GH #788 authority and
divergence-detection decision open.

## §2 Existing machinery

| Symbol | Source | Reuse |
|---|---|---|
| Country asset compiler | `countries/Uganda/_/assets.py` | Replace only stack/dict/stack assembly and accidental index/column reshape. |
| Earnings concat repair | `countries/Uganda/_/earnings.py`, GH #772 | Reuse direct wave concatenation; avoid singleton squeeze and pandas 3 dense alignment. |
| Household crosswalk | `countries/Uganda/_/uganda.py::id_walk` and `updated_ids.json` | Keep exactly once and preserve `id_converted`; private measurement proves mapped concat equals the canonical raw cache, while unmapped concat does not. |
| Canonical build | `country.py::_aggregate_wave_data` / `load_from_waves` | Reference only; no framework or dispatch changes. |
| IO | `local_tools.get_dataframe` / `to_parquet` | Preserve sanctioned reader/writer and external-root redirect. |
| Existing checks | `tests/test_table_structure.py`, `test_uganda_invariance.py`, `test_assets_additive.py` | Run focused existing checks; do not weaken assertions or rewrite baselines. |
| Script testing precedent | `tests/test_gh772_uganda_earnings_shape.py` | Execute actual shipped script with only IO boundaries stubbed; keep imported module state isolated. |

No prior task ledger for the assets compiler or GH #788 was found. Definitions
and reuse rules remain anchored on `STANDING.md` §§2–5.

## §3 Definitions & conventions in force

- `add-feature/assets/SKILL.md`: item-level `(t, i, j)`, `Value` is the asset
  stock measure; preserve source observations, not a household total.
- Uganda `_/CONTENTS.org`, “There are TWO builders for a country table”: it
  explicitly says “assets still disagrees with its own script” and contrasts
  wave `Value, (t, i, j)` with script `assets, (i, t, m)`. This diagnosis
  confirms that recorded contradiction. Its September measurement of 116,808
  assets rows describes the API route, not the faulty country script.
- Uganda CONTENTS durables TODO: existing assets use gsec14 present estimated
  value (h14q05), not the unwired 365-day gsec15d durable-purchase flow.
- `STANDING.md` §4: cached parquets are pre-finalization and `id_converted`
  survives; market/cluster metadata belongs to the runtime interface.

## §4 Invariants & assumptions

Preserve every supplied row, including existing all-null wave observations;
create no absent household/item/wave combinations. Retain item `j`, `Value`
name and dtype, ordering, and existing household ID crosswalk. No wave input,
ownership/zero filter, item-label mapping, reducer, data scheme, fixture,
baseline, or scientific policy changes. The canonical served API has 4,237
post-label duplicate keys; this narrow compiler repair must leave that output
unchanged rather than invent a new aggregation policy.

CONTENTS LOGBOOK/WAITING/TODO were read: panel-ID/weight caveats, stock versus
flow, null-key collapse, #829 producer caveat, crop-unit and nonfood recall
questions remain separate. GH #809 makes hashless country outputs stale; the
older trust-once passage is explicitly historical. Neither this repair nor
cache restamping decides GH #788's authority policy. No shared L2 reads through
the API, global clear, DVC CLI, or source-tree parquet writes are permitted.

## §5 Reuse decision

Use `pd.concat(waves, names=['t'])`, then the existing `id_walk` and writer.
Synthetic tests execute the real script on disjoint waves, singleton rows,
multiple items, all-null observations, and a wave-specific household crosswalk.
Private real-data verification must execute the script/Makefile route before
raw structural checks, compare exactly with existing canonical output, and
check the served API remains unchanged. A warm API call alone would rebuild
the hashless country parquet and conceal the defective route.

## §6 Open questions for the human

No additional design decision blocks this authorized convergence repair.
GH #788 remains open for general authoritative-builder and divergence detection
policy. Do not close it through this change.

### Phase 3 — verification

Pending focused negative control, fixed regressions, actual Makefile/private
script replay, raw canonical comparison, invariance and cache-scope measurement.
