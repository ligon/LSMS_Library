# Prior-Art Ledger — GH #830 (coverage grader: cross-wave scale check on monetary columns)

> Per-task ledger. Inherits `STANDING.md`; cites rather than copies.

**Search tier used:** ripgrep+git floor (gitnexus MCP did not connect; `rg` sweep of
`coverage_matrix.py`, `diagnostics.py`, `currency.py`, `bench/feature_audit/scan.py`).

## §1 Task, restated
`coverage_matrix.grade_feature` grades each wave by slicing `t` and running
`diagnostics.is_this_feature_sane` on the slice; every check there is a shape check
(present / non-null / dtype / unique index). A monetary column 100x off in ONE wave
(Uganda `crop_production` 2015-16 `Value_sold`, GH #829, now guarded in the wave
script) graded `sane`. Add a warn-only cross-wave median-ratio note on monetary
columns, computed in `grade_feature` where the whole country frame is present. No tier
changes. Read-path only.

## §2 Existing machinery
| symbol | path | what it does | tested? | decision |
|---|---|---|---|---|
| `currency._monetary_columns(table, country)` | `lsms_library/currency.py:172` | seed `_DEFAULT_MONETARY` ∪ `monetary: true` flags (data_info.yml + country data_scheme.yml), minus `monetary: false` | yes (`tests/test_currency*.py`) | **reuse** — the triage record's "8 `monetary: true` columns" misses `crop_production.Value_sold`, which is monetary only via the seed |
| `currency.currency_for(country, wave)` | `currency.py:143` | (country, wave) → ISO code via `Currency: overrides` | yes | **reuse** as the redenomination boundary test |
| `currency._redenomination_waves` | `currency.py:226` | override-wave set; docstring says "no caller" | — | not used (would make its docstring stale in a file the task does not name) |
| `coverage_matrix._cell/_report_summary` | `coverage_matrix.py` | cell record / detail string | yes | **extend** — append `warn: scale_ratio[...]` to `detail` |
| `quantity_audit` Site Q (`cell ≥ 30 rows`) | CLAUDE.md §Site Q | support floor convention | yes | **reuse** the floor of 30 |
| `scan.py::_null_read_findings` | `bench/feature_audit/scan.py:228` | structured finding from a ledger | — | **mirror** as `_scale_findings` (`check="scale"`) |

## §3 Definitions in force
- monetary column: `currency._monetary_columns` (per CLAUDE.md "Per-country currency for monetary columns", `data_info.yml:1138`).
- redenomination: `data_info.yml` `Currency:` overrides (GhanaLSS 2007, Tajikistan 2000, Peru 1991, Azerbaijan, Serbia and Montenegro).
- "no allowlist" doctrine (CLAUDE.md, `_grain_strict`): applies to known-bad CELLS. A currency code per wave is a fact about the data's unit, not a cell exemption.

## §4 Invariants
- Grade is derived only from `SanityReport.ok` (`tier_from_report`); the check must not touch it. Enforced: the note is appended to `detail` only; tests assert tiers unchanged.
- The `SanityReport` returned by the fake env in tests is ONE shared object across waves — never mutate it (advisor, 2026-10-14).
- `coverage_matrix.py` / `scan.py` are read-path: no build fingerprint reaches them (`tests/test_build_transform_hash.py` green after the change; zero cache cost).
- The index of a served frame is routinely non-unique → positional masking, never label `.loc`.

## §5 Reuse decision
| quantity | decision | reason |
|---|---|---|
| monetary column set | reuse `_monetary_columns` | single source of truth; includes the #829 column |
| comparable wave pairs | reuse `currency_for` | existing currency fact; answers `decision_for_human` without a new list |
| statistic | new (`monetary_scale_breaks`) | nothing cross-wave exists; per-item median-of-ratios with pooled fallback; floor 30; tolerance 5 (stated) |
| exposure | extend `detail`, new `scale` finding in scan.py | mirrors `null_content` |

## §6 Decisions and remaining work
- The approved release workplan accepts tolerance 5 as a warning-only first
  increment. Keep ISO-code boundaries and the support floor unchanged; neither
  gap-aware/CPI adjustment nor persistent-break detection is implemented here.
- The full independent verdict at
  `slurm_logs/backlog_review_2026-10-14/verdicts/grader-scale-830.md` reports a
  read-only sweep over 87 warm monetary country-tables: four records, all Ghana.
  GhanaLSS community_prices Price 1998-99: 5.56x vs 1991-92 (per-item[108]);
  GhanaLSS food_acquired Price 1991-92: 0.16x vs 1998-99 (per-item[45]);
  GhanaSPS crop_production Value_sold 2009-10: 0.167x vs 2017-18 and 2017-18:
  6.0x vs 2009-10 (both per-item[16]). These are four records across three
  cells, not four defective tables. The reviewer attributes the GhanaLSS pair
  to GHC-era inflation; the GhanaSPS pair remains unadjudicated. This is cited
  review evidence, not a new census by the release worker.
- GhanaSPS CONTENTS.org, "Sales: three instruments, three grains, one honest
  asymmetry": 2009-10 revenue is season-specific A83/A124 (cedis + pesewas/100);
  2013-14 sales are absent by design (no plotid); 2017-18 sums nine buyer
  channels' quantity × unit price. The level difference is a triage candidate,
  not proof of a unit defect.
- Extend record diagnostics with `neighbour_forms`, preserving the existing
  three-value neighbour tuples. `form` is the common form or `mixed`; both
  detail consumers must print each neighbour's own form. Reuse `_pair_ratio`,
  without changing its statistic (§5), tolerance, or tiers (§4).
- Persistent breaks, unconfigured redenominations, community_prices grain,
  gap-aware tolerance, item coverage and unit mislabels remain residual work.

---
### Phase 3 — verification
- `monetary_scale_breaks` — OK (§2/§5): reuses `_monetary_columns` + `currency_for`; no reimplementation of either.
- `_scale_notes` / grade_feature hook — OK (§4): detail-only; tiers pinned unchanged by tests.
- `scan._scale_findings` — OK (§2): mirrors `_null_read_findings`, no new ledger.
- Positive control: Uganda `crop_production` with the #829 ×100 undone in memory → `2015-16 Value_sold` median 960 vs 90,000 / 120,000, ratio 0.0098 (per-item over 21 crops); served frame → 0 breaks.
