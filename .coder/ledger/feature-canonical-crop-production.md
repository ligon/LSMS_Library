# Prior-Art Ledger — `Feature('crop_production')` canonical-index assembly (#569 / #775)

> Per-task ledger. Inherits `.coder/ledger/STANDING.md`; cites `CLAUDE.md` and
> `lsms_library/data_info.yml` rather than re-copying them.

**Search tier used:** ripgrep + git (floor). GitNexus MCP was `CONNECTION_CLOSED`
this session and there is no `.gitnexus/` index in this mirror, so the
CLAUDE.md-sanctioned substitutes were used and are DECLARED here: `rg`/`grep`
call-site sweeps for `index_info`, `_canonical_index_levels`,
`_harmonize_country_frame`, `_compute_no_v_join`, `fabricate_missing_levels`,
`_CROP_LEVELS`/`_PLOT_LEVELS`; plus a `data_scheme.yml` sweep of every country's
`crop_production` index and a warm read of every built frame.

## §1 Task, restated

`Feature('crop_production')` has no `index_info` entry, so
`_canonical_index_levels` returns `[]`, `_harmonize_country_frame` cannot align
anything, and `Feature.__call__` falls through to the **modal index-shape**
filter (`feature.py` ~:659-677): frames whose `index.names` tuple is not the most
common one are dropped with a `UserWarning`. Measured on the warm corpus, that
keeps 8 of 15 countries. It also *penalises movement toward the canonical index*:
`feat/854-malawi-shipped-factors` adds the canonical `condition` level to Malawi,
Malawi's shape becomes its own singleton, and 131,379 rows (81% of the assembly
in the red-team's four-country call) are excluded
(`slurm_logs/2026-09-09_epar_curation/REDTEAM_P2_854_malawi.org`, item 1b, FAIL).
Uganda — which has always had `condition` — is excluded for the same reason. And
per #775 the survivor set depends on the *order of the country arguments*, not on
the data.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_canonical_index_levels` | `feature.py:59` | parses `Index Info > index_info` `(t, v, i)` tuple strings | indirectly | **reuse** |
| `_fabricates_missing_levels` | `feature.py:78` | `#506` opt-in: add a missing canonical level as `pd.NA` | `tests/test_feature.py::TestFabricateMissingLevels` | **reuse, untouched** |
| `_harmonize_country_frame` | `feature.py:231` | per-country alignment: drop all-NaN cols, reorder to canonical order (#498), drop extra levels + collapse | `tests/test_feature.py` | **extend** |
| `_collapse_duplicate_index` | `feature.py:168` | the Feature-side `groupby().first()` / additive-sum collapse, audited (#323) | grain tests | **reuse (must not fire)** |
| modal filter | `feature.py:659-677` (inline in `__call__`) | keeps the modal `index.names`; argument-order dependent (#775) | none | **extract + extend** |
| `_compute_no_v_join` / `_no_v_join_tables` | `country.py:4997` / `5028` | a table whose `index_info` omits `v` is exempted from the API-time v-join | `tests/test_no_v_join_declarative.py` | **must not change** |
| `_CROP_LEVELS` / `_resolve_crop_level` | `transformations.py:4179` / `4183` | `j` vs `crop` alias, resolved per call | yes | **left alone** (operates on `Country()` frames, which keep native names) |
| `_PLOT_LEVELS` / `_resolve_plot_level` | `transformations.py` | `plot` vs `plot_id` twin | yes | same |
| `attach_currency` | `currency.py` | `crop_production` **is** monetary → `Feature` appends a `currency` level to the canonical list | yes | **reuse** |

## §3 Definitions & conventions in force

- **Canonical crop grain** — `data_info.yml:590` (`KgFactor` note): "Crop rows sit
  at `(t, i, plot, j, u, condition, season)` and are never summed across `u` or
  `condition`". The `index_info` form adds `v` (as `food_acquired`'s does), because
  `v` is joined at API time and `index_info` describes the **returned** index —
  `CLAUDE.md` §"`sample()` and Cluster Identity".
- **`u` missing-unit sentinel = `Unknown`** — `data_info.yml:5-40`, verbatim: "A
  survey that records an amount and no unit must put `Unknown` on `u`, NOT a null…
  a NaN on a declared index level is a DEFERRED SILENT DELETION".
- **`condition` missing sentinel = `unknown_condition`** — `data_info.yml:518-523`:
  "it is a real value, never NA, because a null index key is silently dropped by
  the duplicate collapse".
- **`season`** — no `Columns:` entry yet; vocabularies diverge (Uganda `A`/`B`,
  GhanaSPS `major`/`minor`/`annual`). Documented in this change, **without** a
  `spellings:` block (a `spellings` block is read as a CLOSED vocabulary by
  `diagnostics._check_declared_spellings` — see the `u` comment at
  `data_info.yml:33-40` for the identical reasoning).
- **#569's ask** — register `plot_labor` / `crop_production` / `plot_inputs` in
  `index_info` *after* harmonizing `plot_id → plot` and `crop → j`.
- **#775's ask** — "resolve on a declared canonical index from `data_info.yml`'s
  `index_info` rather than on whichever shape happens to be modal among the
  arguments."

## §4 Invariants & assumptions

- **`v` must stay in the registered index.** `_compute_no_v_join` (`country.py:5011`)
  skips the API-time v-join for any table whose `index_info` omits `v`. Registering
  `crop_production` *without* `v` would silently drop `v` from every
  `Country(...).crop_production()` frame in the corpus. Registered **with** `v`.
- **A fabricated index level must never be null.** `groupby(dropna=True)` deletes a
  row on a NaN key (`CLAUDE.md` §"Grain Collapse" §3b). Every level this change adds
  carries a declared **string sentinel**.
- **Adding a constant index level cannot create duplicates**, so nothing this change
  does can reach `_collapse_duplicate_index`. Verified empirically (§Phase 3).
- **`#506`'s `pd.NA` fabrication for `interview_date` must stay byte-identical** —
  it is a separate, earlier opt-in and is not folded into the sentinel mechanism.
- **Promotion must null-fill.** Mali / Tanzania / Nigeria carry `u` as a *column*
  with NaNs; promoting it to an index level without filling would manufacture the
  exact deferred-deletion hazard `data_info.yml:9-20` warns about.
- **`Feature` is read-path.** No cache fingerprint may move (§Phase 3 probe).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| canonical level list | reuse `_canonical_index_levels` | already parses `index_info` |
| level-name aliasing | **new** (`Index Info > level_aliases`) | no config-driven rename mechanism exists; `transformations._CROP_LEVELS` resolves per-call on `Country()` frames and is the wrong layer for assembly |
| missing-level fill | **new** (`Index Info > missing_level_sentinels`) | `#506`'s `fabricate_missing_levels` fills with `pd.NA`, which §4 forbids for a declared index level |
| level present as a column | **new** (promotion) | lossless; strictly better than fabricating a sentinel over a real value |
| kept-shape selection | **extend** (extract `_select_kept_shape`) | the modal rule stays for unregistered features; only its tie-break and its canonical preference change |
| the v-join | reuse, unchanged | §4 |

### The chosen option (A, narrowed) and what was rejected

**Chosen — (A) register + rename + sentinel, with a promotion branch.** One
`index_info` entry, one `level_aliases` map, one `missing_level_sentinels` map, and
a three-branch per-level resolution: *already a level* → nothing; *present as a
column* → promote (null-filled with the sentinel); *absent* → fabricate the
sentinel. Plus a deterministic kept-shape rule that prefers the canonical shape.

- **(B) subset/superset union rule** was rejected as the primary mechanism: without
  the rename, `('plot','crop')` and `('plot_id','j')` are neither subsets nor
  supersets of each other, so B alone keeps nothing extra. With the rename, B and A
  coincide except that B would carry the *union* of levels, meaning a level nobody
  declared canonical could enter the index. A is B plus a declared target.
- **The red-team's own suggestion (add `crop_production` to `fabricate_missing_levels`)**
  was rejected: it fills with `pd.NA`, and `condition`'s own `data_info.yml` note
  says the sentinel exists precisely so the value is "never NA".
- **Filling `plot` for EthiopiaRHS** was rejected. EthiopiaRHS ships a documented
  COARSE household-grain version of the item-level table
  (`.claude/skills/cross-country-features/SKILL.md` §2: "treat it as a separate
  table"; GH #512). It stays excluded — now deterministically, with the same loud
  warning.

## §6 Open questions for the human

- **`unknown_condition` on a country whose instrument never asked about condition
  conflates *not-asked* with *asked-but-missing*** — the same distinction the
  coverage matrix's `absent_verdicts` machinery exists to keep apart. Taken here
  because the alternative (a null key) is a silent deletion, and because
  `data_info.yml` already defines the sentinel that way. If a `not_asked_condition`
  spelling is wanted, it is a one-line config change.
- **`season` vocabularies are not harmonized**: Uganda `A`/`B`, GhanaSPS
  `major`/`minor`/`annual`. They assemble (the *level* exists) but do not compare.
  Follow-up, not fixed here.
- **`plot_features` is registered on `plot_id`; `crop_production` is canonicalized
  on `plot`.** A cross-feature join now needs a rename in one direction. Aligning
  the two is a #569 follow-up.
- **Malawi `feat/854`'s `condition` values** (`shelled`, `unshelled`,
  `shell_not_applicable`) are outside the `condition` spellings list in
  `data_info.yml:525-545`. Not this task's change, but that branch should either
  extend the list or map onto it.
- `Feature('crop_production')` now carries `u` as an index level for Mali,
  Tanzania and Nigeria where `Country(name).crop_production()` carries it as a
  column. That is a deliberate assembly-time bridge; moving `u` into those three
  countries' declared indexes is the #569 follow-up.

---
### Phase 3 — verification (measured)

Measured warm, `trust_cache=True`, with a scratch `LSMS_DATA_DIR` of symlinks to
the shared cache plus Uganda from `wt-824-uganda/.data` and (for the 7-level run)
Malawi from `wt-854-malawi/.data` with its own `_/` config. The shared cache was
verified untouched afterwards (`find … -newer` → empty). Library identity
asserted on every invocation (`'wt-feature-canonical' in lsms_library.__file__`).

**Per-country shape and rows.** `Feature('crop_production')(all 15)`, before
(base `8b03b799`) vs after. Every kept country's Feature row count equals its
`Country(name).crop_production()` row count exactly.

| country | `Country()` index (native) | rows | base Feature | after |
|---|---|---:|---:|---:|
| Benin | `(i,t,v,plot,crop,u)` | 9,056 | 9,056 | 9,056 |
| Burkina_Faso | `(i,t,v,plot,crop,u)` | 15,587 | 15,587 | 15,587 |
| CotedIvoire | `(i,t,v,plot,crop,u)` | 22,216 | 22,216 | 22,216 |
| Ethiopia | `(i,t,v,plot_id,j,u)` | 85,519 | **0** | 85,519 |
| EthiopiaRHS | `(i,t,v,j,u)` | 17,523 | 0 | 0 (no `plot`; by design) |
| GhanaSPS | `(i,t,v,plot_id,j,u,season)` | 25,109 | **0** | 25,109 |
| Guinea-Bissau | `(i,t,v,plot,crop,u)` | 10,579 | 10,579 | 10,579 |
| Malawi (6-level) | `(i,t,v,plot,crop,u)` | 131,379 | 131,379 | 131,379 |
| Malawi (7-level, #854) | `(i,t,v,plot,crop,u,condition)` | 131,548 | **0** | 131,548 |
| Mali | `(i,t,v,plot,crop)` + `u` col | 35,060 | **0** | 35,060 |
| Niger | `(i,t,v,plot,crop,u)` | 46,341 | 46,341 | 46,341 |
| Nigeria | `(i,t,v,plot,crop)` + `u` col | 62,844 | **0** | 62,844 |
| Senegal | `(i,t,v,plot,crop,u)` | 8,416 | 8,416 | 8,416 |
| Tanzania | `(i,t,v,plot_id,j)` + `u` col | 14,126 | **0** | 14,126 |
| Togo | `(i,t,v,plot,crop,u)` | 11,563 | 11,563 | 11,563 |
| Uganda | `(i,t,v,plot,j,u,condition,season)` | 130,606 | **0** | 130,606 |
| **total kept** | | | **8 / 255,137** | **14 / 608,401** |

With the #854 Malawi config: base kept **7 countries / 123,758 rows** (Malawi
excluded, −131,548); after, **14 / 608,570**.

**Assembled index**: `['country','t','v','i','plot','j','u','condition','season','currency']`
— named, canonical order, `is_unique == True` in every run.

**Zero rows lost or collapsed.** `grain_reports()` is EMPTY after every
assembly (0 before, 0 after), on both the 6- and 7-level Malawi runs. Adding a
constant level or promoting a column can only refine an index, so
`_collapse_duplicate_index` is structurally unreachable from this path; pinned
by `test_no_collapse_and_no_row_loss_on_a_duplicate_prone_frame`.

**Nulls filled on promotion** (the `u` column → `u` level, sentinel `Unknown`):
Mali 1,910; Nigeria 13,138; Tanzania 2,395. Each was a NaN that would have been
a deferred silent deletion had the level been promoted unfilled.

**GH #775 — argument-order dependence.** Base, `crop_production` over
`{Uganda, Ethiopia, Tanzania, Malawi(7)}`: three orders → three different single
survivors (`['Uganda']` 130,606 / `['Malawi']` 131,548 / `['Tanzania']` 14,126).
After: all **24 permutations** return the same four countries and 361,799 rows.
`nonfood_expenditures(['Togo','Uganda','Nigeria'])` — #775's own example — is
also order-independent now (824,963 rows, all three, in every order).

**Cache fingerprints — 0 of 26 moved.** `build_transforms_fingerprint` for 9
tables + `None`, and `Country(c)._table_cache_hash(t, c.waves)` for
Malawi/Uganda/Ethiopia/Nigeria × 4 tables, base vs branch, identical. `Feature`
is read-path and the global `data_info.yml` is not an input to
`Wave._input_hash` (which hashes the *wave's* `data_info.yml` by path).

**Tests.** `tests/test_feature_canonical_index.py` — 35 new (34 fast + 1 slow,
data-gated with a clean skip). Fast tier: `5,400 passed, 174 skipped, 13 failed`;
all 13 failures reproduce on base `8b03b799` in this environment (label-selection
end-to-end, `test_api_discoverability::test_dispatcher_kwargs_are_discoverable`
— the pre-existing undocumented `valuation` kwarg from #585 —, a CPU-affinity
test, and a subprocess `countries_root` override). **0 new failures.**
`make api-audit` reports the same single pre-existing undocumented kwarg.

**No other registered feature changed.** The canonical-preference in
`_select_kept_shape` applies to EVERY feature with an `index_info` entry, not
just `crop_production`, so it had to be measured rather than argued: a feature
whose canonical shape is held by a minority would flip its survivor set from
majority to canonical. Base vs branch, warm and WRITE-FREE (only countries whose
L2 parquets already exist were asked for, so nothing rebuilt), over the other
**14** registered/derived features — `cluster_features`, `household_roster`,
`household_characteristics`, `plot_features`, `food_acquired`, `interview_date`,
`shocks`, `assets`, `individual_education`, `food_prices`, `food_quantities`,
`livestock`, `people_last7days`, `nonfood_expenditures`: **14 of 14 identical**
in kept-country set, per-country row count AND index names (e.g. `food_acquired`
20 countries / 7,816,788 rows, `household_roster` 33 / 2,876,892, `assets` 23 /
3,523,085 — unchanged to the row). Zero flips. The canonical shape is already
modal for all of them, which the #498 reorder guarantees whenever a country has
all canonical levels.

**Process defect to disclose: 65 parquets were written into the SHARED cache.**
The scratch `LSMS_DATA_DIR` was a tree of SYMLINKS into
`/global/scratch/.../cache/lsms_library`, so a build that fired wrote *through*
the symlink. The `Feature('crop_production')` measurements wrote nothing
(checked immediately: `find -newer` empty); the **full fast test tier**, run with
that same `LSMS_DATA_DIR`, cold-built 65 missing `var/` and `{wave}/_/` parquets
between 00:54 and 00:59 (Malawi/Nigeria `food_acquired`, several `sample`,
`household_roster`, `cluster_features`, `housing`, …). They are ordinary rebuilds
from **unmodified country configs** — this branch touches only the global
`lsms_library/data_info.yml` and `feature.py`, neither of which is an input to
`Wave._input_hash` / `Country._table_cache_hash` (0 of 26 probed, above) and
neither of which affects *stored* parquet content (spellings/dtype/kinship are
read-path). `crop_production` parquets were NOT among them, so every number in
this ledger is unaffected. The cache is warmer, not wrong; nothing was deleted.
The later cross-feature sweep was re-run with a warm-only presence filter and
wrote **0** files. **Lesson for the next agent: a symlink farm is not a
read-only data root** — either verify every needed parquet exists first, or copy.

**GitNexus substitutes declared** (MCP `CONNECTION_CLOSED`, no index): `rg` call-site
sweeps for every symbol touched. Blast radius of the edit:
`_harmonize_country_frame` has exactly two call sites (`Feature.__call__` and
`tests/test_feature.py`), both updated; the inline modal filter had one; the new
helpers are additive. `_canonical_index_levels` is also read by
`bench/feature_audit/scan.py` (only to name levels; its `CANONICAL_FEATURES` list
is a hand-curated subset that already omits `livestock` /
`people_last7days` / `nonfood_expenditures`, so it is deliberately NOT widened here).

### Anchored verdicts

- `_index_info_section` — **OK (§2)**: factors the three existing re-reads of
  `data_info.yml` into one; no behaviour change (`_canonical_index_levels`
  returns the same list for every registered table).
- `_level_aliases` / `_rename_index_levels` — **OK (§5)**: not a REINVENTION of
  `transformations._CROP_LEVELS`. That resolves a level name *per transform call*
  on a `Country()` frame, which must keep its native names; this renames *once*
  at assembly. Both now cite the same config fact.
- `_missing_level_sentinels` / `_align_to_canonical_levels` — **OK (§4, §5)**:
  not a reinvention of `_fabricates_missing_levels` — that fills `pd.NA`, which
  §4 forbids for a declared index level. Pinned distinct by
  `test_no_feature_declares_both_mechanisms`.
- `_select_kept_shape` — **OK (§3)**: the modal rule is preserved verbatim for
  unregistered features (`test_modal_rule_survives_for_an_unregistered_feature`);
  only the canonical preference and the tie-break are new.
- v-join — **OK (§4)**: `crop_production` is still absent from
  `_no_v_join_tables()`; pinned by `test_canonical_index_keeps_v`.
- Cache — **OK (§4)**: 0 of 26 probed fingerprints moved.
