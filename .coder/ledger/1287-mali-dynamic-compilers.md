# Prior-Art Ledger — GH #1287 Mali declared-wave compilers

**Search tier used:** git + ripgrep + stdlib AST floor; no project import.
**Base:** public d65e8b13683409ab0005f1a5590208cd7d94ae62.

## §1 Task, restated

Seven country compilers currently swallow every wave read exception. Select
contributors through Country('Mali', preload_panel_ids=False).waves, each
Wave.data_scheme and Wave.wave_folder; preserve concat and all subsequent
assertions, id_walk placement and sanctioned writer exactly. The seven are
food_coping, crop_production, plot_inputs, livestock, plot_labor,
people_last7days and plot_features. food_acquired is excluded. Prepare portable
synthetic tests and a proposed guarded finite launcher; do not run tests now.

## §2 Existing machinery

| symbol | path:line | behavior / test evidence | decision |
|---|---|---|---|
| Country.waves | lsms_library/country.py:2802 | authoritative labels; establishes wave_folder_map; existing coverage-matrix / 818 ledgers | reuse |
| Country.__getitem__ | lsms_library/country.py:3103 | constructs Wave with mapped physical folder | reuse |
| Wave.data_scheme | lsms_library/country.py:1262 | own script and YAML declarations, absent _ means no tables | reuse |
| Wave.wave_folder | lsms_library/country.py:1191 | physical directory, not necessarily logical year | reuse |
| get_dataframe / to_parquet | STANDING.md §2/4 | sanctioned reader/writer | reuse unchanged |
| seven compiler tails | countries/Mali/_/{table}.py | pd.concat, uniqueness, one var write; source AST oracle is d65 | reuse byte-for-byte |
| recovered seven-compiler-extraction.patch | recovery Mali output, source ae308d93 | declaration gating/error chaining; fixed four-WAVES remains, no present acceptance | extend only loop |
| prior error guard | .coder/ledger/1281-wave-config-error.md | declared failures propagate before concat/write | reuse invariant |

## §3 Definitions & conventions in force

Inherits STANDING.md §3 and lsms_library/data_info.yml. Country CONTENTS.org
Overview distinguishes EAC-I 2014/2017 and EHCVM 2018/2021. CONTENTS.org
plot_features / Oddities says "id_walk deliberately NOT applied at the country
level": moved households otherwise create nine duplicate keys. Wave declarations
mean wiring, not presence of a stale cached parquet. Runtime-derived food is
out of scope; no schema, registry, Makefile, mapping or scientific helper edit.

## §4 Invariants & assumptions

Inherits STANDING.md §4 and CLAUDE.md data/cache conventions. Unwired waves
must never be probed. A declared read failure aborts with table, logical wave,
physical path and original exception cause before any country write; earlier
successful reads must not become partial output. Empty readable declared frames
remain contributions under the unchanged tail. No declared contributions retain
the original assertion. Pre-walk identity, order, dtypes, attrs and null masks
follow unchanged pd.concat and writer; no scientific transform is introduced.
The public CONTENTS tree-sale scaling/ruling conflicts with later private
as-recorded sales source: preserve public d65 science verbatim and report the
conflict; do not import the private country module or claim science acceptance.
Existing Leger yes-sale missing amounts and all other country caveats remain.

## §5 Reuse decision

Reuse Country/Wave discovery and metadata; extend the recovered declaration
check to actual waves and mapped folders, retaining chained RuntimeError.
No common framework helper: this isolated country fix needs only the existing
Country/Wave API. New tests exercise compiler behavior with synthetic pandas
frames and stubbed library interfaces; no real Country or sanctioned IO runs.
The launcher is review-only new control code, never imported by the product.

## §6 Open questions for the human

None blocks source preparation. Root review is required before any proposed
synthetic test process. This unit does not decide the separate science dispute,
claim real API validation, or spend held data/custody/native budgets.

### Phase 3 — verification

- OK (§2): seven loops reuse Country.waves, Country[t], Wave.data_scheme and
  Wave.wave_folder; no fixed WAVES or logical-folder construction remains.
- OK (§4): exact public d65 assert-pieces/concat/uniqueness/writer AST tails
  match in all seven files; no id_walk, scientific, schema, registry, Make or
  config/module edit is included. Existing sales-interpretation conflict held.
- OK (§4/5): eleven portable synthetic unittest methods prepared, not executed;
  current/future/mapped/unwired/missing/corrupt/permission/metadata failures,
  empty/nonunique frames, no partial write, attrs/dtypes/nulls/config checks.
- OK (§5): nine Python files compile with stdlib only; source pins agree;
  bounded check 0.099s, 18,380KiB RSS, core38, threads1, 8GiB, timeout120s.
- HELD (§6): tests, pandas/project imports, real Country/Wave discovery and all
  actual source/cache/selected IO unexecuted. Guard/runtime allowlist integration
  unexecuted; reviewed runtime pin/outer containment remain root decisions.
- Preparation-only sparse-checkout git add refusal for new review_control was
  resolved with git add --sparse in this NEW checkout; no old packet/root change.
