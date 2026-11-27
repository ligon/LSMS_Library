# Prior-Art Ledger — GH #1288 ordinary country-script DVC source freshness

Search tier: ripgrep and Git. Extends `STANDING.md`, not an inventory of the corpus.

## §1 Task

When a country script literally references a declared DVC source, replacement of only the sidecar must not leave an ordinary L2-country parquet fresh under its old hash. Confirm the scope against Nigeria's existing country scripts and the new W2 example in PR #1821. Keep distinct cache coverage, dependency and source provenance contracts intact.

## §2 Existing machinery

| Symbol | Binding | Reuse |
|---|---|---|
| `Country._table_cache_hash` | `country.py`, country script/module/config hashes, wave set union and build registry digests | Adds the actual missing country source inputs. It is excluded as a callable from the build registry's self-referential closure. |
| `scan_script_data_refs` | `local_tools.py`, AST string-literal scanner | Keep the same finite declaration grammar. Country script refs resolve relative to the script, as `_make_declared_inputs` already does. |
| `cached_file_hash`, `source_fingerprint` | `local_tools.py`, stat-sensitive byte hashes and existing source token convention | Ordinary hashing must use the sidecar only, not the raw fallback of the generic source helper. |
| `Wave._input_hash` | `country.py`, existing per-wave YAML/script source fold | Remains authoritative for already covered sources. The current wave scanner uses its legacy `Wave/Data` reference root. |
| `_make_declared_inputs` | `country.py`, finite Make raw and Other closure | Resolves country literals, but calls `_table_cache_hash` and cannot be called recursively there. Keep the independent Make manifest. |
| `_read_l2_country_cache` | `country.py`, freshness guard plus coverage | Rejects mismatched hashes before serving a local country parquet. No change to the read gate, writers or coverage. |

## §3 Contracts

Use `STANDING.md` §§3–4 and the **Cache Behavior** section of AGENTS.md. See the static-closure refinement in `1288-zero-exit-freshness-repair.md`. That repair explicitly excluded ordinary country-script literal declarations. The exact Nigeria #1821 read-only metadata witness is a regression target, not evidence of production stale data.

## §4 Invariants

The ordinary hash remains wave-set independent. Paths in its new fragments must be checkout-independent. Only actual country-side literal DVC sidecars are read, no raw payload or provider. Ordinary validation must not read outside the declared country root. A file without a new eligible input gets no marker and keeps its existing hash. Do not certify scripts outside the existing UTF-8 literal scanner, dynamically constructed paths, infer a dependency through a managed parquet, or change L2-wave/Make/assumed-fresh behavior. Separate #1797 Other-table transitivity and #1798 mid-request publication.

## §5 Reuse decision

Use the finite existing scanner, path-resolution convention and sidecar hash rather than a second parser or currency-specific exception. Exercise the real country hash, sanctioned parquet metadata and actual ordinary read gate with invented fixtures. Keep direct Make tests rather than replacing one proof with another. Census before and after on the same checkout identity must report exact rollout scope.

## §6 Open questions

The pinned tracked-sidecar census found no overlap between these country-script literal source paths and existing wave coverage. Seven ordinary country hashes change, seven additional literal-bearing JSON caches stay unchanged, and none of the seven resolves a registered DVC materialization stage. No rollout shortcut or duplicate-discovery path is needed. Computed paths, input changes during rebuild, and omitted DVC stage dependencies remain outside this proof.

## §7 Verification

- The original read gate returned the old frame after a sidecar replacement with unchanged size and restored mtime. The new gate input rejects it. Two real default orchestration routes (no stages and unavailable DVC repository) each publish a fresh successor after an invented country builder gets different input, then serve it warm. The raw source and its provider are not used by these tests.
- 209 focused offline tests passed against the isolated candidate. Fourteen independent cases passed, including the byte-filename, portable path, and no-provider controls. The selected files also cover the pre-existing direct Make repair, content staleness, and coverage manifest behavior. Those contracts did not need a new policy.
- Two tests confirm the scan cannot erase the existing script-byte hash for valid non-UTF-8 scripts or unencodable path literals. A third preserves ordinary hash drift on legal POSIX byte filenames by ASCII-escaping the country-relative key. These are invented fixtures, not survey data.
- At base `d18b47de60e49b2369ec85a212ba494381b4a4e9`, identical metadata-only scans covered 2,879 hashes on both sides. Only the country hashes of EthiopiaRHS `community_prices` and Nigeria `anthropometry`, `crop_production`, `livestock`, `people_last7days`, `plot_inputs`, `plot_labor` moved. No wave hash moved. An independent probe compared all 42 declared/global build transform identities and four sampled script-import build closures without a difference.

- A private metadata-only composition with #1821 confirms that replacing only the actual W2 lookup sidecar moves Nigeria crop production’s full and `2013Q1` ordinary hashes. #1821 alone does not move either hash. Wave and unrelated sample hashes stay fixed in both controls. No native source is opened.
