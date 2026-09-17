# Prior-Art Ledger — holder-gated access (design; no issue yet)

> Per-task ledger. Living, git-tracked snapshot of the machinery, definitions,
> and conventions that bear on THIS task. Inherits `STANDING.md`; cites
> `CLAUDE.md` and `SkunkWorks/data_access_restrictions.org` rather than
> re-copying. Design note: `SkunkWorks/holder_gated_access.org`.

**Search tier used:** ripgrep + git floor, plus one `Explore` subagent sweep
(read-only, 47 tool calls) over provenance, catalog, licence files, DVC config,
config entry points and `capability.py`. gitnexus MCP failed to connect this
session; no `.gitnexus/` index in the mirror. Line anchors as of `6666e0fea`.

## §1 Task, restated
Gate the S3 read cache per *access class* rather than on one World Bank key,
because 23 of 127 waves come from five holders that are not the World Bank
(plus a sixth holder inside GhanaSPS 2017-18) and each holder's terms are its
own. A wave declares its class in `Documentation/SOURCE.org`; a per-file
override covers waves that mix holders; a framework registry says what each
class's mechanism is (`open` / `attestation` / `api-key` / `gpg-recipients` /
`unrecorded`) and what evidence a user's `config.yml` must hold. The gate sits
on the S3-fetch *miss* branch only, so warm-cache reads stay credential-free.
Design only; nothing implemented.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|---|---|---|---|---|
| `_auto_unlock_s3` | `data_access.py:816-893` | decrypts the bundled reader blob, writes `s3_creds`; NO key check | `tests/test_s3_unlock.py` (pins the gate-less behaviour) | extend: called lazily after the gate, not at import |
| `_validate_wb_api_key`, `_wb_key_validated` | `data_access.py:645-673` | one network round-trip, memoised per process | `tests/test_data_access.py` | extend: parameterise by NADA host |
| `permissions()` / `can_read` / `can_write` | `data_access.py:902-957` | resource-level view, globally memoised in `_cached_permissions`; `path` arg reserved-unused | `tests/test_data_access.py` | keep for writes; per-fetch check is NEW (memoisation makes per-path impossible here) |
| `_check_remote_access` | `data_access.py:570-636` | readability = creds file exists and non-empty, by URL scheme | yes | reuse unchanged |
| `_ensure_dvc_pulled` | `local_tools.py:314-` ; `get_remote` at `:449` and `:721` | sidecar-md5 fetch straight from S3, bypassing `Repo.fetch` (GH #763) | yes | extend: gate call on the miss branch, before `:449` and `:721` |
| `get_data_file` | `data_access.py:2078-` ; `permissions(path)` at `:2123`, inverted `!= "wb_api"` at `:2128-2131`, `"wb_api" not in perms` at `:2183` | local -> DVC -> WB fallback chain | yes | extend: remove the inverted test; inherits the gate via `_ensure_dvc_pulled` |
| `_fernet_decrypt`, `_derive_fernet_key`, `_gpg_decrypt` | `data_access.py:681-740` | symmetric decryption helpers (GH #741) | `tests/test_s3_unlock.py` | reuse for the shared blob; `gpg-recipients` needs asymmetric `gpg --decrypt` (new) |
| `config.get(key, env_var=, default=)` | `config.py:70-88` | env -> file -> default | yes | reuse for `entitlements.*` |
| `config.microdata_api_key()` | `config.py:93-95` | the one key slot | yes | keep as alias of `entitlements.wb-nada.api_key` |
| `provenance.parse_source_org` / `render_source_org` / `WaveProvenance` | `provenance.py:185-238`, `:241-286`, `:128-148` | `#+KEY:` reader/writer; unknown keys -> `extra` | `tests/test_provenance.py` | extend: promote `ACCESS_CLASS` to a field |
| `Wave.license` / `Country.provenance` | `country.py:1177-1187`, `:2312-2316` | read `LICENSE.org` exactly; miss bare `LICENSE` (2 waves) | partial | fix the reader |
| `_build_registry._EXCLUDED_CALLABLES` | `_build_registry.py:116-` | read-path / guard callables kept out of build fingerprints | `tests/test_null_read_guard.py` | extend: add the gate callable |
| `push_to_cache` / `push_to_cache_batch` | `data_access.py` (§"Push to cache") | lock-retrying writer | yes | reuse unchanged (D1: pushes never blocked) |
| `scripts/reencrypt_s3_creds.py` | whole file | generator for the symmetric `.enc` | `test_key_derivation_matches_the_generator` | sibling pattern for `scripts/encrypt_class.py` (new), which derives its recipient set from the grant store |
| (none) — grant store | `lsms_library/countries/.access/{keys/<FPR>.asc, grants.yml}` (new) | public keys keyed by fingerprint, each with the classes it grants and `authorized_by`; loader refuses a grant without it | -- | new; `load_verdicts()` is the refusal precedent, `blessed.csv` the accretion precedent |
| `capability.SeriesCapability` | `capability.py:112-143` | what a series measures; `validation` ladder; no holder field; WB idno grammar only | `tests/test_capability.py` | not reused: wrong axis (instrument, not terms) |
| `coverage_matrix.BLOCKER_KINDS["licensed"]` | `coverage_matrix.py:333` | declared, undocumented, consumer file absent | no | not reused; out of scope |

## §3 Definitions & conventions in force
- **D1 / D2 / D3, "mechanism not authority", shared-compute first-class**:
  `SkunkWorks/data_access_restrictions.org` §"Decisions taken (EL, 2026-09-04)"
  and §"Shared-compute". Inherited by id; the NADA-proxy step is reversed by
  EL 2026-09-17 (design note §"Reversed").
- **Three-tier credential model**: `CLAUDE.md` §"Data Access" — describes
  intent; row 1 ("data-access calls raise `RuntimeError`") is false
  (`data_access.py` has one `raise`, `:450`, a subprocess timeout).
- **The passphrase is cosmetic and must not be "fixed"**: `CLAUDE.md` §"Data
  Access", `data_access.py:529-531`, `scripts/reencrypt_s3_creds.py` docstring.
- **`countries_root()` is read-only at runtime**: `CLAUDE.md` §"`countries_root()`
  is READ-ONLY". Decrypted plaintext lands in `data_root()` L1, never in-tree.
- **Never invoke the `dvc` CLI**: `CLAUDE.md` §"Never invoke the `dvc` CLI".
  Encrypted blobs are pushed through `push_to_cache*`.
- **`PROVENANCE_SOURCE` records where a wave was GOT, not what may be DONE**:
  09-04 note; `provenance.py:42-56, 169-182`.
- **Warn-by-default / `LSMS_*_STRICT` lever**: `CLAUDE.md` §Sites R, B, Q, I.
  Applied here to `unrecorded` only (design note D5).
- **Cache-hash levers**: `_BUILD_INPUT_SUFFIXES` `country.py:706`; non-recursive
  globs `country.py:933,945,996`; `CLAUDE.md` §"Cache Behavior".

## §4 Invariants & assumptions
- The S3 object key is `md5[:2]/md5[2:]` — no country, wave, holder or licence
  (`scripts/dvc_remote_reachability.py:39-41`). Prefix-scoped IAM cannot
  address a holder. Moot under D1 (encrypt-before-push), recorded so nobody
  re-proposes a second bucket.
- Zero of 9,977 `.dvc` sidecars use per-output `remote:`; both fetch sites read
  `[core] remote` (`local_tools.py:449, :721`). Moot under D1.
- `_ensure_dvc_pulled` is reached from `get_dataframe` and is NOT in
  `_EXCLUDED_CALLABLES`: an edit inside it is expected to move every table's
  hash once (`CLAUDE.md` Site R, 37/37). Unmeasured this session (no venv).
- `permissions()` is memoised process-wide: a per-path check cannot live there.
- `Wave._input_hash` never hashes `Documentation/`: editing `SOURCE.org` moves
  no cache hash (verified from the globs; consistent with the 123-wave
  provenance backfill having cost no re-warm).
- 8 wave dirs have no `SOURCE.org` (Afghanistan x2, Brazil, Bulgaria x5); 23
  have no licence file; 2 have bare `LICENSE` the reader cannot see.
- `LSMS_SKIP_AUTH=1` is what the data-free CI job runs under
  (`tests/conftest.py:5`); it must keep meaning "skip everything".
- The 09-04 shared-compute measurement: no config, no key, GhanaLSS
  `food_acquired` 5,259,344 rows from a warm cache. Must survive.
- GhanaSPS 2017-18 already holds 30 pre-anonymization files in the open
  bucket, terms unrecorded (`LICENSE.org:34-56`, dated 2026-08-19).
- `gpg-recipients` reintroduces the `gpg` binary GH #741 removed from the
  reader path. Accepted for that class only (design note).

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| per-wave class carrier | extend `provenance.py` (`ACCESS_CLASS` keyword) | 09-04 design; reader/writer exist; zero cache cost |
| per-file override | new `Documentation/ACCESS.yml` | `LICENSE.org` is verbatim prose by rule; GhanaSPS 2017-18 needs it |
| class registry | new `lsms_library/access_classes.yml` | refines 09-04 (four keywords per wave -> one keyword + registry); sibling of `derivations.yml`, no hash cost |
| user evidence | reuse `config.get()`; new `entitlements:` map | 09-04 `grants:` shape, renamed; `microdata_api_key` kept as alias |
| validator | extend `_validate_wb_api_key` by host | NADA software is shared; precondition to measure |
| gate | new `access.check_fetch` on the miss branch | `permissions()` is memoised; warm path must stay free (09-04) |
| shared reader blob unlock | reuse `_auto_unlock_s3`, called lazily after the gate | the mechanism is fine; its call site was the bug |
| encrypted class | new `gpg --encrypt --recipient` tool + decrypt-on-miss | no asymmetric encryption in tree; symmetric Fernet/gpg helpers do not fit revocation |
| second remote / bucket | rejected | D1 |
| push gate | rejected | D1 |
| strict lever | reuse the `LSMS_*_STRICT` pattern for `unrecorded` only | other mechanisms refuse by default (stated design) |

## §6 Open questions for the human
The first draft's four were resolved by EL on 2026-09-17 (design note
§"Decisions taken", D4–D8): `--recipient` per pubkey; `unrecorded` served
with a warning; GhanaSPS 2017-18 supersession established
(`slurm_logs/ghana_audit/SUPERSESSION_ghanasps_w3_2026-09-17.org` — every
wired variable in the six pre-anonymization files is in the public release);
`attestation`/`api-key` refuse; a site entitlement layer (`LSMS_SITE_CONFIG`,
unioned with the user's config) for classes rather than a bypass flag.

Remaining: the terms of the 100 non-Dataverse GhanaSPS 2017-18 files, and
whether the `00_hh_info` geography (GH #579) is kept. Blocks Phase 4 only.

Additional §2 row from D8: `config._config_dir` / `config.get`
(`config.py:27-44, 70-88`) — extend with a site layer between user config
and default; `platformdirs.site_config_path` is the natural default.

## Out of scope, noted so they are not lost
- `pyproject.toml:7` says `CC-BY-4.0`; `LICENSE.txt` is CC BY-NC-SA 4.0.
- `Liberia/_/population.yml:37` claims `Liberia/2018-19/SOURCE.org` lacks the
  provenance keywords; it has them (`SOURCE.org:5-14`).
- `coverage_matrix.BLOCKER_KINDS["licensed"]` declared, undocumented,
  `.coder/coverage/blocked_sources.csv` does not exist.
- `CLAUDE.md` "Countries Without Microdata" lists Nepal as NSO-held; all three
  Nepal `SOURCE.org` are `worldbank`/`lsms` (the data is absent, the record is
  WB — two facts). South Africa / Armenia `datafirst` / `central` are WB
  collections, not holders.

---
### Phase 3 — verification (fill at task end)
Design only this session; no symbols changed. To fill when Phase 1 lands.
