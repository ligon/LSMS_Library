# Prior-Art Ledger — 1032 read-only DVC runtime

**Search tier:** ripgrep + Git and stdlib AST; installed dependency source only.

## §1 Task
Source-only prototype on actual d6044e280596988dac3a9b5bab78f4e5c9feb9d4. Make ordinary DVC metadata scratch private without copying/pinning the live country tree or changing managed L1 and producer/strict semantics. No import/test/runtime/provider/network/census is authorized in this phase.

## §2 Existing machinery
Reuse local_tools._build_dvcfs/refresh_s3_credentials, _ensure_dvc_pulled and both L1 layouts; Country.load_dataframe_with_dvc's separate Repo construction; paths.data_root/countries_root; tests/test_gh1001_no_intree_credential_write.py private subprocess fixtures and tests/test_dvc_caching.py stage fixtures. DVC3.67.1 supplied-Repo construction is documented, tmp/SCM/lazy hooks are private. Source design/capability inventory: reports/1032-readonly-dvc-source-design-20261109 (manifest cd732223e7bfc0be5984855f2563b6c6d7f622dbc523d1a599f8200b691761ff).

## §3 Definitions
Inherit STANDING.md §§3–4 and CLAUDE.md read-only country metadata/sanctioned I/O contract. Original #1032 comment6268237478 corrects import-only acceptance; old19×4 read is historical. Repo keeps live root, local filesystem, config.local/relative remotes, sidecar and stage identity; no HEAD snapshot. Current14 committed dvc.yaml files are empty, but working-tree stage overrides remain compatibility scope.

## §4 Invariants
No metadata mirror/global registry/monkeypatch/protocol. Scratch cleanup is owner-PID-only; no automatic TemporaryDirectory finalizer; fork children cannot delete parent scratch or reuse inherited Repo/SQLite for a new provider. Keep dependency declaration dvc[s3]>=3.67.0,<4.0.0; tested3.67.1 differs from capability compatibility. Unsupported hooks fail before source mutation, outside unavailable-source fallback. Preserve credentials and partial-stanza guard, normal retry/C HEAD/window semantics, errors and strict policies. Stage commands/output paths are never redirected: valid private-output stage can work; in-tree writer on read-only source retains its failure.

## §5 Reuse decision
Extend two Repo construction sites with one small runtime adapter and original DVCFileSystem type. Reuse DVC Config/SCM/index/readers, public supplied-Repo seam; explicitly disclose private tmp/SCM/lazy capability hooks. Existing data_root/dvc-cache remains object cache; private scratch is not a second object store.

## §6 Review boundary
Root authorized prototype only. Freeze source, hooks and finite UNRUN controls before any imports/tests/runtime. No dependency range edit is planned; compatibility claims require later controls across installed versions. No science/producer policy decision is inferred.

### Phase 3
UNRUN — source-only prototype.
