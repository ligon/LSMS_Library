# Prior-Art Ledger - GH1149 staged reacquisition

Search tier: ripgrep + git, complete current country CONTENTS, live GitHub
issues/comments, and installed DVC hash implementation. Parent: release
f8d236694c26fe9e709c5548f02c70f74ea72c33. Code-only synthetic acquisition;
no actual World Bank/DVC acquisition is authorized in this task.

## §1 Task, restated

Implement the six-part Option C ruling in GH1149 (2026-10-20): stage the whole
archive, resolve incoming basenames case-insensitively to unique recursive
Data sidecar paths, retain archive-relative paths for new members, compare
before placement, refuse ambiguity, and report recursive leftovers without
retiring them. Keep confirmation/push and held-release provenance gates.

## §2 Existing machinery

| Symbol | Anchor at parent | Tested / decision |
|--------|------------------|-------------------|
| `_extract_all_from_zip` | `lsms_library/data_access.py:1141` | Real extraction exercised by `tests/test_wave_provenance.py::wb`; extend flat/no-overwrite behavior to staged verified refresh |
| `_AcquiredRelease`, `_ACQUIRED_RELEASES` | `data_access.py:1207` | Completion tests at `test_wave_provenance.py:2483`; extend existing report carrier, no parallel ledger |
| `_record_acquired_release` | `data_access.py:1562` | Acquisition/push/provenance tests at `test_wave_provenance.py:2075`; retain authoritative writer |
| `populate_and_push`, `add_wave` | `data_access.py:1652`, `:2156` | Existing completion and primary-source routing tests; extend their flat file collection to actual acquired membership |
| `get_data_file`, `_fetch_destination`, `_acquisition` | `data_access.py:2444`, `:88`, `:119` | `test_data_access.py`, `test_wave_provenance.py`; retain routing and acquisition opt-in |
| `push_to_cache_batch` | `data_access.py:1426` | Existing writer/lock tests; reuse unchanged, fake receipt only in this task |
| `acquired_release`, `write_provenance` | `lsms_library/provenance.py` | `test_wave_provenance.py`; reuse preserved unknown/scalar/primary-source facts |
| `fobj_md5` | installed `dvc_data/hashfile/hash.py:107` | Existing dependency canonical hashing; reuse with sidecar algorithm, explicit synthetic raw/legacy CRLF controls |

Installed `dvc/output.py:408` interprets an explicit `hash: md5` as raw MD5;
an absent `hash` means legacy `md5-dos2unix` stored in the `md5` field. The
existing dependency implements its text/binary decision and chunking. Do not
substitute a CSV-only newline policy or compare a legacy checksum as raw MD5.

## §3 Definitions and conventions in force

Inherit `STANDING.md` sections3/4 and `CLAUDE.md:208` Data Access. The GH1149
Option C issue body is authoritative. Current `.claude/skills/add-wave/SKILL.md`
explicitly defers staged nested refresh to1149; update that text after repair.

The held release is the release successfully acquired, never catalog freshness.
GH1128/1163 completion and primary-source facts remain binding. Existing
recursive leftover scanning is already in release0.16; only membership/path
semantics and flat collection remain unfinished.

Nigeria idiosyncrasy (`Nigeria/_/CONTENTS.org:516`): W5 holds v01,106 files in
nested PP/PH directories; v02 adds `NGA_HouseholdGeovariables_Y5.dta` and
`totcons_final.dta`, revises `sect2b_harvestw5.dta` and
`sect3d_harvestw5.dta`. Preserve PP/PH layout and all scientific rules, including
the amended1035 quantity-aware card rule and WAITING nonfood recall caveat.

Ethiopia idiosyncrasy (`Ethiopia/_/CONTENTS.org:1077`): W5 records v02 and the
old `eth_plotgeovariables_y5.dta` name, while v03 replaces it with
`Pub_ETH_PlotGeovariables_Y5.dta`. A documented contradiction is not silently
adjudicated: CONTENTS says "whose coordinates are **CONFIDENTIAL** anyway";
live1127 comment6282011092 says "The redaction is in a different file" and
identifies the old plot-named sidecar as a duplicate household-geovariables
blob. The existing1127 acquisition owns that note's correction. No coordinate
or raster feature wiring belongs in1149.

## §4 Invariants and assumptions

- Resolve and compare every staged member before any destination write.
  Ambiguous sidecars/incoming destinations raise naming paths. Archive paths
  and symlink destinations cannot escape Data. No old file/sidecar deletion.
- A verified-identical staged member can establish current release membership;
  an unverified existing raw file cannot. Revised matched sidecars are reported
  before replacing raw content; sidecars move only through existing push helper.
- Failed/partial download, comparison, placement or requested push never stamps
  a complete held release. Retain prior SOURCE facts byte-for-byte on failure.
- Only actual resolved release members are pushed; nested files participate,
  leftover earlier-release files do not accidentally become current members.
- All fixtures use synthetic payloads. Full member basenames derive from the
  actual held106Nigeria/68Ethiopia sidecars plus cited additions/removal; this
  does not prove original downloaded archive-relative names or data equality.
- Offline/write guards before imports; private fake countries/tmp/data/config;
  exact root-prepared DVCtmp exceptions only. No real credentials/acquisition.
- Actual549Country/39framework/1362wave fingerprint census before data builds;
  expected0 movement is only a prediction. No exclusion to manufacture0.
- GH1206: numeric frame equality needs explicit EA/nonmissing numeric checks;
  pandas frame check_exact/zero tolerances alone is insufficient.

## §5 Reuse decision

| Quantity/behavior | Decision | Reason |
|-------------------|----------|--------|
| Acquisition/public return and error handling | extend | Keep existing list/report and recoverable IO failure behavior; ambiguity deliberately raises |
| Recursive path resolution and complete staging plan | new inside existing extraction helper | No existing staged recursive placement implementation found |
| File checksum | reuse dependency `fobj_md5` | Preserve raw vs legacy text policy exactly |
| Held-release/push completion | extend existing carrier and gate | Avoid new provenance/download pipeline |
| Real-release member-list controls | new tests | Existing fixtures contain only a/b basenames and miss nested layouts |

## §6 Open questions for the human

None: Option C is settled. Exact original zip-relative directory spellings for
the cited2026-10-19 downloads are not established by held sidecars/issues;
synthetic fixtures will label their chosen archive paths explicitly. No real
acquisition or data validation is claimed.

### Phase3 verification

Implementation commit: f85a871fb (full SHA in the private source receipts).
All source/scientific country config, provenance and sidecars remain unchanged.

- `_extract_all_from_zip` - OK (sections2-5): complete staging/comparison before
  placement, unique recursive case-insensitive matching, archive-relative new
  paths, destination/parent collisions and escapes refused, revised and every
  leftover sidecar reported before placement. Atomic per-file replacement
  protects a hardlinked held blob. This is not whole-release rollback.
- Checksums - OK (sections2/3): reuse installed DVC `fobj_md5` with explicit raw
  or legacy text-aware algorithm. Literal byte comparison, not checksum equality
  alone, verifies an existing raw file without rewriting it.
- `_AcquiredRelease`, `_acquired_data_files`, callers and held-release gate - OK
  (sections2/4): actual written or byte-verified membership drives nested pushes
  and provenance; unverified skips, errors, partial/wrong push receipts or raised
  pushes preserve SOURCE facts. Empty/failed calls clear their completion record.
  A local push-only fallback still cannot certify a downloaded release.
- Confirmation/source-primary facts - OK (sections3/4): existing controls and
  new decline/partial tests pass. No acquisition or source authority decision
  is supplied by these synthetic tests.
- Member fixtures - OK (sections3/4):174 exact held-sidecar names/SHA receipts
  plus cited additions/removal, with synthetic bytes and archive directory names.
  The product reproduces104unchanged/2revised/2new Nigeria and67unchanged/
  1removed/1new Ethiopia. This is not an actual downloaded-release comparison.
- Tests:266 passed,0 failed/error/skipped in the current committed-source
  five-file selection (45 staged acquisition,157 provenance,41 data access,
  17 in-tree source policy,6 runtime-write policy). One expected mocked bad-GPG
  warning remains. Actual-parent controls are separate immutable cohorts:
  minimal nested1FAIL;43 identical new/changed cases41FAIL2PASS; later4 new
  empty/failed completion controls4FAIL. These are not one combined parent run.
- Earlier failures remain: first candidate259cases256PASS3FAIL (one fault-fixture
  construction error, two controls tied to flat/direct-open placement); those
  three corrected controls3PASS, then262PASS, then current266PASS after the
  final two-line failure-cleanup/provenance wording change. No old XML/log was
  overwritten. Earlier tested source bytes reconstructed after that last change
  match the independently saved SHA; original source metadata was not invented.
- Fingerprints: actual549 Country,39 framework and1362 declared-wave keysets
  match parent exactly,0 hash moves,0 data-endpoint writes, including a census
  bound to current committed product source. No hash exclusions were changed.
- Scoped warm audit: two independently copied Nigeria/Ethiopia sample caches
  are current/full-coverage, with a SHA-bound Nigeria internal panel companion
  (no country-hash claim for that companion).12 strict comparisons across the
  saved cross-head and separate-process groups pass, including typed attrs,
  axes/codes/unused levels, masks and explicit EA/nonmissing numeric checks.
  Nine negative controls reject tiny/nextafter, nested Series, mask/dtype/attrs/
  order/unused-level faults. Nigeria49770/Ethiopia25914/Feature75684 rows.
- Audit limits: phase1 sample only (no sample phase2 grid), all5 ordered scanner
  records are the same5 existing WARNs byte-for-byte. Six main warning pairs
  (10 occurrences/head) have literal category/message equality; filenames only
  are qualified by the known checkout prefixes, with line numbers equal.
  The first already-warm cohort predates the final two-line acquisition-only
  change; the separately pinned follow-up uses current product source.
  All warm byte/mode/mtime/inode endpoints remain unchanged. No raw/L1 payloads
  were copied and no country source-cold build or broad output audit occurred.
- API surface audit: strict success with0 data-endpoint writes. Skill guidance
  reflects the implemented contract; no country idiosyncrasy or scientific
  definition changed. New-file retirement and real978/1127 acquisition/content
  review remain separate authorized workflows for the dispatcher.

Author evidence is under the owner-only framework1149-fix private root; final
safe allowlist, checker pins, source protection and immutable release manifest
are provided to the dispatcher. Independent review and human merge remain.
Sue.
