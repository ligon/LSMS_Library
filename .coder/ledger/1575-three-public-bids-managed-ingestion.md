# Prior-Art Ledger — GH1575 three public BID managed holdings

**Phase1 / source-only proposal, 2026-11-08.** Initial plan base `e97a469262393d06563c8bf90c9ff465388c0173`; executable proposal base `585021853847a2ccfeb0d7f48a6daa11f2cce0e4`; new detached proposal tree. This Phase1/2 text records the pre-execution freeze; any later actual custody/readback result is separately sealed before Phase3 finalization. Inherits `STANDING.md` / `CLAUDE.md`; the earlier1575 document investigation and four-country documentary candidate remain immutable.

## §1 Scope and existing decision

The user authorized adding missing source documents. Admit only the three already-fetched public original2018 BIDs for Togo, Guinea-Bissau and CotedIvoire through the existing normal managed holding path; no refetch or parse is needed. ROOT reviews the concrete finite transaction/operator, not a new user scientific decision. No sample, strata, weights, defaults or production code change follows. The two new private GB2021 FR/EN PDFs already have matching committed sidecars and are excluded.

## §2 Existing machinery / reuse

| Existing symbol or evidence | Reuse / limit |
|---|---|
| `data_access.push_to_cache` | One exact documentation path per transaction; normal managed add/credentialpath/push, no hand-run DVC or manually manufactured sidecar. |
| `_s3_writer_credentialpath`, `_run_dvc_with_lock_retry` | Preserve existing credential restoration and known-lock-only retries; no new retry policy or exported secrets. |
| `get_data_file` | Fresh separate reader tree containing generated sidecar but no PDF; empty private L1 and reader-only profile; ordinary readback must match original binary hashes/size. No WB fallback. |
| Accepted1574 V2 `DocGuard`, metadata bootstrap, phase/operator | Existing narrowly bound own-repository btime marker and magic/ldconfig metadata path; normal managed DVC child boundary is qualified, not an OS sandbox claim. |
| Closed1575 fetch/parse/root-reviewed receipts | Source URL, actual response filename, date, PDF binary identities and prior text findings; no new PDF read is performed for this plan. |

The five critical library/config source objects are byte-identical to the accepted1574 proposal source. Exact receipts and source bindings are in the external plan packet. Do not copy1574's known finalization bug: it incorrectly required an authorized generated `.gitignore` to retain its old hash after push and successful readback. New postconditions must enumerate sidecar/ignore/SOURCE/CONTENTS changes explicitly and preserve all other objects.

## §3 Country facts and contradictions

Personal full current CONTENTS reads, including LOGBOOK/WAITING/TODO, are authenticated byte-identical at this base. Togo's2018 folder is named `2018` and its native data live in Data1. Its earlier “Lomé Commune” rural prose conflicts with the later BID table's GrandLomé allocation; no geographic bridge or correction is inferred. Guinea-Bissau's SAB is urban-only and region labels are Portuguese; the older vague caveat and later community-prices ruling remain visible. CotedIvoire has uppercase CIV/nested Menage source names and older region×milieu design prose versus later region-only wiring accounts; the separate CILSS design is not this EHCVM holding. Holding these PDFs resolves document custody only.

## §4 Invariants / intended finite transaction

- Destinations use the actual official HTTP attachment filenames under each existing wave's `Documentation/`. No new wave, source catalog, acquisition layout or registration is created.
- Inputs are the immutable private originals fetched once for1575. Revalidate regular-file ownership/mode/stable-FD/PDFmagic/size/MD5/SHA before any future copy; original bytes remain unchanged. No download, PDF parser, renderer or source table is needed.
- Each document has one normal managed writer and one fresh reader-only readback in distinct processes/private roots. Exactly three content-addressed document objects are in scope, plus the three generated sidecars/ignore lines and append-only source/availability notes. The operation is not atomic across objects: retain partial outcomes and stop on first failed transaction.
- Profile copies, if admitted by ROOT's executable review, are private0600, source profiles unchanged/unprinted, empty WB config/no auto-unlock; remove writer authority before readback and remove all ephemeral profiles/config.local in finally. No manual native/auth state or credential value extraction.
- No shared L1/L2 write, raw/harmonized source, Country/Feature/Make, scientific derivation, test suite or rewarm. Only private own DVC state/cache/tmp/index and explicitly enumerated metadata files may change. Tracked DVC config stays exact.
- Source and CONTENTS availability text is appended only after that document's successful remote readback. Existing historical claims and all substantive1575 design caveats are retained. Metadata-only hash impact is not claimed measured; no code/config build input is intended to change.

## §5 Proposal status

Plan/identity/source-grounding packet: `B/reports/1575-three-bids-managed-ingestion-plan-20261108/`. This ledger remains a phase1/2 record; actual outcomes must be attached and reviewed for Phase3. It creates no standalone holding or publication claim. Concrete executable reuse/diff, fixed inputs, allowed write paths, profile locations, manifest and no-source checks are next for ROOT review before the finite managed operation. No GitHub or remote publication ran. ROOT owns final integration/review/publication.
