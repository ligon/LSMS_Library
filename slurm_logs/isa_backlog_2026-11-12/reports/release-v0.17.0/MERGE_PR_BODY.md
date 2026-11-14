<!-- DRAFT body for the development -> master merge PR "Release v0.17.0".
     Before opening:
       1. land the 2026-11-12 batch, then re-run the closing audit (CHECKLIST.md, step 2);
       2. replace <SHA> and fill in the "Closes" block;
       3. keep ONE keyword per issue, one issue per line;
       4. write no other "close"/"fix"/"resolve" + issue-reference text anywhere in this body.
          GitHub does not parse negation: "does not close" followed by an issue reference
          CLOSES that issue.
     Delete this comment before submitting. -->

Release v0.17.0: merge `development` (<SHA>) into `master`.

This is the first release since v0.16.0 (2026-10-21, `097283459f80016b0f00a51b814a0cb964a42789`).
It carries 355 merged PRs, verified by ancestry, plus the six-branch 2026-11-12 integration batch.
The batch covers Malawi 2024-25; EHCVM-2 2021-22 for Togo, Benin and Côte d'Ivoire; Nigeria
post-planting `community_prices`; kg-factor evidence; sold kilograms; and served-wave
corrections.

Highlights:
- **Served numbers move in many countries.** The largest moves:
  - single-pass `id_walk`;
  - `'Unknown'` for a missing unit, cold and warm alike;
  - EHCVM sized units, with pooling of unsized labels;
  - released EHCVM unit weights for food kilograms;
  - rebuilt panel crosswalks for Nigeria, Ethiopia and Burkina Faso;
  - EHCVM region × milieu strata;
  - `Farm-level` crop sales;
  - households with no market label kept under `market=`.
- **A failure no longer reads as absence.** Some calls that used to serve 0 rows now raise the
  cause.
- **New opt-in API:** `period=` on food quantities and expenditures, `pool_waves=` on
  `median_price_valuation` (whose default is now within-wave), and `area_t=` on `yield_kg`.
- **Expect a corpus-wide cold rebuild of the harmonized caches on first use.**
  `LSMS_CACHE_SCHEMA` is unchanged; the rebuild comes from build-fingerprint movement. L1
  source blobs are reused.

Full notes: [`docs/releases/v0.17.0.md`](docs/releases/v0.17.0.md), drafted from the
release-preparation `RELEASE_NOTES.md`. The closing audit is in `CLOSES.md`.

Issue closing: GitHub acts only on the lines below.
- 191 issues resolved by this work were already closed by hand on development and need no
  keyword. They are listed in the release notes.
- As of `e5fdd2fa8`, no open issue was fully resolved. The 57 touched open issues all state
  remaining work.
- The lines below are therefore limited to issues the 2026-11-12 batch resolves, confirmed after
  it lands.

<!-- CLOSES BLOCK: one line per issue, "Closes #N". Empty at e5fdd2fa8. Candidates to verify
     after the batch, NOT to paste blindly: 1275 1604 1697 552 587 1509 1068 1129 1560 1571
     1707 1708 1536 1519 (numbers written without "#" on purpose). -->

This release uses a merge commit to preserve development ancestry (never "Squash and merge").

Sue

🤖 Generated with [Claude Code](https://claude.com/claude-code)
