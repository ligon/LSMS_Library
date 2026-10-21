# Independent complete release-note review

Sue reviewed the frozen notes and inventory against v0.15.0 commit
`cb44e3522a2426ba03431ca9a218cc63fe03f6c9` and development
`acbd8eb34710e85d3bf66eb04f13dd0d971d1f3c`.

**Acceptable for publication with the two API clarifications now verified in the parent's copied documents. No remaining material blocker found.**

The frozen producer drafts remain unchanged. Review identified two corrections:

1. `MIGRATION_NOTE.md:81` used `Feature(...).attrs`, but attrs belongs to the returned DataFrame. The publication copy now explicitly says the frame returned by Feature carries the metadata.
2. The canonical index table omitted the inherited default monetary `currency` axis. The publication copy now labels its base indexes as `currency=None` with no numeraire, and explains the unchanged default currency/numeraire axis; the community-price release paragraph is similarly qualified. This matches `feature.py:940,1017-1023` and `currency.py:248-305` at acbd.

Scope and evidence checks:

- Independently checked all 200 supplied metadata candidates against both commit ancestries: exactly the 50 inventory records belong after the actual v0.15 tag and through acbd. Every recorded body SHA matches the preserved metadata. All 47 behavior/validation entries are linked; the three evidence-only omissions (#1016, #1158, #1166) are reasonable. Git's ordinary merge subjects supply 49 entries; #1153 is the additional two-parent merge with the nonstandard `Merge PR` subject, not an omitted change.
- Static acbd source checks support the index/sentinel, country-preserving reduction/rate, unavailable-unit, mapping-error, sampling, reader and dependency-floor contracts. Country-specific reported counts are explicitly attributed to their individual PR comparisons, not invented as a whole-release counterfactual. Native shape and science limitations remain qualified.
- There are zero differences from validated 532 to acbd under `lsms_library`, `tests`, `pyproject.toml`, `poetry.lock`, `Makefile`, `scripts`, `bin` and `.github/workflows`. The implementation identity claim is therefore supported by independently inspected Git objects.
- The local 8,851-pass and remote 6,388-pass cohorts, their skips/xfails/non-strict XPASS, exact-source pin and installed-config-only packaging scope agree with the frozen reports. No new published artifact, full installed microdata, fresh dependency-resolution or universally adjudicated skip claim is made.
- The notes correctly bound the full paired cold corpus/16-feature audits to 976-to-852, separate later scoped checks and 532 suites, and retain the 19 source/wiring gaps, five tenure findings, coarse RHS exclusion, Malawi parcel/docstring contradiction, #1149 staging and #932 enforcement limits. They do not claim tag-to-tag full-corpus equivalence or universal scientific approval.

No project tests, package imports, Country/Feature data calls, source edits, Git/GitHub mutations or network requests were performed for this review. Only this private review note was written. Release owner still owns the final master gate, tag, publication and published-artifact verification.

Sue

Frozen release draft SHA256: `81524af49cf76f6d89a9d2b6756283dc5158f76fa68a16bd215ea0dd599b1d12`.

Frozen migration draft SHA256: `1ae2c56026e6ef593df79d00b297d7fa3baf90d29493661f9c5fb94b045aaf51`.

Frozen inventory SHA256: `19b7b27bf0a9b31a46badb4e9643120f44b661819912d34092eb5405eb365404`.

Corrected publication release notes SHA256: `2d32325566523f7366de247e98e9e33160f85d30312525891a46680df5bfaaae`.

Corrected publication migration SHA256: `196923a6075b54b9a1aab41cfb12842b36451290d4065b864957f0eb3b4842ba`.
