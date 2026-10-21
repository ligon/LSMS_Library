# Release-note scope and reproduction

Release owner copies `RELEASE_NOTES.md` to `docs/releases/v0.16.0.md` and
`MIGRATION_NOTE.md` to `docs/migration/v0.16.0.md`.
The release notes' relative migration link and its backlink then resolve.
The notes describe the complete change set from actual v0.15.0 ancestry to
reviewed development `acbd8eb34710e85d3bf66eb04f13dd0d971d1f3c`, not just the
recent nine-issue increment.

Input is the parent's fresh complete 200-entry merged-PR export:
`/local/job40255479/control/release-v016/merged-prs.json`.
It includes full bodies, merge SHAs, merge dates and PR URLs.
The metadata SHA256 is in `PR_INVENTORY.json`.

Read-only ancestry verification and topic/omission checks:

```sh
# From a clone containing the reviewed git objects. No checkout is changed.
git rev-parse 'v0.15.0^{}'
git show --no-patch --format=fuller v0.15.0
/local/job40255479/venv_img/bin/python \
 /local/job40255479/control/release-v016/notes/inventory.py \
 --metadata /local/job40255479/control/release-v016/merged-prs.json \
 --notes-dir /local/job40255479/control/release-v016/notes
```

The script checks each of the 200 merge commits with `git merge-base
--is-ancestor` against both the actual v0.15 commit and the supplied development
frontier. Exactly 50 are in development and absent from v0.15. All 50 map to a
release section or an explicit documentation-only omission. The other 150
metadata entries are excluded with recorded ancestry reasons. It also verifies
that source-pinned evidence links name files present at development.

This is an inventory of changes, not an independent issue-closure ruling.
The previously completed closure audit remains the authority for its32
reviewed issue dispositions; related partial issues are kept explicit here.
No runtime code, tests, Country/Feature API, acquisition, data cache or network
was executed for note preparation. Python was used only for metadata/text and
git-object processing. No source/repository/GitHub mutation was performed.

Evidence sources read:

- `docs/releases/v0.15.0.md` for style and the prior release boundary.
- `.claude/skills/release/SKILL.md` for release/version/closure boundaries.
- `slurm_logs/framework_campaign_2026-10-20/release-drafts/` for the approved
  recent increment, not a complete history.
- Full bodies of the post-tag merged PRs and the frozen current reader/date
  contract comments to reconcile early PR residuals with later repairs.
- Final campaign status, packaging/corpus report paths, the merged landing
  evidence and the independently completed closure/migration reviews.

The notes deliberately do not assert that the tag, upload or installed PyPI
artifacts already exist. The release owner adds actual release-commit and
published-artifact results after verification. All producer/reviewer/source
writers remain outside this notes-only task; the notes writer stops after
handoff.

Validation cohorts are intentionally separate: matched549-request/16-feature
audits compare976-to852, with older post-v0.15 merges already present at976.
Final local8851/manual6388 tests cover532; later source-byte identity binds
that implementation to landed developmentacbd. No whole-tag-to-tag cold
row-conservation measurement is claimed.
