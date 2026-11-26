# #1288 - Declared input drift during default country publication

## Scope

Follow #1797's country dependency graph. Do not change the wave hash
rules, producer routing, Make receipts, or numeric calculations. The
tagged orchestrator does invalidate existing wave/country hashes once. Only prevent
the default parent writers from certifying output with a newer hash
than the inputs it started with. Existing explicit Make already checks
its transaction. Keep the separate #1790 no-op/coverage policy.

A controlled sidecar switch immediately after reading Uganda income's
first input leaves old income values under the new input hash in both
default writers, with or without a Repo. A second reader serves the old
values without running the builder. A real output was written, so an
unchanged-target check is not a fix.

## Plan

After the actual default build wave union is chosen, snapshot the opted-in
hash before touching the producer, then use the snapshot at each parent
country write. Refuse publication if the current fingerprint no longer
matches. Use the original fingerprint even after comparison, since input
change may occur before a writer finishes. Ordinary undeclared tables
retain the same late hashing behavior.

The legacy script can have written a hashless artifact before the parent
runs. Refusal does not delete or attempt to restore it. The ordinary
country gate will not serve it as fresh. This is not atomic default
publication, an ABA proof, a change in direct legacy CLI Make or a new
policy on dvc.yml stages that own their own output. Measure all conservative
framework-hash movement for review; do not invalidate shared caches.

## Acceptance to date

The pinned parent publishes old values at the post-change input hash in
three independent fixture executions: absent Repo, Repo with no
materialize stage, and actual local DVC two-stage consolidation. The
parent's guarded warm calls reuse the old stamp unchanged. First staging
attempts used an unavailable Python command and later an in-config output;
they are preserved as fixture failures. The final staged witness uses a
pinned executable and a producer target under the separate managed root.

The candidate refuses each post-drift stamp and retries to the updated
values with full coverage. For the two legacy script cases only the
hashless first artifact remains, and for consolidation no selected
country artifact survives. The existing explicit Make control refuses
to promote its selected output with no code change. 21 source-free tests
passed, plus the separately rerun portable stage target. The exact
composition with #1790 and #1796 passes 80 tests (two deselected).

The 38-country census found all 2,849 nonempty entries move, 556 country
and 2,293 wave. Thirty unavailable entries remain unavailable. This is
tagged orchestrator hash movement, not a claimed value change or shared
rebuild. The draft is stacked on #1797. Release sequencing, hosted
development-base checks after retarget, and genuinely atomic default
publication remain separate.
