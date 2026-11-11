# PR data classifier refusal concurrency race

Per STANDING.md, preserve the required check, eligibility and exact checkout
policy; this repair changes no library, country, scientific or data behavior.

On head399909b43b46e8e7829a9b66eb520b03bb51f41c, older draft edit run
42896968850 was created20:36:31. Its classifier130112507017 refused the draft
before checkout. Ready run42896972948, created20:36:33, classified successfully.
Its required validation job130112565924 was created20:36:38 and canceled39,
with runner0 and no steps. Its annotation states: "Canceling since a higher
priority waiting request for pr-data-1690-validation exists". The older failed
classifier's always-dependent job130112575567 was created39, started41 and
failed45 at the eligibility guard, skipping checkout/credentials/data. Empty
classifier output fell into the same validation concurrency group, so the
late refused request displaced an eligible validation. No data had started.

The repair assigns only successful known validate/metadata classifications
their existing distinct groups. Refused or uncertain outcomes receive a
run-specific refused namespace and retain the required pr-data-validation
name. Metadata's nonrequired name also requires classification success, so a
partial metadata output cannot hide a failed classifier. The always condition,
failing precheckout guard, exact H/B/merge check, credentials, complete data
recipe, bounded workers/deadlines and cancel-in-progress policy are unchanged.
Valid new H/B requests retain the same validation replacement group.

Runtime source464a397c3c98cc2ef5bcd554d9104a415f766a5a passed15 focused cases
in7.40pytest seconds/14.601wrapper under ordinary tests/conftest and plugin
autoload, MAIN/private roots, thread1/build1, n2/loadfile, unmapped user/network
and retained no-data fences. Controller and both workers asserted exact import
identity; all profiles active with0denials, config/data remained empty, source
clean and bindings exact. Controls evaluate the actual workflow expressions'
finite string/boolean subset, not an independent routing implementation:
late draft refusal, nine refused/uncertain states, valid changed H/B replacement,
successful metadata isolation and existing complete guard/recipe contracts.
This is not a hosted Actions engine simulation or a fresh full test suite.

Primary source/job facts, original classifier/guard logs, annotation, patch,
JUnit, runtime receipts/identities and report are retained at
/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261103/reports/932-classifier-refusal-concurrency-race-20261110/.
No old canceled validation was rerun, no PR state was toggled and no native,
provider, source-data, collector or passing data campaign was replayed. The new
head's ordinary required workflow must establish its own full validation;
prior canceled/failed checks remain recorded and are not passing evidence.
