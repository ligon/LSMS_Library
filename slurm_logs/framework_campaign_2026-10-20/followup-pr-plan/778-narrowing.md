Proposed scope clarification for #778; **draft only, not posted**.

The two actionable remaining items are implemented and independently reviewed in PR #1162, pending landing/release validation:

1. The actual remaining forced country controls are Guyana's destructive cold build, India's forced employment build and Togo's forced plot build. These now own physical private L2 copies with only L1 linked. The named GhanaLSS interview/visit and synthetic dfs-merge tests already used private roots and were left unchanged. The measured old three-control stand-in changed 16 file contents and replaced 18 files; the isolated controls leave their outer cache unchanged. Both Guyana and India still execute when that outer L2 is empty.
2. Both assumed-fresh country data reads now treat `FileNotFoundError` as a cache miss and enter the ordinary path. This does not suppress corruption, permission, mapping or genuine source failures. Six final controls fail against the old gate; all relevant candidate controls pass.

The carried item 3 remains an explicit limitation of `assume_cache_fresh`: it can serve a country script's unstamped output at the final path without verifying its hash. PR #1162 does not change that trust contract and does not claim to solve it. Any request to strengthen that behavior belongs in a separately scoped policy task. The separate corruption/`LSMS_NO_CACHE` decision remains #1071; it is not resolved by a missing-file handler.

PR #1030's earlier isolation/raw-reader work remains resolved. The earlier partial-aggregate explanation was not reproduced and no new writer lock or partial-manifest policy is claimed.

Closure choices:

- Keep the present issue wording: use **Addresses #778** and leave it open for the carried note.
- Explicitly narrow the actionable scope to items 1 and 2, retaining item 3 above as a documented trust-mode limitation/policy follow-up: then **Fixes #778** is an accurate intended closure after landing and release bookkeeping. It still does not claim item 3 is fixed.

No new policy implementation is needed for the current PR. Do not silently relabel the trust-mode observation as a fixed defect.

If the owner wants a separate decision issue, draft title: **DECISION: should assume_cache_fresh serve unstamped country-script outputs?** Its evidence is only the carried Nigeria crop_production observation; behavior elsewhere was not established by #778. Ask whether the intentional trust-mode bypass should remain or require another explicit check, with compatibility/performance and genuine failure boundaries measured before implementation. This draft creates no such issue and proposes no default.
