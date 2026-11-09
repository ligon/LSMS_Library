# Prior-Art Ledger — GH #1135 GhanaLSS measurement config

Search tier: own Git + ripgrep + source text / stdlib AST only. Inherits
[STANDING.md](STANDING.md) §§3–4, [CLAUDE.md](../../CLAUDE.md), and the
canonical `lsms_library/data_info.yml`; none is changed.

## §1 Task, restated

Port one historical `panel.cluster_id_measurement` record into
`lsms_library/countries/GhanaLSS/_/sampling.yml`, at the existing 1988-89
boundary. Exact receiving base: `79f5979cf8022e64928c5150950dfb00456994da`.
Only this ledger and that config are owned. This is source-only curation,
not a new measurement or a runtime/scientific acceptance of GH #1135.

## §2 Existing machinery

| Source | Existing responsibility | Reuse / verification |
|---|---|---|
| `GhanaLSS/_/sampling.yml:17–33` | Rotating-panel, wave-scoped boundary; 712 linked households and 79 bijective cluster pairs in `cluster_id_note` | Reuse committed historical facts; preserve every existing byte |
| `GhanaLSS/_/CONTENTS.org:389–418` | Cluster numbering; dated 2026-10-19 measurement and lower-bound caveat | Read all 5,856 lines before config editing, including TODO vocabulary and all dated notes; no separate LOGBOOK/WAITING body |
| `GhanaLSS/_/CONTENTS.org:1192–1211` | PANELC person-to-household linkage: 714 households; rotation by design | Keep the distinct historical linkage population; do not substitute it for the measurement denominator |
| `lsms_library/sampling.py:201–205` | Base panel field allow-list lacks `cluster_id_measurement` | Deliberate interim mismatch; coordinator owns the shared schema/tests |
| Frozen `APPROVED-SCHEMA.md:1–9` | Approved count names, provenance, omissions and country corrections | Reuse verbatim contract; Nigeria/Tanzania corrections binding but outside this country's edit |
| Frozen `reviewed-sampling.py.txt:209–260,717–828` | Count meanings, allowed fields, coherence and provenance | Read-only source, never imported/executed; names `tests/test_sampling.py`, but no tests run here |
| Frozen `COUNTRY-INPUT.json` | One assigned old record, reviewed source `f1cf7d06f30367cd1e34a4b3db4012f951b2e497` | Reuse literals 712 / 712 / 79 and derived-from-data / high provenance; dated own locators |

Frozen assets live under `$ISA_CONTEXT_ROOT`; their exact paths and SHA256
bindings are in the dispatch evidence `source-read-receipt.json`. Repo skill
frontmatter was enumerated as text; `make api-audit` is prohibited by this
task. Read the supplied shared prior-art-ledger skill, repo STANDING/README/
template, add-feature/sample (and parent), and cross-country-features skills.
Runtime recipes and skill review pauses are superseded by the explicit
headless, source-only task authorization.

## §3 Definitions & conventions in force

Per STANDING.md §3 and CLAUDE.md “sample() and Cluster Identity”, `v` belongs
to sample and represents a sampling-cluster ID. The frozen reviewed source
defines N as linked households with `v` on both sides, D as changed IDs
including all causes, and K as clean one-to-one renumbered earlier IDs among
linked households, not a count of physical retained PSUs.

Relevant idiosyncrasy: GLSS2 retained half of GLSS1's workloads but renumbered
their EAs from 1xxx to 2xxx. CONTENTS.org:404–410 records: “712 households link
GLSS1 to GLSS2 and *none* keeps its =v=” and “The 79 is a *lower bound* on the
retained workloads”. Preserve the workload/EA distinction; never infer ~88
or the survey's design PSU counts as K.

Quoted historical tension: CONTENTS.org:1198–1199 says “Deduplicating to
household level yields 714 panel households.” Its 2026-10-19 cluster section
instead records 712 sample-linked households. The frozen definition uses
the with-`v` sample population, and the current boundary note already uses
712. No new data inspection adjudicates the difference here. No authority
blocks copying the dated 712 measurement; no document is assumed stale.

## §4 Invariants & assumptions

Inherit STANDING.md §4. No sample/v/weight/default/science/mover-policy edit.
The only config addition is `panel.cluster_id_measurement` under 1988-89;
all existing notes, design/oversample/EA/source fields and locators must stay
byte-exact. CONTENTS.org is unowned and remains unchanged. No unrecorded
country quirk was established by this source-only read.

Counts are plain-decimal nonnegative integers: N=712, D=712, K=79. Unknown
net-of-movers count and its `mover_rule` are omitted, not zero or
`not_applicable`. No assigned quantity is structurally inapplicable, so no
`not_applicable` list is added. Source/confidence/locator belong to the new
measurement itself. Preserve historical 2026-10-19 qualification, not a
current sample-accuracy claim.

No package import, sampling parser execution, pytest/collection, Country,
Feature, getter/build, header/DTA/capture/API/census, Make/DVC, network,
acquisition/provider, cache/rewarm or weights work is authorized. Prepared
PROJECT_PYTHON is used only for stdlib text/AST/Git proofs. Existing setup
identity is read from the supplied project-runtime.json; no new bootstrap
import is needed.

## §5 Reuse decision

| Quantity / behavior | Decision | Reason |
|---|---|---|
| 712 linked, 712 changed, 79 renumbered | Reuse | Existing own config/docs and assigned old record agree as historical evidence |
| Net of movers | Reuse unknown | No settled count; never manufacture a rule or a zero |
| New nested metadata | Extend config only | Frozen optional schema; shared implementation belongs to coordinator |
| Verification | Reuse own Git + text/AST | Removing only the inserted block must reproduce the base file; all other tracked mode/type/blob identities must remain exact |

## §6 Open questions for coordinator

None blocking this source-only port. Shared-schema integration, runtime
verification, current measurements and full issue closure remain coordinator
gates. Use the dispatch's absolute mutable channels for any later questions;
no human approval prompt or external messages.

### Phase 3 — verification

- OK (§2/§5): the single 1988-89 record copies all three assigned literals,
  712 / 712 / 79, with `derived-from-data` / `high` and dated own-file
  locators. Config-only commit:
  `30ae409dfe51be10c632748fc85893e60eae18d7`; initial ledger-only commit:
  `a8a90346af7e5c95bbe06be7ee3eda31b72635ba`.
- OK (§3): the note explicitly says historical 2026-10-19, ported without
  remeasurement. The 79 remains a linked-ID lower bound, not a design count.
  The 714 PANELC statement is preserved; no cause of the 714/712 difference
  is inferred, and no new authority contradicting the assigned field was
  established.
- OK (§4): unknown net-of-movers count, mover rule and `not_applicable` are
  absent. Plain-decimal spelling and static K <= D <= N coherence pass;
  field names and provenance values match the frozen source's AST constants.
  The frozen source is never imported or executed.
- OK (§4/§5): removing only the new 15-line block (config lines 30–44)
  reproduces the original 4,746 bytes exactly, SHA256
  `c12793f8640372b80a42c561fb15b5b3161852c5eb8be6cf8bf5e1db4ee33f92`.
  Every current nonmeasurement byte is preserved. All 19,247 nonowned
  recursive tracked entries have the base's identical mode/type/object ID;
  CONTENTS.org and shared code/tests/other countries stay exact.
- OK (§4): `git diff --check` passes. Source-only proof and reviewable
  nonowned-object manifest are in the dispatch evidence directory, alongside
  the five canonical reports and the stdlib `verify-source.py` recipe.
  Evidence is external to the tracked tree.
- UNRUN (§4/§6): package/parser/tests/data/API/build/cache/network gates.
  Base schema rejection of this new field is deliberate coordinator staging;
  this verifies a source-only metadata port, not runtime behavior, current
  scientific accuracy or full issue closure. Private publication is recorded
  separately in the dispatch report after the final ledger-only commit.
