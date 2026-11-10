# Burkina Faso queue refill — 2026-11-09 v2

Base: `4381d701ffb06ab447a37c66362119d1c1e23009`.
Checkout: `/local/job41069444/mirrors/dispatch_be57bcc1f7f74b92b83d9901f60cae14`.
Search tier: git + ripgrep; no GitNexus call is available. Inherits
[STANDING.md](STANDING.md), especially §§3–4; AGENTS.md and CLAUDE.md are
byte-identical at this base. The dispatch authorizes bounded autonomous work
and reserves scientific decisions and GitHub publication to ROOT.

## §1 Task, restated

Reconcile Burkina Faso's own portions of #1483, #1565, #1437 and #1148
against current issue bodies/comments, committed source and existing evidence.
Deliver narrow source-backed corrections or documentary findings without
running a country table, accessing native data or choosing new scientific
defaults. Preserve component, wave, passage, unit and eligibility boundaries.

## §2 Existing machinery

| Source | Existing machinery / evidence | Reuse |
|---|---|---|
| `countries/Burkina_Faso/_/CONTENTS.org` | Personally read all 3,385 lines, including TODO/WAITING and all dated corrections, before task changes | Country-specific authority/history |
| `Burkina_Faso/2018-19/_/plot_inputs.py` | Existing household/input builder; used, gift and purchase quantities retain separate units; uniqueness guard | Existing output semantics; source-read tests `test_burkina_faso_plot_inputs_identity.py` and `test_gh1188_burkina_input_acquisition_fields.py`; neither run here |
| `Burkina_Faso/_/data_scheme.yml:253` | Existing `plot_inputs` registry/grain note; says "no parcel-level input data" | Correct only a demonstrated documentary overclaim; do not introduce a plot allocation |
| `Burkina_Faso/2021-22/_/data_info.yml:70` | Panel and CS roster source declarations | Inspect source only; no eligibility change |
| `.coder/ledger/1148-burkina-interview-status.md` | Frozen passage-status diagnosis and separate unresolved household-mask policy | Reuse; do not recompute status census |
| `.coder/ledger/1437-harvest-routing-documentary-check.md` | Completed own-2021 printed-route checks, no deployed-CAPI authority | Reuse; no repeat document cohort |
| `.coder/ledger/851-period-api-current-port.md` | Current port of approved declared-exposure conversion | Reconcile current code before describing #1148 dependency |

Full-read binding: `CONTENTS.org` SHA256
`9fef29cf986d4918a21f49b9abbe25c4a4e101b6c7b5a0647ebd626899ec73b4`,
226,693 bytes, 3,385 newline-terminated lines, git source SHA as above.

## §3 Definitions and conventions in force

See STANDING.md §3, canonical `lsms_library/data_info.yml`, and country notes.
Relevant idiosyncrasies: 2014 EMC passage comes from each food filename;
EHCVM `vague` is a sample split, not a repeated food ask. The 2018 household
input roster has no parcel column; this does not establish that other modules
ask no parcel-input question. 2021 sample/roster include panel and CS, while
many other modules still cover panel only. No component coverage is promoted.

Tensions to retain and reconcile: the older #1148 country note says
"`period`, `RecallRecord.exposure_days(column)` and `recall.to_period` are
absent", whereas the final dated #851 entry says the record now supports
"explicit derived-table period conversion". Historical counts are not
rerun. The #1437 printed finished-harvest route is already confirmed; no
released blank's cause follows from paper routing alone.

## §4 Invariants and assumptions

Inherit STANDING.md §4, with the dispatch's stricter boundary: no native
parser/header/rows, Country/Feature/getter/provider/Make producer, raw
acquisition, cache clear/write/rewarm, Feature audit or corpus sweep.
No shared source/framework/index/schema/registry/map changes. No source
publication except the dispatch's private result repository. No subagents.
The original country-document prefix must survive byte-for-byte if appended.
No new maps, weights, strata, eligibility, unit factors, recall windows or
scientific defaults. Actual pure tests require mounted image, exact import
identity, private roots/guards and closed logs; unrun gates stay unrun.

## §5 Reuse decision

| Item | Decision | Reason |
|---|---|---|
| Fertilizer inputs | reuse existing builder/grain; investigate documentation | New wave routing and parcel representation need source/decision gates |
| Leavers | reuse current roster/finalizer behavior as inspected | A new reason column can change all-NA eligibility; no policy supplied |
| Harvest blanks | reuse completed own-document finding | Deployed CAPI/export evidence and Burkina total decision remain absent |
| Household recall exposure | reuse #851 port and frozen #1148 diagnosis | Declared exposure and household completion are different contracts |
| Source/evidence checks | new bounded task receipt | Required by dispatch; no equivalent existing task-specific receipt |

## §6 Open questions for ROOT

All four issue bodies and all current comments were read through the primary
GitHub connector: #1483 has no comments, #1565 no comments, #1437 one comment
(Burkina waits for a 2021 crop builder), and #1148 no comments. Exact holds
and finite next-operand plans were sent through the absolute dispatch channel.
No response is inferred from elapsed time. Full issues remain open:

- #1483: 2021 household input-slot/unit authority and new wave verification;
  parcel representation and NPK/DAP grade decisions remain separate.
- #1565: choose the destination for departure reasons and explicit treatment
  of all-null roster rows before adding non-null payload to those rows.
- #1437: authenticated deployed country/passage CAPI/export version remains
  absent; Burkina total derivation and crop-wave integration remain held.
- #1148: #851 integration is already delivered, but completion semantics,
  carrier precedence, passage-4 code binding and unknown-household mask remain
  undecided. Do not use food rows as interview evidence.

The required image mount under private roots/false credentials failed with
exit 1 (`fusermount3: mount failed: Operation not permitted`). The source-only
`make api-audit` attempt failed with exit 2 because Poetry was absent (recipe
error 127). Source/static frontmatter enumeration was performed separately.
No substitute venv, installation, library import or runtime test was attempted.

## Phase 3 — verification

Grounding commit `0e399586c` preceded source edits. Product documentation
commit `d32592828` changes only country registry comments and appends 78
country-note lines. No executable, map, eligibility or data declaration changed.

- OK (§§2–5): removed the whole-instrument absence claim from the household
  input-grain comment; own public catalog 4290 section 16A has separate
  fertilizer quantity/unit pairs. New DAP catch-all detail remains a hold.
- OK (§§3–4): own public 6224 departure-reason catalog includes 14 categories
  and two death categories. Catalog counts are explicitly distinct from held
  row identity and current source/runtime measurements; no membership change.
- OK (§§2–4): #1671 merge ancestry and current source establish completion of
  #1148's #851 integration prerequisite only. Old WAITING history is preserved.
- OK (§4): stdlib source-byte/history checks exited 0: original 226,693-byte
  country prefix exact, registry non-comment bytes exact, 851/1437 merges are
  base ancestors, only the three owned paths differ, private config/data/cache
  roots empty. `git diff --check` exited 0. These are source checks, not pytest.
- UNRUN: pure metadata/unit tests, Country/Feature, native headers/rows,
  Make producers, CI, hash census and cache acceptance. The mount/setup
  failures above remain failures, with closed logs in the private receipt.

Direct public HTML page identities/hashes and the differing cached web-tool
version headings are preserved in the private evidence index. No claim of held
release equivalence follows. Final artifact names: REPORT.md, RESULT.json,
SOURCE-PROOF.json, DECISIONS.md and EVIDENCE-INDEX.json in the dispatch evidence
directory. ROOT owns acceptance, scientific rulings and all GitHub publication.
