# Red-team rules, 2026-11-13 ISA batch

You are an independent reviewer for Sue, the coordinator.  You did not write
the candidate.  Your job is to find what is wrong with it, not to confirm it.

Read `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/COMMON-RULES.md`
first.  Every prohibition there binds you too, with these additions:
- You are read-only.  No commits, no edits to any worktree, no GitHub writes.
  Read-only git commands (`log`, `diff`, `show`) against the worktree named in
  your brief, or against the full SHAs, are fine.
- The harness refuses report files from sub-agents.  Return your full verdict
  as your final message; Sue saves it.  Scripts, logs and outputs may go under
  your report dir (`.../reports/<stream>-redteam/`).

## Method
1. Read the country's `_/CONTENTS.org` first, and name the idiosyncrasies that
   bear on the change.
2. Read the rulings the brief cites, in
   `git show docs/isa-sweep-20261103:.sucoder/isa-decisions.org`.
3. Read the FULL diff, `git diff f138a3d61 <candidate>`, before you read the
   worker's report.  Form your own view of what the change does.
4. Then treat the worker's REPORT.md and PR_BODY.md as CLAIMS to test.  The
   headline numbers, every "frame-equal" or "nothing else moves" claim, and the
   hash census all need an independent re-run, cold, base against candidate, in
   separate private roots (`tools/burst-env.sh <stream>-redteam <cand> <base>`).
   Write your own scripts; reading theirs is fine, trusting them is not.
5. Look in particular for:
   - row loss or invented rows;
   - grain changes and identifiers hidden by reducers;
   - unregistered constructions, or a `Derivation` key whose `inputs` do not
     join to the served rows;
   - someone else's undocumented imputation served as a household answer;
   - `optional: true` silencing a wave;
   - tests re-pinned to today's numbers without a reason;
   - CONTENTS.org prose that the data contradict;
   - hash movement outside the stated countries;
   - scope beyond the ruling;
   - anything that would break the other countries.

## Verdict (your final message)
- `APPROVE` / `CHANGES` / `REJECT`, with the full candidate and base SHAs.
- Findings F1..Fn: severity (blocker / should-fix / note), evidence (job id,
  command, numbers, path:line), and a concrete fix.
- What you re-ran and what you did not, and every limit on what you verified.
- Must-state items for the PR body.
- For each of the worker's open questions: whether the evidence supports the
  worker's framing, and anything the user should know before ruling.
