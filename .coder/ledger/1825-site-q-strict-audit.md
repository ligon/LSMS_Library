# Prior-art ledger — #1825 strict Site Q audit output

## Task and baseline

On development d18b47de60e49b2369ec85a212ba494381b4a4e9 an invented applied-quantity outlier on a Niger-shaped frame reaches all three audit phases. The direct Country-shaped build failure includes the row diagnostic. Actual Feature catches the same strict exception and includes its detail in a wrapper warning. The records and cluster examples contain both invented row-key markers. #1767 and #1773 compose cleanly and show the same strict residual for purchase quantity, although their normal warning is safe.

## Reuse and contract

- `quantity_audit._emit_quantity_report` is the single existing choice between detailed Site Q warnings and strict errors. The public report ledger and direct local diagnostics retain their existing purpose. The emitter is serve-time, and the relevant check is excluded from build-transform fingerprints. Use STANDING.md and the Site Q section of AGENTS.md for that distinction.
- `bench/feature_audit/scan.py::_build` wraps the three actual build routes. The app-facing `Feature` method intentionally degrades a per-country exception into a warning. Do not infer that wrapping warning from string matching or change Feature behavior.
- #1767 classifies the typed warnings and withholds local details from their findings. It does not handle strict failures, even on a composition with #1773's purchase screen.
- Scope the fixed message to strict Site Q diagnostics during the audit build invocation. Retain failure visibility, public local strict text, report ledger, typing, threshold, ordinary warning taxonomy and all output data. No global environment change, warning filters beyond the existing wrapper, new acquisition, or product data edits.

## Verification plan

Require a real emitter and Feature wrapper on invented data in all three phases. Assert that recorded and clustered detail withholds row keys and magnitude while loading outcomes remain recognizable. Confirm ordinary detailed diagnostics after normal and exceptional unwind, tolerating nested audit calls, and preserve unrelated exception messages. Compare the exact composed pending warning-taxonomy and purchase changes as well as the live-base route. Measure fingerprints rather than assuming no churn.
