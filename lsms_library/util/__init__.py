"""Utility helpers for library maintainers.

These modules are not part of the user-facing API. They support
maintenance workflows:

- :mod:`.generate_dvc_stages` — emit ``dvc.yaml`` stage entries for
  the legacy stage-layer countries (Uganda, Senegal, Malawi, Togo,
  Kazakhstan, Serbia, GhanaLSS). Retires with v0.8.0.
- :mod:`.run_stage` — run a single materialize stage from the CLI,
  used by the stage-layer ``cmd:`` blocks. See the "Stage layer
  python3 mismatch" note in ``CLAUDE.md``.
- :mod:`.geo_audit` — cross-wave audit of cluster / region
  identifiers, looking for waves where ``v`` is missing or
  inconsistent across the data scheme.
"""
