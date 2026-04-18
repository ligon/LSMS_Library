# Broad-except Anti-pattern Audit

**Date**: 2026-04-18
**Author**: Catalog compiled from PR #183 discussion
**Scope**: `lsms_library/` (production) + `tests/` (test suite). Country scripts under `lsms_library/countries/` are clean (0 instances).

## Summary

| Area | Count | Notes |
|---|---|---|
| `lsms_library/country.py`             | 11 | Includes the two that silently swallowed the `labels=` bug (PR #183) |
| `lsms_library/data_access.py`         | 12 | Network/HTTP paths — should narrow to `OSError`, `urllib.error.URLError`, `json.JSONDecodeError` |
| `lsms_library/diagnostics.py`         | 15 | Report-as-Check — broad is *nearly* defensible but hides panel-ID bugs from the runner |
| `lsms_library/local_tools.py`         |  9 | Format fallback chain (dta/sav/parquet); 1 **bare `except:`** on date parsing |
| `lsms_library/util/geo_audit.py`      |  9 | Audit tool that silently continues |
| `lsms_library/data_access.py`, various|  — | (above) |
| `lsms_library/__init__.py`            |  5 | Startup-time DVC UI tweaks + auto-unlock |
| `lsms_library/feature.py`             |  1 | Cross-country aggregation; at least emits a `warnings.warn` |
| `lsms_library/transformations.py`     |  1 | kg-factor inference; comment documents intent |
| `lsms_library/config.py`              |  1 | YAML config read |
| `lsms_library/categorical_mapping/ai_agent.py` | 1 | AI helper |
| **Production total**                  | **65** | |
| `tests/*`                             | 12 | Separate triage pass |
| **Grand total**                       | **77** | |

## Proposed policy

> The framework should not silently swallow exceptions. Every `except` clause should:
> 1. Catch a **specific** exception type, not `Exception` or bare `except:`.
> 2. Either **re-raise**, **return a typed sentinel the caller must handle**, or **log at WARNING or higher** (not `debug`, and not `pass`).
> 3. If a broad catch is genuinely needed (diagnostic reporters, top-level crash handlers), include an in-line comment stating why and ensure the exception is at minimum logged with its traceback.

The one clause in the codebase that already follows this norm is `country.py:2420` — `except Exception as e:  # broad catch intentional: diagnostic method`. That's the template.

## Patterns and per-instance triage

### Pattern A — dispatch swallows user errors (HIGH severity, fix now)

These sit in the runtime dispatch path of `Country.<feature>()`. They hide `KeyError`, `TypeError`, and programmer errors from user-facing kwargs indistinguishably from "upstream parquet missing." PR #183 shipped a `labels=` feature that is silently ignored through these.

| Location | Current behaviour | Proposed fix |
|---|---|---|
| `country.py:2307` | `except Exception: derived = None` in `_FOOD_DERIVED` closure | Remove entirely. If `_aggregate_wave_data('food_acquired')` raises, the exception is authoritative — the legacy `!make` fallback is dead code since v0.7.0 retired stages. |
| `country.py:2336` | `except Exception: pass` in `_ROSTER_DERIVED` closure | Same; remove. |
| `country.py:916, 918` | nested `except Exception: pass` around global categorical-mapping load | Narrow to `(FileNotFoundError, yaml.YAMLError, ParserError)`; let other errors propagate — a malformed `.org` file deserves a loud failure. |
| `country.py:1055` | `except Exception: continue` in `_market_lookup` cover-page scan | Narrow to `(KeyError, FileNotFoundError)`; otherwise a typo in a cover-page spec disappears. |
| `country.py:1111` | `except Exception: self._sample_v_cache = False` in `_join_v_from_sample` | Narrow to `(KeyError, ValueError, AttributeError)`. Truly novel errors should surface. |

### Pattern B — "optimistic optimization" (MEDIUM severity)

Reads a cache or derived structure and falls back if it's cold. Broad catch is defensible only if paired with a `warnings.warn` that names the specific fallback taken.

| Location | Notes |
|---|---|
| `country.py:1893` | cache read failure; already `logger.debug(...)` — upgrade to `warnings.warn(category=UserWarning)` and narrow to `(OSError, pyarrow.lib.ArrowInvalid)` |
| `country.py:2086` | `_log_issue(...); raise` — this one actually *does* re-raise; the broad catch is a tee-into-log. **Keep**, document inline. |
| `transformations.py:276` | kg-factor inference; has a comment. Narrow to `(ValueError, ZeroDivisionError, KeyError)`. |

### Pattern C — diagnostic / reporter code (LOW severity; keep but annotate)

`diagnostics.py` has 15 clauses that all funnel into `Check(..., "fail", str(e))` return values. The broad catch is the point — a broken diagnostic shouldn't crash the runner. But each clause should still narrow to types we expect, not `Exception`.

| Location | Current | Proposed |
|---|---|---|
| `diagnostics.py:50, 442, 1017, 1033, 1048` | silently return `{}` / `None` / `pass` | Narrow to `(OSError, FileNotFoundError, pyarrow.lib.ArrowInvalid)`. An actual bug in the diagnostic must surface. |
| `diagnostics.py:724, 780, 870, 883, 922, 981, 1167, 1226, 1333, 1398` | return `Check(name, "fail", str(e))` | Acceptable pattern — the `e` is surfaced to the runner. Still worth narrowing to avoid swallowing `KeyboardInterrupt`. Document inline. |
| `country.py:2420` | already `except Exception as e:  # broad catch intentional: diagnostic method` | **Keep** — template for other reporters. |

### Pattern D — startup / optional integration (LOW; document and narrow)

`__init__.py` and parts of `local_tools.py` catch during optional DVC UI setup, GPG autounlock, and format-sniffing.

| Location | Notes |
|---|---|
| `__init__.py:131, 136, 154, 176, 179` | DVC UI + auto-unlock; narrow to `(ImportError, AttributeError, RuntimeError, subprocess.CalledProcessError)`. Log at WARN if auto-unlock is expected to succeed. |
| `local_tools.py:213, 219, 247, 328` | DVC sidecar detection; narrow to `(FileNotFoundError, OSError, ImportError)`. |

### Pattern E — data-access network path (MEDIUM; narrow in one pass)

Every `except Exception` in `data_access.py` guards a `urllib.request.urlopen(...)` or `json.loads(...)` call.

- **Proposed fix** (applies to all 12 instances): replace with `except (urllib.error.URLError, urllib.error.HTTPError, socket.timeout, TimeoutError, json.JSONDecodeError, OSError) as exc:`. Keep the existing `logger.error` / `logger.debug` payload.
- Affected lines: `data_access.py:224, 248, 263, 392, 412, 433, 511, 559, 657, 744, 830, 1174`.

### Pattern F — file-format fallback chain (MEDIUM; narrow to parser errors)

`local_tools.py` uses a cascade of `pyreadstat.read_sav → read_dta → dvc.api.open → get_data_file`. Each level currently broad-catches on any error.

- Affected: `local_tools.py:413, 421, 444, 506`.
- **Proposed fix**: catch `(ValueError, pyreadstat.ReadstatError, OSError, NotImplementedError)`. Corrupt-file reports should be distinguishable from programmer bugs (e.g. wrong kwargs).

### Pattern G — silent state-silencing (HIGH; fix ASAP)

Pure state-silencing — a failure causes an empty dict / skipped check / blank audit entry with no visible signal.

| Location | Fix |
|---|---|
| `config.py:61` | Narrow to `(yaml.YAMLError, OSError)`. A typo in user config shouldn't present as "no config found." |
| `util/geo_audit.py:85, 103, 131, 148, 243` | Each `continue` should log at WARN with the country name. |
| `feature.py:141` | Already warns — narrow to `(KeyError, ValueError, AttributeError, OSError)`. |

### Pattern H — bare `except:` (HIGHEST; fix immediately)

`except:` without any type catches `SystemExit` and `KeyboardInterrupt` — a runaway loop cannot be Ctrl-C'd.

- `local_tools.py:1353` — bare `except:` on `pd.to_datetime(interview_date)`. **Fix**: `except (ValueError, TypeError, pd.errors.ParserError): final_interview_date = None`.

### Pattern I — tests (MEDIUM; clean up in a sweep)

12 instances across 7 test files. Tests should almost never broad-catch — a test that hides its own failure is worse than one that crashes.

- Files: `tests/test_age_dtype_consistency.py` (2), `tests/generate_baseline.py` (3), `tests/test_dvc_caching.py` (1), `tests/test_locality_deprecation.py` (2), `tests/test_uganda_api_vs_replication.py` (2), `tests/test_table_structure.py` (1), `tests/test_sample.py` (1).
- **Proposed fix**: replace each with the narrowest type the test actually needs, or use `pytest.raises(ExactType)` / `pytest.warns(...)` if the test is asserting a failure mode.

## Handoff options

### Option 1 — single follow-up agent, one PR

Pros: consistent tone, one review cycle, the narrowing is mechanical.
Cons: large diff; hard to bisect if a narrowed `except` exposes a genuine bug that was being silently masked.

Prompt sketch:
> Using `SkunkWorks/except_exception_audit.md` as the punch list, narrow every `except Exception` in production (starting with Pattern A, then H, then G) to specific types. For each narrowed clause, add a line comment stating what types we now catch and why. Do **not** change `country.py:2420`-style diagnostics beyond adding a narrow type list. After Pattern A, run the full test suite; any newly-exposed failures are features of the fix — triage and file follow-up issues.

### Option 2 — file one issue per pattern, accept staggered PRs (RECOMMENDED)

Nine issues (Patterns A–I). Each is scoped small enough for a single-day PR and leaves the tree bisectable.

| Issue title | Scope | Data required? |
|---|---|---|
| fix: dispatch closures silently swallow user errors (PR #183 followup) | Pattern A | No — covered by new `test_food_labels.py` |
| fix: bare `except:` in `local_tools.interview_date` date parsing     | Pattern H | No |
| fix: silent state-silencing in `config.py`, `geo_audit.py`, `feature.py` | Pattern G | Partial — `feature.py` needs Feature() smoke test |
| narrow: `data_access.py` network excepts to HTTP/OS/JSON             | Pattern E | **Yes — needs network + WB key** |
| narrow: `local_tools.py` format fallback chain                       | Pattern F | **Yes — needs `.dta`/`.sav` fixtures** |
| narrow: `diagnostics.py` Check reporters                             | Pattern C | **Yes — full diag runner** |
| narrow: `__init__.py`, `local_tools.py` sidecar/startup              | Pattern D | Minimal — DVC install |
| narrow: `country.py` optimistic-optimization caches                  | Pattern B | **Yes — warm caches** |
| cleanup: `tests/` broad-except sweep                                 | Pattern I | No |

### Option 3 — hybrid

Do Patterns A + H + G (cache-free, ~5 instances) in a direct follow-up PR against `development` right now (mechanical, low-risk, immediately recovers the silent-fail bug class that motivated this audit). File issues for Patterns B–F (all require real data to verify) and I (tests).

## Appendix: grep reproduction

```
cd lsms_library
grep -rn --include='*.py' -E '^\s*except\s*:|except Exception|except BaseException' .
```
