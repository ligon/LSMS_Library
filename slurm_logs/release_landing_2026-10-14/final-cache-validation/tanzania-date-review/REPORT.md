SCOPE DEVIATIONS

None. The replay reads the completed private Tanzania raw date and sample parquets, uses in-memory helpers, and makes zero table API calls or cache writes. SHA-256, size, mtime, device and inode of both inputs are unchanged. No source or baseline edits were made.

Tanzania reproduces the other country named in existing #798. Its warm all-wave API loses 5,893 valid dates because whole-column datetime coercion infers one format for mixed strings. This is separate from six dates already null in the raw cache.

| Wave | Raw cache | Original null dates | Cold API | Warm API | Additional parse loss |
|---|---:|---:|---:|---:|---:|
| 2008-09 | 3,265 | 2 | 3,263 | 3,263 | 0 |
| 2010-11 | 3,924 | 1 | 3,923 | 3,923 | 0 |
| 2012-13 | 5,010 | 0 | 5,010 | 5,010 | 0 |
| 2014-15 | 4,341 | 3 | 4,338 | 4,338 | 0 |
| 2019-20 | 1,184 | 0 | 1,184 | 0 | 1,184 |
| 2020-21 | 4,709 | 0 | 4,709 | 0 | 4,709 |
| Total | 22,433 | 6 | 22,427 | 16,534 | 5,893 |

The complete raw parquet has fingerprint `f0d346d45b3e3dea7ebcccfa57ef544919732a183b280ea6328f590b5b0f44e0`. Its country hash is fresh and its footer covers all six waves. Older waves have space-separated timestamps; every newer-wave value uses the ISO `T` separator. `_normalize_dataframe_index` preserves the raw frame exactly. The existing `_join_v_from_sample` also preserves every row when supplied a preloaded raw sample lookup; the stub has no table method, so this replay cannot invoke a hidden API build.

Declared dtype enforcement calls whole-column `pd.to_datetime(..., errors='coerce')`. Under pandas 3.0.6 this creates 5,893 additional NaT values, precisely the newer waves. Canonical dtype enforcement makes no further change. After `dropna(how='all')`, the replay exactly reproduces the warm served fingerprint `d593293bb3d884ed191bfb83f73cc63d0d9954c3b87204bd0bde010cc21e9215`.

Per-wave parsing and `format='mixed'` are exactly frame-equal in memory. Removing only the six original null dates reproduces the full cold served fingerprint `53085328789e3e2a72ae3b4f0150abe99e571e46fc76e680897f2e8f6832fd3c`. This is a diagnostic counterfactual, not an implemented production repair.

The neighboring Mali replay establishes identical extracted function text for `_enforce_declared_dtypes`, `_enforce_canonical_dtypes`, `_finalize_result`, and `to_parquet` at v0.14.0, pre-train 452302adcfdbf3af538ef06375df1ac6c5aaa8a0, cold-corpus 7fa05179c8519f44ea329ba6387f5d6d6441a8ea and final 3914d8804d7b2936d75041cea98bbc9ddb6d4c9f. The old issue already names Tanzania's two later waves. This is source-route attribution plus independently recorded prior behavior, not a claim that every historical dependency environment was rebuilt.

Named country idiosyncrasy: the first four NPS rounds share the 2008-15 physical folder and backcast household identity, while the later Extended and Refresh subpanels are disjoint. CONTENTS says, "2019-20 and 2020-21 have zero household overlap by design." Its WAITING #114/LOGBOOK retains an unresolved grain decision despite the closed issue. Those design facts do not explain the loss: normalization and the actual sample-v join are lossless, and coercion alone reproduces it. The October 14 #798 claim that "both halves no longer reproduce on development" is contradicted by this warm call; a successful cold call is insufficient evidence of warm parity.

Coordinator disposition: retain #798 as an explicit existing limitation. Complete byte-verified raw caches remain separately eligible for promotion after the remaining gates and review. No production repair is included in this release change; no universal cold/warm API equality is claimed. Stable aggregate evidence is in replay.json and the source generator replay.py. The optional ordinary single-wave workaround is validated separately on a new physical diagnostic copy, never on the original or final promotion roots.
