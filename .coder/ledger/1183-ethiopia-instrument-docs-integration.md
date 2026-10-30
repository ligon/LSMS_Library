# Ethiopia #1183 official-instrument evidence integration

## 1. Scope and evidence

Documentation only, based on development `824ae20cdb1a9827f2ae1a978f816c627d64e9e7`. Reuse the frozen source-only worker `01e6872be2905c57d49b4e26f0841db6671493c4`, its document/source bindings and independent Mali review (MD SHA256 `aa7797e4aa1d401cdde32d473ba98b118cf8fc63ed3105605a522d97d1f3ce75`). No raw source, API, cache, test or production change.

## 2. Prior-art decisions

Reuse `.coder/ledger/1183-1184-ethiopia-seed-purchases.md`, same-country `_yesno_bool_col` and the missing-is-not-No #1250/#1253 precedent. Preserve #1184 sum(min_count=1), W1 direct field/crop joins and W2–W5 holder/crop unique-plot attachment. Other countries' seed-retention definitions are contextual precedents, not Ethiopian eligibility authority.

## 3. Definitions

Inherit `.coder/ledger/STANDING.md`, `CLAUDE.md` and `lsms_library/data_info.yml`. Read Ethiopia CONTENTS including live #1183 retention caveats, unrelated TODO and historical notes. Missing truth values, structural quantity skips and observation retention are distinct questions.

## 4. Invariants

All runtime/config/test bytes remain current development. Keep #1183 open, with no blanket missing-to-No inference, amount imputation, crop-only retention, deletion, global decoder change or join-policy change. Ordinary bounded investigation is operationally authorized; only an inconclusive scientific interpretation needs policy adjudication.

## 5. Validation plan

Only three documentation paths: append CONTENTS; this ledger; a qualified country report including exact independent review. Check Git scope/diff whitespace and preservation of existing CONTENTS bytes. No project imports/tests or historical cohort replay. Public CI and live conflict-free exact-head gates remain required before merge.
