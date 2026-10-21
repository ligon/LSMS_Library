"""Read private JUnit evidence; emit aggregate outcomes, never survey rows."""
from pathlib import Path
import argparse
import collections
import hashlib
import json
import re
import xml.etree.ElementTree as ET


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def category(kind, message):
    if kind == "pytest.xfail":
        if message.startswith("GH #602:"):
            return "known_tenure_vocabulary_defect"
        if "source data has no sampling weights" in message:
            return "existing_expected_missing_sample_weights"
    if kind != "pytest.skip":
        raise ValueError(kind)
    if re.fullmatch(r"no cache for .+ at data_root\(\)", message):
        return "absent_at_collection_raw_cache_gate"
    if "has no declared index" in message:
        return "missing_declared_index"
    if "has no declared columns" in message:
        return "missing_declared_columns"
    if "no microdata (CLAUDE.md" in message or "microdata not available (see CLAUDE.md)" in message:
        return "explicit_microdata_unavailable_message"
    exact = {
        "no non-null weights in any wave": "no_nonnull_sample_weights",
        "no weight column": "no_sample_weight_column",
        "need weight and Rural": "sample_weight_and_rural_precondition",
        "no raw sample parquet to read (cold data root?)": "ambiguous_raw_sample_helper_returned_none",
        "no refreshment-sample households identified": "no_identified_refreshment_households",
        "need both weight columns": "sample_two_weight_columns_precondition",
        "set LSMS_EXPECTED_ROOT to assert which checkout is imported": "optional_import_identity_environment_unset",
        "no pinned totals for GhanaLSS": "missing_pinned_comparison_totals",
        "interview_date.parquet vanished/rewritten mid-run (concurrent cache mutation, GH #330)": "read_retry_exhausted_exception_type_unrecorded",
        "case is for Togo": "other_country_parameter_combination",
        "case is for Benin": "other_country_parameter_combination",
    }
    if message in exact:
        return exact[message]
    if "expansion-scale wave(s); nothing to compare" in message:
        return "fewer_than_two_comparable_expansion_waves"
    if message.startswith("Uganda replication dir not found;"):
        return "external_replication_directory_absent"
    if "not cached (requires data build)" in message or message in {
        "food_prices.parquet not in cache", "food_quantities.parquet not in cache"
    } or message.startswith("Requires cached Uganda household_characteristics"):
        return "legacy_derived_table_cache_gate"
    raise ValueError("Unclassified reason: " + message)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--xml", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--corpus-tsv", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    summary = json.loads(args.summary.read_text())
    assert digest(args.xml) == summary["junit_sha256"]
    cases = list(ET.parse(args.xml).iter("testcase"))
    counts = collections.Counter()
    reasons = {}
    cache_pairs = collections.Counter()
    credential = 0
    xfails = []
    for case in cases:
        assert case.find("failure") is None and case.find("error") is None
        skipped = case.find("skipped")
        if skipped is None:
            continue
        kind, message = skipped.get("type"), skipped.get("message", "")
        label = category(kind, message)
        counts[(kind, label)] += 1
        credential += int(bool(re.search(r"credentials?|NoCredentials|authentication|S3", message, re.I)))
        key = (kind, message)
        if key not in reasons:
            # Long xfail messages contain only label counts, but are unnecessary
            # in a public aggregate. Keep exact prefix + digest of full reason.
            prefix = message
            if label == "known_tenure_vocabulary_defect":
                prefix = message.split(" labels:", 1)[0] + " labels:"
            elif label == "external_replication_directory_absent":
                prefix = message.split(" or install", 1)[0]
            reasons[key] = {
                "kind": kind, "category": label, "count": 0,
                "exact_message_prefix": prefix,
                "full_message_sha256": hashlib.sha256(message.encode()).hexdigest(),
                "test_functions": set(),
            }
        reasons[key]["count"] += 1
        # Parameter values can contain household identifiers; strip all of them.
        function = case.get("classname") + "::" + case.get("name").split("[", 1)[0]
        reasons[key]["test_functions"].add(function)
        if label == "absent_at_collection_raw_cache_gate":
            cache_pairs[message.removeprefix("no cache for ").removesuffix(" at data_root()")] += 1
        if kind == "pytest.xfail":
            # These nine parameter values are country names, verified by caller.
            xfails.append({"country": case.get("name").split("[")[-1].rstrip("]").split("/")[0],
                           "category": label, "test_function": function})
    for value in reasons.values():
        value["test_functions"] = sorted(value["test_functions"])
    skip_count = sum(n for (k, _), n in counts.items() if k == "pytest.skip")
    xfail_count = sum(n for (k, _), n in counts.items() if k == "pytest.xfail")
    assert (len(cases), skip_count, xfail_count) == (7700, 314, 9)
    assert summary["counts"]["passed"] == 7376 and summary["counts"]["xpassed"] == 1
    import csv
    with args.corpus_tsv.open() as stream:
        corpus = {(row["country"] + "/" + row["table"]): row["status"]
                  for row in csv.DictReader(stream, delimiter="\t")}
    cache_classes = collections.Counter()
    cache_details = []
    for pair, count in sorted(cache_pairs.items()):
        if corpus.get(pair) == "error":
            label = "also_recorded_corpus_error"
        elif pair.endswith("/panel_ids"):
            label = "panel_id_parquet_gate"
        elif pair.rsplit("/", 1)[1] in {"food_prices", "food_quantities", "food_expenditures"}:
            label = "derived_food_parquet_gate"
        else:
            raise ValueError(pair)
        cache_classes[label] += count
        cache_details.append({"pair": pair, "skip_count": count, "classification": label,
                              "saved_corpus_status": corpus.get(pair)})
    result = {
        "master_sha": summary["master_sha"], "actual_make_release_exit_code": summary["exit_code"],
        "summary_sha256": digest(args.summary), "private_xml_sha256": digest(args.xml),
        "corpus_tsv_sha256": digest(args.corpus_tsv), "testcases": len(cases),
        "terminal_counts": summary["counts"], "junit_skip": skip_count, "junit_xfail": xfail_count,
        "junit_no_failure_error_or_skip": len(cases) - skip_count - xfail_count,
        "junit_passed_count_includes_nonstrict_xpass": True,
        "credential_guard_skip_messages": credential,
        "skip_categories": {label: n for (kind, label), n in counts.items() if kind == "pytest.skip"},
        "xfail_categories": {label: n for (kind, label), n in counts.items() if kind == "pytest.xfail"},
        "xfails": xfails, "raw_cache_gate_subcategories": dict(cache_classes),
        "raw_cache_gate_pairs": cache_details,
        "reason_evidence": sorted(reasons.values(), key=lambda row: (row["kind"], row["category"], row["exact_message_prefix"])),
        "limits": [
            "Skip causes are the observed messages/guards, not independent scientific validation of every asserted source limitation.",
            "No per-test cache-state history proves which None branch the generic raw-sample helper took.",
            "Guyana read retry catches either FileNotFoundError or ArrowInvalid; exact exception and causal writer are unrecorded.",
            "The isolated Guyana follow-up is separate evidence and does not alter these original run counts.",
            "JUnit counts non-strict XPASS as a testcase without a skip/failure/error; terminal summary supplies the split.",
        ],
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k: result[k] for k in ["testcases", "credential_guard_skip_messages", "skip_categories", "xfail_categories", "raw_cache_gate_subcategories"]}, indent=2))


if __name__ == "__main__":
    main()
