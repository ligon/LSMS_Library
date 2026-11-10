"""Source-bound PR guards with invented events; no tokens, data or API calls."""
from copy import deepcopy
import importlib.util
from pathlib import Path
import textwrap

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / '.github/workflows/pr-data-validation.yml'


def _inline(marker):
    text = WORKFLOW.read_text()
    body = text.split('# BEGIN_' + marker + '\n', 1)[1].split('# END_' + marker, 1)[0]
    lines = textwrap.dedent(body).strip().splitlines()
    assert lines[0] == "python3 - <<'PY'" and lines[-1] == 'PY'
    namespace = {'__name__': 'source_bound_guard'}
    exec(compile('\n'.join(lines[1:-1]), str(WORKFLOW), 'exec'), namespace)
    return namespace


@pytest.fixture
def classify():
    return _inline('PR_DATA_EVENT_GATE')['classify_event']


def event(action='opened'):
    return {'action': action, 'repository': {'full_name': 'owner/project'},
            'pull_request': {'number': 17, 'draft': False,
                             'head': {'sha': 'a' * 40, 'repo': {'full_name': 'owner/project'}},
                             'base': {'sha': 'b' * 40, 'ref': 'development',
                                      'repo': {'full_name': 'owner/project'}}}}


@pytest.mark.parametrize('action', ['opened', 'synchronize', 'reopened', 'ready_for_review'])
def test_ready_source_events_require_validation(classify, action):
    assert classify(event(action), 'owner/project', 'pull_request') == {
        'mode': 'validate', 'head': 'a' * 40, 'base': 'b' * 40}


@pytest.mark.parametrize('changes', [
    {'base': {'ref': {'from': 'master'}, 'sha': {'from': 'd' * 40}}},
    {'base': {'ref': {'from': 'master'}, 'sha': {'from': 'd' * 40}},
     'title': {'from': 'before'}, 'body': {'from': None}},
])
def test_base_retarget_is_validation_even_without_a_new_head(classify, changes):
    value = event('edited')
    value['changes'] = changes
    assert classify(value, 'owner/project', 'pull_request')['mode'] == 'validate'


@pytest.mark.parametrize('changes', [
    {'title': {'from': 'before'}}, {'body': {'from': None}},
    {'title': {'from': 'before'}, 'body': {'from': 'before'}},
])
def test_known_text_edits_emit_only_metadata_mode(classify, changes):
    value = event('edited')
    value['changes'] = changes
    actual = classify(value, 'owner/project', 'pull_request')
    assert actual == {'mode': 'metadata', 'head': 'a' * 40, 'base': 'b' * 40}


@pytest.mark.parametrize('changes', [
    {'title': {'from': []}}, {'title': {'from': {}}},
    {'title': {'from': None}}, {'title': {'from': 17}},
    {'body': {'from': []}}, {'body': {'from': {}}},
    {'body': {'from': False}}, {'title': {}}, {'body': {}},
    {'title': {'from': 'before', 'extra': 'unknown'}},
    {'body': {'from': None, 'extra': 'unknown'}},
    {'title': {'from': 'before'}, 'body': {'from': []}},
    {'base': {}}, {'base': {'ref': {'from': 'master'}}},
    {'base': {'sha': {'from': 'd' * 40}}},
    {'base': {'ref': {}, 'sha': {'from': 'd' * 40}}},
    {'base': {'ref': {'from': []}, 'sha': {'from': 'd' * 40}}},
    {'base': {'ref': {'from': 'master'}, 'sha': {'from': {}}}},
    {'base': {'ref': {'from': 'master'}, 'sha': {'from': 'd' * 40, 'extra': 1}}},
    {'base': {'ref': {'from': 'master'}, 'sha': {'from': 'd' * 40}, 'extra': {}}},
    {'base': {'ref': {'from': 'master'}, 'sha': {'from': 'd' * 40}},
     'title': {'from': []}},
    {'base': {'ref': {'from': 'master'}, 'sha': {'from': 'd' * 40}},
     'future_source_field': {'from': 'old'}},
])
def test_malformed_nested_edits_cannot_select_metadata_success(classify, changes):
    value = event('edited')
    value['changes'] = changes
    before = deepcopy(value)
    with pytest.raises(ValueError):
        classify(value, 'owner/project', 'pull_request')
    assert value == before


@pytest.mark.parametrize('fault', [
    'draft', 'unknown_draft', 'fork', 'foreign_base', 'wrong_base', 'foreign_event',
    'unknown_action', 'missing_head', 'non_sha_head', 'missing_changes',
    'empty_changes', 'unknown_changes', 'malformed_text', 'malformed_base',
])
def test_ineligible_and_uncertain_events_refuse_before_any_checkout(classify, fault):
    value = event()
    if fault == 'draft': value['pull_request']['draft'] = True
    elif fault == 'unknown_draft': del value['pull_request']['draft']
    elif fault == 'fork': value['pull_request']['head']['repo']['full_name'] = 'fork/project'
    elif fault == 'foreign_base': value['pull_request']['base']['repo']['full_name'] = 'other/project'
    elif fault == 'wrong_base': value['pull_request']['base']['ref'] = 'master'
    elif fault == 'foreign_event': value['repository']['full_name'] = 'other/project'
    elif fault == 'unknown_action': value['action'] = 'closed'
    elif fault == 'missing_head': del value['pull_request']['head']['sha']
    elif fault == 'non_sha_head': value['pull_request']['head']['sha'] = 'refs/heads/topic'
    else:
        value['action'] = 'edited'
        if fault == 'empty_changes': value['changes'] = {}
        elif fault == 'unknown_changes': value['changes'] = {'future_source_field': {'from': 'old'}}
        elif fault == 'malformed_text': value['changes'] = {'title': 'untyped'}
        elif fault == 'malformed_base': value['changes'] = {'base': None}
    before = deepcopy(value)
    with pytest.raises(ValueError):
        classify(value, 'owner/project', 'pull_request')
    assert value == before


def test_dispatch_cannot_impersonate_a_pr_event(classify):
    with pytest.raises(ValueError, match='pull_request event'):
        classify(event(), 'owner/project', 'workflow_dispatch')


@pytest.mark.parametrize('result', ['failure', 'cancelled', 'skipped', '', None])
def test_classifier_failure_never_becomes_required_success(result):
    guard = _inline('PR_DATA_RESULT_GATE')['guard_mode']
    with pytest.raises(ValueError):
        guard(result, 'validate')


@pytest.mark.parametrize('mode', ['', 'unexpected', None])
def test_missing_or_unknown_mode_is_not_a_metadata_success(mode):
    guard = _inline('PR_DATA_RESULT_GATE')['guard_mode']
    with pytest.raises(ValueError):
        guard('success', mode)


@pytest.mark.parametrize('mode,run_data', [('validate', True), ('metadata', False)])
def test_only_validation_mode_reaches_credentialed_steps(mode, run_data):
    assert _inline('PR_DATA_RESULT_GATE')['guard_mode']('success', mode) is run_data


def _checkout():
    spec = importlib.util.spec_from_file_location('pr_data_checkout', ROOT / '.github/scripts/pr_data_checkout.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.verify_checkout


def test_exact_synthetic_merge_is_the_only_accepted_checkout():
    merge, base, head = 'c' * 40, 'b' * 40, 'a' * 40
    assert _checkout()(merge, [base, head], merge, base, head) == {
        'merge': merge, 'base': base, 'head': head}


@pytest.mark.parametrize('fault', ['bare_head', 'wrong_merge', 'advanced_base',
                                   'new_head', 'one_parent', 'three_parents',
                                   'swapped_parents', 'moving_ref'])
def test_wrong_checkout_head_base_or_parent_shape_refuses(fault):
    merge, base, head = 'c' * 40, 'b' * 40, 'a' * 40
    actual, parents = merge, [base, head]
    if fault == 'bare_head': actual, parents = head, [base]
    elif fault == 'wrong_merge': actual = 'd' * 40
    elif fault == 'advanced_base': parents[0] = 'd' * 40
    elif fault == 'new_head': parents[1] = 'd' * 40
    elif fault == 'one_parent': parents = [base]
    elif fault == 'three_parents': parents.append('d' * 40)
    elif fault == 'swapped_parents': parents.reverse()
    elif fault == 'moving_ref': actual = 'refs/pull/17/merge'
    with pytest.raises(ValueError):
        _checkout()(actual, parents, merge, base, head)


def _workflow():
    return yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)


def test_required_job_is_always_run_and_metadata_has_separate_name_and_concurrency():
    workflow = _workflow()
    assert set(workflow['on']) == {'pull_request'}
    assert workflow['on']['pull_request']['branches'] == ['development']
    assert 'edited' in workflow['on']['pull_request']['types']
    assert 'concurrency' not in workflow
    classify_job, validation = workflow['jobs']['classify'], workflow['jobs']['validation']
    assert classify_job['permissions'] == {} and len(classify_job['steps']) == 1
    assert 'uses' not in classify_job['steps'][0]
    assert validation['if'] == '${{ always() }}' and validation['needs'] == 'classify'
    assert validation['name'] == "${{ needs.classify.outputs.mode == 'metadata' && 'pr-data-metadata-edit' || 'pr-data-validation' }}"
    assert validation['concurrency']['group'] == "pr-data-${{ github.event.pull_request.number }}-${{ needs.classify.outputs.mode == 'metadata' && 'metadata' || 'validation' }}"
    assert validation['concurrency']['cancel-in-progress'] == 'true'
    steps = validation['steps']
    assert steps[0]['id'] == 'guard' and 'uses' not in steps[0]
    assert steps[1]['uses'] == 'actions/checkout@v4'
    assert steps[1]['with'] == {'ref': '${{ github.sha }}', 'fetch-depth': '2',
                              'persist-credentials': 'false'}
    assert 'pr_data_checkout.py' in steps[2]['run']
    assert all(step['if'] == "steps.guard.outputs.run_data == 'true'" for step in steps[1:])
    assert 'secrets.' not in str(classify_job) and 'secrets.' not in str(steps[:3])


def test_existing_data_recipe_is_reused_without_weaker_test_or_credential_gate():
    workflow = _workflow()
    existing = yaml.load((ROOT / '.github/workflows/ci.yml').read_text(), Loader=yaml.BaseLoader)
    reused = deepcopy(workflow['jobs']['validation']['steps'][3:])
    for step in reused:
        del step['if']
    flag = '--public-test-failures'
    full = next(step for step in reused if step.get('name') == 'Run full test suite')
    assert full['run'].split().count(flag) == 1
    assert sum(step.get('run', '').split().count(flag)
               for job in workflow['jobs'].values() for step in job.get('steps', [])) == 1
    assert all(flag not in step.get('run', '').split() for step in reused if step is not full)
    assert all(flag not in step.get('run', '').split()
               for job in existing['jobs'].values() for step in job.get('steps', []))
    # This wrapper-only diagnostic option changes no child command or gate.
    assert full['run'].count(' ' + flag + ' ') == 1
    full['run'] = full['run'].replace(' ' + flag + ' ', ' ', 1)
    # Only the dedicated PR workflow uses bounded, file-grouped workers.
    parallel = 'poetry run pytest -n 2 --dist=loadfile '
    assert full['run'].count(parallel) == 1
    full['run'] = full['run'].replace(parallel, 'poetry run pytest ', 1)
    assert reused == existing['jobs']['data-tests']['steps'][1:]
    assert workflow['jobs']['validation']['env'] == existing['jobs']['data-tests']['env']
    assert workflow['jobs']['validation']['timeout-minutes'] == '360'
    assert int(workflow['jobs']['validation']['timeout-minutes']) >= int(
        existing['jobs']['data-tests']['timeout-minutes'])
