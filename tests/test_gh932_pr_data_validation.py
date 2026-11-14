"""Source-bound PR guards with invented events; no tokens, data or API calls."""
from copy import deepcopy
import importlib.util
import re
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
    # GH #1714 (ruling 2026-11-13, option a): the workflow no longer runs on
    # `edited`, and no edited event selects any mode, well formed or not.  A
    # text edit can neither stand in for a validation nor, as a newer run on
    # the same head, hide one (#1711: required check "Expected").
    {'title': {'from': 'before'}}, {'body': {'from': None}},
    {'title': {'from': 'before'}, 'body': {'from': 'before'}},
    {'base': {'ref': {'from': 'master'}, 'sha': {'from': 'd' * 40}}},
    {'base': {'ref': {'from': 'master'}, 'sha': {'from': 'd' * 40}},
     'title': {'from': 'before'}, 'body': {'from': None}},
    {'title': {'from': []}}, {'title': {'from': None}}, {'body': {'from': False}},
    {'title': {}}, {'body': {}}, {'base': {}}, {'base': {'ref': {'from': 'master'}}},
    {'base': {'ref': {'from': 'master'}, 'sha': {'from': 'd' * 40}},
     'future_source_field': {'from': 'old'}},
    {}, None,
])
def test_every_edited_event_is_refused_and_selects_no_mode(classify, changes):
    value = event('edited')
    if changes is not None:
        value['changes'] = changes
    before = deepcopy(value)
    with pytest.raises(ValueError, match='Unsupported pull_request action'):
        classify(value, 'owner/project', 'pull_request')
    assert value == before


def test_no_edit_of_title_body_or_base_triggers_a_run():
    """A run whose jobs all skip is still a run, so `edited` must not trigger."""
    triggers = _workflow()['on']['pull_request']
    assert triggers['types'] == ['opened', 'synchronize', 'reopened',
                                 'ready_for_review', 'converted_to_draft']
    assert 'edited' not in triggers['types']
    # The documented ways to re-validate after a retarget without a push.
    assert {'synchronize', 'reopened', 'ready_for_review'} <= set(triggers['types'])
    # This pins only the workflow's WRITTEN statement that reusing an earlier
    # validation after a retarget relies on the ruleset's strict setting, so
    # the statement is not deleted silently.  It does not check the ruleset
    # itself: that lives in GitHub settings, which tests cannot read (it was
    # read from the rulesets API on 2026-11-13).
    assert 'strict_required_status_checks_policy: true' in WORKFLOW.read_text()


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


@pytest.mark.parametrize('mode', ['', 'unexpected', None, 'metadata'])
def test_missing_unknown_or_retired_mode_is_refused(mode):
    # 'metadata' was the title/body-edit mode, retired with `edited` (GH #1714).
    guard = _inline('PR_DATA_RESULT_GATE')['guard_mode']
    with pytest.raises(ValueError):
        guard('success', mode)


def test_only_validation_mode_reaches_credentialed_steps():
    assert _inline('PR_DATA_RESULT_GATE')['guard_mode']('success', 'validate') is True


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


def test_required_job_is_always_run_under_one_static_name():
    workflow = _workflow()
    assert set(workflow['on']) == {'pull_request'}
    assert workflow['on']['pull_request']['branches'] == ['development']
    assert 'edited' not in workflow['on']['pull_request']['types']
    assert 'concurrency' not in workflow
    classify_job, validation = workflow['jobs']['classify'], workflow['jobs']['validation']
    assert classify_job['permissions'] == {} and len(classify_job['steps']) == 1
    assert 'uses' not in classify_job['steps'][0]
    # A job skipped by a conditional counts as a passing required check, so the
    # required job must run even when the classifier fails, and then fail.
    assert validation['if'] == '${{ always() }}' and validation['needs'] == 'classify'
    # GH #1714: no event renames the required job any more.
    assert validation['name'] == 'pr-data-validation'
    assert validation['concurrency']['group'] == "pr-data-${{ github.event.pull_request.number }}-${{ needs.classify.result == 'success' && needs.classify.outputs.mode == 'validate' && 'validation' || format('refused-{0}', github.run_id) }}"
    assert validation['concurrency']['cancel-in-progress'] == 'true'
    steps = validation['steps']
    assert steps[0]['id'] == 'guard' and 'uses' not in steps[0]
    assert steps[1]['uses'] == 'actions/checkout@v4'
    assert steps[1]['with'] == {'ref': '${{ github.sha }}', 'fetch-depth': '2',
                              'persist-credentials': 'false'}
    assert 'pr_data_checkout.py' in steps[2]['run']
    # No step after the guard is conditional: the guard's failure stops them
    # all, and its success runs them all, so a green required check cannot
    # come from skipped data steps.
    assert all('if' not in step for step in steps)
    assert 'secrets.' not in str(classify_job) and 'secrets.' not in str(steps[:3])


def test_existing_data_recipe_is_reused_without_weaker_test_or_credential_gate():
    workflow = _workflow()
    existing = yaml.load((ROOT / '.github/workflows/ci.yml').read_text(), Loader=yaml.BaseLoader)
    reused = deepcopy(workflow['jobs']['validation']['steps'][3:])
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


def _render_job_field(field, result, mode, run_id, number=17):
    """Evaluate this workflow's string/boolean subset, not arbitrary Actions syntax."""
    values = {'needs.classify.result': result, 'needs.classify.outputs.mode': mode,
              'github.run_id': run_id, 'github.event.pull_request.number': number}
    template = _workflow()['jobs']['validation'][field] if field == 'name' else (
        _workflow()['jobs']['validation']['concurrency']['group'])

    def expression(match):
        text = re.sub(r"(?:needs|github)\.[A-Za-z_.]+",
                      lambda key: repr(values[key.group()]), match.group(1))
        text = text.replace('&&', ' and ').replace('||', ' or ')
        return str(eval(text, {'__builtins__': {},
                              'format': lambda template, *args: template.format(*args)}))

    return re.sub(r"\$\{\{ (.*?) \}\}", expression, template)


def test_late_draft_refusal_cannot_cancel_ready_validation(classify):
    # The incident's late event was an edit of a draft; `edited` no longer
    # triggers (GH #1714), and converted_to_draft is the draft event that does.
    draft = event('converted_to_draft')
    draft['pull_request']['draft'] = True
    with pytest.raises(ValueError, match='draft'):
        classify(draft, 'owner/project', 'pull_request')
    ready = classify(event('ready_for_review'), 'owner/project', 'pull_request')
    ready_group = _render_job_field('group', 'success', ready['mode'], 200)
    # Older event's dependent job arrives after the ready job, as in the incident.
    late_refusal = _render_job_field('group', 'failure', '', 199)
    assert ready_group == 'pr-data-17-validation'
    assert late_refusal == 'pr-data-17-refused-199'
    assert late_refusal != ready_group
    assert _render_job_field('name', 'failure', '', 199) == 'pr-data-validation'
    with pytest.raises(ValueError):
        _inline('PR_DATA_RESULT_GATE')['guard_mode']('failure', '')


@pytest.mark.parametrize('result,mode', [
    ('failure', ''), ('failure', 'validate'), ('failure', 'metadata'), ('cancelled', ''),
    ('skipped', ''), ('', ''), (None, None), ('success', ''),
    ('success', 'unexpected'), ('success', 'metadata'),
])
def test_refused_or_uncertain_requests_have_run_specific_groups_and_fail(result, mode):
    group = _render_job_field('group', result, mode, 199)
    assert group == 'pr-data-17-refused-199'
    assert _render_job_field('name', result, mode, 199) == 'pr-data-validation'
    assert group != _render_job_field('group', result, mode, 200)
    assert group != _render_job_field('group', 'success', 'validate', 200)
    with pytest.raises(ValueError):
        _inline('PR_DATA_RESULT_GATE')['guard_mode'](result, mode)


@pytest.mark.parametrize('change', ['head', 'base'])
def test_new_valid_head_or_base_retains_same_validation_cancellation_group(classify, change):
    earlier = event('synchronize')
    later = deepcopy(earlier)
    later['pull_request'][change]['sha'] = 'c' * 40
    first = classify(earlier, 'owner/project', 'pull_request')
    second = classify(later, 'owner/project', 'pull_request')
    assert first[change] != second[change]
    assert _render_job_field('group', 'success', first['mode'], 199) == (
        _render_job_field('group', 'success', second['mode'], 200))
    assert _workflow()['jobs']['validation']['concurrency']['cancel-in-progress'] == 'true'


@pytest.mark.parametrize('result,mode', [
    ('success', 'validate'), ('success', 'metadata'), ('failure', ''), (None, None),
])
def test_every_run_reports_the_required_name(result, mode):
    """GH #1714: no run of this workflow names its job anything else, so no
    newer run on the same head can leave pr-data-validation 'Expected'."""
    assert _render_job_field('name', result, mode, 199) == 'pr-data-validation'
