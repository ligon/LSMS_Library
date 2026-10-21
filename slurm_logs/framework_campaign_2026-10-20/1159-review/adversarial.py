from pathlib import Path
import json, os, subprocess, math
import pandas as pd
import lsms_library
from lsms_library import transformations as T
from lsms_library.paths import countries_root, data_root
root = Path('/local/job40255479/worktrees/1159-pooled-reducers').resolve()
assert Path.cwd().resolve() == root
assert Path(lsms_library.__file__).resolve().is_relative_to(root)
assert countries_root().resolve() == root / 'lsms_library/countries'
assert data_root().resolve() == Path(os.environ['LSMS_DATA_DIR']).resolve()
sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
assert sha == '45f80dcb50da189b7864034b1d92863dca68de09'
rows = [(c, '2000-01', 'h1', 'p1', item, 'Maize', 'Kg', q)
        for c, quantities in [('A', [1., 4.]), ('B', [2., 8.])]
        for q in quantities for item in ['CustomSeed', 'CustomFert']]
base = pd.DataFrame(rows, columns=['country','t','i','plot_id','input','j','u','Quantity'])
checks = []
for form in ['categorical_index', 'categorical_column', 'string_index', 'index_precedence']:
    frame = base.copy(deep=True)
    if form.startswith('categorical'):
        frame['country'] = pd.Categorical(frame['country'], categories=['B','A','Unused'], ordered=True)
    elif form == 'string_index':
        frame['country'] = pd.array(frame['country'], dtype='string')
    frame = frame.set_index(['country','t','i','plot_id','input','j','u'])
    if form == 'categorical_column':
        frame = frame.reset_index('country')
    if form == 'index_precedence':
        frame['country'] = pd.NA
    for unit_column in [False, True]:
        given = frame.reset_index('u') if unit_column else frame
        original = given.copy(deep=True)
        for fn, kwargs, column, share in [
            (T.seed_kg, {'seed_label':'CustomSeed'}, 'Seed_kg', 1.),
            (T.nitrogen_kg, {'nitrogen_content':{'customfert':.2}}, 'Nitrogen_kg', .2)]:
            out = fn(given, unit_kg={'kg':3.}, **kwargs)
            assert out.index.names == ['country','t','i','plot_id']
            assert len(out) == 2 and set(out.index.get_level_values('country')) == {'A','B'}
            assert math.isclose(out.loc[('A','2000-01','h1','p1'),column], 15. * share, rel_tol=1e-14, abs_tol=1e-14)
            assert math.isclose(out.loc[('B','2000-01','h1','p1'),column], 30. * share, rel_tol=1e-14, abs_tol=1e-14)
            pd.testing.assert_frame_equal(given, original)
            checks.append({'form':form,'unit_column':unit_column,'reducer':fn.__name__,'passed':True})
result = {'source_sha':sha,'import_path':str(Path(lsms_library.__file__).resolve()),'controls':checks,'passed':len(checks)}
Path('/local/job40255479/control/framework-campaign/review-1159/adversarial.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'independent_controls_passed':len(checks),'source_sha':sha}))
