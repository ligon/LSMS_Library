"""The sample component is the cover source, not a weight inference (GH #1704)."""
import pandas as pd
import pytest

from lsms_library import Country
from lsms_library.local_tools import df_data_grabber


@pytest.mark.parametrize('wave,panel', [
    ('2016-17', 'y3_hhid'), ('2019-20', 'y4_hhid'), ('2024-25', 'y5_hhid'),
])
def test_stacked_sample_declares_component_on_both_files(monkeypatch, wave, panel):
    obj = Country('Malawi')[wave]
    plan = obj.column_mapping('sample', obj.resources['sample'])
    covers = {f: m for f, m in plan.items() if str(f).endswith('.dta')}
    assert len(covers) == 2
    for f, m in covers.items():
        is_panel = f.startswith('Panel/')
        column = panel if is_panel else 'case_id'
        assert m['myvars']['SampleComponent'][0] == [column]
        monkeypatch.setattr('lsms_library.local_tools.get_dataframe',
                            lambda *a, **kw: pd.DataFrame({column: ['101']}))
        result = df_data_grabber(f, {'i': column},
                                 SampleComponent=m['myvars']['SampleComponent'])
        assert result.SampleComponent.tolist() == ['IHPS' if is_panel else 'IHS']


def test_refresh_interview_preserves_ihs_source_label():
    obj = Country('Malawi')['2024-25']
    sample = obj.formatting_functions['sample']
    helper = sample.__globals__['ihps24_refreshed']
    # Replace the cover lookup only for the duration of the test.
    try:
        sample.__globals__['ihps24_refreshed'] = lambda: {'p_refresh'}
        frame = pd.DataFrame(
            {'SampleComponent': ['IHS', 'IHPS', 'IHPS'],
             '_ihs6_case_id': [None, '101', None],
             'ihps_hhid': [None, 'p_refresh', 'p_kept'],
             'ihps_weight': [float('nan'), 2.5, 3.5]},
            index=pd.Index(['cs-24-101', 'p_refresh', 'p_kept'], name='i'))
        out = sample(frame)
    finally:
        sample.__globals__['ihps24_refreshed'] = helper
    assert out.SampleComponent.to_dict() == {'cs-24-101': 'IHS', 'p_kept': 'IHPS'}
    assert out.ihps_hhid.to_dict() == {'cs-24-101': 'p_refresh', 'p_kept': 'p_kept'}
    assert out.ihps_weight.to_dict() == {'cs-24-101': 2.5, 'p_kept': 3.5}
