"""Check actual private audit cache values behind rebuild-warning families."""
import json
from pathlib import Path

import numpy as np
import pandas as pd

root=Path('/local/job40255479/canonical-audit-compare')
features=['cluster_features','sample','household_roster','household_characteristics','food_acquired']
report=[]
for feature in features:
    paths={label:{p.parent.parent.name:p for p in (root/f'data-{label}').glob(f'*/var/{feature}.parquet')}
           for label in ['baseline','combined']}
    assert paths['baseline'].keys()==paths['combined'].keys(), feature
    for country in sorted(paths['baseline']):
        a,b=[pd.read_parquet(paths[label][country]) for label in ['baseline','combined']]
        order_only=False
        try:
            pd.testing.assert_frame_equal(a,b,check_exact=True)
        except AssertionError:
            assert a.index.names==b.index.names and list(a.dtypes)==list(b.dtypes)
            ah,bh=[pd.util.hash_pandas_object(f,index=True).to_numpy() for f in [a,b]]
            pd.testing.assert_frame_equal(a.iloc[np.argsort(ah,kind='stable')],
                                          b.iloc[np.argsort(bh,kind='stable')],check_exact=True)
            order_only=True
        report.append({'country':country,'feature':feature,'rows':len(a),
                       'all_index_and_cell_values_equal':True,'row_order_only_difference':order_only})
(root/'local-cache-controls.json').write_text(json.dumps({'frames':report,
    'all_frames_equal_ignoring_row_order':True,'raw_values_and_identifiers_private':True},indent=2,sort_keys=True)+'\n')
print(json.dumps({'compared_frames':len(report),'rows':sum(x['rows'] for x in report),
                  'order_only_cells':[(x['country'],x['feature']) for x in report if x['row_order_only_difference']]}))
