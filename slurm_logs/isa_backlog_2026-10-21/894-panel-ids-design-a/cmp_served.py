"""assert_frame_equal of cold-built sample()/household_roster(), base vs branch; per-wave i-set equality."""
import sys, pandas as pd
a, b = sys.argv[1], sys.argv[2]
for C in ['Malawi', 'Nigeria']:
    for t in ['sample', 'household_roster']:
        x = pd.read_parquet(f'{a}/{C}_{t}.parquet'); y = pd.read_parquet(f'{b}/{C}_{t}.parquet')
        try:
            pd.testing.assert_frame_equal(x, y); eq = 'assert_frame_equal OK'
        except AssertionError as e:
            eq = 'DIFF: ' + str(e)[:300]
        xi = x.index.to_frame(index=False); yi = y.index.to_frame(index=False)
        waves = sorted(set(xi.t) | set(yi.t))
        iseq = all(set(xi.loc[xi.t == w, 'i']) == set(yi.loc[yi.t == w, 'i']) for w in waves)
        print(f'{C}/{t}: shape {x.shape} vs {y.shape}; {eq}; per-wave i sets equal in all {len(waves)} waves: {iseq}')
