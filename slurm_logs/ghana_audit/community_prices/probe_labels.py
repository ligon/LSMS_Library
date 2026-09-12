"""Value labels + row-structure checks on the GhanaLSS price files (read-only)."""
import pandas as pd
from lsms_library.paths import countries_root
from lsms_library.local_tools import get_dataframe

assert 'wt-glss-prices' in str(countries_root()), countries_root()
root = countries_root() / 'GhanaLSS'
pd.set_option('display.width', 250); pd.set_option('display.max_rows', 400)


def labels(path, cols):
    try:
        cats = get_dataframe(str(path), convert_categoricals=True, categories_only=True)
    except Exception as e:
        print('categories_only failed:', e); cats = None
    print(type(cats))
    if isinstance(cats, dict):
        for c in cols:
            print(f'--- {c}:', cats.get(c))
    else:
        print(cats)


def main():
    print('===== 2016-17 g7price labels')
    p = root / '2016-17/Data/g7price.dta'
    labels(p, ['unita', 'unitb', 'unitc', 'eatype', 'hhsincom', 'region', 'district'])
    df = get_dataframe(str(p), convert_categoricals=False)
    print('rows per (clust, ln, itname):', df.groupby(['clust', 'ln', 'itname']).size().value_counts().to_dict())
    print('rows per (clust, bname):', df.groupby(['clust', 'bname']).size().value_counts().head(10).to_dict())
    print('rows per (clust, ln):', df.groupby(['clust', 'ln']).size().value_counts().head(10).to_dict())
    print('bname per ln (should be 1):', df.groupby('ln')['bname'].nunique().value_counts().to_dict())
    print('eatype:', df['eatype'].value_counts().to_dict())
    print('hhsincom:', df['hhsincom'].value_counts().to_dict())
    print('unita top:', df['unita'].value_counts().head(15).to_dict())
    print('unitoa top:', df['unitoa'].value_counts().head(15).to_dict())
    print('sample bnames:', sorted(df['bname'].unique())[:60])

    print('\n===== 2012-13 labels')
    for f in ['price_sec0', 'price_sec1', 'price_sec2']:
        p = root / f'2012-13/Data/PRICES/{f}.dta'
        print('---', f)
        labels(p, ['region', 'district', 'market', 'eainclude', 'fcode', 'nfcode'])
    s1 = get_dataframe(str(root / '2012-13/Data/PRICES/price_sec1.dta'), convert_categoricals=False)
    print('rows per (clust, fcode):', s1.groupby(['clust', 'fcode']).size().value_counts().to_dict())
    s0 = get_dataframe(str(root / '2012-13/Data/PRICES/price_sec0.dta'), convert_categoricals=False)
    print('sec0 rows per clust:', s0.groupby('clust').size().value_counts().to_dict())
    print('eainclude:', s0['eainclude'].value_counts(dropna=False).to_dict())
    print('clusters per market:', s0.groupby('market')['clust'].nunique().describe().to_dict())
    s2 = get_dataframe(str(root / '2012-13/Data/PRICES/price_sec2.dta'), convert_categoricals=False)
    print('rows per (clust, nfcode):', s2.groupby(['clust', 'nfcode']).size().value_counts().to_dict())
    print('s2desc per nfcode (nunique) head:', s2.groupby('nfcode')['s2desc'].nunique().head(20).to_dict())

    print('\n===== 1991-92 G3PRICE structure')
    g3 = get_dataframe(str(root / '1991-92/Data/Prices/G3PRICE.DTA'), convert_categoricals=False)
    print('rows per (clust,item,time):', g3.groupby(['clust', 'item', 'time']).size().value_counts().to_dict())
    print('time values:', sorted(g3['time'].dropna().unique()))
    print('loc5:', g3['loc5'].value_counts(dropna=False).to_dict())
    print('items:', sorted(g3['item'].dropna().unique().astype(int)))
    print('p NaN share:', g3['p'].isna().mean(), ' p==0 share:', (g3['p'] == 0).mean())
    print('clust range:', g3['clust'].min(), g3['clust'].max())
    print('rows per (clust,item):', g3.groupby(['clust', 'item']).size().value_counts().head(10).to_dict())

    print('\n===== 1998-99 G4PRICE structure')
    g4 = get_dataframe(str(root / '1998-99/Data/Prices/G4PRICE.DTA'), convert_categoricals=False)
    print('rows per (clust,item):', g4.groupby(['clust', 'item']).size().value_counts().to_dict())
    print('loc5:', g4['loc5'].value_counts(dropna=False).to_dict())
    print('items:', sorted(g4['item'].dropna().unique().astype(int)))
    print('price NaN share:', g4['price'].isna().mean(), ' price==0:', (g4['price'] == 0).mean())
    print('clust range:', g4['clust'].min(), g4['clust'].max())

    for w in ['1987-88', '1988-89']:
        print(f'\n===== {w} PRICE.DAT structure')
        d = get_dataframe(str(root / f'{w}/Data/PRICE.DAT'))
        print('rows per (CLUST, ITEMNO):', d.groupby(['CLUST', 'ITEMNO']).size().value_counts().to_dict())
        print('items:', sorted(d['ITEMNO'].unique()))
        print('CLUST range:', d['CLUST'].min(), d['CLUST'].max(), 'n', d['CLUST'].nunique())
        if 'TYPRES' in d: print('TYPRES:', d['TYPRES'].value_counts(dropna=False).to_dict())
        print('MOINT/YRINT:', d.groupby(['YRINT', 'MOINT']).size().to_dict())
        print('PRICE1U == PRICE1/QUAN1 ?', ((d['PRICE1'] / d['QUAN1'] - d['PRICE1U']).abs() < 1e-3).mean())
        m = d[['PRICE1U', 'PRICE2U', 'PRICE3U']].mean(axis=1)
        print('PRICE == mean(PRICEnU)/DEFL ?', ((m / d['DEFL'] - d['PRICE']).abs() < 1e-3).mean())
        print('all-NaN QUAN rows:', d[['QUAN1', 'QUAN2', 'QUAN3']].isna().all(axis=1).mean())


if __name__ == '__main__':
    main()
